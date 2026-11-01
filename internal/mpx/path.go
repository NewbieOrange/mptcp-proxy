package mpx

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type outItem struct {
	seq        uint64
	data       []byte
	n          int64
	fin        bool
	received   bool
	lowLatency bool
	assigned   *Path
}

func (it *outItem) size() int64 {
	return it.n
}

type Path struct {
	id           uint32
	clientPathID uint16
	remote       string

	session *Session
	conn    net.Conn
	sendQ   chan *outItem
	done    chan struct{}

	writerOnce  sync.Once
	readerOnce  sync.Once
	earlyReader bool
	dead        atomic.Bool
	writeMu     sync.Mutex

	outstandingBytes atomic.Int64
	rateBps          atomic.Int64
	rttNanos         atomic.Int64
	rxBytes          atomic.Uint64
	txBytes          atomic.Uint64

	rateMu        sync.Mutex
	rateWindow    time.Time
	rateWindowLen int64
	rateLast      time.Time

	// Scheduler credits are accessed while the session sender lock is held.
	credit   float64
	creditAt time.Time
}

func newPath(s *Session, id uint32, clientPathID uint16, conn net.Conn, earlyReader bool) *Path {
	return &Path{
		id:           id,
		clientPathID: clientPathID,
		remote:       conn.RemoteAddr().String(),
		session:      s,
		conn:         conn,
		sendQ:        make(chan *outItem, s.cfg.pathQueueChunks()),
		done:         make(chan struct{}),
		earlyReader:  earlyReader,
	}
}

func (p *Path) start() {
	p.startWriter()
	if !p.earlyReader {
		p.startReader()
	}
}

func (p *Path) startWriter() {
	p.writerOnce.Do(func() {
		if p.dead.Load() {
			return
		}
		go p.writeLoop()
	})
}

func (p *Path) startReader() {
	p.readerOnce.Do(func() {
		if p.dead.Load() {
			return
		}
		go p.readLoop()
	})
}

func (p *Path) markDead() bool {
	if !p.dead.CompareAndSwap(false, true) {
		return false
	}
	close(p.done)
	_ = p.conn.Close()
	return true
}

func (p *Path) enqueue(it *outItem) bool {
	if p.dead.Load() {
		return false
	}
	select {
	case p.sendQ <- it:
		return true
	default:
		return false
	}
}

func (p *Path) noteRecvAck(n int64) {
	if n <= 0 {
		return
	}
	now := time.Now()
	p.rateMu.Lock()
	if p.rateWindow.IsZero() {
		p.rateWindow = now
		p.rateLast = now
	}
	p.rateWindowLen += n
	elapsed := now.Sub(p.rateLast)
	if elapsed >= 200*time.Millisecond {
		inst := p.rateWindowLen * int64(time.Second) / int64(elapsed)
		old := p.rateBps.Load()
		if old == 0 {
			old = inst
		} else {
			old = (old*3 + inst) / 4
		}
		p.rateBps.Store(old)
		p.rateWindowLen = 0
		p.rateLast = now
	}
	p.rateMu.Unlock()
}

func (p *Path) noteRTT(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	old := p.rttNanos.Load()
	if old == 0 {
		old = int64(rtt)
	} else {
		old = (old*3 + int64(rtt)) / 4
	}
	p.rttNanos.Store(old)
}

func (p *Path) rtt() time.Duration {
	return time.Duration(p.rttNanos.Load())
}

func (p *Path) sendPing() bool {
	now := time.Now().UnixNano()
	if err := p.writeControl(framePing, 0, uint64Bytes(uint64(now))); err != nil {
		p.session.pathFailed(p, err)
		return false
	}
	return true
}

func (p *Path) refillCredit(now time.Time, chunkSize int) {
	rate := p.rateBps.Load()
	if rate <= 0 {
		rate = 1 << 20
	}
	if rate < 64<<10 {
		rate = 64 << 10
	}
	// Over-subscribe the token rate slightly so a path with spare capacity
	// becomes backlogged and its measured rate can grow toward the real link
	// rate. Queue limits stop any path from building a large backlog.
	rate = rate * 3 / 2
	if p.creditAt.IsZero() {
		p.creditAt = now
		return
	}
	elapsed := now.Sub(p.creditAt).Seconds()
	if elapsed > 0 {
		p.credit += elapsed * float64(rate)
	}
	limit := float64(rate) * 0.2
	if limit < float64(chunkSize) {
		limit = float64(chunkSize)
	}
	if maxLimit := float64(32 * chunkSize); limit > maxLimit {
		limit = maxLimit
	}
	if p.credit > limit {
		p.credit = limit
	}
	p.creditAt = now
}

func (p *Path) writeLoop() {
	var ticker *time.Ticker
	var tick <-chan time.Time
	if p.session.cfg.Keepalive > 0 {
		ticker = time.NewTicker(p.session.cfg.Keepalive)
		defer ticker.Stop()
		tick = ticker.C
	}

	if !p.sendPing() {
		return
	}
	for {
		select {
		case it := <-p.sendQ:
			p.session.sender.signal()

			var err error
			if it.fin {
				err = p.writeControl(frameFin, 0, uint64Bytes(it.seq))
			} else {
				err = p.writeData(it.seq, it.data)
			}
			if err != nil {
				p.session.pathFailed(p, err)
				return
			}
		case <-tick:
			if !p.sendPing() {
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *Path) readLoop() {
	for {
		if p.session.cfg.ReadTimeout > 0 {
			_ = p.conn.SetReadDeadline(time.Now().Add(p.session.cfg.ReadTimeout))
		}
		f, err := readFrame(p.conn)
		if err != nil {
			p.session.pathFailed(p, err)
			return
		}

		switch f.typ {
		case frameData:
			if len(f.payload) <= 8 {
				p.session.pathFailed(p, fmt.Errorf("%w: short data frame", errProtocol))
				return
			}
			seq := binary.BigEndian.Uint64(f.payload[:8])
			data := f.payload[8:]
			p.rxBytes.Add(uint64(len(data)))
			p.session.receiver.handleData(seq, data, p)

		case frameAck:
			if len(f.payload) != 8 {
				p.session.pathFailed(p, fmt.Errorf("%w: bad ack frame", errProtocol))
				return
			}
			p.session.sender.handleAck(binary.BigEndian.Uint64(f.payload))

		case frameRecvAck:
			if len(f.payload) != 8 {
				p.session.pathFailed(p, fmt.Errorf("%w: bad receive ack frame", errProtocol))
				return
			}
			p.session.sender.handleRecvAck(binary.BigEndian.Uint64(f.payload), p)

		case frameFin:
			if len(f.payload) != 8 {
				p.session.pathFailed(p, fmt.Errorf("%w: bad fin frame", errProtocol))
				return
			}
			p.session.receiver.handleFin(binary.BigEndian.Uint64(f.payload), p)

		case framePing:
			if len(f.payload) != 8 {
				p.session.pathFailed(p, fmt.Errorf("%w: bad ping frame", errProtocol))
				return
			}
			if err := p.writeControl(framePong, 0, f.payload); err != nil {
				p.session.pathFailed(p, err)
				return
			}

		case framePong:
			if len(f.payload) != 8 {
				p.session.pathFailed(p, fmt.Errorf("%w: bad pong frame", errProtocol))
				return
			}
			sent := int64(binary.BigEndian.Uint64(f.payload))
			p.noteRTT(time.Since(time.Unix(0, sent)))

		case frameReset:
			p.session.fail(fmt.Errorf("%w: peer reset", errProtocol))
			return

		default:
			p.session.pathFailed(p, fmt.Errorf("%w: unexpected frame type %d", errProtocol, f.typ))
			return
		}
	}
}

func (p *Path) writeData(seq uint64, data []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.dead.Load() {
		return net.ErrClosed
	}
	p.setWriteDeadline()
	err := writeDataFrame(p.conn, seq, data)
	p.clearWriteDeadline()
	if err == nil {
		p.txBytes.Add(uint64(len(data)))
	}
	return err
}

func (p *Path) writeControl(typ frameType, flags byte, payload []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if p.dead.Load() {
		return net.ErrClosed
	}
	p.setWriteDeadline()
	err := writeFrame(p.conn, typ, flags, payload)
	p.clearWriteDeadline()
	return err
}

func (p *Path) setWriteDeadline() {
	if p.session.cfg.WriteTimeout > 0 {
		_ = p.conn.SetWriteDeadline(time.Now().Add(p.session.cfg.WriteTimeout))
	}
}

func (p *Path) clearWriteDeadline() {
	_ = p.conn.SetWriteDeadline(time.Time{})
}

func uint64Bytes(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func putUint64(dst []byte, v uint64) {
	binary.BigEndian.PutUint64(dst, v)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
