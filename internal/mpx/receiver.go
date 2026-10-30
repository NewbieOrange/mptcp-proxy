package mpx

import (
	"fmt"
	"net"
	"sync"
)

type inItem struct {
	seq  uint64
	data []byte
	path *Path
}

// receiver reassembles sequence-numbered chunks and writes them to the local
// stream in order. Receive ACKs are sent as soon as a chunk is buffered, while
// delivery ACKs are sent only after the bytes have been written. The sender's
// window is released by delivery ACKs, so it also bounds reassembly memory.
type receiver struct {
	s    *Session
	mu   sync.Mutex
	cond *sync.Cond

	expected     uint64
	pending      map[uint64]*inItem
	pendingBytes int64

	finSeen    bool
	finSeq     uint64
	finPath    *Path
	finApplied bool

	maxPendingBytes  int64
	maxPendingChunks int
	closed           bool
}

func newReceiver(s *Session) *receiver {
	r := &receiver{
		s:                s,
		pending:          make(map[uint64]*inItem),
		maxPendingBytes:  s.cfg.WindowBytes*4 + int64(s.cfg.ChunkBytes)*8,
		maxPendingChunks: int(s.cfg.WindowBytes/int64(s.cfg.ChunkBytes))*4 + 1024,
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *receiver) handleData(seq uint64, data []byte, p *Path) {
	var ack, recvAck bool
	var fail error

	r.mu.Lock()
	switch {
	case r.closed:
		r.mu.Unlock()
		return
	case r.finSeen && seq >= r.finSeq:
		fail = fmt.Errorf("%w: data after fin", errProtocol)
	case r.finApplied && seq >= r.expected:
		fail = fmt.Errorf("%w: data after fin was applied", errProtocol)
	case seq < r.expected:
		ack = true
	case r.pending[seq] != nil:
		recvAck = true
	default:
		if r.pendingBytes+int64(len(data)) > r.maxPendingBytes || len(r.pending) >= r.maxPendingChunks {
			fail = fmt.Errorf("%w: receive buffer limit exceeded", errProtocol)
			break
		}
		r.pending[seq] = &inItem{seq: seq, data: data, path: p}
		r.pendingBytes += int64(len(data))
		recvAck = true
		r.cond.Broadcast()
	}
	r.mu.Unlock()

	if fail != nil {
		r.s.fail(fail)
		return
	}
	if recvAck {
		r.s.sendAckType(frameRecvAck, seq, p)
	}
	if ack {
		r.s.sendAck(seq, p)
	}
}

func (r *receiver) handleFin(seq uint64, p *Path) {
	var ack bool
	var fail error

	r.mu.Lock()
	switch {
	case r.closed:
		r.mu.Unlock()
		return
	case r.finApplied:
		ack = true
	case r.finSeen && seq == r.finSeq:
		// Duplicate FIN. Wake the delivery loop in case it is waiting on the
		// final gap.
		r.cond.Broadcast()
	case r.finSeen:
		fail = fmt.Errorf("%w: conflicting fin sequence", errProtocol)
	case seq < r.expected:
		fail = fmt.Errorf("%w: fin before delivered data", errProtocol)
	default:
		r.finSeen = true
		r.finSeq = seq
		r.finPath = p
		r.cond.Broadcast()
	}
	r.mu.Unlock()

	if fail != nil {
		r.s.fail(fail)
		return
	}
	if ack {
		r.s.sendAck(seq, p)
	}
}

func (r *receiver) run() {
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}

		var batch []*inItem
		for {
			it, ok := r.pending[r.expected]
			if !ok {
				break
			}
			delete(r.pending, r.expected)
			r.pendingBytes -= int64(len(it.data))
			r.expected++
			batch = append(batch, it)
		}

		finNow := r.finSeen && r.expected == r.finSeq
		if finNow {
			r.finApplied = true
		}
		finPath := r.finPath
		finSeq := r.finSeq
		r.mu.Unlock()

		for _, it := range batch {
			if err := writeAll(r.s.dest, it.data); err != nil {
				r.s.fail(fmt.Errorf("write to stream: %w", err))
				return
			}
			r.s.sendAck(it.seq, it.path)
		}

		if finNow {
			if err := closeWrite(r.s.dest); err != nil {
				r.s.fail(fmt.Errorf("half-close stream: %w", err))
				return
			}
			r.s.sendAck(finSeq, finPath)
			r.s.inboundDone()
			return
		}

		if len(batch) == 0 {
			r.mu.Lock()
			for !r.closed && !r.readyLocked() {
				r.cond.Wait()
			}
			closed := r.closed
			r.mu.Unlock()
			if closed {
				return
			}
		}
	}
}

func (r *receiver) readyLocked() bool {
	if _, ok := r.pending[r.expected]; ok {
		return true
	}
	return r.finSeen && r.expected == r.finSeq
}

func (r *receiver) stop() {
	r.mu.Lock()
	r.closed = true
	r.cond.Broadcast()
	r.mu.Unlock()
}

func closeWrite(c net.Conn) error {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := c.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return nil
}
