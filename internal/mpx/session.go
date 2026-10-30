package mpx

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Session struct {
	id     [16]byte
	cfg    Config
	logger interface {
		Printf(string, ...any)
	}
	verbose bool

	dataMu sync.Mutex
	source net.Conn
	dest   net.Conn

	sender   *sender
	receiver *receiver

	pathMu       sync.Mutex
	paths        []*Path
	nextPathID   uint32
	pathsStarted bool
	noPathTimer  *time.Timer
	noPathEpoch  uint64

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	closeOnce sync.Once
	closed    chan struct{}

	inDone  atomic.Bool
	outDone atomic.Bool

	createdAt time.Time
	onClose   func(*Session)
}

func newSession(id [16]byte, cfg Config, onClose func(*Session)) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		id:        id,
		cfg:       cfg,
		logger:    cfg.Logger,
		verbose:   cfg.Verbose,
		ctx:       ctx,
		cancel:    cancel,
		closed:    make(chan struct{}),
		onClose:   onClose,
		createdAt: time.Now(),
	}
	s.sender = newSender(s)
	s.receiver = newReceiver(s)
	return s
}

func (s *Session) attach(source, dest net.Conn) error {
	s.dataMu.Lock()
	defer s.dataMu.Unlock()
	if s.isClosed() {
		return errSessionClosed
	}
	s.source = source
	s.dest = dest
	return nil
}

// addPath registers a component connection. It starts the path immediately if
// the session has already started; otherwise start will do it.
func (s *Session) addPath(conn net.Conn, clientPathID uint16) (*Path, error) {
	select {
	case <-s.closed:
		return nil, errSessionClosed
	default:
	}

	s.pathMu.Lock()
	if s.isClosed() {
		s.pathMu.Unlock()
		_ = conn.Close()
		return nil, errSessionClosed
	}
	s.cancelNoPathTimerLocked()
	id := s.nextPathID
	s.nextPathID++
	p := newPath(s, id, clientPathID, conn)
	s.paths = append(s.paths, p)
	started := s.pathsStarted
	s.pathMu.Unlock()

	if started {
		p.start()
	}
	s.sender.pathAdded()
	return p, nil
}

func (s *Session) start() {
	s.startOnce.Do(func() {
		if s.isClosed() {
			return
		}
		s.pathMu.Lock()
		if s.isClosed() {
			s.pathMu.Unlock()
			return
		}
		s.pathsStarted = true
		paths := append([]*Path(nil), s.paths...)
		s.pathMu.Unlock()

		if s.source == nil || s.dest == nil {
			s.closeWithError(errSessionClosed)
			return
		}
		for _, p := range paths {
			p.start()
		}
		go s.sender.run()
		go s.receiver.run()
		s.sender.signal()
	})
}

func (s *Session) abortIfNoPaths() {
	s.pathMu.Lock()
	empty := len(s.paths) == 0 && !s.pathsStarted
	s.pathMu.Unlock()
	if empty {
		s.closeWithError(fmt.Errorf("%w: setup timed out", errHandshake))
	}
}

func (s *Session) dropClientPath(clientPathID uint16) {
	s.pathMu.Lock()
	var old *Path
	for _, p := range s.paths {
		if p.clientPathID == clientPathID {
			old = p
			break
		}
	}
	s.pathMu.Unlock()
	if old != nil {
		s.pathFailed(old, fmt.Errorf("replaced by a reconnected path"))
	}
}

func (s *Session) pathCount() int {
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	return len(s.paths)
}

func (s *Session) availablePaths() []*Path {
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	out := make([]*Path, 0, len(s.paths))
	for _, p := range s.paths {
		if !p.dead.Load() {
			out = append(out, p)
		}
	}
	return out
}

func (s *Session) pickControlPath() *Path {
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	var best *Path
	var bestBytes int64
	for _, p := range s.paths {
		if p.dead.Load() {
			continue
		}
		n := p.outstandingBytes.Load()
		if best == nil || n < bestBytes {
			best, bestBytes = p, n
		}
	}
	return best
}

func (s *Session) pathFailed(p *Path, cause error) {
	if !p.markDead() {
		return
	}
	// markDead closes the connection, which makes the read and writer loops
	// exit. Removing the path here makes it invisible to the scheduler.
	s.pathMu.Lock()
	idx := -1
	for i, candidate := range s.paths {
		if candidate == p {
			idx = i
			break
		}
	}
	if idx >= 0 {
		s.paths = append(s.paths[:idx], s.paths[idx+1:]...)
	}
	if len(s.paths) == 0 {
		s.armNoPathTimerLocked()
	}
	s.pathMu.Unlock()

	if s.verbose {
		s.logger.Printf("session %s path %d (%s) failed: %v", shortSessionID(s.id), p.id, p.remote, cause)
	}

	s.sender.reschedule(p)
}

// The caller holds pathMu.
func (s *Session) armNoPathTimerLocked() {
	if s.isClosed() || s.cfg.NoPathTimeout <= 0 {
		return
	}
	s.noPathEpoch++
	epoch := s.noPathEpoch
	if s.noPathTimer != nil {
		s.noPathTimer.Stop()
	}
	s.noPathTimer = time.AfterFunc(s.cfg.NoPathTimeout, func() {
		s.pathMu.Lock()
		expired := s.noPathEpoch == epoch && len(s.paths) == 0 && !s.isClosed()
		s.pathMu.Unlock()
		if expired {
			s.closeWithError(fmt.Errorf("%w: all paths were down for %s", errNoPaths, s.cfg.NoPathTimeout))
		}
	})
}

// The caller holds pathMu.
func (s *Session) cancelNoPathTimerLocked() {
	if s.noPathTimer != nil {
		s.noPathTimer.Stop()
		s.noPathTimer = nil
	}
	s.noPathEpoch++
}

func (s *Session) sendAck(seq uint64, preferred *Path) {
	s.sendAckType(frameAck, seq, preferred)
}

func (s *Session) sendAckType(typ frameType, seq uint64, preferred *Path) {
	var payload [8]byte
	putUint64(payload[:], seq)

	for attempts := 0; attempts < 4; attempts++ {
		p := preferred
		if p == nil || p.dead.Load() {
			p = s.pickControlPath()
		}
		if p == nil {
			return
		}
		if err := p.writeControl(typ, 0, payload[:]); err != nil {
			s.pathFailed(p, err)
			preferred = nil
			continue
		}
		return
	}
}

func (s *Session) inboundDone() {
	if s.inDone.CompareAndSwap(false, true) && s.outDone.Load() {
		s.closeWithError(nil)
	}
}

func (s *Session) outboundDone() {
	if s.outDone.CompareAndSwap(false, true) && s.inDone.Load() {
		s.closeWithError(nil)
	}
}

func (s *Session) fail(err error) {
	s.closeWithError(err)
}

func (s *Session) closeWithError(err error) {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.cancel()

		s.pathMu.Lock()
		s.cancelNoPathTimerLocked()
		paths := append([]*Path(nil), s.paths...)
		s.paths = nil
		s.pathMu.Unlock()
		for _, p := range paths {
			p.markDead()
		}

		s.dataMu.Lock()
		source, dest := s.source, s.dest
		s.dataMu.Unlock()
		if source != nil {
			_ = source.Close()
		}
		if dest != nil && dest != source {
			_ = dest.Close()
		}
		s.sender.stop()
		s.receiver.stop()

		if s.verbose {
			s.logSummary(err, paths)
		}
		if s.onClose != nil {
			s.onClose(s)
		}
	})
}

func (s *Session) logSummary(reason error, paths []*Path) {
	var parts []string
	for _, p := range paths {
		parts = append(parts, fmt.Sprintf("path%d[t=%s rx=%d tx=%d rate=%d]", p.id, p.remote, p.rxBytes.Load(), p.txBytes.Load(), p.rateBps.Load()))
	}
	status := "complete"
	if reason != nil {
		status = reason.Error()
	}
	s.logger.Printf("session %s closed (%s) after %s: %s", shortSessionID(s.id), status, time.Since(s.createdAt).Round(time.Millisecond), strings.Join(parts, " "))
}

func (s *Session) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}
