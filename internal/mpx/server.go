package mpx

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type serverEntry struct {
	sess     *Session
	initDone chan struct{}
	initErr  error
}

// Server accepts path connections and joins them into sessions. Every session
// is forwarded to the same configured target address.
type Server struct {
	cfg    Config
	target string
	logger interface {
		Printf(string, ...any)
	}

	mu       sync.Mutex
	listener net.Listener
	sessions map[[16]byte]*serverEntry
	closed   bool
}

func NewServer(cfg Config, target string) (*Server, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if _, _, err := net.SplitHostPort(target); err != nil {
		return nil, fmt.Errorf("invalid target %q: %w", target, err)
	}
	return &Server{
		cfg:      cfg,
		target:   target,
		logger:   cfg.Logger,
		sessions: make(map[[16]byte]*serverEntry),
	}, nil
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return errSessionClosed
	}
	s.listener = ln
	s.mu.Unlock()

	s.logger.Printf("server listening on %s, target %s", ln.Addr(), s.target)
	defer func() {
		s.mu.Lock()
		if s.listener == ln {
			s.listener = nil
		}
		s.mu.Unlock()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.listener
	entries := make([]*serverEntry, 0, len(s.sessions))
	for _, e := range s.sessions {
		entries = append(entries, e)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, e := range entries {
		e.sess.closeWithError(errSessionClosed)
	}
	return nil
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) handleConn(conn net.Conn) {
	owned := true
	defer func() {
		if owned {
			_ = conn.Close()
		}
	}()

	_ = conn.SetDeadline(time.Now().Add(s.cfg.HandshakeTimeout))
	f, err := readFrame(conn)
	if err != nil {
		if s.cfg.Verbose {
			s.logger.Printf("handshake from %s: %v", conn.RemoteAddr(), err)
		}
		return
	}
	if f.typ != frameHello {
		if s.cfg.Verbose {
			s.logger.Printf("handshake from %s: unexpected frame %d", conn.RemoteAddr(), f.typ)
		}
		return
	}
	hello, err := decodeHello(f.payload)
	if err != nil {
		if s.cfg.Verbose {
			s.logger.Printf("handshake from %s: %v", conn.RemoteAddr(), err)
		}
		return
	}
	if f.flags&helloFlagProbe != 0 {
		s.handleProbe(conn, hello)
		return
	}
	resume := f.flags&helloFlagResume != 0

	entry, err := s.getEntry(hello.sessionID, resume)
	if err != nil {
		if errors.Is(err, errSessionNotFound) {
			_ = writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, helloStatusSessionNotFound, hello.pathID, 0, "session no longer exists; reconnect is not possible"))
		}
		return
	}

	timer := time.NewTimer(s.cfg.HandshakeTimeout)
	defer timer.Stop()
	select {
	case <-entry.initDone:
	case <-timer.C:
		_ = writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, helloStatusTargetFailed, hello.pathID, 0, "target initialization timed out"))
		return
	case <-entry.sess.closed:
		_ = writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, helloStatusSessionClosed, hello.pathID, 0, "session closed"))
		return
	}

	if entry.initErr != nil {
		status := helloStatusTargetFailed
		if errors.Is(entry.initErr, errSessionClosed) {
			status = helloStatusSessionClosed
		}
		_ = writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, status, hello.pathID, 0, entry.initErr.Error()))
		return
	}
	if entry.sess.isClosed() {
		_ = writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, helloStatusSessionClosed, hello.pathID, 0, "session closed"))
		return
	}

	if err := writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, helloStatusOK, hello.pathID, 0, "")); err != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})

	// A reconnecting client reuses the same per-path index. Discard any stale
	// server-side connection for that index before installing the fresh one.
	entry.sess.dropClientPath(hello.pathID)

	if _, err := entry.sess.addPath(conn, hello.pathID); err != nil {
		return
	}
	owned = false
	entry.sess.start()

	if s.cfg.Verbose {
		s.logger.Printf("session %s: path from %s (client path %d)", shortSessionID(hello.sessionID), conn.RemoteAddr(), hello.pathID)
	}
}

func (s *Server) handleProbe(conn net.Conn, hello helloInfo) {
	dialer := net.Dialer{Timeout: s.cfg.DialTimeout, KeepAlive: 30 * time.Second}
	target, err := dialer.Dial("tcp", s.target)
	status := helloStatusOK
	message := ""
	if err != nil {
		status = helloStatusTargetFailed
		message = err.Error()
	} else {
		_ = target.Close()
	}
	_ = writeFrame(conn, frameHelloAck, 0, encodeHelloAck(hello.sessionID, status, hello.pathID, 0, message))
	if s.cfg.Verbose {
		s.logger.Printf("probe from %s: status=%d", conn.RemoteAddr(), status)
	}
}

func (s *Server) getEntry(id [16]byte, resume bool) (*serverEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errSessionClosed
	}
	if e, ok := s.sessions[id]; ok {
		if !e.sess.isClosed() {
			return e, nil
		}
		delete(s.sessions, id)
	}
	if resume {
		return nil, errSessionNotFound
	}

	e := &serverEntry{
		initDone: make(chan struct{}),
	}
	e.sess = newSession(id, s.cfg, func(sess *Session) {
		s.mu.Lock()
		if current := s.sessions[id]; current != nil && current.sess == sess {
			delete(s.sessions, id)
		}
		s.mu.Unlock()
	})
	s.sessions[id] = e
	go s.initialize(e)
	time.AfterFunc(s.cfg.HandshakeTimeout, e.sess.abortIfNoPaths)
	return e, nil
}

func (s *Server) initialize(e *serverEntry) {
	dialer := net.Dialer{Timeout: s.cfg.DialTimeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.Dial("tcp", s.target)
	if err == nil {
		if attachErr := e.sess.attach(conn, conn); attachErr != nil {
			_ = conn.Close()
			err = attachErr
		}
	}
	e.initErr = err
	close(e.initDone)
	if err != nil {
		e.sess.closeWithError(fmt.Errorf("connect target %s: %w", s.target, err))
	}
}
