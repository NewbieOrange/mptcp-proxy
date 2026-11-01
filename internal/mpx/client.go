package mpx

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// Client accepts local TCP connections and carries each one over all of its
// configured paths.
type Client struct {
	cfg    Config
	paths  []string
	logger interface {
		Printf(string, ...any)
	}

	mu       sync.Mutex
	listener net.Listener
	sessions map[*Session]struct{}
	closed   bool
}

// NewClient validates the path list and constructs a client. Paths are
// "host:port" strings, normally the rinetd listeners on a, b, c, and d.
func NewClient(cfg Config, paths []string) (*Client, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("at least one path is required")
	}
	if len(paths) > 1<<16-1 {
		return nil, fmt.Errorf("too many paths (maximum 65535)")
	}
	clean := make([]string, 0, len(paths))
	for _, raw := range paths {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("invalid path %q: %w", raw, err)
		}
		clean = append(clean, addr)
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("at least one path is required")
	}
	return &Client{
		cfg:      cfg,
		paths:    clean,
		logger:   cfg.Logger,
		sessions: make(map[*Session]struct{}),
	}, nil
}

func (c *Client) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return c.Serve(ln)
}

func (c *Client) Serve(ln net.Listener) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = ln.Close()
		return errSessionClosed
	}
	c.listener = ln
	c.mu.Unlock()

	c.logger.Printf("client listening on %s over %d path(s): %s", ln.Addr(), len(c.paths), strings.Join(c.paths, ", "))
	defer func() {
		c.mu.Lock()
		if c.listener == ln {
			c.listener = nil
		}
		c.mu.Unlock()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if c.isClosed() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go c.handle(conn)
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	ln := c.listener
	sessions := make([]*Session, 0, len(c.sessions))
	for s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, s := range sessions {
		s.closeWithError(errSessionClosed)
	}
	return nil
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type pathAttemptResult struct {
	index int
	ok    bool
	err   error
}

func (c *Client) handle(local net.Conn) {
	handedOff := false
	defer func() {
		if !handedOff {
			_ = local.Close()
		}
	}()

	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		c.logger.Printf("generate session id: %v", err)
		return
	}

	sess := newSession(id, c.cfg, nil)
	if err := sess.attach(local, local); err != nil {
		return
	}
	handedOff = true
	sess.onClose = func(s *Session) {
		c.mu.Lock()
		delete(c.sessions, s)
		c.mu.Unlock()
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		sess.closeWithError(errSessionClosed)
		return
	}
	c.sessions[sess] = struct{}{}
	c.mu.Unlock()

	initial := make(chan pathAttemptResult, len(c.paths))
	for i, addr := range c.paths {
		go c.maintainPath(sess, i, addr, initial)
	}

	// Start as soon as the first path is usable. Waiting for every path adds
	// the latency of the slowest handshake to every new application
	// connection; the remaining paths can join while traffic is already
	// flowing.
	timer := time.NewTimer(c.cfg.ConnectWait)
	defer timer.Stop()
	pending := len(c.paths)
	ready := false
	for pending > 0 && !ready {
		select {
		case r := <-initial:
			pending--
			if r.ok {
				ready = true
			}
		case <-timer.C:
			pending = 0
		case <-sess.closed:
			return
		}
	}

	if !ready && sess.pathCount() == 0 {
		c.logger.Printf("session %s: no paths connected", shortSessionID(id))
		sess.closeWithError(errNoPaths)
		return
	}
	sess.start()
}

func (c *Client) maintainPath(sess *Session, index int, addr string, initial chan<- pathAttemptResult) {
	backoff := c.cfg.ReconnectMin
	firstAttempt := true
	resumed := false

	for {
		if sess.isClosed() {
			return
		}
		if !firstAttempt && !waitContext(sess.ctx, backoff) {
			return
		}

		conn, err := c.connectPath(sess.ctx, sess.id, uint16(index), addr, resumed)
		if err != nil {
			initialAttempt := firstAttempt
			if firstAttempt {
				initial <- pathAttemptResult{index: index, ok: false, err: err}
				firstAttempt = false
			}
			if errors.Is(err, errSessionNotFound) {
				sess.closeWithError(fmt.Errorf("path %d: %w", index, err))
				return
			}
			if initialAttempt || c.cfg.Verbose {
				c.logger.Printf("session %s path %d (%s): connect failed: %v", shortSessionID(sess.id), index, addr, err)
			}
			backoff = nextBackoff(backoff, c.cfg.ReconnectMax)
			continue
		}

		p, err := sess.addPath(conn, uint16(index))
		if err != nil {
			_ = conn.Close()
			if sess.isClosed() {
				return
			}
			if firstAttempt {
				initial <- pathAttemptResult{index: index, ok: false, err: err}
				firstAttempt = false
			}
			if c.cfg.Verbose {
				c.logger.Printf("session %s path %d (%s): add path: %v", shortSessionID(sess.id), index, addr, err)
			}
			backoff = nextBackoff(backoff, c.cfg.ReconnectMax)
			continue
		}

		resumed = true
		backoff = c.cfg.ReconnectMin
		connectedAt := time.Now()
		if firstAttempt {
			initial <- pathAttemptResult{index: index, ok: true}
			firstAttempt = false
		} else {
			c.logger.Printf("session %s path %d (%s) reconnected", shortSessionID(sess.id), index, addr)
		}

		<-p.done
		if sess.isClosed() {
			return
		}
		if time.Since(connectedAt) >= 10*time.Second {
			backoff = c.cfg.ReconnectMin
		} else {
			backoff = nextBackoff(backoff, c.cfg.ReconnectMax)
		}
		if c.cfg.Verbose {
			c.logger.Printf("session %s path %d (%s) disconnected; will reconnect", shortSessionID(sess.id), index, addr)
		}
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	current *= 2
	if current > maximum {
		return maximum
	}
	return current
}

func waitContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *Client) connectPath(ctx context.Context, id [16]byte, pathID uint16, addr string, resume bool) (net.Conn, error) {
	dialer := net.Dialer{Timeout: c.cfg.DialTimeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(c.cfg.HandshakeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	var flags uint8
	if resume {
		flags |= helloFlagResume
	}
	if err := writeFrame(conn, frameHello, flags, encodeHello(id, pathID)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("send hello: %w", err)
	}
	f, err := readFrame(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read hello acknowledgement: %w", err)
	}
	if f.typ != frameHelloAck {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: expected hello acknowledgement, got frame %d", errHandshake, f.typ)
	}
	ack, err := decodeHelloAck(f.payload)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if ack.sessionID != id {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: session id mismatch", errHandshake)
	}
	if ack.status != helloStatusOK {
		_ = conn.Close()
		if ack.message == "" {
			ack.message = "server rejected path"
		}
		if ack.status == helloStatusSessionNotFound {
			return nil, fmt.Errorf("%w: %s", errSessionNotFound, ack.message)
		}
		return nil, fmt.Errorf("%w: %s", errHandshake, ack.message)
	}
	_ = conn.SetDeadline(time.Time{})
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
