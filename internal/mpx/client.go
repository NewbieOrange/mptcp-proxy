package mpx

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sort"
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

	ctx    context.Context
	cancel context.CancelFunc

	monitorOnce sync.Once
	healthMu    sync.Mutex
	healthReady bool
	healthKnown map[int]bool
	healthUp    map[int]bool
	healthRTT   map[int]time.Duration

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
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		cfg:         cfg,
		paths:       clean,
		logger:      cfg.Logger,
		ctx:         ctx,
		cancel:      cancel,
		healthKnown: make(map[int]bool),
		healthUp:    make(map[int]bool),
		healthRTT:   make(map[int]time.Duration),
		sessions:    make(map[*Session]struct{}),
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

	if c.cfg.FastOpen {
		c.startMonitor()
	}
	mode := ""
	if c.cfg.FastOpen {
		mode = " fastopen"
	}
	c.logger.Printf("client listening on %s over %d path(s)%s: %s", ln.Addr(), len(c.paths), mode, strings.Join(c.paths, ", "))
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

	c.cancel()
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

func (c *Client) startMonitor() {
	c.monitorOnce.Do(func() {
		go c.monitorLoop()
	})
}

func (c *Client) monitorLoop() {
	for {
		c.probeAll()
		timer := time.NewTimer(c.cfg.HealthInterval)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *Client) probeAll() {
	var wg sync.WaitGroup
	for i, addr := range c.paths {
		wg.Add(1)
		go func(index int, address string) {
			defer wg.Done()
			c.probePath(index, address)
		}(i, addr)
	}
	wg.Wait()

	c.healthMu.Lock()
	c.healthReady = true
	c.healthMu.Unlock()
}

func (c *Client) probePath(index int, addr string) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.HandshakeTimeout)
	defer cancel()

	start := time.Now()
	conn, err := c.connectPathFlags(ctx, id, uint16(index), addr, helloFlagProbe)
	if err != nil {
		c.markPathDown(index)
		if c.cfg.Verbose {
			c.logger.Printf("health path %d (%s): %v", index, addr, err)
		}
		return
	}
	_ = conn.Close()
	rtt := time.Since(start)
	c.markPathUp(index, rtt)
	if c.cfg.Verbose {
		c.logger.Printf("health path %d (%s): ok rtt=%s", index, addr, rtt.Round(time.Microsecond))
	}
}

func (c *Client) markPathUp(index int, rtt time.Duration) {
	if !c.cfg.FastOpen {
		return
	}
	c.healthMu.Lock()
	c.healthKnown[index] = true
	c.healthUp[index] = true
	c.healthRTT[index] = rtt
	c.healthMu.Unlock()
}

func (c *Client) markPathDown(index int) {
	if !c.cfg.FastOpen {
		return
	}
	c.healthMu.Lock()
	c.healthKnown[index] = true
	c.healthUp[index] = false
	c.healthMu.Unlock()
}

func (c *Client) pathIndices() []int {
	if !c.cfg.FastOpen {
		out := make([]int, len(c.paths))
		for i := range c.paths {
			out[i] = i
		}
		return out
	}

	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	if !c.healthReady {
		out := make([]int, len(c.paths))
		for i := range c.paths {
			out[i] = i
		}
		return out
	}
	var out []int
	for i := range c.paths {
		if c.healthKnown[i] && c.healthUp[i] {
			out = append(out, i)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return c.healthRTT[out[i]] < c.healthRTT[out[j]]
	})
	return out
}

func (c *Client) healthRTTFor(index int) time.Duration {
	if !c.cfg.FastOpen {
		return 0
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	if !c.healthKnown[index] || !c.healthUp[index] {
		return 0
	}
	return c.healthRTT[index]
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

	indices := c.pathIndices()
	if len(indices) == 0 {
		c.logger.Printf("session %s: no healthy paths", shortSessionID(id))
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

	initial := make(chan pathAttemptResult, len(indices))
	for _, i := range indices {
		go c.maintainPath(sess, i, c.paths[i], initial)
	}

	// Start as soon as the first path is usable. Waiting for every path adds
	// the latency of the slowest handshake to every new application
	// connection; the remaining paths can join while traffic is already
	// flowing.
	timer := time.NewTimer(c.cfg.ConnectWait)
	defer timer.Stop()
	pending := len(indices)
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
	if c.cfg.FastOpen {
		go c.watchFastOpenSetup(sess)
	}
}

func (c *Client) watchFastOpenSetup(sess *Session) {
	timer := time.NewTimer(c.cfg.ConnectWait)
	defer timer.Stop()
	select {
	case <-sess.established:
	case <-sess.closed:
	case <-timer.C:
		sess.closeWithError(fmt.Errorf("%w: no path completed the fastopen handshake", errHandshake))
	}
}

func (c *Client) maintainPath(sess *Session, index int, addr string, initial chan<- pathAttemptResult) {
	if c.cfg.FastOpen {
		c.maintainPathFastOpen(sess, index, addr, initial)
		return
	}

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
			c.markPathDown(index)
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
		c.markPathUp(index, 0)
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
		c.markPathDown(index)
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

func (c *Client) maintainPathFastOpen(sess *Session, index int, addr string, initial chan<- pathAttemptResult) {
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

		flags := uint8(0)
		if resumed {
			flags |= helloFlagResume
		}
		start := time.Now()
		conn, err := c.dialPath(sess.ctx, sess.id, uint16(index), addr, flags)
		if err != nil {
			c.markPathDown(index)
			initialAttempt := firstAttempt
			if firstAttempt {
				initial <- pathAttemptResult{index: index, ok: false, err: err}
				firstAttempt = false
			}
			if initialAttempt || c.cfg.Verbose {
				c.logger.Printf("session %s path %d (%s): connect failed: %v", shortSessionID(sess.id), index, addr, err)
			}
			backoff = nextBackoff(backoff, c.cfg.ReconnectMax)
			continue
		}

		p, err := sess.addPathEarly(conn, uint16(index), c.healthRTTFor(index))
		if err != nil {
			_ = conn.Close()
			c.markPathDown(index)
			if sess.isClosed() {
				return
			}
			if firstAttempt {
				initial <- pathAttemptResult{index: index, ok: false, err: err}
				firstAttempt = false
			}
			backoff = nextBackoff(backoff, c.cfg.ReconnectMax)
			continue
		}

		// The path can now write. Application data is allowed to run ahead of
		// HELLO_ACK and is buffered by the server until it installs the path.
		wasFirst := firstAttempt
		if firstAttempt {
			initial <- pathAttemptResult{index: index, ok: true}
			firstAttempt = false
		}

		ackErr := c.awaitHelloAck(sess.ctx, conn, sess.id)
		if ackErr != nil {
			sess.pathFailed(p, ackErr)
			c.markPathDown(index)
			if sess.isClosed() {
				return
			}
			if errors.Is(ackErr, errSessionNotFound) || errors.Is(ackErr, errTargetUnavailable) {
				sess.closeWithError(fmt.Errorf("path %d: %w", index, ackErr))
				return
			}
			if wasFirst || c.cfg.Verbose {
				c.logger.Printf("session %s path %d (%s): fastopen handshake failed: %v", shortSessionID(sess.id), index, addr, ackErr)
			}
			backoff = nextBackoff(backoff, c.cfg.ReconnectMax)
			continue
		}

		p.startReader()
		sess.markPathEstablished()
		c.markPathUp(index, time.Since(start))
		resumed = true
		backoff = c.cfg.ReconnectMin
		connectedAt := time.Now()
		if !wasFirst {
			c.logger.Printf("session %s path %d (%s) reconnected", shortSessionID(sess.id), index, addr)
		}

		<-p.done
		if sess.isClosed() {
			return
		}
		c.markPathDown(index)
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
	var flags uint8
	if resume {
		flags |= helloFlagResume
	}
	return c.connectPathFlags(ctx, id, pathID, addr, flags)
}

func (c *Client) connectPathFlags(ctx context.Context, id [16]byte, pathID uint16, addr string, flags uint8) (net.Conn, error) {
	conn, err := c.dialPath(ctx, id, pathID, addr, flags)
	if err != nil {
		return nil, err
	}
	if err := c.awaitHelloAck(ctx, conn, id); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *Client) dialPath(ctx context.Context, id [16]byte, pathID uint16, addr string, flags uint8) (net.Conn, error) {
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

	if err := writeFrame(conn, frameHello, flags, encodeHello(id, pathID)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("send hello: %w", err)
	}
	return conn, nil
}

func (c *Client) awaitHelloAck(ctx context.Context, conn net.Conn, id [16]byte) error {
	f, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("read hello acknowledgement: %w", err)
	}
	if f.typ != frameHelloAck {
		return fmt.Errorf("%w: expected hello acknowledgement, got frame %d", errHandshake, f.typ)
	}
	ack, err := decodeHelloAck(f.payload)
	if err != nil {
		return err
	}
	if ack.sessionID != id {
		return fmt.Errorf("%w: session id mismatch", errHandshake)
	}
	if ack.status != helloStatusOK {
		if ack.message == "" {
			ack.message = "server rejected path"
		}
		switch ack.status {
		case helloStatusSessionNotFound:
			return fmt.Errorf("%w: %s", errSessionNotFound, ack.message)
		case helloStatusTargetFailed, helloStatusSessionClosed:
			return fmt.Errorf("%w: %s", errTargetUnavailable, ack.message)
		default:
			return fmt.Errorf("%w: %s", errHandshake, ack.message)
		}
	}
	_ = conn.SetDeadline(time.Time{})
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
