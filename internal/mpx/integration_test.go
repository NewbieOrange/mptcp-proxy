package mpx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		WindowBytes:      8 << 20,
		ChunkBytes:       32 << 10,
		PathQueueBytes:   256 << 10,
		HandshakeTimeout: 5 * time.Second,
		DialTimeout:      2 * time.Second,
		ConnectWait:      2 * time.Second,
		Keepalive:        5 * time.Second,
		ReadTimeout:      20 * time.Second,
		WriteTimeout:     10 * time.Second,
		Logger:           log.New(io.Discard, "", 0),
		Verbose:          false,
	}
}

type testListener struct {
	ln     net.Listener
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

func startTestListener(t *testing.T, handle func(net.Conn)) *testListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tl := &testListener{ln: ln, conns: make(map[net.Conn]struct{})}
	t.Cleanup(tl.Close)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			tl.mu.Lock()
			if tl.closed {
				tl.mu.Unlock()
				c.Close()
				return
			}
			tl.conns[c] = struct{}{}
			tl.mu.Unlock()
			go func() {
				defer func() {
					tl.mu.Lock()
					delete(tl.conns, c)
					tl.mu.Unlock()
					c.Close()
				}()
				handle(c)
			}()
		}
	}()
	return tl
}

func (tl *testListener) Addr() string { return tl.ln.Addr().String() }

func (tl *testListener) Close() {
	tl.mu.Lock()
	if tl.closed {
		tl.mu.Unlock()
		return
	}
	tl.closed = true
	conns := make([]net.Conn, 0, len(tl.conns))
	for c := range tl.conns {
		conns = append(conns, c)
	}
	tl.mu.Unlock()
	_ = tl.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

type countWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (cw countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n.Add(int64(n))
	return n, err
}

// copyWithDelay models one-way propagation delay without serializing every
// read behind the previous delayed write. Packets enqueued close together stay
// close together when they arrive.
func copyWithDelay(dst io.Writer, src io.Reader, delay time.Duration, count *atomic.Int64) error {
	type packet struct {
		at   time.Time
		data []byte
	}
	ch := make(chan packet, 128)
	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				data := append([]byte(nil), buf[:n]...)
				ch <- packet{at: time.Now().Add(delay), data: data}
			}
			if err != nil {
				readErr <- err
				close(ch)
				return
			}
		}
	}()
	for p := range ch {
		if wait := time.Until(p.at); wait > 0 {
			time.Sleep(wait)
		}
		n, err := dst.Write(p.data)
		count.Add(int64(n))
		if err != nil {
			return err
		}
	}
	err := <-readErr
	if err == io.EOF {
		return nil
	}
	return err
}

type rateWriter struct {
	w    io.Writer
	rate int64
	mu   sync.Mutex
	next time.Time
}

func (rw *rateWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	now := time.Now()
	if rw.next.Before(now) {
		rw.next = now
	}
	rw.next = rw.next.Add(time.Duration(int64(len(p))) * time.Second / time.Duration(rw.rate))
	wait := time.Until(rw.next)
	rw.mu.Unlock()
	if wait > 0 {
		time.Sleep(wait)
	}
	return rw.w.Write(p)
}

type testRelay struct {
	listener       *net.Listener
	target         string
	bytes          atomic.Int64
	bytesPerSecond int64
	oneWayDelay    time.Duration
	mu             sync.Mutex
	conns          map[net.Conn]struct{}
	closed         bool
	once           sync.Once
}

func startTestRelay(t *testing.T, target string) *testRelay {
	return startTestRelayRated(t, target, 0)
}

func startTestRelayRated(t *testing.T, target string, bytesPerSecond int64) *testRelay {
	return startTestRelaySimulated(t, target, bytesPerSecond, 0)
}

func startTestRelayDelayed(t *testing.T, target string, oneWayDelay time.Duration) *testRelay {
	return startTestRelaySimulated(t, target, 0, oneWayDelay)
}

func startTestRelaySimulated(t *testing.T, target string, bytesPerSecond int64, oneWayDelay time.Duration) *testRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &testRelay{
		listener:       &ln,
		target:         target,
		bytesPerSecond: bytesPerSecond,
		oneWayDelay:    oneWayDelay,
		conns:          make(map[net.Conn]struct{}),
	}
	t.Cleanup(r.Close)
	go r.acceptLoop()
	return r
}

func (r *testRelay) Addr() string { return (*r.listener).Addr().String() }

func (r *testRelay) acceptLoop() {
	for {
		src, err := (*r.listener).Accept()
		if err != nil {
			return
		}
		if !r.track(src) {
			src.Close()
			return
		}
		go r.forward(src)
	}
}

func (r *testRelay) track(c net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.conns[c] = struct{}{}
	return true
}

func (r *testRelay) untrack(c net.Conn) {
	r.mu.Lock()
	delete(r.conns, c)
	r.mu.Unlock()
}

func (r *testRelay) forward(src net.Conn) {
	defer r.untrack(src)
	defer src.Close()

	dst, err := net.DialTimeout("tcp", r.target, 2*time.Second)
	if err != nil {
		return
	}
	if !r.track(dst) {
		dst.Close()
		return
	}
	defer r.untrack(dst)
	defer dst.Close()

	dstOut := io.Writer(dst)
	srcOut := io.Writer(src)
	if r.bytesPerSecond > 0 {
		dstOut = &rateWriter{w: dstOut, rate: r.bytesPerSecond}
		srcOut = &rateWriter{w: srcOut, rate: r.bytesPerSecond}
	}

	copyOneWay := func(w io.Writer, from io.Reader) {
		if r.oneWayDelay > 0 {
			_ = copyWithDelay(w, from, r.oneWayDelay, &r.bytes)
			return
		}
		_, _ = io.Copy(countWriter{w: w, n: &r.bytes}, from)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		copyOneWay(dstOut, src)
		closeTCPWrite(dst)
	}()
	go func() {
		defer wg.Done()
		copyOneWay(srcOut, dst)
		closeTCPWrite(src)
	}()
	wg.Wait()
}

func closeTCPWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

func (r *testRelay) DropConnections() {
	r.mu.Lock()
	conns := make([]net.Conn, 0, len(r.conns))
	for c := range r.conns {
		conns = append(conns, c)
	}
	r.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (r *testRelay) Close() {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		conns := make([]net.Conn, 0, len(r.conns))
		for c := range r.conns {
			conns = append(conns, c)
		}
		r.mu.Unlock()
		_ = (*r.listener).Close()
		for _, c := range conns {
			_ = c.Close()
		}
	})
}

func startEchoTarget(t *testing.T) *testListener {
	t.Helper()
	return startTestListener(t, func(c net.Conn) {
		_, _ = io.Copy(c, c)
	})
}

func startProxyPair(t *testing.T, target string, pathCount int) (string, []*testRelay) {
	return startProxyPairRated(t, target, pathCount, 0)
}

func startProxyPairRated(t *testing.T, target string, pathCount int, bytesPerSecond int64) (string, []*testRelay) {
	t.Helper()
	cfg := testConfig()

	srv, err := NewServer(cfg, target)
	if err != nil {
		t.Fatal(err)
	}
	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(serverLn) }()
	t.Cleanup(func() { _ = srv.Close() })

	relays := make([]*testRelay, pathCount)
	pathAddrs := make([]string, pathCount)
	for i := range relays {
		relays[i] = startTestRelayRated(t, serverLn.Addr().String(), bytesPerSecond)
		pathAddrs[i] = relays[i].Addr()
	}

	cli, err := NewClient(cfg, pathAddrs)
	if err != nil {
		t.Fatal(err)
	}
	clientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = cli.Serve(clientLn) }()
	t.Cleanup(func() { _ = cli.Close() })

	return clientLn.Addr().String(), relays
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i>>7)
	}
	return b
}

func testTransfer(t *testing.T, proxyAddr string, data []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(45 * time.Second))

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(data)
		if err == nil {
			err = conn.(*net.TCPConn).CloseWrite()
		}
		writeErr <- err
	}()

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write request: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestSinglePath(t *testing.T) {
	target := startEchoTarget(t)
	proxyAddr, _ := startProxyPair(t, target.Addr(), 1)
	testTransfer(t, proxyAddr, pattern(4<<20))
}

func TestMultipathEchoUsesEveryPath(t *testing.T) {
	target := startEchoTarget(t)
	proxyAddr, relays := startProxyPair(t, target.Addr(), 4)
	data := pattern(8 << 20)

	testTransfer(t, proxyAddr, data)

	for i, r := range relays {
		if r.bytes.Load() == 0 {
			t.Errorf("path %d carried no bytes", i)
		}
	}
}

func TestHalfCloseAndOrderedDelivery(t *testing.T) {
	request := pattern(3 << 20)
	target := startTestListener(t, func(c net.Conn) {
		body, err := io.ReadAll(c)
		if err != nil {
			return
		}
		var response [16]byte
		binary.BigEndian.PutUint64(response[0:8], uint64(len(body)))
		binary.BigEndian.PutUint64(response[8:16], uint64(crc32.ChecksumIEEE(body)))
		_, _ = c.Write(response[:])
	})

	proxyAddr, _ := startProxyPair(t, target.Addr(), 4)
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(request)
		if err == nil {
			err = conn.(*net.TCPConn).CloseWrite()
		}
		writeErr <- err
	}()

	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if len(response) != 16 {
		t.Fatalf("response length = %d, want 16", len(response))
	}
	gotLen := binary.BigEndian.Uint64(response[0:8])
	gotSum := binary.BigEndian.Uint64(response[8:16])
	if gotLen != uint64(len(request)) || gotSum != uint64(crc32.ChecksumIEEE(request)) {
		t.Fatalf("target saw len=%d crc=%08x, want len=%d crc=%08x", gotLen, gotSum, len(request), crc32.ChecksumIEEE(request))
	}
}

func TestPathFailureFailover(t *testing.T) {
	target := startEchoTarget(t)
	proxyAddr, relays := startProxyPair(t, target.Addr(), 3)
	data := pattern(16 << 20)

	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(data)
		if err == nil {
			err = conn.(*net.TCPConn).CloseWrite()
		}
		writeErr <- err
	}()

	deadline := time.Now().Add(10 * time.Second)
	for relays[1].bytes.Load() < 1<<20 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	relays[1].Close()

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write request: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("echo mismatch after path failure: got %d bytes, want %d", len(got), len(data))
	}
}

func TestConcurrentSessions(t *testing.T) {
	target := startEchoTarget(t)
	proxyAddr, _ := startProxyPair(t, target.Addr(), 4)
	data := pattern(2 << 20)

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", proxyAddr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(45 * time.Second))
			go func() {
				_, _ = conn.Write(data)
				_ = conn.(*net.TCPConn).CloseWrite()
			}()
			got, err := io.ReadAll(conn)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, data) {
				errs <- fmt.Errorf("echo mismatch: got %d want %d", len(got), len(data))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestAllPathsReconnectDuringTransfer(t *testing.T) {
	const pathRate = 4 << 20
	target := startEchoTarget(t)
	proxyAddr, relays := startProxyPairRated(t, target.Addr(), 4, pathRate)
	data := pattern(8 << 20)

	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(data)
		if err == nil {
			err = conn.(*net.TCPConn).CloseWrite()
		}
		writeErr <- err
	}()

	deadline := time.Now().Add(10 * time.Second)
	for relays[0].bytes.Load()+relays[1].bytes.Load()+relays[2].bytes.Load()+relays[3].bytes.Load() < 1<<20 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for _, r := range relays {
		r.DropConnections()
	}

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write request: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("echo mismatch after reconnect: got %d bytes, want %d", len(got), len(data))
	}
	for i, r := range relays {
		if r.bytes.Load() == 0 {
			t.Errorf("path %d carried no bytes", i)
		}
	}
}

func TestResumeUnknownSessionIsRejected(t *testing.T) {
	target := startEchoTarget(t)
	srv, err := NewServer(testConfig(), target.Addr())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	var id [16]byte
	id[0] = 1
	if err := writeFrame(conn, frameHello, helloFlagResume, encodeHello(id, 0)); err != nil {
		t.Fatal(err)
	}
	f, err := readFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := decodeHelloAck(f.payload)
	if err != nil {
		t.Fatal(err)
	}
	if ack.status != helloStatusSessionNotFound {
		t.Fatalf("status = %d, want session-not-found", ack.status)
	}
}

func TestUploadAndDownloadAggregateIndependently(t *testing.T) {
	const (
		pathRate = 4 << 20
		size     = 8 << 20
	)
	data := pattern(size)
	sum := crc32.ChecksumIEEE(data)

	t.Run("upload", func(t *testing.T) {
		target := startTestListener(t, func(c net.Conn) {
			body := make([]byte, size)
			if _, err := io.ReadFull(c, body); err != nil {
				return
			}
			var response [12]byte
			binary.BigEndian.PutUint64(response[0:8], uint64(len(body)))
			binary.BigEndian.PutUint32(response[8:12], crc32.ChecksumIEEE(body))
			_, _ = c.Write(response[:])
		})
		proxyAddr, _ := startProxyPairRated(t, target.Addr(), 4, pathRate)
		conn, err := net.Dial("tcp", proxyAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

		start := time.Now()
		if _, err := conn.Write(data); err != nil {
			t.Fatal(err)
		}
		var response [12]byte
		if _, err := io.ReadFull(conn, response[:]); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		gotLen := binary.BigEndian.Uint64(response[0:8])
		gotSum := binary.BigEndian.Uint32(response[8:12])
		if gotLen != size || gotSum != sum {
			t.Fatalf("target saw len=%d crc=%08x, want len=%d crc=%08x", gotLen, gotSum, size, sum)
		}
		if elapsed > 1500*time.Millisecond {
			t.Fatalf("upload took %s; four %d MiB/s paths should aggregate", elapsed, pathRate>>20)
		}
		t.Logf("upload throughput: %.1f MiB/s", float64(size)/elapsed.Seconds()/(1<<20))
	})

	t.Run("download", func(t *testing.T) {
		target := startTestListener(t, func(c net.Conn) {
			_, _ = c.Write(data)
			closeTCPWrite(c)
		})
		proxyAddr, _ := startProxyPairRated(t, target.Addr(), 4, pathRate)
		conn, err := net.Dial("tcp", proxyAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

		start := time.Now()
		got := make([]byte, size)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		if crc32.ChecksumIEEE(got) != sum {
			t.Fatal("download payload mismatch")
		}
		if elapsed > 1500*time.Millisecond {
			t.Fatalf("download took %s; four %d MiB/s paths should aggregate", elapsed, pathRate>>20)
		}
		t.Logf("download throughput: %.1f MiB/s", float64(size)/elapsed.Seconds()/(1<<20))
	})
}

func TestMultipathAggregatesRateLimitedPaths(t *testing.T) {
	const (
		pathRate = 4 << 20 // 4 MiB/s in each direction per path
		size     = 16 << 20
	)
	target := startEchoTarget(t)
	proxyAddr, relays := startProxyPairRated(t, target.Addr(), 4, pathRate)

	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	data := pattern(size)
	start := time.Now()
	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(data)
		if err == nil {
			err = conn.(*net.TCPConn).CloseWrite()
		}
		writeErr <- err
	}()
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if !bytes.Equal(got, data) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// One path would need about 4 seconds. Four paths should move the same
	// bytes in about one second; allow a wide margin for slow CI machines.
	if elapsed > 2500*time.Millisecond {
		t.Fatalf("aggregate transfer took %s; four %d MiB/s paths should be much faster than one", elapsed, pathRate>>20)
	}
	for i, r := range relays {
		if r.bytes.Load() == 0 {
			t.Errorf("path %d carried no bytes", i)
		}
		t.Logf("path %d carried %d bytes", i, r.bytes.Load())
	}
	t.Logf("aggregate throughput: %.1f MiB/s over 4 paths", float64(size)/elapsed.Seconds()/(1<<20))
}
