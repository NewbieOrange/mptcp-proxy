package mpx

import (
	"io"
	"net"
	"sort"
	"testing"
	"time"
)

func startProxyPairDelays(t *testing.T, target string, delays []time.Duration) string {
	return startProxyPairDelaysConfig(t, target, delays, testConfig())
}

func startProxyPairDelaysConfig(t *testing.T, target string, delays []time.Duration, cfg Config) string {
	t.Helper()
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

	pathAddrs := make([]string, len(delays))
	for i, d := range delays {
		r := startTestRelayDelayed(t, serverLn.Addr().String(), d)
		pathAddrs[i] = r.Addr()
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
	if cfg.FastOpen {
		deadline := time.Now().Add(5 * time.Second)
		for {
			cli.healthMu.Lock()
			ready := cli.healthReady
			cli.healthMu.Unlock()
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("health probes did not become ready")
			}
			time.Sleep(time.Millisecond)
		}
	}
	return clientLn.Addr().String()
}

func oneRoundTrip(addr string) (time.Duration, error) {
	start := time.Now()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte{'x'}); err != nil {
		return 0, err
	}
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func stats(t *testing.T, name string, samples []time.Duration) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	n := len(samples)
	var total time.Duration
	for _, d := range samples {
		total += d
	}
	t.Logf("%-24s min=%s median=%s p95=%s avg=%s", name,
		samples[0].Round(10*time.Microsecond),
		samples[n/2].Round(10*time.Microsecond),
		samples[(n*95)/100].Round(10*time.Microsecond),
		(total / time.Duration(n)).Round(10*time.Microsecond))
}

func measureNewConnections(t *testing.T, name, addr string, n int) {
	t.Helper()
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		d, err := oneRoundTrip(addr)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		samples = append(samples, d)
	}
	stats(t, name, samples)
}

func measurePersistent(t *testing.T, name, addr string, n int) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	var b [1]byte
	if _, err := conn.Write([]byte{'w'}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, b[:]); err != nil {
		t.Fatal(err)
	}
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if _, err := conn.Write([]byte{'x'}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(start))
	}
	stats(t, name, samples)
}

func TestLatencySimulation(t *testing.T) {
	target := startEchoTarget(t)
	const (
		fast = 10 * time.Millisecond
		slow = 50 * time.Millisecond
		n    = 20
	)

	single := startTestRelayDelayed(t, target.Addr(), fast)
	measureNewConnections(t, "single relay (10ms)", single.Addr(), n)
	measurePersistent(t, "single relay persistent", single.Addr(), n)

	cases := []struct {
		name   string
		delays []time.Duration
	}{
		{"1 path 10ms", []time.Duration{fast}},
		{"2 paths all 10ms", []time.Duration{fast, fast}},
		{"5 paths all 10ms", []time.Duration{fast, fast, fast, fast, fast}},
		{"5 paths slow last", []time.Duration{fast, fast, fast, fast, slow}},
		{"5 paths slow first", []time.Duration{slow, fast, fast, fast, fast}},
	}
	for _, tc := range cases {
		addr := startProxyPairDelays(t, target.Addr(), tc.delays)
		measureNewConnections(t, tc.name, addr, n)
		measurePersistent(t, tc.name+" persistent", addr, n)
	}

	fastCfg := testConfig()
	fastCfg.FastOpen = true
	fastCfg.HealthInterval = time.Hour
	for _, tc := range []struct {
		name   string
		delays []time.Duration
	}{
		{"fastopen 5 paths all 10ms", []time.Duration{fast, fast, fast, fast, fast}},
		{"fastopen slow first", []time.Duration{slow, fast, fast, fast, fast}},
		{"fastopen slow last", []time.Duration{fast, fast, fast, fast, slow}},
	} {
		addr := startProxyPairDelaysConfig(t, target.Addr(), tc.delays, fastCfg)
		measureNewConnections(t, tc.name, addr, n)
		measurePersistent(t, tc.name+" persistent", addr, n)
	}
}
