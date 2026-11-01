package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mptcp-proxy/internal/mpx"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

type commonFlags struct {
	window           *int64
	chunk            *int
	queue            *int64
	handshakeTimeout *time.Duration
	dialTimeout      *time.Duration
	noPathTimeout    *time.Duration
	keepalive        *time.Duration
	readTimeout      *time.Duration
	writeTimeout     *time.Duration
	verbose          *bool
}

func addCommonFlags(fs *flag.FlagSet) *commonFlags {
	return &commonFlags{
		window:           fs.Int64("window", 16<<20, "per-session send window in bytes"),
		chunk:            fs.Int("chunk", 64<<10, "multipath data chunk size in bytes"),
		queue:            fs.Int64("queue", 512<<10, "approximate per-path send queue in bytes"),
		handshakeTimeout: fs.Duration("handshake-timeout", 10*time.Second, "multipath handshake/setup timeout"),
		dialTimeout:      fs.Duration("dial-timeout", 5*time.Second, "dial timeout for one path or the target"),
		noPathTimeout:    fs.Duration("no-path-timeout", 10*time.Minute, "close a session after all paths have been down this long"),
		keepalive:        fs.Duration("keepalive", 20*time.Second, "path keepalive interval"),
		readTimeout:      fs.Duration("read-timeout", 2*time.Minute, "close a path after this long without frames"),
		writeTimeout:     fs.Duration("write-timeout", time.Minute, "timeout for a blocked path write"),
		verbose:          fs.Bool("v", false, "log per-session and per-path activity"),
	}
}

func (f *commonFlags) config(logger *log.Logger) mpx.Config {
	return mpx.Config{
		WindowBytes:      *f.window,
		ChunkBytes:       *f.chunk,
		PathQueueBytes:   *f.queue,
		HandshakeTimeout: *f.handshakeTimeout,
		DialTimeout:      *f.dialTimeout,
		NoPathTimeout:    *f.noPathTimeout,
		Keepalive:        *f.keepalive,
		ReadTimeout:      *f.readTimeout,
		WriteTimeout:     *f.writeTimeout,
		Logger:           logger,
		Verbose:          *f.verbose,
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "client":
		runClient(os.Args[2:])
	case "server":
		runServer(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  mptcp-proxy client -listen 127.0.0.1:8080 -paths a:8080,b:8080,c:8080,d:8080
  mptcp-proxy server -listen :8080 -target target:8080

Run "mptcp-proxy client -h" or "mptcp-proxy server -h" for all options.
`)
}

func runClient(args []string) {
	var paths stringList
	fs := flag.NewFlagSet("mptcp-proxy client", flag.ExitOnError)
	common := addCommonFlags(fs)
	listen := fs.String("listen", "127.0.0.1:8080", "local TCP address to accept application connections on")
	connectWait := fs.Duration("connect-wait", 5*time.Second, "maximum time to establish the first path")
	reconnectMin := fs.Duration("reconnect-min", 250*time.Millisecond, "minimum delay before reconnecting a failed path")
	reconnectMax := fs.Duration("reconnect-max", 5*time.Second, "maximum delay before reconnecting a failed path")
	fastOpen := fs.Bool("fastopen", false, "send application bytes before path handshakes complete and probe paths in the background")
	healthInterval := fs.Duration("health-interval", 30*time.Second, "background path/target probe interval in fastopen mode")
	fs.Var(&paths, "paths", "comma-separated path addresses (host:port); may be repeated")
	_ = fs.Parse(args)

	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "mptcp-proxy client: at least one -paths address is required")
		os.Exit(2)
	}
	logger := log.New(os.Stderr, "client ", log.LstdFlags|log.Lmicroseconds)
	cfg := common.config(logger)
	cfg.ConnectWait = *connectWait
	cfg.ReconnectMin = *reconnectMin
	cfg.ReconnectMax = *reconnectMax
	cfg.FastOpen = *fastOpen
	cfg.HealthInterval = *healthInterval

	client, err := mpx.NewClient(cfg, paths)
	if err != nil {
		logger.Fatalf("configuration: %v", err)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatalf("listen: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		logger.Printf("shutting down")
		_ = client.Close()
	}()

	if err := client.Serve(ln); err != nil {
		logger.Fatalf("serve: %v", err)
	}
}

func runServer(args []string) {
	fs := flag.NewFlagSet("mptcp-proxy server", flag.ExitOnError)
	common := addCommonFlags(fs)
	listen := fs.String("listen", ":8080", "TCP address on which to accept multipath connections")
	target := fs.String("target", "", "target TCP address (host:port) for proxied sessions")
	_ = fs.Parse(args)

	if *target == "" {
		fmt.Fprintln(os.Stderr, "mptcp-proxy server: -target host:port is required")
		os.Exit(2)
	}
	logger := log.New(os.Stderr, "server ", log.LstdFlags|log.Lmicroseconds)
	server, err := mpx.NewServer(common.config(logger), *target)
	if err != nil {
		logger.Fatalf("configuration: %v", err)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatalf("listen: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		logger.Printf("shutting down")
		_ = server.Close()
	}()

	if err := server.Serve(ln); err != nil {
		logger.Fatalf("serve: %v", err)
	}
}
