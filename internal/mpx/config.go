package mpx

import (
	"fmt"
	"io"
	"log"
	"time"
)

// Config controls buffering, timeouts, and logging for one endpoint.
type Config struct {
	// WindowBytes bounds unacknowledged application bytes per session.
	// Increase it for links with a large bandwidth-delay product.
	WindowBytes int64

	// ChunkBytes is the maximum amount of stream data carried by one frame.
	ChunkBytes int

	// PathQueueBytes is the approximate per-path send queue limit.
	PathQueueBytes int64

	HandshakeTimeout time.Duration
	DialTimeout      time.Duration
	ConnectWait      time.Duration

	// ReconnectMin and ReconnectMax control client-side path reconnect
	// backoff. NoPathTimeout closes a session if every path has been down for
	// this long (the target connection is released on the server).
	ReconnectMin  time.Duration
	ReconnectMax  time.Duration
	NoPathTimeout time.Duration

	Keepalive    time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	Logger  *log.Logger
	Verbose bool
}

func (c Config) withDefaults() (Config, error) {
	if c.WindowBytes == 0 {
		c.WindowBytes = 16 << 20
	}
	if c.ChunkBytes == 0 {
		c.ChunkBytes = 64 << 10
	}
	if c.PathQueueBytes == 0 {
		c.PathQueueBytes = 512 << 10
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ConnectWait == 0 {
		c.ConnectWait = 5 * time.Second
	}
	if c.ReconnectMin == 0 {
		c.ReconnectMin = 250 * time.Millisecond
	}
	if c.ReconnectMax == 0 {
		c.ReconnectMax = 5 * time.Second
	}
	if c.NoPathTimeout == 0 {
		c.NoPathTimeout = 10 * time.Minute
	}
	if c.Keepalive == 0 {
		c.Keepalive = 20 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 2 * time.Minute
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = time.Minute
	}
	if c.Logger == nil {
		c.Logger = log.New(io.Discard, "", 0)
	}

	if c.ChunkBytes < 1024 || c.ChunkBytes > maxDataChunkSize {
		return c, fmt.Errorf("chunk size must be between 1024 and %d bytes", maxDataChunkSize)
	}
	if c.WindowBytes < int64(c.ChunkBytes)*2 {
		return c, fmt.Errorf("window must be at least twice the chunk size")
	}
	if c.PathQueueBytes < int64(c.ChunkBytes) {
		return c, fmt.Errorf("path queue must be at least one chunk")
	}
	if c.HandshakeTimeout <= 0 || c.DialTimeout <= 0 || c.ConnectWait <= 0 {
		return c, fmt.Errorf("connect and handshake timeouts must be positive")
	}
	if c.ReconnectMin <= 0 || c.ReconnectMax < c.ReconnectMin || c.NoPathTimeout <= 0 {
		return c, fmt.Errorf("reconnect and no-path timeouts must be positive (max >= min)")
	}
	if c.Keepalive < 0 || c.ReadTimeout < 0 || c.WriteTimeout < 0 {
		return c, fmt.Errorf("timeouts cannot be negative")
	}
	return c, nil
}

func (c Config) pathQueueChunks() int {
	n := int(c.PathQueueBytes / int64(c.ChunkBytes))
	if n < 2 {
		n = 2
	}
	return n
}

func shortSessionID(id [16]byte) string {
	const hex = "0123456789abcdef"
	b := make([]byte, 12)
	for i := 0; i < 6; i++ {
		b[i*2] = hex[id[i]>>4]
		b[i*2+1] = hex[id[i]&0xf]
	}
	return string(b)
}
