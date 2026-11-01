// Package mpx implements a small, private multipath transport used by the
// mptcp-proxy client and server subcommands.
package mpx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	protocolVersion = 1
	protocolMagic   = "MPX1"

	frameHeaderSize  = 6
	maxFramePayload  = 1 << 20
	maxDataChunkSize = maxFramePayload - 8
)

type frameType uint8

const (
	frameHello frameType = iota + 1
	frameHelloAck
	frameData
	frameAck
	frameFin
	frameReset
	framePing
	framePong
	frameRecvAck
)

const (
	helloStatusOK byte = iota
	helloStatusBadVersion
	helloStatusBadRequest
	helloStatusTargetFailed
	helloStatusSessionClosed
	helloStatusSessionNotFound
)

const (
	helloFlagResume uint8 = 1 << 0
	helloFlagProbe  uint8 = 1 << 1
)

var (
	errProtocol          = errors.New("mpx: protocol error")
	errSessionClosed     = errors.New("mpx: session closed")
	errSessionNotFound   = errors.New("mpx: session no longer exists on server")
	errTargetUnavailable = errors.New("mpx: target unavailable")
	errNoPaths           = errors.New("mpx: no usable paths")
	errFrameTooLarge     = errors.New("mpx: frame too large")
	errHandshake         = errors.New("mpx: handshake failed")
)

type wireFrame struct {
	typ     frameType
	flags   uint8
	payload []byte
}

type helloInfo struct {
	sessionID [16]byte
	pathID    uint16
}

type helloAckInfo struct {
	sessionID    [16]byte
	status       byte
	clientPathID uint16
	serverPathID uint32
	message      string
}

func readFrame(r io.Reader) (wireFrame, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return wireFrame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[2:6])
	if n > maxFramePayload {
		return wireFrame{}, fmt.Errorf("%w: %d bytes", errFrameTooLarge, n)
	}
	payload := make([]byte, int(n))
	if n != 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return wireFrame{}, err
		}
	}
	return wireFrame{
		typ:     frameType(hdr[0]),
		flags:   hdr[1],
		payload: payload,
	}, nil
}

func writeFrame(w io.Writer, typ frameType, flags byte, payload []byte) error {
	var hdr [frameHeaderSize]byte
	hdr[0] = byte(typ)
	hdr[1] = flags
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(payload)))
	bufs := net.Buffers{hdr[:], payload}
	_, err := bufs.WriteTo(w)
	return err
}

func writeDataFrame(w io.Writer, seq uint64, data []byte) error {
	var hdr [14]byte
	hdr[0] = byte(frameData)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(data)+8))
	binary.BigEndian.PutUint64(hdr[6:14], seq)
	bufs := net.Buffers{hdr[:], data}
	_, err := bufs.WriteTo(w)
	return err
}

func writeUint64Frame(w io.Writer, typ frameType, v uint64) error {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], v)
	return writeFrame(w, typ, 0, payload[:])
}

func encodeHello(sessionID [16]byte, pathID uint16) []byte {
	b := make([]byte, 23)
	copy(b[0:4], protocolMagic)
	b[4] = protocolVersion
	binary.BigEndian.PutUint16(b[5:7], pathID)
	copy(b[7:23], sessionID[:])
	return b
}

func decodeHello(p []byte) (helloInfo, error) {
	if len(p) != 23 || string(p[0:4]) != protocolMagic {
		return helloInfo{}, fmt.Errorf("%w: bad hello", errHandshake)
	}
	if p[4] != protocolVersion {
		return helloInfo{}, fmt.Errorf("%w: unsupported version %d", errHandshake, p[4])
	}
	var h helloInfo
	h.pathID = binary.BigEndian.Uint16(p[5:7])
	copy(h.sessionID[:], p[7:23])
	return h, nil
}

func encodeHelloAck(sessionID [16]byte, status byte, clientPathID uint16, serverPathID uint32, message string) []byte {
	b := make([]byte, 28+len(message))
	copy(b[0:4], protocolMagic)
	b[4] = protocolVersion
	b[5] = status
	binary.BigEndian.PutUint16(b[6:8], clientPathID)
	binary.BigEndian.PutUint32(b[8:12], serverPathID)
	copy(b[12:28], sessionID[:])
	copy(b[28:], message)
	return b
}

func decodeHelloAck(p []byte) (helloAckInfo, error) {
	if len(p) < 28 || string(p[0:4]) != protocolMagic {
		return helloAckInfo{}, fmt.Errorf("%w: bad hello acknowledgement", errHandshake)
	}
	if p[4] != protocolVersion {
		return helloAckInfo{}, fmt.Errorf("%w: unsupported version %d", errHandshake, p[4])
	}
	var h helloAckInfo
	h.status = p[5]
	h.clientPathID = binary.BigEndian.Uint16(p[6:8])
	h.serverPathID = binary.BigEndian.Uint32(p[8:12])
	copy(h.sessionID[:], p[12:28])
	h.message = string(p[28:])
	return h, nil
}
