package mpx

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// sender turns one byte stream into sequence-numbered chunks and distributes
// them over the live paths. Unacknowledged chunks are retained so they can be
// moved to another path if a component connection fails.
type sender struct {
	s    *Session
	mu   sync.Mutex
	cond *sync.Cond

	unacked      map[uint64]*outItem
	unackedBytes int64
	nextSeq      uint64
	pending      []*outItem

	finActive bool
	finAcked  bool
	finSeq    uint64
	finItem   *outItem

	closed bool
}

func newSender(s *Session) *sender {
	sn := &sender{
		s:       s,
		unacked: make(map[uint64]*outItem),
	}
	sn.cond = sync.NewCond(&sn.mu)
	return sn
}

func (sn *sender) run() {
	chunkSize := sn.s.cfg.ChunkBytes
	for {
		if !sn.waitForWindow() {
			return
		}

		buf := make([]byte, chunkSize)
		n, err := sn.s.source.Read(buf)
		if n > 0 {
			sn.mu.Lock()
			if sn.closed {
				sn.mu.Unlock()
				return
			}
			it := &outItem{
				seq:  sn.nextSeq,
				data: buf[:n],
				n:    int64(n),
				// Keep short bursts on the lowest-RTT path. Once more than
				// two chunks are in flight, switch to throughput scheduling.
				lowLatency: sn.unackedBytes+int64(n) <= int64(2*chunkSize),
			}
			sn.nextSeq++
			sn.unacked[it.seq] = it
			sn.unackedBytes += int64(n)
			sn.pending = append(sn.pending, it)
			sn.dispatchLocked()
			sn.mu.Unlock()
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				sn.finish()
			} else if !sn.isClosed() {
				sn.s.fail(fmt.Errorf("read from stream: %w", err))
			}
			return
		}
	}
}

func (sn *sender) waitForWindow() bool {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for !sn.closed {
		sn.dispatchLocked()
		if sn.unackedBytes+int64(sn.s.cfg.ChunkBytes) <= sn.s.cfg.WindowBytes {
			return true
		}
		sn.cond.Wait()
	}
	return false
}

func (sn *sender) finish() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed || sn.finActive {
		return
	}
	sn.finActive = true
	sn.finSeq = sn.nextSeq
	it := &outItem{seq: sn.finSeq, fin: true}
	sn.finItem = it
	sn.pending = append(sn.pending, it)
	sn.dispatchLocked()
}

func (sn *sender) handleAck(seq uint64) {
	if sn == nil {
		return
	}
	var completed bool
	sn.mu.Lock()
	if sn.closed {
		sn.mu.Unlock()
		return
	}
	if it, ok := sn.unacked[seq]; ok {
		delete(sn.unacked, seq)
		sn.unackedBytes -= it.n
		if sn.unackedBytes < 0 {
			sn.unackedBytes = 0
		}
		if it.assigned != nil {
			it.assigned.outstandingBytes.Add(-it.n)
			it.assigned = nil
		}
	}
	if sn.finActive && seq == sn.finSeq && !sn.finAcked {
		sn.finAcked = true
		if sn.finItem != nil {
			sn.finItem.assigned = nil
		}
		completed = true
	}
	sn.cond.Broadcast()
	sn.mu.Unlock()

	if completed {
		sn.s.outboundDone()
	}
}

// handleRecvAck is a fast path acknowledgement: the peer has the chunk in its
// reassembly buffer, so it no longer counts against the path's capacity even
// if it cannot be delivered yet because an earlier chunk is missing.
func (sn *sender) handleRecvAck(seq uint64, p *Path) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed {
		return
	}
	it, ok := sn.unacked[seq]
	if !ok || it.received {
		return
	}
	it.received = true
	if it.assigned != nil {
		it.assigned.outstandingBytes.Add(-it.n)
		it.assigned = nil
	}
	p.noteRecvAck(it.n)
}

func (sn *sender) reschedule(failed *Path) {
	sn.mu.Lock()
	if sn.closed {
		sn.mu.Unlock()
		return
	}
	for _, it := range sn.unacked {
		if it.assigned == failed {
			failed.outstandingBytes.Add(-it.size())
			it.assigned = nil
			sn.pending = append(sn.pending, it)
		}
	}
	if sn.finItem != nil && !sn.finAcked && sn.finItem.assigned == failed {
		sn.finItem.assigned = nil
		sn.pending = append(sn.pending, sn.finItem)
	}
	sn.cond.Broadcast()
	sn.dispatchLocked()
	sn.mu.Unlock()
}

func (sn *sender) dispatchLocked() {
	for {
		for len(sn.pending) > 0 {
			it := sn.pending[0]
			if it.fin {
				if !sn.finActive || sn.finAcked {
					sn.pending = sn.pending[1:]
					continue
				}
			} else if sn.unacked[it.seq] != it || it.received {
				sn.pending = sn.pending[1:]
				continue
			}
			break
		}
		if len(sn.pending) == 0 || sn.closed {
			return
		}

		paths := sn.s.availablePaths()
		if len(paths) == 0 {
			return
		}
		it := sn.pending[0]
		p := choosePath(paths, sn.s.cfg.ChunkBytes, it.lowLatency)
		if p == nil {
			sn.cond.Wait()
			continue
		}

		sn.pending = sn.pending[1:]
		it.assigned = p
		p.outstandingBytes.Add(it.size())
		p.credit -= float64(it.size())
		if p.enqueue(it) {
			continue
		}

		// The path died between selection and enqueue. Put the item back and
		// wait for the path failure to wake the scheduler.
		p.outstandingBytes.Add(-it.size())
		p.credit += float64(it.size())
		it.assigned = nil
		sn.pending = append([]*outItem{it}, sn.pending...)
		sn.cond.Wait()
	}
}

func (sn *sender) isClosed() bool {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	return sn.closed
}

func (sn *sender) pathAdded() {
	if sn == nil {
		return
	}
	sn.mu.Lock()
	if sn.closed {
		sn.mu.Unlock()
		return
	}
	sn.cond.Broadcast()
	sn.mu.Unlock()

	// The main sender may be blocked reading its source socket. Flush any
	// chunks left pending by an outage onto the path that just appeared.
	go sn.flushPending()
}

func (sn *sender) flushPending() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed {
		return
	}
	sn.dispatchLocked()
}

func (sn *sender) signal() {
	if sn == nil {
		return
	}
	sn.mu.Lock()
	sn.cond.Broadcast()
	sn.mu.Unlock()
}

func (sn *sender) stop() {
	sn.mu.Lock()
	sn.closed = true
	sn.cond.Broadcast()
	sn.mu.Unlock()
}

func choosePath(paths []*Path, chunkSize int, lowLatency bool) *Path {
	now := time.Now()
	var best *Path
	var bestCredit float64
	var fastest *Path
	var fastestRTT int64
	for _, p := range paths {
		p.refillCredit(now, chunkSize)
		if p.dead.Load() || len(p.sendQ) >= cap(p.sendQ) {
			continue
		}
		if best == nil || p.credit > bestCredit {
			best = p
			bestCredit = p.credit
		}
		rtt := p.rttNanos.Load()
		if rtt > 0 && (fastest == nil || rtt < fastestRTT || (rtt == fastestRTT && p.credit > fastest.credit)) {
			fastest = p
			fastestRTT = rtt
		}
	}
	if lowLatency && fastest != nil {
		return fastest
	}
	return best
}
