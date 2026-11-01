# Protocol

The wire protocol is intentionally small. It runs over ordinary, reliable TCP
connections, one per path. There is no dependency on kernel MPTCP.

All integers are big endian.

## Frame format

Every frame starts with a 6-byte header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 1 | frame type |
| 1 | 1 | flags (currently zero) |
| 2 | 4 | payload length |

The maximum payload is 1 MiB. A DATA payload contains 8 extra bytes for its
sequence number, so the largest application chunk is 1 MiB - 8 bytes.

Frame types:

| Value | Name | Direction | Payload |
| ---: | --- | --- | --- |
| 1 | HELLO | client to server | see below |
| 2 | HELLO_ACK | server to client | see below |
| 3 | DATA | both | `uint64 seq` followed by bytes |
| 4 | ACK | both | `uint64 seq` |
| 5 | FIN | both | `uint64 final_seq` |
| 6 | RESET | both | reserved; current implementation closes the TCP path instead |
| 7 | PING | both | `uint64 timestamp_ns` |
| 8 | PONG | both | echo of the PING payload |
| 9 | RECV_ACK | both | `uint64 seq`; chunk reached the peer's reassembly buffer |

Sequence numbers identify chunks in one direction. They start at zero and
increase by one for each DATA frame. Sequence numbers are independent in each
direction. `final_seq` in a FIN is the sequence number immediately after the
last DATA chunk.

## HELLO

HELLO has a fixed 23-byte payload:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `MPX1` |
| 4 | 1 | protocol version (1) |
| 5 | 2 | client's path index |
| 7 | 16 | random session ID |

All paths belonging to one application connection use the same session ID. The
client dials the configured paths concurrently and sends HELLO on each one.

Bit 0 of the HELLO frame header flags is `RESUME`. It is set when the client is
reconnecting a path that previously belonged to an established session. If the
server no longer has that session, it replies with `session not found` instead
of creating a new target connection; the client then closes the application
connection because its stream state cannot be resumed.

Bit 1 is `PROBE`. It asks the server to dial the target, return a normal
HELLO_ACK, and close both connections without creating a session or installing
the path. The client uses this on a separate control connection to check that a
relay and the target are healthy, then evicts failing paths from new
connections.

## HELLO_ACK

HELLO_ACK has a 28-byte fixed part followed by an optional UTF-8 error message:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `MPX1` |
| 4 | 1 | protocol version |
| 5 | 1 | status |
| 6 | 2 | client's path index, echoed |
| 8 | 4 | server path index (currently zero) |
| 12 | 16 | session ID, echoed |
| 28 | var | status message |

Status values:

| Value | Meaning |
| ---: | --- |
| 0 | OK |
| 1 | unsupported protocol version |
| 2 | malformed request |
| 3 | target connection failed |
| 4 | session closed |
| 5 | session not found (a resume attempt arrived after the server dropped the session) |

The server creates a session on the first HELLO with a given session ID,
connects to its configured target, and waits for additional paths. The client
starts forwarding as soon as the first path completes its handshake (or fails
after the connect-wait deadline). The remaining paths join the already-active
session, so a slow handshake does not delay the application connection.

In fastopen mode, the client also allows DATA frames to follow HELLO on the
same TCP connection before HELLO_ACK arrives. The server buffers them until the
target connection is ready and the path is installed. If the target connection
or handshake fails, the client closes the session rather than continuing with
unacknowledged optimistic bytes.

## Data transfer and flow control

The sender reads from its source socket in chunks and assigns consecutive
sequence numbers. It retains each chunk until an ACK for that sequence number
arrives.

The scheduler is rate-aware. It measures how quickly each path returns
RECV_ACKs and refills a per-path send credit at that measured byte rate, with a
small oversubscription factor so a path with spare capacity can become
backlogged and reveal that capacity. The next chunk goes to a path with credit
and a free queue slot. Credits are capped and path queues are small, so a slow
path cannot accumulate an unbounded backlog. Per-path TCP supplies ordered
delivery, loss recovery, and congestion control.

Each path also tracks RTT from PING/PONG probes. When a sender is idle or has at
most two chunks in flight, it prefers the lowest-RTT path. This keeps short
request/response bursts and interactive traffic off high-latency paths; bulk
transfers use the rate-based multipath scheduler.

In fastopen mode, RTTs measured by the background health probes are copied into
newly created paths. The first application chunks can therefore use the
lowest-latency path immediately, before the new path's own PING/PONG completes.

Upload and download use separate scheduler and sequence-number instances.
Neither direction limits the other.

The receiver keeps a map of out-of-order chunks. It sends RECV_ACK as soon as
a chunk is buffered, even if earlier chunks are still missing. This lets the
sender keep the faster paths busy instead of treating them as slow merely
because head-of-line ordering is waiting on another path.

A dedicated delivery loop writes chunks to the destination socket in sequence
order. It sends a normal ACK only after the destination write succeeds.
Consequently:

- data received but not yet delivered still occupies the sender's window;
- unacknowledged data is bounded by the sender's window;
- the receiver does not release window space for data it cannot deliver yet;
- a blocked destination applies backpressure to all paths.

On the sender, the number of unacknowledged bytes cannot exceed `-window`
(default 16 MiB). Chunks remain in memory until acknowledged because they may
need to be sent again after a path failure.

## Path failure

TCP may deliver a broken connection as EOF, a reset, or a timeout. The path is
then removed from the session's live set.

Every unacknowledged chunk assigned to the failed path is marked unassigned and
put back in the scheduler queue. It may already have reached the receiver just
before the path died. The receiver handles this as a duplicate:

- a DATA frame with `seq < expected` is acknowledged with a delivery ACK and
  discarded;
- a DATA frame for a sequence number already buffered is answered with a
  RECV_ACK and discarded.

The client independently re-dials every configured path with exponential
backoff. A reconnected path sends HELLO with the same session ID and the
`RESUME` flag, then joins the existing session. Because sequence numbers and
unacknowledged chunks are retained during the outage, the byte stream resumes
without restarting.

The server keeps a session after its last path disappears. If no path returns
within `-no-path-timeout`, the session and its target connection are released.
A resume attempt after that point receives `session not found`; the client
closes the application connection because it cannot safely restart the stream
mid-connection.

## Session teardown and half-close

When a source socket reaches EOF, the sender queues a FIN with the next
sequence number. The FIN may arrive before some earlier DATA chunks, so the
receiver waits until every sequence number below `final_seq` has been written
to the destination. It then calls `CloseWrite` on the destination and sends an
ACK for the FIN sequence.

Each direction completes independently. The session closes its component
connections only after both directions have finished: all local data has been
acknowledged and delivered, and the peer's FIN has been received and applied.

## Keepalives and timeouts

Each path writer sends an immediate PING when the path starts and another every
`-keepalive` interval. The peer echoes it as a PONG, and the sender keeps an
EWMA of the measured RTT for latency-aware scheduling. Readers apply `-read-timeout` to every frame read and writers apply
`-write-timeout` to each write. A timeout removes that path and triggers the
same failover path as any other connection error.

## Security boundary

The protocol has no encryption, authentication, or authorization. Anyone who
can reach the server port can ask it to connect to the configured target. Bind
it behind a firewall restricted to the relay hosts, or run it inside a VPN or
other encrypted tunnel.
