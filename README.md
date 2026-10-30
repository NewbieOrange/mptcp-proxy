# mptcp-proxy

A userspace multipath TCP proxy. It does **not** use the kernel MPTCP stack and
does not require MPTCP support in the network. Each local TCP connection is
carried as one private, sequenced stream spread over several ordinary TCP
connections.

```text
 application
     |
     | 127.0.0.1:8080
     v
 mptcp-proxy client ===== path 0 -> a:8080 ===== rinetd ==\
                    ===== path 1 -> b:8080 ===== rinetd ===\
                    ===== path 2 -> c:8080 ===== rinetd ====> dst:8080  mptcp-proxy server --> target:port
                    ===== path 3 -> d:8080 ===== rinetd ===/
```

The server opens **one** TCP connection to `target:port` per session and joins
all four component connections into that one byte stream. The application sees
a normal TCP connection to `127.0.0.1:8080`.

## Features

- Splits each connection into 64 KiB chunks and schedules them across all live
  paths. Scheduling is rate-aware: paths that deliver faster earn more send
  credits, while small per-path queues limit head-of-line delay behind a slow
  path.
- Upload and download are scheduled independently. Each direction can use the
  full aggregate capacity of all paths.
- Independent sequence numbers and acknowledgement windows in each direction.
- Fast receive acknowledgements drive path scheduling; delivery
  acknowledgements are sent only after data has been written to the destination
  socket, which bounds reassembly memory.
- In-flight chunks on a failed path are automatically assigned to the remaining
  paths. There is no duplicate-delivery problem: sequence numbers remove
  duplicates at the receiver.
- Failed paths are re-dialed with exponential backoff and rejoin the existing
  session when they recover. The session survives a period with no paths;
  uploads and downloads resume where they stopped.
- Correct TCP half-close handling in both directions.
- Per-path application keepalives and read/write timeouts.
- No external Go dependencies.

## Build

Go 1.24 or newer is required.

```sh
make
```

This produces one executable with `client` and `server` subcommands:

```text
bin/mptcp-proxy
```

```sh
./bin/mptcp-proxy help
./bin/mptcp-proxy client -h
./bin/mptcp-proxy server -h
```

## Deployment

### 1. Target

Run the actual service on `target`, for example `target:8080`.

### 2. Destination proxy (`dst`)

Run the server on the destination host. It will accept every path connection on
port 8080 and open one connection to the target for each session.

```sh
./bin/mptcp-proxy server \
  -listen :8080 \
  -target target:8080
```

The server's port must be reachable from `a`, `b`, `c`, and `d`.

### 3. Relay hosts (`a`, `b`, `c`, `d`)

Install [rinetd](https://github.com/samhocevar/rinetd) on each relay host and
forward its local port 8080 to `dst:8080`. A typical `/etc/rinetd.conf` is:

```text
# bind address   bind port   destination       destination port
0.0.0.0          8080        203.0.113.10      8080
logfile          /var/log/rinetd.log
```

Restart rinetd after changing the file:

```sh
systemctl restart rinetd
```

rinetd only relays ordinary TCP. The multipath protocol runs end to end between
the client and `dst`, through rinetd.

### 4. Client

On the machine running the application:

```sh
./bin/mptcp-proxy client \
  -listen 127.0.0.1:8080 \
  -paths a:8080,b:8080,c:8080,d:8080
```

Then point the application at `127.0.0.1:8080`. Every accepted application
connection opens all four paths and uses them concurrently in both directions.
Each path has its own reconnect loop: if one fails, it is removed immediately,
in-flight chunks move to the remaining paths, and the client keeps trying to
re-establish that path with backoff. A recovered path rejoins the same session.
The session stays open while all paths are temporarily down, up to
`-no-path-timeout`.

`-paths` may also be repeated:

```sh
./bin/mptcp-proxy client -listen 127.0.0.1:8080 \
  -paths a:8080 -paths b:8080 -paths c:8080 -paths d:8080
```

## Verifying that all paths are used

Run either process with `-v`. Each closed session reports the bytes carried by
every path:

```text
client ... session 5d1c9e7a4b02 closed (complete) after 812ms:
  path0[t=a:8080 rx=... tx=...] path1[...] path2[...] path3[...]
```

Each path should show non-zero traffic for a sufficiently large transfer. A
single small request may legitimately fit on one path.

## Tuning

| Flag | Default | Meaning |
| --- | ---: | --- |
| `-window` | 16 MiB | Unacknowledged bytes allowed per session. The main throughput knob for high bandwidth-delay-product links. |
| `-chunk` | 64 KiB | Application bytes per multipath frame. |
| `-queue` | 512 KiB | Approximate per-path data backlog. Smaller means less head-of-line delay; larger smooths bursts. |
| `-connect-wait` | 5s | How long the client waits for all paths when a connection starts. |
| `-dial-timeout` | 5s | Timeout for dialing one path or the target. |
| `-reconnect-min` | 250ms | Initial delay before re-dialing a failed client path. |
| `-reconnect-max` | 5s | Maximum reconnect backoff. |
| `-no-path-timeout` | 10m | Close a session if every path has been down this long. The client and server close the application/target connection together. |
| `-keepalive` | 20s | Path keepalive interval. |
| `-read-timeout` | 2m | Close a path if no frame arrives for this long. |
| `-write-timeout` | 1m | Close a path if a write is blocked for this long. |
| `-v` | off | Per-session and per-path logging. |

For high-bandwidth, high-latency paths, set the window to at least the sum of
the path BDPs, with room for reassembly. For example, four 1 Gbit/s paths with
40 ms RTT have about 20 MB of combined BDP:

```sh
./bin/mptcp-proxy client ... -window 33554432
./bin/mptcp-proxy server ... -window 33554432
```

The client and server do not have to use identical settings, but keeping the
window values in the same range is recommended.

## How the aggregation works

The sender assigns increasing sequence numbers to chunks. Each path earns send
credits at a rate derived from how fast it has recently been delivering data.
The next chunk goes to a live path with credit and queue space. Because a fast
path delivers and acknowledges more chunks, it earns credit faster and receives
a larger share. Per-path queues are deliberately small so a slow path cannot
accumulate a large backlog and delay everything behind it. TCP itself handles
reliability and congestion control on each path.

Upload and download each have their own sender, sequence space, window, and
path accounting, so both directions aggregate independently.

The receiver buffers out-of-order chunks and writes the stream to the local
socket strictly in sequence order. It sends a fast "received" acknowledgement
when a chunk enters the reassembly buffer, which is used for path scheduling. A
separate delivery acknowledgement is sent only after the chunk has been written
to the destination. The delivery acknowledgment drives the end-to-end flow
control window, so a fast sender cannot fill memory on a blocked destination.

If a path dies, every unacknowledged chunk assigned to it is put back in the
scheduler queue and sent over another path. Duplicates are acknowledged and
discarded by sequence number. The client keeps a reconnect loop for every
configured path. A recovered path performs the handshake with the existing
session ID and rejoins the live session without disturbing stream sequence
numbers.

More protocol details are in [docs/protocol.md](docs/protocol.md).

## Security and operational notes

- **The protocol is not encrypted or authenticated.** Use it over a trusted
  network, WireGuard/IPsec, or SSH tunnels. Restrict the server port so only
  `a`, `b`, `c`, and `d` can connect; otherwise the server can be abused as an
  open TCP proxy to its configured target.
- One server process forwards to one `target:port`. Run separate server
  instances if you need multiple targets.
- If the server process or its session state is lost, an existing client
  session cannot be resumed. A reconnect handshake is rejected and the
  application connection is closed rather than silently corrupting the stream.
  New application connections work normally after the server returns.
- A path that is alive but extremely slow can still add some head-of-line
  delay. Reducing `-queue` limits how much work can pile up behind it.
- Memory use is roughly bounded by `-window` plus the path queues per active
  connection, in each direction of the proxy.

## Test

```sh
make test
make test-race
```

The integration tests emulate rate-limited rinetd paths with in-process TCP
relays. They cover large transfers, upload and download aggregation, every path
being used, half-close, concurrent sessions, failover of a path in the middle
of a transfer, and recovery after all paths disconnect and reconnect.

## License

MIT. See [LICENSE](LICENSE).
