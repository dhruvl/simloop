# Third-party libraries under simulation

[docs/supported-api.md](supported-api.md) states which asyncio APIs simloop
simulates. This page answers the next question — what happens when a real
library runs on top of them — with evidence rather than intent: every row
below is the output of a script anyone can re-run.

Recorded **2026-08-04**, against simloop 0.2.0 (unreleased) on Python 3.12
with OpenSSL 3.5.7.

## What a probe is

A probe is a small script in `probes/` that drives one library's happy path
under a `SimLoop` — between two simulated hosts where the library talks to a
network — and reports a single verdict:

- `works: <what was exercised>` — the probe ran to the end of its own script
  and described what it did.
- `fenced: <message>` — a `SimulationFenceError` was raised during the run.
  The message is reproduced exactly.
- `fails: <exception>` — the run ended some other way: an exception from the
  library, or the probe's virtual-time budget running out.

A verdict is a statement about that one run and nothing more. `works` means
those calls, on that version, produced that result; it is not a support
claim, and the same library may well fence one call later. The probes drive
happy paths only: no retries, no reconnection, no concurrency beyond what the
probe itself starts. TLS is a happy path they now drive, with certificates
minted in memory for the sim hostnames the probes use.

## Regenerating the table

```
uv run --group probes python probes/report.py
```

and paste the output below. The probe libraries are pinned exactly in the
`probes` dependency group in `pyproject.toml`, so the version column
describes what actually ran; `uv run pytest` does not install them, and
`probes/` is never packaged. The probes are deliberately not part of CI — a
third-party release should not break simloop's build — which is why this page
carries a date instead.

| Library | Version | Verdict | Notes |
|---|---|---|---|
| aiohttp (server) | 3.14.3 | works: one GET over the sim network: 'HTTP/1.1 200 OK', body 'hello from the simulation' | AppRunner + loop.create_server on a sim host; raw sim-stream client. |
| aiohttp (web.TCPSite) | 3.14.3 | works: web.TCPSite(...).start() bound port 8081 on a sim host | The documented AppRunner + TCPSite startup path, nothing else. |
| anyio | 4.14.2 | works: task group, memory object stream (one, two, three), anyio.sleep and move_on_after; virtual clock reached 1.75s | Asyncio backend only; nothing here touches a socket. |
| redis (RESP wire protocol) | n/a | works: PING, SET and GET round trips over one connection: ['+PONG', '+OK', '0'] | Hand-rolled RESP over sim streams; no client library, no real server. |
| websockets | 17.0.1 | works: handshake, one echoed frame ('HELLO') and close over ws:// | asyncio server and client on two sim hosts, ws:// only. |
| websockets (wss) | 17.0.1 | works: handshake, one echoed frame ('HELLO') and close over wss:// | asyncio server and client on two sim hosts, over wss://. |
| aiohttp (client) | 3.14.3 | works: ClientSession GET returned 'hello from the simulation' | ClientSession GET at a sim host answered by a raw stream server. |
| aiohttp (client, https) | 3.14.3 | works: ClientSession GET over https returned 'hello from the simulation' | ClientSession GET over https at a sim host with a minted certificate. |
| httpx | 0.28.1 | works: AsyncClient GET returned 'hello from the simulation' | AsyncClient GET at a sim host answered by a raw stream server. |
| httpx (https) | 0.28.1 | works: AsyncClient GET over https returned 'hello from the simulation' | AsyncClient GET over https; the TLS engine is anyio's, not the loop's. |

Rows are grouped: the libraries that need nothing but the loop and its
streams first, then the client stacks that expect a socket object
underneath them.

## Reading the rows

**anyio** needs nothing but tasks, futures and timers, so its asyncio backend
runs unchanged. That matters beyond anyio itself: it is the concurrency layer
under httpx, starlette and anything built on `anyio.to_thread`-free code.

**aiohttp's server** answers requests over the simulated network, both
through `loop.create_server` directly and through the documented
`AppRunner` + `web.TCPSite` startup path — the one `web.run_app` uses.
The two rows exist because they once differed: `TCPSite` reads
`server.sockets` during startup, which the simulated server did not answer
until it learned to report an empty tuple (there are no sockets in a
simulation, and the stdlib documents the tuple as possibly empty).

**websockets** completes a handshake, echoes a frame and closes cleanly
over `ws://` between two sim hosts. Its `serve()` reads the same
`server.sockets` attribute during startup — only to log where it is
listening — so it was unblocked by the same empty tuple.

**redis** has no row for a client library: every async Redis client needs a
live server to reach its first command, and a live server is exactly what a
simulation does not have. What is testable without one is the pattern those
clients are built on — a length-prefixed request/response protocol on one
long-lived connection — so the probe speaks RESP by hand against a small
server on a second sim host, and the row claims no more than that.

**aiohttp's client** issues its GET and reads the body back. Its connector
resolves the name through `loop.getaddrinfo`, then hands the addresses to
`aiohappyeyeballs`, which creates a real `AF_INET` stream socket, calls
`loop.sock_connect` on it, and passes that socket to
`loop.create_connection(sock=...)`. The simulation answers the sequence
without letting the socket reach a network: `sock_connect` resolves the
target against the host table and records it, moving no packet and no
clock, and the `create_connection` call closes the real descriptor and
opens a simulated connection to the recorded address, paying the same
single round trip a direct `create_connection` would. The connector's
`setsockopt(TCP_NODELAY)` lands on the stand-in object
`get_extra_info("socket")` returns, which accepts option calls and does
nothing with them.

**httpx** completes the same request through httpcore and anyio. anyio
resolves the name (the simulated resolver accepts the ASCII-encoded form
anyio sends) and connects through `loop.create_connection`, with no socket
of its own. The step that used to end this row comes next: httpcore asks
the new stream who it is connected to, and anyio answers by reading the
socket object out of `transport.get_extra_info("socket")`
(`anyio/abc/_sockets.py`, `extra_attributes`) and calling `getpeername()`
on it. A transport with no operating-system socket now answers with a
stand-in that reports the peer's synthetic address and port, so the
introspection succeeds and the response body comes back.

Every client probe makes one request against a responder that sends
`Connection: close`, so no row says anything about connection reuse.
The piece a pool depends on is the descriptor `fileno()` returns: httpcore
polls it to decide whether a pooled connection has died, and the
simulation backs it with a parked descriptor the transport owns, which
stays unreadable while the peer is alive and becomes readable once the
peer's EOF arrives; a reset or a teardown closes it and `fileno()` returns
`-1`, which the same poll reads as dead just as well. That contract is
pinned by the test suite, not by these rows.

**aiohttp over `https://`** completes the same two-call connect its
`http://` path uses, with `ssl` and `server_hostname` riding alongside
`sock` in the `create_connection` call. What the connector does after the
connect is answered by the two layers together: `sslcontext` and
`ssl_object` come from the TLS layer, `peername` and the `setsockopt` on
the stand-in socket from the simulated transport underneath it. The
certificate is minted for the sim hostname `web` and the client context
trusts that authority and nothing else, so the row says OpenSSL really
verified rather than that verification was turned off.

**httpx over `https://`** reaches no loop TLS API at all, which is why it
is worth its own row. httpcore wraps the byte stream with anyio's
`TLSStream`, which drives an `ssl` memory BIO inside the process and sends
the handshake as ordinary bytes over the simulated connection. Nothing in
simloop is involved in that handshake; what changed is that the simulation
now has a peer on the other end that speaks TLS back, so the request
completes instead of dying of httpx's own `ConnectTimeout`.

**websockets over `wss://`** completes a handshake, echoes a frame and
closes with both ends inside the simulation. It is the only row that
drives `create_server(ssl=...)` and `create_connection(ssl=...)` in one
run.

## Not tested

- **asyncpg**: reaching its first fence needs a live PostgreSQL server to
  connect to, which no probe can provide; it is untested rather than
  fenced-or-not.
- **The rest of TLS**: the three TLS rows drive a server certificate, one
  cipher suite and TLS 1.3. Client certificates, a peer restricted to TLS
  1.2, ALPN and h2 negotiation, and session resumption are not probed.
- Anything that reaches outside the loop by design — threads, subprocesses,
  signals, real DNS. Those are fences, listed in
  [docs/supported-api.md](supported-api.md), not compatibility questions.
  Executors left this list: `run_in_executor` now runs the function inline,
  which the same page describes; `anyio.to_thread` stays out because its
  worker threads are real ones the loop never sees.
