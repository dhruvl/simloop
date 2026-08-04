# Third-party libraries under simulation

[docs/supported-api.md](supported-api.md) states which asyncio APIs simloop
simulates. This page answers the next question — what happens when a real
library runs on top of them — with evidence rather than intent: every row
below is the output of a script anyone can re-run.

Recorded **2026-08-01**, against simloop 0.2.0 (unreleased) on Python 3.12.

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
happy paths only: no TLS, no retries, no reconnection, no concurrency beyond
what the probe itself starts.

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
| aiohttp (client) | 3.14.3 | works: ClientSession GET returned 'hello from the simulation' | ClientSession GET at a sim host answered by a raw stream server. |
| httpx | 0.28.1 | works: AsyncClient GET returned 'hello from the simulation' | AsyncClient GET at a sim host answered by a raw stream server. |

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

Both client probes make one request against a responder that sends
`Connection: close`, so neither row says anything about connection reuse.
The piece a pool depends on is the descriptor `fileno()` returns: httpcore
polls it to decide whether a pooled connection has died, and the
simulation backs it with a parked descriptor the transport owns, which
stays unreadable while the peer is alive and becomes readable once the
peer's EOF arrives; a reset or a teardown closes it and `fileno()` returns
`-1`, which the same poll reads as dead just as well. That contract is
pinned by the test suite, not by these rows.

Both client rows are `http://` only, and the two stacks stop differently
on `https://`. aiohttp asks for TLS through `create_connection(ssl=...)`,
which fences:

```
simloop does not simulate 'create_connection(ssl=...)'; see docs/supported-api.md for the supported asyncio subset
```

httpx reaches no fence. httpcore wraps the byte stream with anyio's
`TLSStream`, which drives an `ssl` memory BIO inside the process and
sends the handshake as ordinary bytes over the simulated connection, so
`loop.start_tls` is never called and nothing stops the attempt. Where it
ends is up to whatever is listening: aimed at the plaintext responder
these probes use, the handshake goes unanswered and the request dies of
httpx's own `ConnectTimeout`. That timeout is a one-off measurement
rather than a row — no probe on this page requests `https://`.

## Not tested

- **asyncpg**: reaching its first fence needs a live PostgreSQL server to
  connect to, which no probe can provide; it is untested rather than
  fenced-or-not.
- **TLS anywhere**: no probe on this page requests `https://` or `wss://`.
  simloop fences `start_tls` and `create_connection(ssl=...)`, but a stack
  that runs its handshake in memory reaches neither — it reaches a simulated
  network with nothing on it that speaks TLS unless the test puts it there.
- Anything that reaches outside the loop by design — threads, subprocesses,
  signals, real DNS. Those are fences, listed in
  [docs/supported-api.md](supported-api.md), not compatibility questions.
  Executors left this list: `run_in_executor` now runs the function inline,
  which the same page describes; `anyio.to_thread` stays out because its
  worker threads are real ones the loop never sees.
