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
| aiohttp (web.TCPSite) | 3.14.3 | fails: AttributeError: 'SimServer' object has no attribute 'sockets' | The documented AppRunner + TCPSite startup path, nothing else. |
| anyio | 4.14.2 | works: task group, memory object stream (one, two, three), anyio.sleep and move_on_after; virtual clock reached 1.75s | Asyncio backend only; nothing here touches a socket. |
| redis (RESP wire protocol) | n/a | works: PING, SET and GET round trips over one connection: ['+PONG', '+OK', '0'] | Hand-rolled RESP over sim streams; no client library, no real server. |
| websockets | 17.0.1 | fails: AttributeError: 'SimServer' object has no attribute 'sockets' | asyncio server and client on two sim hosts, ws:// only. |
| aiohttp (client) | 3.14.3 | fenced: simloop does not simulate 'sock_connect'; see docs/supported-api.md for the supported asyncio subset | ClientSession GET at a sim host answered by a raw stream server. |
| httpx | 0.28.1 | fails: ConnectError: [Errno 8] Name or service not known: b'web' | AsyncClient GET at a sim host answered by a raw stream server. |

Rows are grouped: the libraries expected to run on the loop first, then the
client stacks expected to leave it.

## Reading the rows

**anyio** needs nothing but tasks, futures and timers, so its asyncio backend
runs unchanged. That matters beyond anyio itself: it is the concurrency layer
under httpx, starlette and anything built on `anyio.to_thread`-free code.

**aiohttp's server** answers requests over the simulated network. The probe
starts it with `AppRunner` and `loop.create_server` rather than `web.TCPSite`,
because `TCPSite` does not get that far: it reads `server.sockets`, which the
simulated server does not have (there are no sockets in the simulation). That
is its own row, since `TCPSite` is what aiohttp's documentation and
`web.run_app` use.

**websockets** stops at the same attribute, in its own `serve()`. Both
libraries read `sockets` only to report the address they bound — aiohttp to
learn the port when `0` was requested, websockets to log where it is
listening. Neither probe got past that line, so neither says anything about
what the handshake or the frames would do.

**redis** has no row for a client library: every async Redis client needs a
live server to reach its first command, and a live server is exactly what a
simulation does not have. What is testable without one is the pattern those
clients are built on — a length-prefixed request/response protocol on one
long-lived connection — so the probe speaks RESP by hand against a small
server on a second sim host, and the row claims no more than that.

**aiohttp's client** leaves the simulation at its first connection attempt.
The fence, verbatim:

```
simloop does not simulate 'sock_connect'; see docs/supported-api.md for the supported asyncio subset
```

Its connector resolves the name through `loop.getaddrinfo` (which the
simulation answers), then hands the addresses to `aiohappyeyeballs`, which
opens a real socket and calls `loop.sock_connect` on it. Raw sockets are
fenced, so the simulation stops there rather than letting a real connection
out.

**httpx** never reaches a fence. It goes through httpcore and anyio, and
anyio ASCII-encodes host names before resolving them
(`idna2008_resolve` in `anyio/_core/_sockets.py`), so `loop.getaddrinfo`
receives `b'web'` rather than `'web'`. The simulated resolver only accepts
`str`, raises `socket.gaierror`, and httpx reports that as a `ConnectError`.
The failing call is the resolver, not the connection: what anyio would do
after resolving is untested here.

## Not tested

- **asyncpg**: reaching its first fence needs a live PostgreSQL server to
  connect to, which no probe can provide; it is untested rather than
  fenced-or-not.
- **TLS anywhere**: `start_tls` is fenced, so `https://` and `wss://` are out
  of scope for every probe on this page.
- Anything that reaches outside the loop by design — threads, executors,
  subprocesses, signals, real DNS. Those are fences, listed in
  [docs/supported-api.md](supported-api.md), not compatibility questions.
