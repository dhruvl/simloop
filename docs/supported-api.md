# Supported asyncio subset

simloop runs real, unmodified asyncio code on a simulated event loop. This
page is the honest contract: what is simulated, what works unchanged on top,
and what is fenced off because it would escape the simulation and break
determinism. Fenced APIs raise `SimulationFenceError` (a subclass of
`NotImplementedError`) naming the offending call.

## Simulated by SimLoop

| API | Behavior under simulation |
|---|---|
| `loop.call_soon` / `call_later` / `call_at` | Seeded ready-queue ordering; `(deadline, seq)` timer tie-break |
| `loop.time()` / `asyncio.sleep` | Virtual clock starting at 0.0; never waits on wall time. What `loop.time()` *reads* is shifted by whatever offset the calling task's host is configured with; durations such as `asyncio.sleep` cost the same everywhere |
| `loop.create_task` / `asyncio.create_task` | Real stdlib `Task`s, including custom task factories |
| `loop.create_future` | Real stdlib `Future`s |
| `run_until_complete` / `run_forever` / `stop` / `close` | Deadlock detection: raises `SimulationDeadlockError` when nothing can run |
| Handle / timer cancellation | Honored and recorded in the scheduling trace |
| Exception handling | Unhandled failures fail the run at `run_until_complete`; `set_exception_handler` supported |
| `sim.random` / `sim.uuid4` / `sim.time` | Seed-derived streams inside a run; stdlib fallback outside |

## Works unchanged on top of the loop

`asyncio.Queue`, `asyncio.Event`, `asyncio.Lock`, `asyncio.Semaphore`,
`asyncio.gather`, `asyncio.TaskGroup`, `asyncio.timeout`, `asyncio.wait_for`
— everything built purely on futures, tasks and timers. Timeouts fire in
virtual time. Each of these claims is exercised by the test suite.

## Simulated network

Networking runs over an in-memory packet layer with seeded fault injection.
Hosts are named machines; a task started via `loop.net.host("name").create_task(...)`
— and every task it spawns — belongs to that host. Tasks never started under
a host belong to an implicit `driver` host.

| API | Behavior under simulation |
|---|---|
| `loop.create_connection` / `create_server`, `asyncio.open_connection` / `start_server` | Real transports and protocols over reliable, ordered in-memory streams; connecting costs one round trip of virtual latency; connecting to a closed port raises `ConnectionRefusedError` |
| `loop.create_datagram_endpoint` | Unreliable messaging: per-link drop, duplication, and latency apply per datagram |
| `loop.net.set_defaults` / `set_link` | Per-direction latency ranges, drop and duplication probabilities, drawn from a seed-derived stream |
| `loop.net.partition` / `heal` | Silent blackhole: datagrams are lost, stream traffic is held and resumes intact after healing; nothing errors — only your own timeouts fire |
| `loop.net.crash` | A host's tasks are cancelled and it goes silent; no reset is sent — peers cannot tell a crash from a partition |
| `loop.net.restart` / `host.restart()` | The counterpart to a crash: the host comes back as a fresh incarnation. Liveness is all that is revived — the old tasks stay cancelled, its listeners and binds are gone, and the caller boots whatever should run on the machine again, the same way it booted it the first time. Cancellation is requested at crash and lands on the next scheduler step, so a restart in the same step can briefly coexist with a dying task that swallows `CancelledError`. A packet is checked against liveness when it arrives, so traffic due during the dead window is lost; a packet that was already in flight and lands after the machine is back is delivered, and finds a host that no longer holds the old incarnation's connections. Peers still learn about the outage only from their own timeouts |
| `host.disk` | Storage that survives the crash: a `MutableMapping` per host, where state a real process would fsync belongs. Writes are atomic at assignment; there is no partial-write model. Values are stored as given, so mutating a stored object afterwards is the caller's own aliasing, exactly as with a cache in front of a real disk |
| `loop.net.set_clock` / `clock_offset` | Per-host clock skew, in seconds. The offset changes what that host's tasks *read*: `loop.time()` (and `sim.time()` with it) returns true time plus the offset, and a deadline handed to `call_at` is interpreted on the calling task's clock. Durations are immune — `asyncio.sleep`, `asyncio.timeout`, `wait_for` and `call_later` cost the same everywhere, which is exactly what a wrong wall clock does to a real machine. By default the driver and unconfigured hosts read true time; the driver can be given an offset too. Trace timestamps stay on the true clock, so skew never perturbs scheduling and traces from skewed runs stay comparable |
| `transport.abort()` | Peer gets `connection_lost(ConnectionResetError)` |
| `loop.getaddrinfo` | Resolves against the host table, never DNS: a registered host name, its synthetic address, or a loopback-shaped name (`None`, `""`, `localhost`, `127.0.0.1`, `0.0.0.0`) meaning the calling task's own host. Returns stdlib-shaped rows — `(AF_INET, SOCK_STREAM, IPPROTO_TCP, "", (address, port))` and the `SOCK_DGRAM` / `IPPROTO_UDP` row — filtered by `family`, `type` and `proto`. Ports are numeric (`int`, a digit string, or `None` for 0); resolver `flags` have nothing to vary |
| `loop.getnameinfo` | Reverse lookup: a synthetic address maps back to its host name, and a host name (what `get_extra_info("peername")` reports) maps to itself. `NI_NUMERICHOST` returns the address instead; services are always numeric |
| `loop.net.address` / `hostname` | The mapping itself: every registered host owns one synthetic IPv4 address from `10.7.0.0/16` — `10.7.0.1`, `10.7.0.2`, ... handed out in registration order, starting with the implicit `driver` host |
| `loop.sock_connect` + `create_connection(sock=...)` | The two-call connect sequence aiohttp's connector performs. On an `AF_INET` stream socket, `sock_connect` places the target in the host table and records it against the socket — no packet moves and no virtual time passes, and an address that is not a `(host, port)` pair, or a target the host table cannot place, raises `OSError` there. `create_connection(sock=...)` then claims that recorded address: it closes the real descriptor (the loop takes ownership, as the stdlib does) and opens a simulated connection, paying the same one round trip a direct connect pays, so a closed port raises `ConnectionRefusedError` from this call rather than the first. Passing a socket that no `sock_connect` on this loop parked raises `OSError`; passing `host`/`port` alongside `sock` raises `ValueError`. Binding a source address first — `TCPConnector(local_addr=...)` — is not supported: the connector binds the real socket before the simulation is consulted, and a synthetic address belongs to no real interface, so the bind fails outside the loop |
| `transport.get_extra_info("socket")` (streams) | A stand-in object, not a network socket: `family` / `type` / `proto` report `AF_INET` / `SOCK_STREAM` / `IPPROTO_TCP`, `getsockname()` and `getpeername()` return `(synthetic address, port)` tuples for the two ends, and `setsockopt`, `shutdown` and `close` are accepted and do nothing. `fileno()` returns a parked descriptor the transport owns, created on first call, that polls unreadable while the peer is alive and readable once the peer's EOF arrives — which is how a pool that checks readability sees a dead connection. A reset, a local close or the end of the run closes the descriptor instead, and `fileno()` reports `-1` from then on; a readability poll reads that as dead too. The number itself is assigned by the operating system, so it is not reproducible across runs and is outside the determinism guarantee — nothing in a trace depends on it. No bytes ever cross it: `recv`, `send` and anything else not listed above raise `AttributeError` rather than pretend. Datagram transports still report `None` |

Names and their synthetic addresses are interchangeable wherever an endpoint
is accepted, so a client can resolve a name and connect to what it got back.
Anything the host table cannot answer — an unknown name, an unassigned
address, an `AF_INET6` or `SOCK_RAW` request, a service name — raises
`socket.gaierror(EAI_NONAME)`. Resolution is a pure lookup, not a scheduling
decision: it never blocks and records no trace event.

Partitions, crashes and reboots are all silent, so time is the only
failure detector the code under test has — and `set_clock` lets that
detector be wrong. A lease holder whose clock runs fast and an issuer
whose clock runs slow disagree about when the lease expired, which is the
disagreement leases exist to survive, and a test can produce it on
purpose. The converse is worth knowing before reaching for it: a protocol
that puts durations on the wire rather than timestamps is immune to skew
by construction — in `examples/jobqueue/` only the broker reads a clock,
so skewing a worker changes nothing the cluster decides. Clock faults
reach only code that compares timestamps taken on different machines.

Limitations, stated honestly: write-side flow control is not simulated
(`drain()` never blocks, write buffers are unbounded, the peer cannot pause
your writes); there is no retransmission or congestion model — streams are
reliable by construction; and addressing is IPv4-only and entirely synthetic
— there are no routes, no netmasks, and no service-name database.

## Fenced

Anything that reaches outside the simulation raises `SimulationFenceError`:
executors and threads (`run_in_executor`, `call_soon_threadsafe`), signal
handlers, subprocesses, file-descriptor callbacks (`add_reader` /
`add_writer`), loop-level TLS upgrades (`start_tls`,
`create_connection(ssl=...)`), `sendfile`, and pipes. TLS a library
performs in memory reaches no loop API and so reaches no fence; what that
means in practice is in [docs/compatibility.md](compatibility.md).

The socket calls are fenced with one exception. `sock_connect` on an
`AF_INET` stream socket is simulated — it is how client stacks reach the
network, and the table above says what it does. Every other socket kind
fences there — datagram, raw and IPv6 alike — and the rest of the family
stays fenced outright: `sock_recv`, `sock_recv_into`, `sock_sendall`,
`sock_sendto`, `sock_recvfrom`, `sock_recvfrom_into`, `sock_accept` and
`sock_sendfile`. Client stacks do not need them: the socket they connect
is upgraded into a transport instead of being read and written directly.
