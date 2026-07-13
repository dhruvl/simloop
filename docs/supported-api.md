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
| `loop.time()` / `asyncio.sleep` | Virtual clock starting at 0.0; never waits on wall time |
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
| `transport.abort()` | Peer gets `connection_lost(ConnectionResetError)` |

Limitations, stated honestly: write-side flow control is not simulated
(`drain()` never blocks, write buffers are unbounded, the peer cannot pause
your writes); there is no retransmission or congestion model — streams are
reliable by construction; addresses are host names, not IPs, and
`getaddrinfo` stays fenced.

## Fenced

Anything that reaches outside the simulation raises `SimulationFenceError`:
executors and threads (`run_in_executor`, `call_soon_threadsafe`), signal
handlers, subprocesses, raw sockets (`sock_*`), file-descriptor callbacks
(`add_reader` / `add_writer`), name resolution (`getaddrinfo` /
`getnameinfo`), TLS upgrades, `sendfile`, and pipes.
