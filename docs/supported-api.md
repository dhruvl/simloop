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

## Planned: simulated network

`create_connection`, `create_server`, `create_datagram_endpoint` and the
streams API, backed by in-memory transports with seeded fault injection
(latency, drops, duplication, reordering, partitions).

## Fenced

Anything that reaches outside the simulation raises `SimulationFenceError`:
executors and threads (`run_in_executor`, `call_soon_threadsafe`), signal
handlers, subprocesses, raw sockets (`sock_*`), file-descriptor callbacks
(`add_reader` / `add_writer`), name resolution (`getaddrinfo` /
`getnameinfo`), TLS upgrades, `sendfile`, and pipes.
