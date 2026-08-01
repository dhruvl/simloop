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

## The trace

Every scheduling decision and every network verdict is appended to a trace
whose SHA-256 is what proves a replay was exact. `simloop.TraceEvent` is a
`NamedTuple`, so an event compares and unpacks as `(kind, when, seq, label,
host)`:

| field | what it holds |
|---|---|
| `kind` | `schedule`, `run`, `cancel`, `advance` or `net` |
| `when` | virtual time, always on the true clock — `set_clock` changes what a host reads, never what a trace records |
| `seq` | the scheduled handle's number; the packet's uid on a `net` event; `-1` on a clock advance |
| `label` | the qualified callback name, or a network verb and the link it crossed (`send a>b`) |
| `host` | the machine the event belongs to, or `""` for the simulation itself |

A `schedule` event names the host that *asked* for the callback, while `run`
and `cancel` name the host the callback belongs to. The difference is the
point: a wakeup that crosses machines is a `schedule` on one host and a `run`
on another. An empty host means the event belongs to the simulation rather
than to any machine — a clock advance, which is global; the network's own
delivery step, which happens on the wire between two machines rather than on
either of them; and every `net` event, whose label already says which
machines it concerns.

`send` is a packet going onto the wire and `deliver` is that same packet
arriving. Both carry the packet's uid in `seq`, so the two ends of one
crossing pair up. The other verbs are usually what happened instead: `drop` (a
lossy link, or a datagram meeting a partition), `hold` and `release` (a stream
packet parked by a partition, then put back on the wire when it heals), `dup`
(a duplicate on its way as well, under the same uid), and `lost` (it reached a
machine that was gone, or a port with nothing bound). "Usually", because the
second kind of loss follows a `deliver` rather than replacing it: the packet
did reach the machine, and only then found nothing to take it. `crash` and
`restart` name a machine instead of a link.

Hashes are comparable within a version, not across versions: the host field
and the `deliver` events are new in 0.2.0, so every workload's trace hashes
differ from the ones 0.1.0 recorded — see the
[changelog](../CHANGELOG.md). What a hash promises is unchanged: same seed,
same code, same interpreter, same hash.

`simloop.timeline_html(events, limit=5000)` renders a trace as a
self-contained HTML page — one lane per machine plus one for the simulation,
a dot per scheduling decision, an arrow for every `send` its `deliver`
answered, and a stub for every one that never arrived. Only the last `limit`
events are drawn, and the page says so when it dropped any; `limit=None`
draws the whole run.

## Exploring schedules

`@sim_test(seeds=N)` and `simloop.explore(fn, seeds)` run a workload once per
seed on a fresh loop and stop at the first failure. Under pytest these
options override what the decorator asked for:

| option | effect |
|---|---|
| `--simloop-seeds=N` | run every `@sim_test` under seeds 0..N-1 |
| `--simloop-replay=SEED` | run every `@sim_test` at exactly this seed |
| `--simloop-jobs=N` | spread a test's seeds over N worker processes; the workload has to pickle |
| `--simloop-shrink`, `--simloop-shrink-budget=N` | minimize the failing schedule toward FIFO (experimental, costs runs) |
| `--simloop-policy=random\|pct` | how the scheduler picks the next ready callback |
| `--simloop-pct-depth=N` | ordering constraints PCT aims to hit (default 3) |
| `--simloop-timeline[=DIR]` | draw each failing seed's trace to `simloop-timeline-seed<N>.html`, in `DIR` if given and where pytest was invoked otherwise |

The timeline is written per failing seed and named in the failure report. A
page that cannot be written says so in the report rather than replacing the
failure with its own.

### Scheduling policies

`random`, the default, is one seeded uniform draw over the ready queue per
step: the schedule the seed names, and the only policy a report says nothing
about.

`pct` schedules by priority instead, after Burckhardt et al., *A Randomized
Scheduler with Probabilistic Guarantees of Finding Bugs* (ASPLOS 2010). Every
chain of work draws a distinct random priority, the highest-priority ready
entry always runs, and at `depth - 1` randomly chosen steps the running chain
is demoted below every chain still holding its first draw. What that buys is
a floor: a bug that needs `depth` scheduling constraints met in order is hit
with probability at least 1/(n · horizon^(depth-1)) on *every* run, where n
is the number of chains and the horizon is the step count the change points
are spread over. Chains are priced as they turn up — an owner nobody has seen
draws its priority on first sight, the standard adaptation for work created
while the run is going — so n is however many chains the run ended up
containing, not a count anyone knew in advance, and where tasks spawn tasks
the bound is best read per run and after the fact. Uniform draws promise
nothing at any depth.

It is not a faster search, and the repository measures its own claim. On the
planted lost-update race in `tests/test_explore.py` — two ordering
constraints in a twelve-step run, searched at the default depth — uniform
draws reached the first failing seed after 2.0 seeds on average and PCT after
92.75, a failure rate of 474 seeds per 1,000 against 21. A shallow race in a
short run is the shape a uniform draw is already ideal for. PCT is for the
depth a uniform draw is unlikely to stumble into, and what it offers there is
the bound, not a speed-up.

The rest of the honest print:

- The guarantee is per run. Nothing here says how many runs a campaign needs,
  and a lower bound on a probability is not a promise that a search finds
  anything.
- `depth` is a guess about a bug nobody has seen yet. Too low and the change
  points cannot express the interleaving; too high and they spread thinner
  over the same run.
- The horizon is measured rather than assumed: seed 0 runs once under the
  seeded schedule, and its step count times 1.5 — floored at 100, and widened
  further if `depth - 1` change points need the room — is the horizon. That
  measuring run is one extra run of the workload per exploration, unless seed
  0 is the only seed asked for: it runs the seeded schedule anyway, so there
  is nothing left to size. Seed 0 is also explored on its own account, always
  under the seeded schedule, because measuring a PCT run would measure the
  number it is supposed to produce; the report says so when the failing seed
  is that one.
- Fixing the calibration seed at 0 is what keeps a found seed replayable:
  replaying it alone measures the same horizon, so it runs the same schedule.
- A run that ends before the horizon passes only some of its change points; a
  run that overruns spends all of them in its first `horizon` steps and
  finishes at fixed priorities. Both are legal schedules, and neither is the
  one the bound describes.
- Callbacks no task owns — timers, protocol callbacks — are each a chain of
  their own that draws once and runs once. A change point landing on one
  spends a demotion on a chain with no future, so read the bound as a
  statement about runs where task chains do the deciding.
- PCT explores sequentially. The horizon is measured in the process that
  explores and never reaches a worker, so `--simloop-policy=pct` together
  with `--simloop-jobs` above 1 is refused rather than quietly ignored.
