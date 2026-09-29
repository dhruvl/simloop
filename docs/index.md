# simloop

Deterministic simulation testing for Python asyncio: seeded scheduling,
virtual time, and a simulated network with fault injection. A failure
simloop finds replays from its seed, on the same code and the same
interpreter; [the replay contract](supported-api.md#the-trace) says exactly
what that covers.

Your real, unmodified networking code runs on a simulated loop. Time is
virtual, so `asyncio.sleep(300)` costs nothing. Every scheduling decision
comes from a per-run PRNG, so one seed pins the entire interleaving. The
network loses, delays, duplicates and partitions traffic on command, and
machines crash, reboot and remember what they wrote down. Rust has
[madsim](https://github.com/madsim-rs/madsim) and
[turmoil](https://github.com/tokio-rs/turmoil); FoundationDB and TigerBeetle
made the technique famous. This is the asyncio one.

## Install

```
pip install simloop
```

Python 3.12+. No runtime dependencies. The pytest plugin ships in the same
package and activates automatically.

## A race, found and replayed

An id allocator reads its counter, writes the next value down, then answers,
with an `await` in the gap, the way a real one has a disk or a peer in the
middle:

```python
import asyncio

from simloop import sim_test


@sim_test(seeds=100)
async def test_every_client_gets_its_own_id():
    loop = asyncio.get_running_loop()
    loop.net.set_defaults(latency=(0.001, 0.050))
    issued = 0
    handed_out = []

    async def handle(reader, writer):
        nonlocal issued
        await reader.readline()
        current = issued
        await asyncio.sleep(0.001)  # a real allocator writes this down first
        issued = current + 1
        writer.write(b"%d\n" % current)
        writer.close()

    async def serve():
        server = await asyncio.start_server(handle, port=9000)
        async with server:
            await server.serve_forever()

    async def take():
        reader, writer = await asyncio.open_connection("ids", 9000)
        writer.write(b"take\n")
        handed_out.append(await reader.readline())
        writer.close()

    loop.net.host("ids").create_task(serve())
    await asyncio.sleep(0.5)

    first = loop.net.host("a").create_task(take())
    await asyncio.sleep(0.010)
    second = loop.net.host("b").create_task(take())
    await asyncio.gather(first, second)
    assert len(set(handed_out)) == 2
```

Three hosts, ordinary `asyncio.start_server` and `asyncio.open_connection`,
and a link whose latency is drawn per packet. The second client starts ten
simulated milliseconds after the first, so on most draws the first handler is
finished before the second one reads the counter. `@sim_test(seeds=100)` runs
the test on 100 fresh simulated loops and stops at the draw where the two
overlap:

```
simloop: failed at seed 36 (36 seeds passed first)
replay: pytest 'tests/test_ids.py::test_every_client_gets_its_own_id' --simloop-replay=36

last 20 trace events:
  [t=0.6153] run      seq=35  SimNetwork._deliver
  [t=0.6153] net      seq=10  deliver b>ids
  [t=0.6209] advance  seq=-1
  ...

runs agree for 15 events; passing then ran _set_result_unless_cancelled, failing ran SimNetwork._deliver
passing run:
  ...
  [t=0.5100] advance  seq=-1
  [t=0.5100] run      seq=5  driver  _set_result_unless_cancelled
  ...
failing run:
  ...
  [t=0.5068] advance  seq=-1
  [t=0.5068] run      seq=6  SimNetwork._deliver
  ...
```

The report names the failing seed and the command that replays it, then
diffs the failing run against the last passing seed, so the first thing the
two runs did differently is one line of output. Each trace line names the
machine whose work it was; lines that name none belong to the simulation
itself, like the clock advancing or a packet crossing the wire. The whole
search takes under half a second of wall clock.

## Where to go next

- [Quickstart](quickstart.md): from `pip install` to a replayed failure and a
  shrunk schedule, one command at a time.
- [Cookbook](cookbook.md): recipes that are known to work, because each one is
  also a test in the repository.
- [Supported API](supported-api.md), the contract: what is simulated, what
  runs unchanged, what fences, and what a trace hash promises.
- [Compatibility](compatibility.md): what aiohttp, anyio, websockets and httpx
  actually do when run under simulation.
- [Design](design.md): why the loop is written from scratch, why one seed
  feeds three RNG streams, and the alternatives those decisions beat.
- [Changelog](changelog.md): what changed, including the changes that move
  trace hashes.

The two flagship demos live in the repository:
[examples/jobqueue/](https://github.com/dhruvl/simloop/tree/main/examples/jobqueue)
is an exactly-once job scheduler tested under hundreds of seeds of partitions
and crashes, and
[examples/raft/](https://github.com/dhruvl/simloop/tree/main/examples/raft) is a
teaching-sized Raft swept under 50,000. Both publish a table of the bugs that
appear when a safeguard is removed, each one replayable from a seed.
