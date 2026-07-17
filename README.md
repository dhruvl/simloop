# simloop

Deterministic simulation testing for Python asyncio: seeded scheduling,
virtual time, and a simulated network with fault injection. Any failure
simloop finds replays exactly from a seed.

Rust has [madsim](https://github.com/madsim-rs/madsim) and
[turmoil](https://github.com/tokio-rs/turmoil); FoundationDB and
TigerBeetle made the technique famous. simloop brings it to asyncio:
your real, unmodified networking code runs on a simulated loop where
time is virtual, every scheduling decision is seeded, and the network
loses, delays, duplicates, and partitions traffic on command.

## Install

```
pip install simloop
```

Python 3.12+. No runtime dependencies. The pytest plugin ships in the
same package and activates automatically.

## Find a bug, then replay it

```python
import asyncio
from simloop import sim_test


@sim_test(seeds=200)
async def test_replies_survive_a_lossy_network():
    loop = asyncio.get_running_loop()
    loop.net.set_defaults(latency=(0.001, 0.050))

    async def serve():
        async def handle(reader, writer):
            writer.write((await reader.readline()).upper())
            writer.close()

        server = await asyncio.start_server(handle, port=8080)
        async with server:
            await server.serve_forever()

    loop.net.host("server").create_task(serve())
    await asyncio.sleep(1.0)

    async with asyncio.timeout(30.0):
        reader, writer = await asyncio.open_connection("server", 8080)
        writer.write(b"hello\n")
        assert await reader.readline() == b"HELLO\n"
```

`@sim_test(seeds=200)` runs the test under 200 seeds, each on a fresh
simulated loop, and stops at the first failure:

```
simloop: failed at seed 41 (41 seeds passed first)
replay: pytest 'tests/test_echo.py::test_replies_survive_a_lossy_network' --simloop-replay=41

last 20 trace events:
  [t=1.0312] net      seq=812  send driver>server
  ...
pending tasks by host:
  server  Task 'Task-2'  awaiting serve_forever  at ...
```

The replay command reproduces the failure exactly — same scheduling
decisions, same fault decisions, same trace. In CI, crank the search
without touching code:

```
pytest --simloop-seeds=1000
```

## What the simulation gives you

- **Seeded scheduling** — the ready queue's execution order comes from a
  per-run PRNG; a seed pins the entire interleaving.
- **Virtual time** — `asyncio.sleep(300)` costs nothing; timeouts fire
  in simulated seconds.
- **A simulated network** — `open_connection` / `start_server` /
  datagram endpoints run over an in-memory packet core with per-link
  latency, drop, and duplication, plus partitions and host crashes:

```python
net = loop.net
net.set_defaults(latency=(0.001, 0.010), drop=0.05)
net.partition({"node1"}, {"node2", "node3"})   # silent blackhole
loop.call_later(5.0, net.heal)                  # heals in virtual time
net.crash("node2")                              # no reset, just silence
```

- **Replayable traces** — every scheduling and fault decision lands in
  an append-only trace whose hash proves a replay is exact.

## Honest limits

Code that goes through the event-loop API is supported; code that
bypasses it is fenced: threads and executors, raw sockets, subprocesses,
signals, TLS, and `getaddrinfo` raise `SimulationFenceError` rather than
silently breaking determinism. Write-side flow control is not simulated.
The full contract is in [docs/supported-api.md](docs/supported-api.md).

## License

MIT
