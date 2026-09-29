# Contributing

Bug reports, fixes and new simulated APIs are all welcome. For anything
larger than a fix, open an issue first so the design can be agreed before the
code is written; [docs/design.md](docs/design.md) explains the choices a
change will be measured against.

## Reporting a bug

The most useful report is a failing test and the command that replays it.
If simloop itself misbehaves (a replay that does not replay, a trace hash
that differs between two runs of one seed, a fence that should not fire),
include the simloop version, the Python version and the OS, and the smallest
workload you have that shows it.

## Working on the code

The project uses [uv](https://docs.astral.sh/uv/):

```
uv sync
uv run pytest                          # fast suite
uv run pytest -q -m "slow or not slow" # what CI runs, seed sweeps included
uv run mypy                            # strict
```

Documentation changes should build cleanly:

```
uv run --group docs mkdocs build --strict
```

A change is ready when all of the following hold:

- The full suite and `mypy` pass. CI runs both on Linux, macOS and Windows
  across every supported Python.
- New behavior has a test. For a new simulated API that usually means a test
  that the same seed replays with the same trace hash.
- A run that does not use the new behavior makes exactly the decisions it
  made before. If a change moves trace hashes for existing workloads, say so
  in the pull request and in `CHANGELOG.md`; it is the first thing an
  upgrade costs.
- `docs/supported-api.md` describes any new or changed contract.

## Benchmarks

If a change touches the scheduling or packet path, rerun the relevant script
in `benchmarks/` against `main` in the same sitting and put both numbers in
the pull request. [benchmarks/README.md](benchmarks/README.md) explains why
numbers measured on different days are not comparable.
