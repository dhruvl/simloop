"""Run every probe in this directory and print the compatibility table.

    uv run --group probes python probes/report.py

Output is the markdown table pasted into docs/compatibility.md: one row per
probe module, ordered so that the libraries expected to run on the loop come
before the client stacks expected to leave it. Nothing here is dated or
timed, so re-running against the same pinned versions prints the same table.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

HERE = Path(__file__).resolve().parent
# Run as a script, sys.path starts at probes/; the package import below needs
# the repo root instead.
if str(HERE.parent) not in sys.path:
    sys.path.insert(0, str(HERE.parent))

from probes import _runner  # noqa: E402

COLUMNS = ("Library", "Version", "Verdict", "Notes")


@dataclass(frozen=True, slots=True)
class Row:
    tier: int
    library: str
    version: str
    verdict: str
    notes: str


def modules() -> list[ModuleType]:
    """Every probe module, in a fixed order that does not depend on the disk."""
    names = sorted(path.stem for path in HERE.glob("probe_*.py"))
    return [importlib.import_module(f"probes.{name}") for name in names]


def _version(distribution: str | None) -> str:
    if distribution is None:
        return "n/a"
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return "not installed"


def collect() -> list[Row]:
    rows = [
        Row(
            tier=module.TIER,
            library=module.LIBRARY,
            version=_version(module.DISTRIBUTION),
            verdict=_runner.run(module.probe),
            notes=module.NOTES,
        )
        for module in modules()
    ]
    return sorted(rows, key=lambda row: (row.tier, row.library))


def table(rows: list[Row]) -> str:
    def cell(text: str) -> str:
        return text.replace("|", r"\|")

    lines = [
        "| " + " | ".join(COLUMNS) + " |",
        "|" + "---|" * len(COLUMNS),
    ]
    lines += [
        f"| {cell(row.library)} | {cell(row.version)} | "
        f"{cell(row.verdict)} | {cell(row.notes)} |"
        for row in rows
    ]
    return "\n".join(lines)


def main() -> None:
    print(table(collect()))


if __name__ == "__main__":
    main()
