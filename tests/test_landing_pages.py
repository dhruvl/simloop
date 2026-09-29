"""The examples on the front pages, run as written.

README.md and docs/index.md each show a test that fails and the report it
fails with. This file reads both straight out of the page, runs the test in
a child pytest, and requires the report to say what the page says: every line
shown, in order, with `...` standing for lines left out. The README's first,
passing example is run too. A page edit that
changes the code, the seed, or the output without changing the others fails
here rather than on a reader's machine.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

# A shown line and a printed line are compared after stripping pytest's `E`
# marker and surrounding whitespace, so indentation and the trailing blanks
# of an empty host field do not count.
_E_MARKER = re.compile(r"^E(?: |$)")


@pytest.fixture(autouse=True)
def _utf8_child_output(monkeypatch: pytest.MonkeyPatch) -> None:
    # Same reason as in test_pytest_plugin.py: the report can carry non-ASCII
    # and the child's stdio must round-trip it on Windows.
    monkeypatch.setenv("PYTHONIOENCODING", "utf-8")


def _fences(page: str) -> list[tuple[str, str]]:
    """Every fenced block on a page, as (info string, body)."""
    text = (_ROOT / page).read_text(encoding="utf-8")
    return re.findall(r"^```(\w*)\n(.*?)^```$", text, flags=re.M | re.S)


def _example(page: str, test_name: str) -> tuple[str, str]:
    """The python block defining `test_name`, and the next plain block after it."""
    blocks = _fences(page)
    for i, (info, body) in enumerate(blocks):
        if info == "python" and f"def {test_name}(" in body:
            for later_info, later_body in blocks[i + 1 :]:
                if later_info == "":
                    return body, later_body
    raise AssertionError(f"{page} no longer shows {test_name} and its output")


def _normalize(line: str) -> str:
    return _E_MARKER.sub("", line).strip()


def _assert_shows(shown: str, printed: list[str]) -> None:
    """Each run of shown lines between `...` markers appears contiguously, in order."""
    lines = [_normalize(line) for line in printed]
    chunks: list[list[str]] = [[]]
    for line in shown.splitlines():
        if line.strip() == "...":
            chunks.append([])
        else:
            chunks[-1].append(_normalize(line))
    start = 0
    for chunk in filter(None, chunks):
        for at in range(start, len(lines) - len(chunk) + 1):
            if lines[at : at + len(chunk)] == chunk:
                start = at + len(chunk)
                break
        else:
            raise AssertionError(
                "the page shows lines the run did not print:\n"
                + "\n".join(chunk)
                + "\n\nfull output:\n"
                + "\n".join(printed)
            )


def _run(
    pytester: pytest.Pytester, path: str, source: str, *args: str
) -> list[str]:
    target = pytester.path / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source, encoding="utf-8")
    result = pytester.runpytest_subprocess(path, *args)
    result.assert_outcomes(failed=1)
    return result.outlines


def test_readme_first_test_passes(pytester: pytest.Pytester) -> None:
    blocks = [
        body
        for info, body in _fences("README.md")
        if info == "python" and "def test_virtual_time_is_free(" in body
    ]
    assert blocks, "README.md no longer shows test_virtual_time_is_free"
    target = pytester.path / "tests" / "test_ledger.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(blocks[0], encoding="utf-8")
    result = pytester.runpytest_subprocess("tests/test_ledger.py")
    result.assert_outcomes(passed=1)


def test_readme_counter_fails_as_shown(pytester: pytest.Pytester) -> None:
    source, shown = _example("README.md", "test_two_increments_both_count")
    _assert_shows(shown, _run(pytester, "tests/test_counter.py", source))


def test_readme_shrink_minimizes_as_shown(pytester: pytest.Pytester) -> None:
    source, shown = _example("README.md", "test_the_audit_sees_every_deposit")
    # The page shows this one without its imports; the counter example above
    # it carries them.
    source = "import asyncio\nfrom simloop import sim_test\n\n\n" + source
    printed = _run(pytester, "tests/test_audit.py", source, "--simloop-shrink")
    _assert_shows(shown, printed)


def test_docs_index_allocator_fails_as_shown(pytester: pytest.Pytester) -> None:
    source, shown = _example("docs/index.md", "test_every_client_gets_its_own_id")
    _assert_shows(shown, _run(pytester, "tests/test_ids.py", source))
