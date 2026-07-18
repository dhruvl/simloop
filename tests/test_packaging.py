"""The published artifacts must contain the package and nothing else."""

import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest

from simloop import __version__

REPO_ROOT = Path(__file__).resolve().parent.parent

SDIST_ALLOWED_FILES = {"PKG-INFO", "pyproject.toml", "README.md", "LICENSE", ".gitignore"}
PACKAGE_PREFIX = "src/simloop/"


def _build(tmp_path: Path, kind: str) -> Path:
    subprocess.run(
        ["uv", "build", f"--{kind}", "--out-dir", str(tmp_path)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    pattern = "*.tar.gz" if kind == "sdist" else "*.whl"
    (artifact,) = tmp_path.glob(pattern)
    return artifact


@pytest.mark.slow
def test_sdist_ships_only_the_package(tmp_path: Path) -> None:
    sdist = _build(tmp_path, "sdist")
    with tarfile.open(sdist) as tar:
        names = [m.name for m in tar.getmembers() if not m.isdir()]
    # every member is "<name>-<version>/<relpath>"
    relpaths = [name.split("/", 1)[1] for name in names]
    unexpected = [
        p
        for p in relpaths
        if p not in SDIST_ALLOWED_FILES and not p.startswith(PACKAGE_PREFIX)
    ]
    assert unexpected == []
    assert "src/simloop/__init__.py" in relpaths


@pytest.mark.slow
def test_wheel_ships_only_the_package(tmp_path: Path) -> None:
    wheel = _build(tmp_path, "wheel")
    with zipfile.ZipFile(wheel) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
    dist_info_prefix = f"simloop-{__version__}.dist-info/"
    unexpected = [
        n
        for n in names
        if not n.startswith("simloop/") and not n.startswith(dist_info_prefix)
    ]
    assert unexpected == []
    assert "simloop/__init__.py" in names
