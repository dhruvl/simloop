"""Give the site the repository's own CHANGELOG.md, without copying it.

The changelog is a root file on purpose: it is what a reader lands on from
PyPI or from the repository, and a second copy under docs/ would be a second
thing to remember. So the build reads that one file and hands MkDocs a page
made from it. The only edit is to its links: they are written relative to the
repository root, where compatibility.md sits under docs/, and on the site
every page is a sibling.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from mkdocs.structure.files import File, Files

REPO_ROOT = Path(__file__).resolve().parent.parent
CHANGELOG = REPO_ROOT / "CHANGELOG.md"


def on_files(files: Files, config: Any) -> Files:
    markdown = CHANGELOG.read_text(encoding="utf-8").replace("](docs/", "](")
    files.append(File.generated(config, "changelog.md", content=markdown))
    return files
