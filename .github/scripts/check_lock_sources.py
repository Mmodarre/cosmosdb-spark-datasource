"""Fail if uv.lock takes a package from anywhere other than PyPI.

``uv lock`` writes the URLs of the index it resolved against, so a lockfile written behind a private
mirror (a default index in ``uv.toml``) holds URLs that CI and users cannot reach. Every package must
come from the index ``https://pypi.org/simple`` with files on ``https://files.pythonhosted.org/``; only
the project itself may have a local source.

Usage: python .github/scripts/check_lock_sources.py [uv.lock]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import tomllib

INDEX = "https://pypi.org/simple"
FILES = "https://files.pythonhosted.org/"
_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\"'\s]*")


def problems(text: str) -> list[str]:
    found = []
    for package in tomllib.loads(text).get("package", []):
        name = package.get("name", "?")
        source = package.get("source", {})
        if source.get("registry") is not None:
            if source["registry"] != INDEX:
                found.append(f"{name}: index {source['registry']}")
        elif not (source.get("editable") == "." or source.get("virtual") == "."):
            found.append(f"{name}: source {source}")
        files = [package.get("sdist", {}), *package.get("wheels", [])]
        found.extend(f"{name}: file {f['url']}" for f in files if "url" in f and not f["url"].startswith(FILES))
    found.extend(f"URL {url}" for url in _URL.findall(text) if not (url == INDEX or url.startswith(FILES)))
    return sorted(set(found))


def main() -> int:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "uv.lock")
    found = problems(path.read_text(encoding="utf-8"))
    for problem in found:
        print(f"::error file={path}::{problem}")
    if found:
        print(
            f"{path} takes {len(found)} package source(s) from outside PyPI. Lock against "
            f"{INDEX}, or rewrite the URLs of a mirror to {INDEX} and {FILES} (the files and hashes are the same).",
            file=sys.stderr,
        )
        return 1
    print(f"OK: every package in {path} comes from {INDEX}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
