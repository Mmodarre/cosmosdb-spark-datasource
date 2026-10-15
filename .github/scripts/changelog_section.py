"""Print the CHANGELOG.md section of a version (Keep a Changelog format).

For a pre-release (e.g. 1.1.0rc1) without its own section, the ``[Unreleased]`` section is used.
Fails if the section is missing or empty.

Usage: python .github/scripts/changelog_section.py VERSION [CHANGELOG.md]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

HEADING = re.compile(r"^## \[(?P<name>[^\]]+)\]")
PRERELEASE = re.compile(r"(a|b|rc|\.dev)\d+$")


def section(text: str, name: str) -> str | None:
    lines: list[str] | None = None
    for line in text.splitlines():
        match = HEADING.match(line)
        if match:
            if lines is not None:
                break
            if match.group("name").lower() == name.lower():
                lines = []
            continue
        if lines is not None:
            if re.match(r"^\[[^\]]+\]: ", line):  # link reference definitions at the end of the file
                break
            lines.append(line)
    return "\n".join(lines).strip() if lines is not None else None


def main() -> int:
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    version = sys.argv[1]
    text = Path(sys.argv[2] if len(sys.argv) == 3 else "CHANGELOG.md").read_text(encoding="utf-8")
    body = section(text, version)
    if body is None and PRERELEASE.search(version):
        body = section(text, "Unreleased")
    if not body:
        print(f"error: CHANGELOG.md has no non-empty section for {version}", file=sys.stderr)
        return 1
    print(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
