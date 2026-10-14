"""Check the built distributions in dist/.

* exactly one wheel and one sdist, both with the version in ``_version.py``;
* the wheel contains only the ``cosmosdb_datasource`` package and its ``.dist-info``;
* the sdist contains only the package sources, README, LICENSE and pyproject.toml;
* the core metadata contains no e-mail addresses.

Usage: python .github/scripts/check_dist.py [--expect-version X.Y.Z] [DIST_DIR]
"""

from __future__ import annotations

import argparse
import re
import sys
import tarfile
import zipfile
from email.parser import HeaderParser
from pathlib import Path

PACKAGE = "cosmosdb_datasource"
DIST_NAME = "cosmosdb_spark_datasource"
ROOT = Path(__file__).resolve().parents[2]
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+")
SDIST_ALLOWED = re.compile(rf"^(PKG-INFO|README\.md|pyproject\.toml|LICENSE[^/]*|src(/{PACKAGE}(/.*)?)?)$")


def source_version() -> str:
    text = (ROOT / "src" / PACKAGE / "_version.py").read_text()
    match = re.search(r'^__version__ = "([^"]+)"$', text, re.MULTILINE)
    if not match:
        raise SystemExit("could not read __version__ from _version.py")
    return match.group(1)


def check_metadata(raw: str, where: str, version: str) -> list[str]:
    errors = []
    headers = HeaderParser().parsestr(raw)
    if headers["Name"] != DIST_NAME.replace("_", "-"):
        errors.append(f"{where}: Name is {headers['Name']!r}")
    if headers["Version"] != version:
        errors.append(f"{where}: Version is {headers['Version']!r}, expected {version!r}")
    for field in ("Author-email", "Maintainer-email"):
        if headers[field]:
            errors.append(f"{where}: {field} is set")
    if EMAIL.search(raw):
        errors.append(f"{where}: contains an e-mail address")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dist", nargs="?", default="dist", type=Path)
    parser.add_argument("--expect-version", help="fail unless the package version equals this value")
    args = parser.parse_args()

    version = source_version()
    errors: list[str] = []
    if args.expect_version is not None and args.expect_version != version:
        errors.append(f"package version {version!r} does not match the expected version {args.expect_version!r}")

    wheels = sorted(args.dist.glob("*.whl"))
    sdists = sorted(args.dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        print(f"expected one wheel and one sdist in {args.dist}, found {wheels + sdists}", file=sys.stderr)
        return 1
    wheel, sdist = wheels[0], sdists[0]
    if wheel.name != f"{DIST_NAME}-{version}-py3-none-any.whl":
        errors.append(f"unexpected wheel name {wheel.name}")
    if sdist.name != f"{DIST_NAME}-{version}.tar.gz":
        errors.append(f"unexpected sdist name {sdist.name}")

    dist_info = f"{DIST_NAME}-{version}.dist-info/"
    with zipfile.ZipFile(wheel) as zf:
        names = zf.namelist()
        stray = [n for n in names if not n.startswith((f"{PACKAGE}/", dist_info))]
        errors += [f"wheel: unexpected file {n}" for n in stray]
        for required in (f"{PACKAGE}/__init__.py", f"{PACKAGE}/py.typed", f"{dist_info}METADATA"):
            if required not in names:
                errors.append(f"wheel: missing {required}")
        if f"{dist_info}METADATA" in names:
            errors += check_metadata(zf.read(f"{dist_info}METADATA").decode(), "wheel METADATA", version)

    top = f"{DIST_NAME}-{version}"
    with tarfile.open(sdist) as tf:
        members = [m.name for m in tf.getmembers()]
        for name in members:
            if name == top:
                continue
            rel = name.removeprefix(f"{top}/")
            if rel == name or not SDIST_ALLOWED.match(rel):
                errors.append(f"sdist: unexpected file {name}")
        pkg_info = tf.extractfile(f"{top}/PKG-INFO")
        if pkg_info is None:
            errors.append("sdist: missing PKG-INFO")
        else:
            errors += check_metadata(pkg_info.read().decode(), "sdist PKG-INFO", version)

    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if not errors:
        print(f"OK: {wheel.name} ({len(names)} entries), {sdist.name} ({len(members)} entries), version {version}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
