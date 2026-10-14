"""Fail unless every test of the given modules ran and passed (none skipped) in a JUnit XML report.

Usage: python .github/scripts/check_junit.py REPORT.xml MODULE [MODULE ...]
       (MODULE as in the report's classname, e.g. tests.test_spark_local)
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET


def main(report: str, modules: list[str]) -> int:
    cases = list(ET.parse(report).getroot().iter("testcase"))
    status = 0
    for module in modules:
        selected = [c for c in cases if c.get("classname", "").startswith(module)]
        not_passed = [
            f"{c.get('name')}: {child.tag}"
            for c in selected
            for child in c
            if child.tag in ("skipped", "failure", "error")
        ]
        if not selected:
            print(f"error: no tests of {module} in {report}", file=sys.stderr)
            status = 1
        elif not_passed:
            print(f"error: {module}: " + "; ".join(not_passed), file=sys.stderr)
            status = 1
        else:
            print(f"OK: {module}: {len(selected)} test(s) passed")
    return status


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1], sys.argv[2:]))
