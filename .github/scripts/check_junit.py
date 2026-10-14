"""Fail unless the tests of the given modules passed in a JUnit XML report.

A test may only be skipped if its skip message starts with one of the ``--allow-skip`` prefixes,
and at least one test per module must have passed.

Usage: python .github/scripts/check_junit.py REPORT.xml MODULE [MODULE ...] [--allow-skip PREFIX ...]
       (MODULE as in the report's classname, e.g. tests.test_spark_local)
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    parser.add_argument("modules", nargs="+")
    parser.add_argument("--allow-skip", action="append", default=[], metavar="PREFIX")
    args = parser.parse_args()

    cases = list(ET.parse(args.report).getroot().iter("testcase"))
    allowed = tuple(args.allow_skip)
    status = 0
    for module in args.modules:
        passed, skipped, problems = 0, 0, []
        for case in cases:
            if not case.get("classname", "").startswith(module):
                continue
            outcome = next((child for child in case if child.tag in ("skipped", "failure", "error")), None)
            if outcome is None:
                passed += 1
            elif outcome.tag == "skipped" and allowed and outcome.get("message", "").startswith(allowed):
                skipped += 1
            else:
                problems.append(f"{case.get('name')}: {outcome.tag} ({outcome.get('message', '')[:200]})")
        if problems or not passed:
            print(f"error: {module}: {passed} passed; " + ("; ".join(problems) or "no test passed"), file=sys.stderr)
            status = 1
        else:
            print(f"OK: {module}: {passed} passed, {skipped} skipped as allowed")
    return status


if __name__ == "__main__":
    sys.exit(main())
