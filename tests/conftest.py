from __future__ import annotations

import itertools
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make ``tests.*`` importable

from cosmosdb_datasource import retry
from cosmosdb_datasource.options import TEST_HOOKS_ENV
from tests.fake_cosmos import FakeCosmosStore

ENDPOINT = "https://fake-account.documents.azure.com:443/"
_counter = itertools.count()


@pytest.fixture(autouse=True)
def _test_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TEST_HOOKS_ENV, "1")


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(retry, "sleep", slept.append)
    return slept


@pytest.fixture
def make_store():
    def factory(**kwargs) -> FakeCosmosStore:
        name = kwargs.pop("name", None) or f"c{next(_counter)}"
        return FakeCosmosStore(name, **kwargs).register()

    return factory


def java_major() -> int | None:
    java = shutil.which("java")
    if not java:
        return None
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30, check=False)
    except Exception:
        return None
    match = re.search(r'version "(\d+)', out.stderr + out.stdout)
    return int(match.group(1)) if match else None


def fake_options(store: FakeCosmosStore, **extra: str) -> dict[str, str]:
    opts = {
        "endpoint": ENDPOINT,
        "database": "db",
        "container": store.name,
        "auth_type": "key",
        "account_key": "dGVzdC1rZXk=",
        "_test_backend": "tests.fake_cosmos:backend_factory",
    }
    opts.update({k: str(v) for k, v in extra.items()})
    return opts


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    os.environ.setdefault(TEST_HOOKS_ENV, "1")
