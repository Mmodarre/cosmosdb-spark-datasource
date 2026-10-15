"""The process-wide client cache and the settings of the clients it creates."""

from __future__ import annotations

import time
import types
from typing import Any

import pytest
from azure.cosmos.cosmos_client import _build_connection_policy

from cosmosdb_datasource import client
from cosmosdb_datasource.auth import SecretToken
from cosmosdb_datasource.errors import CosmosDataSourceError
from cosmosdb_datasource.options import parse_options

BASE = {"endpoint": "https://acct.documents.azure.com:443/", "database": "db", "container": "c"}
KEY = "c2VjcmV0LWFjY291bnQta2V5LXZhbHVl=="
SP = {**BASE, "auth_type": "service_principal", "tenant_id": "t", "client_id": "app"}


class StubClient:
    created: list[StubClient] = []

    def __init__(self, endpoint: str, credential: Any, **kwargs: Any) -> None:
        self.endpoint, self.credential, self.kwargs = endpoint, credential, kwargs
        self.closed = False
        StubClient.created.append(self)

    def get_database_client(self, database: str) -> Any:
        return types.SimpleNamespace(get_container_client=lambda container: types.SimpleNamespace(name=container))

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def stub_client(monkeypatch: pytest.MonkeyPatch) -> type[StubClient]:
    import azure.cosmos

    monkeypatch.setattr(azure.cosmos, "CosmosClient", StubClient)
    StubClient.created = []
    client.close_all()
    yield StubClient
    client.close_all()


def test_timeouts_are_set_in_the_connection_policy() -> None:
    opts = parse_options({**BASE, "account_key": KEY, "read_timeout_seconds": "5", "connection_timeout_seconds": "2.5"})
    kwargs = client._client_kwargs(opts)
    assert "read_timeout" not in kwargs and "connection_timeout" not in kwargs
    policy = _build_connection_policy(dict(kwargs))  # as CosmosClient builds it, on every supported version
    assert (policy.ReadTimeout, policy.RequestTimeout) == (5.0, 2.5)
    assert (policy.RetryOptions.MaxRetryAttemptCount, policy.RetryOptions.MaxWaitTimeInSeconds) == (9, 60.0)
    defaults = _build_connection_policy(client._client_kwargs(parse_options({**BASE, "account_key": KEY})))
    assert defaults.ReadTimeout == 65


def test_a_rotated_service_principal_secret_gets_its_own_client(stub_client) -> None:
    old = parse_options({**SP, "client_secret": "old-secret"})
    new = parse_options({**SP, "client_secret": "new-secret"})
    assert client._client_key(old) != client._client_key(new)
    assert "old-secret" not in repr(client._client_key(old))
    token = SecretToken("tkn", int(time.time()) + 3600)
    client.get_backend(old, token)
    client.get_backend(new, token)
    assert len(stub_client.created) == 2
    assert stub_client.created[1].credential._auth.client_secret == "new-secret"
    cert = parse_options({**SP, "client_certificate": "Y2VydA=="})
    assert client._client_key(cert) != client._client_key(parse_options({**SP, "client_certificate": "b3RoZXI="}))


def test_one_client_per_key_takes_the_newest_token(stub_client) -> None:
    opts = parse_options({**SP, "client_secret": "s", "preferred_regions": "West Europe"})
    first = SecretToken("one", int(time.time()) + 1800)
    later = SecretToken("two", int(time.time()) + 3600)
    backend = client.get_backend(opts, first)
    assert client.get_backend(opts, later) is backend
    (created,) = stub_client.created
    assert created.kwargs["preferred_locations"] == ["West Europe"]
    assert created.kwargs["user_agent_suffix"].startswith("cosmosdb-spark-datasource/")
    assert created.credential.get_token("scope").token == "two"
    keyed = client.get_backend(parse_options({**BASE, "account_key": KEY}))
    assert keyed is not backend and stub_client.created[1].credential == KEY


def test_a_client_that_cannot_be_created_fails_without_the_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    import azure.cosmos

    def fail(endpoint: str, credential: Any, **kwargs: Any) -> Any:
        raise ValueError(f"bad key {credential}")

    monkeypatch.setattr(azure.cosmos, "CosmosClient", fail)
    client.close_all()
    with pytest.raises(CosmosDataSourceError, match="Could not create a Cosmos DB client") as raised:
        client.get_backend(parse_options({**BASE, "account_key": KEY}))
    assert KEY not in str(raised.value)


def test_a_client_is_closed_when_no_stream_reader_holds_it(stub_client) -> None:
    opts = parse_options({**BASE, "account_key": KEY})
    other = parse_options({**BASE, "account_key": KEY, "consistency_level": "Eventual"})
    client.get_backend(opts)
    client.get_backend(other)
    first, second = client.hold(opts), client.hold(opts)
    third = client.hold(other)
    client.release(first)
    assert not any(c.closed for c in stub_client.created)  # still held by the second reader
    client.release(second)
    assert [c.closed for c in stub_client.created] == [True, False]
    assert client.get_backend(opts) is not None and len(stub_client.created) == 3  # a new client
    client.release(third)
    assert stub_client.created[1].closed


def test_stream_readers_release_their_client_when_they_stop(make_store, stub_client, monkeypatch) -> None:
    from cosmosdb_datasource import _base

    calls: list[str] = []
    monkeypatch.setattr(_base.client, "hold", lambda opts: calls.append("hold") or ("key",))
    monkeypatch.setattr(_base.client, "release", lambda key: calls.append(f"release {key}"))
    from tests.conftest import fake_options
    from tests.harness import make_source

    for extra in ({"change_feed_mode": "LatestVersion"}, {"snapshot_stream": "true"}):
        calls.clear()
        ds = make_source(fake_options(make_store(), **extra))
        reader = ds.streamReader(ds.schema())
        reader.latestOffset()
        reader.latestOffset()
        reader.stop()
        reader.stop()
        assert calls == ["hold", "release ('key',)"]
