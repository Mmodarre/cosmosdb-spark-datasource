"""Certificate authentication, integrated cache staleness and slow-request diagnostics."""

from __future__ import annotations

import base64
import datetime
import logging
import time
from typing import Any

import pytest

from cosmosdb_datasource import auth
from cosmosdb_datasource.backend import SdkBackend
from cosmosdb_datasource.diagnostics import SlowRequestLog
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.options import parse_options
from tests.test_read_performance import FakeContainer

BASE = {"endpoint": "https://acct.documents.azure.com:443/", "database": "db", "container": "c"}


def self_signed_pem() -> bytes:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return key_pem + cert.public_bytes(serialization.Encoding.PEM)


def test_certificate_options() -> None:
    cert = base64.b64encode(b"pem").decode()
    opts = parse_options({**BASE, "tenant_id": "t", "client_id": "c", "client_certificate": cert})
    assert opts.auth.auth_type == "service_principal" and opts.auth.client_certificate == cert
    assert cert not in repr(opts) and cert in opts.secrets()
    with pytest.raises(CosmosConfigError, match="exactly one"):
        parse_options({**BASE, "auth_type": "service_principal", "tenant_id": "t", "client_id": "c"})
    with pytest.raises(CosmosConfigError, match="exactly one"):
        parse_options(
            {**BASE, "auth_type": "service_principal", "tenant_id": "t", "client_id": "c", "client_secret": "s"}
            | {"client_certificate": cert}
        )
    with pytest.raises(CosmosConfigError, match="base64"):
        parse_options({**BASE, "tenant_id": "t", "client_id": "c", "client_certificate": "not base64!"})


def test_certificate_credential_is_built_from_the_option() -> None:
    pytest.importorskip("azure.identity")
    from azure.identity import CertificateCredential

    pem = self_signed_pem()
    opts = parse_options(
        {**BASE, "tenant_id": "t", "client_id": "c", "client_certificate": base64.b64encode(pem).decode()}
    )
    credential = auth.build_token_credential(opts.auth)
    assert isinstance(credential, CertificateCredential)


def test_certificate_rotation_builds_a_new_driver_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[Any] = []

    class Credential:
        def __init__(self, **kwargs: Any) -> None:
            built.append(kwargs)

        def get_token(self, *scopes: str) -> Any:
            return type("T", (), {"token": "tok", "expires_on": int(time.time()) + 3600})()

    monkeypatch.setattr(auth, "_identity_module", lambda: type("M", (), {"CertificateCredential": Credential}))
    monkeypatch.setattr(auth, "_DRIVER_TOKENS", auth._DriverTokenCache())
    for cert in (b"one", b"one", b"two"):
        opts = parse_options(
            {**BASE, "tenant_id": "t", "client_id": "c", "client_certificate": base64.b64encode(cert).decode()}
            | {"client_certificate_password": "pw"}
        )
        auth.mint_driver_token(opts.auth, opts.endpoint)
    assert [b["certificate_data"] for b in built] == [b"one", b"two"]
    assert {b["password"] for b in built} == {"pw"}


class RecordingContainer(FakeContainer):
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        super().__init__(pages)
        self.kwargs: list[dict[str, Any]] = []

    def query_items(self, *, response_hook: Any, **kwargs: Any) -> Any:
        self.kwargs.append(kwargs)
        return super().query_items(response_hook=response_hook, **kwargs)


def test_integrated_cache_staleness_is_sent_with_queries() -> None:
    container = RecordingContainer([[{"id": "a"}]])
    list(SdkBackend(container, cache_staleness_ms=500).query_pages("SELECT * FROM c", None, ("", "FF"), 10))
    list(SdkBackend(container).query_pages("SELECT * FROM c", None, ("", "FF"), 10))
    assert container.kwargs[0]["max_integrated_cache_staleness_in_ms"] == 500
    assert "max_integrated_cache_staleness_in_ms" not in container.kwargs[1]
    assert parse_options({**BASE, "account_key": "k", "max_integrated_cache_staleness_ms": "500"})
    with pytest.raises(CosmosConfigError, match="max_integrated_cache_staleness_ms"):
        parse_options({**BASE, "account_key": "k", "max_integrated_cache_staleness_ms": "-1"})


def test_slow_requests_are_logged_and_sampled(caplog: pytest.LogCaptureFixture) -> None:
    log = SlowRequestLog(threshold_ms=100, per_minute=2)
    responses = [{"x-ms-activity-id": "act-1", "x-ms-request-charge": "2.5", "x-ms-throttle-retry-count": "3"}]
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        log.observe("query page", 0.05, responses, items=3)  # below the threshold
        for _ in range(5):
            log.observe("query page", 0.25, responses, items=3)
        log._window -= 61  # a minute later
        log.observe("query page", 0.25, responses, items=3)
    lines = [r.getMessage() for r in caplog.records]
    assert len(lines) == 3
    assert "250 ms (threshold 100 ms)" in lines[0] and "activity=act-1" in lines[0] and "throttle_retries=3" in lines[0]
    assert "request_charge=2.50" in lines[0] and "items=3" in lines[0]
    assert "(3 slow request(s) not logged in the last minute)" in lines[2]


def test_backend_times_each_page(caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests import test_read_performance

    slow_next = test_read_performance.FakePager.__next__

    def delayed(self: Any) -> Any:
        time.sleep(0.03)
        return slow_next(self)

    monkeypatch.setattr(test_read_performance.FakePager, "__next__", delayed)
    backend = SdkBackend(FakeContainer([[{"id": "a"}], [{"id": "b"}]]), slow_requests=SlowRequestLog(10, 10))
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        assert len(list(backend.query_pages("SELECT * FROM c", None, ("", "FF"), 1))) == 2
    assert sum("slow query page" in r.getMessage() for r in caplog.records) == 2
    assert parse_options({**BASE, "account_key": "k", "slow_request_ms": "1000"}).slow_request_ms == 1000.0
    assert parse_options({**BASE, "account_key": "k"}).slow_request_ms is None
