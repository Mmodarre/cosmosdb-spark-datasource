"""Option validation, auth configuration, token handling and secret redaction."""

from __future__ import annotations

import base64
import json
import logging
import pickle
import time

import pytest

from cosmosdb_datasource import auth
from cosmosdb_datasource.auth import PartitionCredential, SecretToken, mint_driver_token
from cosmosdb_datasource.errors import REDACTED, CosmosAuthError, CosmosConfigError, redact
from cosmosdb_datasource.options import TEST_HOOKS_ENV, parse_options, redacted_options

BASE = {"endpoint": "https://acct.documents.azure.com:443/", "database": "db", "container": "c"}
KEY = "c2VjcmV0LWFjY291bnQta2V5LXZhbHVl=="


def opts(**extra):
    return parse_options({**BASE, **extra})


def test_required_options() -> None:
    with pytest.raises(CosmosConfigError, match="endpoint, database, container"):
        parse_options({"account_key": KEY})
    with pytest.raises(CosmosConfigError, match="endpoint"):
        parse_options({**BASE, "endpoint": "http://insecure.example.com", "account_key": KEY})


def test_options_are_case_insensitive_and_typed() -> None:
    o = parse_options(
        {
            **BASE,
            "ACCOUNT_KEY": KEY,
            "Page_Size": "250",
            "partitions_per_feed_range": "AUTO",
            "preferred_regions": "Australia East, Australia Southeast",
            "consistency_level": "session",
            "priority": "low",
            "mode": "failfast",
            "log_metrics": "true",
        }
    )
    assert o.auth.auth_type == "key"
    assert o.page_size == 250
    assert o.partitions_per_feed_range == "auto"
    assert o.preferred_regions == ("Australia East", "Australia Southeast")
    assert o.consistency_level == "Session"
    assert o.priority == "Low"
    assert o.parse_mode == "FAILFAST"
    assert o.log_metrics is True


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"page_size": "x"}, "page_size"),
        ({"page_size": "0"}, ">= 1"),
        ({"partitions_per_feed_range": "0"}, "between 1 and 1024"),
        ({"partitions_per_feed_range": "lots"}, "positive integer or 'auto'"),
        ({"consistency_level": "linearizable"}, "consistency_level"),
        ({"priority": "urgent"}, "priority"),
        ({"mode": "lenient"}, "mode"),
        ({"log_metrics": "maybe"}, "true or false"),
        ({"change_feed_mode": "Incremental"}, "change_feed_mode"),
        ({"start_from": "yesterday"}, "ISO-8601"),
        ({"query": "SELECT * FROM c", "query_parameters": "{bad json"}, "JSON list"),
        ({"query": "SELECT * FROM c", "query_parameters": '[{"name": "x", "value": 1}]'}, "starting with '@'"),
        ({"query": "SELECT * FROM c", "query_parameters": '{"name": "@x"}'}, "JSON list"),
        ({"query_parameters": '[{"name": "@x", "value": 1}]'}, "only applies with 'query' or 'sampling_query'"),
        ({"connection_timeout_seconds": "0"}, "greater than 0"),
        ({"read_timeout_seconds": "-1"}, "greater than 0"),
        ({"throttle_max_wait_seconds": "-1"}, ">= 0"),
        ({"slow_request_log_per_minute": "5"}, "only applies with 'slow_request_ms'"),
        ({"slow_request_ms": "0", "slow_request_log_per_minute": "5"}, "only applies with 'slow_request_ms'"),
        ({"raw_json_column": "_corrupt_record"}, "must differ"),
        ({"max_items_per_trigger": "-5"}, ">= 1"),
    ],
)
def test_invalid_options(extra: dict[str, str], message: str) -> None:
    with pytest.raises(CosmosConfigError, match=message):
        opts(account_key=KEY, **extra)


def test_avad_only_supports_now() -> None:
    o = opts(account_key=KEY, change_feed_mode="AllVersionsAndDeletes")
    assert o.start_from == "Now"
    assert o.change_feed_mode == "AllVersionsAndDeletes"
    with pytest.raises(CosmosConfigError, match="only supports start_from=Now"):
        opts(account_key=KEY, change_feed_mode="AllVersionsAndDeletes", start_from="Beginning")


def test_start_from_variants() -> None:
    assert opts(account_key=KEY).start_from == "Beginning"
    assert opts(account_key=KEY, start_from="now").start_from == "Now"
    o = opts(account_key=KEY, start_from="2026-01-31T10:00:00+10:00")
    assert o.start_from_datetime.isoformat() == "2026-01-31T00:00:00+00:00"
    assert opts(account_key=KEY, start_from="2026-01-31").start_from_datetime.hour == 0


def test_query_parameters_parsed() -> None:
    o = opts(account_key=KEY, query="SELECT * FROM c WHERE c.x = @x", query_parameters='[{"name":"@x","value":3}]')
    assert o.parameters_for_sdk() == [{"name": "@x", "value": 3}]
    assert o.query_explicit


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("partiton_key", r"'partiton_key' \(did you mean 'partition_key'\?\)"),
        ("initial_snapshots", r"did you mean 'initial_snapshot'\?"),
        ("filterPushdown", r"'filterpushdown' \(did you mean 'filter_pushdown'\?\)"),
        ("changeFeedMode", r"did you mean 'change_feed_mode'\?"),
        ("endAt", r"did you mean 'end_at'\?"),
        ("colour", r"Unknown cosmosdb option\(s\): 'colour'\. "),
    ],
)
def test_unknown_options_are_rejected_with_a_suggestion(name: str, message: str) -> None:
    with pytest.raises(CosmosConfigError, match=message):
        opts(account_key=KEY, **{name: "x"})


@pytest.mark.parametrize(
    "name",
    [
        "path",
        "Path",  # .load(path)
        "paths",
        "PATHS",  # .load(path1, path2)
        "isUserSpecifiedSchema",
        "isuserspecifiedschema",
        "ISUSERSPECIFIEDSCHEMA",  # Spark Connect on Databricks serverless, with .schema(...)
    ],
)
def test_options_that_the_engine_adds_are_accepted(name: str, caplog: pytest.LogCaptureFixture) -> None:
    from tests.conftest import fake_options
    from tests.fake_cosmos import FakeCosmosStore
    from tests.harness import make_source

    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        assert opts(account_key=KEY, **{name: "true"}).endpoint == BASE["endpoint"]
        make_source(fake_options(FakeCosmosStore("engine-options"), **{name: "true"}))  # as Spark passes them
    assert caplog.text == ""


def test_unknown_options_can_be_logged_instead(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        o = opts(account_key=KEY, unknown_options="WARN", injectedByRuntime="x", partiton_key="p")
    assert o.partition_key is None  # an unknown name is ignored, also one that looks like a known option
    assert "'injectedbyruntime'" in caplog.text and "did you mean 'partition_key'?" in caplog.text
    with pytest.raises(CosmosConfigError, match="set unknown_options=warn"):
        opts(account_key=KEY, injectedByRuntime="x")
    with pytest.raises(CosmosConfigError, match="set unknown_options=warn"):
        opts(account_key=KEY, unknown_options="error", injectedByRuntime="x")
    with pytest.raises(CosmosConfigError, match="'unknown_options' must be error or warn"):
        opts(account_key=KEY, unknown_options="ignore")


def test_timeouts_take_fractions_of_a_second() -> None:
    o = opts(
        account_key=KEY, connection_timeout_seconds="2.5", read_timeout_seconds="0.5", throttle_max_wait_seconds="1.5"
    )
    assert (o.connection_timeout_seconds, o.read_timeout_seconds, o.throttle_max_wait_seconds) == (2.5, 0.5, 1.5)
    assert opts(account_key=KEY, throttle_max_wait_seconds="0").throttle_max_wait_seconds == 0


@pytest.mark.parametrize(
    ("auth", "message"),
    [
        ({"auth_type": "key", "account_key": KEY, "client_secret": "s"}, "client_secret do not apply to auth_type=key"),
        ({"auth_type": "default", "account_key": KEY}, "account_key do not apply to auth_type=default"),
        ({"auth_type": "default", "client_id": "c"}, "client_id do not apply to auth_type=default"),
        ({"account_key": KEY, "access_token_expires_on": "123"}, "access_token_expires_on do not apply"),
        ({"auth_type": "access_token", "access_token": "t", "tenant_id": "x"}, "tenant_id do not apply"),
        (
            {
                "auth_type": "service_principal",
                "tenant_id": "t",
                "client_id": "c",
                "client_secret": "s",
                "client_certificate_password": "p",
            },
            "only applies with 'client_certificate'",
        ),
        ({"auth_type": "key", "account_key": KEY, "service_credential": "cred"}, "service_credential do not apply"),
    ],
)
def test_credential_options_of_another_auth_type_are_rejected(auth: dict[str, str], message: str) -> None:
    with pytest.raises(CosmosConfigError, match=message):
        parse_options({**BASE, **auth})


def test_test_backend_hook_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TEST_HOOKS_ENV, raising=False)
    with pytest.raises(CosmosConfigError, match="Unknown option '_test_backend'"):
        opts(account_key=KEY, _test_backend="x:y")


@pytest.mark.parametrize(
    ("extra", "auth_type"),
    [
        ({"account_key": KEY}, "key"),
        ({"tenant_id": "t", "client_id": "c", "client_secret": "s3cr3t-value"}, "service_principal"),
        ({"access_token": "tok-value-123"}, "access_token"),
        ({"auth_type": "default"}, "default"),
    ],
)
def test_auth_type_inference(extra: dict[str, str], auth_type: str) -> None:
    assert opts(**extra).auth.auth_type == auth_type


def test_auth_validation_errors() -> None:
    with pytest.raises(CosmosConfigError, match="No credentials configured"):
        opts()
    with pytest.raises(CosmosConfigError, match="set option 'auth_type' explicitly"):
        opts(account_key=KEY, access_token="abc")
    with pytest.raises(CosmosConfigError, match="tenant_id, client_id"):
        opts(auth_type="service_principal", client_secret="x")
    with pytest.raises(CosmosConfigError, match="auth_type"):
        opts(auth_type="managed_identity")


def test_secrets_never_rendered() -> None:
    o = opts(auth_type="service_principal", tenant_id="t", client_id="c", client_secret="sp-secret-value")
    k = opts(account_key=KEY)
    for rendered in (repr(o), str(o), repr(o.auth), repr(k), repr(k.auth)):
        assert "sp-secret-value" not in rendered and KEY not in rendered
    safe = redacted_options({**BASE, "account_key": KEY, "client_secret": "x", "access_token": "y"})
    assert safe["account_key"] == safe["client_secret"] == safe["access_token"] == REDACTED


def test_redact_masks_known_values_and_patterns() -> None:
    sig, account_key = "s" + "ig=", "Account" + "Key="  # built at runtime: no secret-looking literals here
    text = (
        f"key={KEY} Authorization: type=master&ver=1.0&{sig}abcdEFGH123 Bearer eyJhbGciOiJ.eyJzdWIiOiIx.c2lnbmF0dXJl "
        f"{account_key}zzzz;"
    )
    out = redact(text, [KEY])
    assert KEY not in out and "abcdEFGH123" not in out and "zzzz" not in out
    assert "eyJhbGciOiJ" not in out


def test_secret_token_repr_and_pickle() -> None:
    token = SecretToken("very-secret-token", int(time.time()) + 3600)
    assert "very-secret-token" not in repr(token)
    clone = pickle.loads(pickle.dumps(token))
    assert clone.value == "very-secret-token" and clone.expires_on == token.expires_on


def _jwt(exp: int) -> str:
    def b64(d: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip("=")

    return f"{b64({'alg': 'none'})}.{b64({'exp': exp})}.sig"


def test_access_token_expiry_from_jwt_and_expired_error() -> None:
    exp = int(time.time()) + 1800
    token = mint_driver_token(opts(access_token=_jwt(exp)).auth, BASE["endpoint"])
    assert token is not None and token.expires_on == exp
    explicit = mint_driver_token(
        opts(access_token="opaque", access_token_expires_on=str(exp + 5)).auth, BASE["endpoint"]
    )
    assert explicit.expires_on == exp + 5
    with pytest.raises(CosmosAuthError, match="expired"):
        mint_driver_token(opts(access_token=_jwt(int(time.time()) - 10)).auth, BASE["endpoint"])
    assert mint_driver_token(opts(account_key=KEY).auth, BASE["endpoint"]) is None


class _FakeCredential:
    def __init__(self, token: str = "fresh", lifetime: int = 3600, fail: str | None = None) -> None:
        self.calls: list[tuple] = []
        self.token, self.lifetime, self.fail = token, lifetime, fail

    def get_token(self, *scopes, **kwargs):
        from azure.core.credentials import AccessToken

        self.calls.append(scopes)
        if self.fail:
            raise RuntimeError(self.fail)
        return AccessToken(self.token, int(time.time()) + self.lifetime)


def test_driver_mints_with_account_scope_and_falls_back_on_aadsts500011(monkeypatch: pytest.MonkeyPatch) -> None:
    class Picky(_FakeCredential):
        def get_token(self, *scopes, **kwargs):
            if scopes[0] != auth.COSMOS_FALLBACK_SCOPE:
                self.calls.append(scopes)
                raise RuntimeError("AADSTS500011: resource principal not found")
            return super().get_token(*scopes, **kwargs)

    cred = Picky()
    token = auth.get_token(cred, BASE["endpoint"])
    assert token.value == "fresh"
    assert cred.calls[0] == ("https://acct.documents.azure.com/.default",)
    with pytest.raises(CosmosAuthError, match="Could not obtain"):
        auth.get_token(
            _FakeCredential(fail="boom client_secret=sp-secret-value"), BASE["endpoint"], ("sp-secret-value",)
        )


def test_driver_token_cache_reuses_until_near_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[_FakeCredential] = []

    def build(_auth):
        created.append(_FakeCredential())
        return created[-1]

    monkeypatch.setattr(auth, "build_token_credential", build)
    monkeypatch.setattr(auth, "_DRIVER_TOKENS", auth._DriverTokenCache())
    o = opts(auth_type="service_principal", tenant_id="t", client_id="cid", client_secret="s")
    first = mint_driver_token(o.auth, BASE["endpoint"])
    second = mint_driver_token(o.auth, BASE["endpoint"])
    assert first is second and len(created) == 1 and len(created[0].calls) == 1


def test_partition_credential_prefers_shipped_token_then_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    fallback = _FakeCredential(token="worker-minted")
    monkeypatch.setattr(auth, "build_token_credential", lambda _a: fallback)
    o = opts(auth_type="service_principal", tenant_id="t", client_id="c", client_secret="s")
    fresh = PartitionCredential(o.auth, BASE["endpoint"], SecretToken("shipped", int(time.time()) + 3600))
    assert fresh.get_token("scope").token == "shipped" and not fallback.calls
    stale = PartitionCredential(o.auth, BASE["endpoint"], SecretToken("shipped", int(time.time()) + 60))
    assert stale.get_token("scope").token == "worker-minted"
    assert stale.get_token("scope").token == "worker-minted"  # refreshed token is now cached


def test_partition_credential_access_token_has_no_fallback() -> None:
    o = opts(access_token="t")
    nearly = PartitionCredential(o.auth, BASE["endpoint"], SecretToken("tok", int(time.time()) + 30))
    assert nearly.get_token("s").token == "tok"  # still valid: used until it expires
    expired = PartitionCredential(o.auth, BASE["endpoint"], SecretToken("tok", int(time.time()) - 1))
    with pytest.raises(CosmosAuthError, match="expired"):
        expired.get_token("s")


def test_partition_credential_fallback_failure_uses_shipped_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auth, "build_token_credential", lambda _a: _FakeCredential(fail="no network"))
    o = opts(auth_type="default")
    cred = PartitionCredential(o.auth, BASE["endpoint"], SecretToken("shipped", int(time.time()) + 120))
    assert cred.get_token("s").token == "shipped"
    gone = PartitionCredential(o.auth, BASE["endpoint"], SecretToken("shipped", int(time.time()) - 5))
    with pytest.raises(CosmosAuthError, match="Could not refresh"):
        gone.get_token("s")


@pytest.mark.parametrize(
    "extra",
    [{"auth_type": "service_credential", "service_credential": "cosmos-cred"}, {"service_credential": "cosmos-cred"}],
)
def test_service_credential_is_rejected_with_the_alternatives(extra: dict[str, str]) -> None:
    with pytest.raises(CosmosConfigError, match="auth_type=service_credential is not supported") as info:
        opts(**extra)
    message = str(info.value)
    assert "auth_type=service_principal" in message and "getServiceCredentialsProvider" in message
    assert "auth_type=access_token" in message


def test_identity_extra_message(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("azure.identity") or (name == "azure" and args and args[2] and "identity" in args[2]):
            raise ImportError("no azure-identity")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(CosmosAuthError, match=r"\[identity\]"):
        auth.build_token_credential(opts(auth_type="default").auth)


def test_access_tokens_of_different_principals_never_share_a_client() -> None:
    from cosmosdb_datasource.client import _client_key

    a = opts(access_token="token-of-principal-a")
    b = opts(access_token="token-of-principal-b")
    assert _client_key(a) != _client_key(b)
    assert "token-of-principal-a" not in repr(_client_key(a))
    sp1 = opts(auth_type="service_principal", tenant_id="t", client_id="c", client_secret="s1")
    sp2 = opts(auth_type="service_principal", tenant_id="t", client_id="c", client_secret="s2")
    assert _client_key(sp1) != _client_key(sp2)  # a rotated secret: a new client, whose fallback credential works
    assert _client_key(sp1) == _client_key(
        opts(auth_type="service_principal", tenant_id="t", client_id="c", client_secret="s1")
    )


def _sdk_policy(**extra: str):
    from azure.cosmos import cosmos_client

    from cosmosdb_datasource.client import _client_kwargs

    kwargs = _client_kwargs(parse_options({**BASE, "auth_type": "key", "account_key": "a2V5", **extra}))
    kwargs.pop("user_agent_suffix")
    return cosmos_client._build_connection_policy(kwargs)


@pytest.mark.parametrize(("retries", "wait"), [("0", "0"), ("0", "60"), ("3", "7"), (None, None)])
def test_throttle_retry_options_reach_the_sdk_including_zero(retries, wait) -> None:
    extra = {k: v for k, v in (("throttle_max_retries", retries), ("throttle_max_wait_seconds", wait)) if v is not None}
    policy = _sdk_policy(**extra)
    assert policy.RetryOptions.MaxRetryAttemptCount == int(retries or 9)
    assert policy.RetryOptions.MaxWaitTimeInSeconds == int(wait or 60)


def test_connection_policy_keeps_the_other_client_settings() -> None:
    policy = _sdk_policy(preferred_regions="West Europe,North Europe", connection_timeout_seconds="7")
    assert policy.PreferredLocations == ["West Europe", "North Europe"] and policy.RequestTimeout == 7


def test_version_comes_from_the_distribution_metadata() -> None:
    import re
    from importlib.metadata import version
    from pathlib import Path

    import cosmosdb_datasource

    pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
    declared = re.search(r'^version = "([^"]+)"$', pyproject, re.MULTILINE)
    assert declared and cosmosdb_datasource.__version__ == version("cosmosdb-spark-datasource") == declared.group(1)


@pytest.mark.parametrize(
    ("datasource_module", "message"),
    [
        # DBR 14.3 LTS and serverless environment 1: Spark 3.5.0 with only the batch data source API
        ("class DataSource: ...\nclass DataSourceReader: ...\n", "DataSourceStreamReader"),
        (None, "needs PySpark"),
    ],
)
def test_unsupported_runtimes_fail_with_the_minimum_versions(tmp_path, datasource_module, message) -> None:
    import os
    import subprocess
    import sys
    from pathlib import Path

    if datasource_module is not None:
        (tmp_path / "pyspark" / "sql").mkdir(parents=True)
        (tmp_path / "pyspark" / "__init__.py").write_text('__version__ = "3.5.0"\n')
        (tmp_path / "pyspark" / "sql" / "__init__.py").write_text("")
        (tmp_path / "pyspark" / "sql" / "datasource.py").write_text(datasource_module)
    else:
        (tmp_path / "pyspark").mkdir()
        (tmp_path / "pyspark" / "__init__.py").write_text("raise ImportError('no pyspark here')\n")
    src = Path(__file__).resolve().parents[1] / "src"
    result = subprocess.run(
        [sys.executable, "-c", "import cosmosdb_datasource"],
        env={**os.environ, "PYTHONPATH": os.pathsep.join([str(tmp_path), str(src)])},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert (
        message in result.stderr
        and "DBR 15.4 LTS" in result.stderr
        and "serverless environment version 2" in result.stderr
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://acct.documents.azure.com:443/",
        "https://acct.documents.azure.com",
        "https://localhost:8081/",
        "http://localhost:8081/",
        "http://127.0.0.1:8081",
        "http://[::1]:8081/",
        "http://cosmos-emulator:8081/",
        "http://emulator.localhost:8081/",
    ],
)
def test_accepted_endpoints(endpoint: str) -> None:
    assert parse_options({**BASE, "endpoint": endpoint, "account_key": KEY}).endpoint == endpoint


@pytest.mark.parametrize(
    ("endpoint", "message"),
    [
        ("http://acct.documents.azure.com:443/", "must use https://"),
        ("http://10.0.0.5:8081/", "must use https://"),
        ("ftp://acct.documents.azure.com/", "must look like"),
        ("https://acct.documents.azure.com/dbs/x", "must look like"),
        ("https://user@acct.documents.azure.com/", "must look like"),
        ("https://acct.documents.azure.com:notaport/", "must look like"),
        ("acct.documents.azure.com", "must look like"),
    ],
)
def test_rejected_endpoints(endpoint: str, message: str) -> None:
    with pytest.raises(CosmosConfigError, match=message):
        parse_options({**BASE, "endpoint": endpoint, "account_key": KEY})


def test_tls_options_reach_the_sdk(caplog: pytest.LogCaptureFixture) -> None:
    from cosmosdb_datasource.client import _client_key

    default = _sdk_policy()
    assert not default.DisableSSLVerification and default.SSLConfiguration is None
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        unverified = _sdk_policy(tls_verify="false")
    assert unverified.DisableSSLVerification and "not verified" in caplog.text
    bundle = _sdk_policy(tls_ca_bundle="/etc/ssl/corp-ca.pem")
    assert bundle.SSLConfiguration.SSLCaCerts == "/etc/ssl/corp-ca.pem" and not bundle.DisableSSLVerification
    keys = {
        _client_key(opts(account_key=KEY, **extra)) for extra in ({}, {"tls_verify": "false"}, {"tls_ca_bundle": "/x"})
    }
    assert len(keys) == 3
    with pytest.raises(CosmosConfigError, match="cannot be combined"):
        opts(account_key=KEY, tls_verify="false", tls_ca_bundle="/x")


@pytest.mark.parametrize(
    ("extra", "stream", "unused"),
    [
        ({"change_feed_end_wait_seconds": "5"}, False, "change_feed_end_wait_seconds"),
        ({"item_ids": '[["d1", "p1"]]', "partitions_per_feed_range": "4"}, False, "partitions_per_feed_range"),
        ({"item_ids": '[["d1", "p1"]]', "prefetch_pages": "2"}, False, "prefetch_pages"),
        ({"item_ids": '[["d1", "p1"]]', "page_size": "10"}, False, "page_size"),
        ({"partition_key": "p1", "partitions_per_feed_range": "2"}, False, "partitions_per_feed_range"),
        ({"change_feed_mode": "LatestVersion", "filter_pushdown": "true"}, False, "filter_pushdown"),
        ({"change_feed_mode": "LatestVersion", "arrow_batches": "true"}, False, "arrow_batches"),
        ({"change_feed_mode": "LatestVersion", "filter_pushdown": "auto"}, True, "filter_pushdown"),
        ({"change_feed_mode": "LatestVersion", "prefetch_pages": "2"}, True, "prefetch_pages"),
        ({"change_feed_mode": "LatestVersion", "max_integrated_cache_staleness_ms": "5"}, True, "max_integrated"),
        ({"snapshot_stream": "true", "filter_pushdown": "true"}, True, "filter_pushdown"),
        ({"snapshot_stream": "true", "arrow_batches": "false"}, True, "arrow_batches"),
    ],
)
def test_options_without_effect_on_the_read_are_rejected(make_store, extra, stream, unused) -> None:
    from tests.conftest import fake_options
    from tests.harness import make_source

    ds = make_source(fake_options(make_store(), **extra))
    with pytest.raises(CosmosConfigError, match=f"{unused}.* have no effect"):
        if stream:
            ds.streamReader(ds.schema())
        else:
            ds.reader(ds.schema())


def test_query_options_apply_to_an_initial_snapshot(make_store) -> None:
    from tests.conftest import fake_options
    from tests.harness import make_source

    extra = {"prefetch_pages": "2", "max_integrated_cache_staleness_ms": "5", "initial_snapshot": "true"}
    ds = make_source(fake_options(make_store(), change_feed_mode="LatestVersion", **extra))
    assert type(ds.streamReader(ds.schema())).__name__ == "CosmosStreamReader"
