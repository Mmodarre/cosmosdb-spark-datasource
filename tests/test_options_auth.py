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
        parse_options({**BASE, "endpoint": "http://insecure", "account_key": KEY})


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
        ({"query_parameters": "{bad json"}, "JSON list"),
        ({"query_parameters": '[{"name": "x", "value": 1}]'}, "starting with '@'"),
        ({"query_parameters": '{"name": "@x"}'}, "JSON list"),
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


def test_unknown_options_warn(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        opts(account_key=KEY, partiton_key="oops")
    assert "partiton_key" in caplog.text


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
        ({"service_credential": "cosmos-cred"}, "service_credential"),
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


def test_service_credential_unavailable_outside_databricks() -> None:
    with pytest.raises(CosmosAuthError, match="not available in this Python process") as info:
        auth._service_credential_provider("cosmos-cred")
    assert "auth_type=access_token" in str(info.value)


def test_service_credential_uses_databricks_module(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import types

    provider = _FakeCredential(token="uc-token")
    module = types.ModuleType("databricks.service_credentials")
    module.getServiceCredentialsProvider = lambda name: provider if name == "cosmos-cred" else None
    pkg = types.ModuleType("databricks")
    pkg.service_credentials = module
    monkeypatch.setitem(sys.modules, "databricks", pkg)
    monkeypatch.setitem(sys.modules, "databricks.service_credentials", module)
    monkeypatch.setattr(auth, "_DRIVER_TOKENS", auth._DriverTokenCache())
    token = mint_driver_token(opts(service_credential="cosmos-cred").auth, BASE["endpoint"])
    assert token.value == "uc-token"


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
    assert _client_key(sp1) == _client_key(sp2)  # same principal: one client, refreshed tokens


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
