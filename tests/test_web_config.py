from __future__ import annotations

from types import SimpleNamespace

import pytest

from mypr_mcp.config import ConfigError, ConfigStore, validate_config, validate_web_config
from mypr_mcp.config_runtime import RuntimeConfig


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_web_defaults_and_provider_validation():
    assert validate_config({})["web"] == {
        "default_provider": "",
        "timeout_seconds": 30,
        "max_concurrency": 4,
        "providers": {},
    }
    configured = validate_config(
        {
            "web": {
                "default_provider": "kagi",
                "providers": {"kagi": {"api_key_env": "KAGI_API_KEY"}},
            }
        }
    )
    assert configured["web"]["default_provider"] == "kagi"

    with pytest.raises(ConfigError, match="api_key_env"):
        validate_config({"web": {"providers": {"kagi": {}}}})
    with pytest.raises(ConfigError, match="environment variable"):
        validate_config(
            {"web": {"providers": {"kagi": {"api_key_env": "KAGI-API-KEY"}}}}
        )
    with pytest.raises(ConfigError, match="unknown fields"):
        validate_config(
            {
                "web": {
                    "providers": {
                        "kagi": {"api_key_env": "KAGI_API_KEY", "region": "us"}
                    }
                }
            }
        )
    with pytest.raises(ConfigError, match="one of kagi"):
        validate_config({"web": {"providers": {"google": {"api_key_env": "KEY"}}}})


def test_web_scalars_inherit_and_provider_entries_replace(tmp_path):
    workspace = tmp_path / "workspace"
    global_path = tmp_path / "global.toml"
    _write(
        global_path,
        "[web]\ntimeout_seconds = 12\nmax_concurrency = 8\n"
        "[web.providers.kagi]\napi_key_env = 'GLOBAL_KAGI'\n"
        "[web.providers.brave]\napi_key_env = 'BRAVE_KEY'\n",
    )
    _write(
        workspace / ".mypr/config.toml",
        "[web]\ntimeout_seconds = 3\n"
        "[web.providers.kagi]\napi_key_env = 'WORKSPACE_KAGI'\n",
    )
    store = ConfigStore(workspace, global_path)
    web = store.load().values["web"]
    assert web["timeout_seconds"] == 3
    assert web["max_concurrency"] == 8
    assert web["providers"] == {
        "kagi": {"api_key_env": "WORKSPACE_KAGI"},
        "brave": {"api_key_env": "BRAVE_KEY"},
    }


def test_web_provider_tombstone_hides_inherited_entry(tmp_path):
    workspace = tmp_path / "workspace"
    global_path = tmp_path / "global.toml"
    _write(global_path, "[web.providers.kagi]\napi_key_env = 'KAGI_KEY'\n")
    _write(workspace / ".mypr/config.toml", "[web.providers.kagi]\nenabled = false\n")
    assert ConfigStore(workspace, global_path).load().values["web"]["providers"] == {}


def test_web_default_must_point_to_active_provider():
    with pytest.raises(ConfigError, match="active configured provider"):
        validate_web_config({"default_provider": "kagi", "providers": {}})
    with pytest.raises(ConfigError, match="active configured provider"):
        validate_web_config(
            {
                "default_provider": "kagi",
                "providers": {"kagi": {"enabled": False}},
            }
        )
    with pytest.raises(ConfigError, match="timeout_seconds"):
        validate_config({"web": {"timeout_seconds": 0}})
    with pytest.raises(ConfigError, match="timeout_seconds"):
        validate_config({"web": {"timeout_seconds": float("inf")}})
    with pytest.raises(ConfigError, match="max_concurrency"):
        validate_config({"web": {"max_concurrency": 0}})


def test_web_public_paths_round_trip_and_replace_provider_as_a_whole(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    snapshot = store.load()
    saved = store.set(
        "web.providers.kagi",
        {"api_key_env": "KAGI_API_KEY"},
        expected_revision=snapshot.revision,
    )
    assert store.get("web.providers.kagi", snapshot=saved) == {
        "api_key_env": "KAGI_API_KEY"
    }
    assert store.explain("web.providers.kagi", snapshot=saved)["source"] == "workspace"
    with pytest.raises(ConfigError, match="unknown managed configuration field"):
        store.set(
            "web.providers.kagi.api_key_env",
            "OTHER_KEY",
            expected_revision=saved.revision,
        )
    removed = store.unset("web.providers.kagi", expected_revision=saved.revision)
    assert store.get("web.providers.kagi", snapshot=removed) is None


@pytest.mark.asyncio
async def test_runtime_web_apply_records_confirmed_config_and_defers_service_work(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    snapshot = store.load()
    old = snapshot.values["web"]
    desired = validate_web_config(
        {
            "timeout_seconds": 10,
            "providers": {"brave": {"api_key_env": "BRAVE_KEY"}},
        }
    )

    class Service:
        applied_config = old

        async def apply_config(self, config, force=False):
            assert force is True
            return {
                "applied": ["web"],
                "applied_config": config,
                "deferred": [],
                "errors": {},
            }

    settings = RuntimeConfig(SimpleNamespace(web=Service()), store, snapshot)
    response = await settings._apply_web(desired, force=True)
    settings._record_web_application(response)
    assert settings.applied["web"] == desired

    class BusyService(Service):
        async def apply_config(self, config, force=False):
            return {
                "applied": [],
                "applied_config": self.applied_config,
                "deferred": ["web"],
                "errors": {},
            }

    settings.runtime.web = BusyService()
    deferred = await settings._apply_web(validate_web_config({"timeout_seconds": 11}), force=True)
    assert deferred["deferred"] == ["web"]
    assert settings.applied["web"] == desired
