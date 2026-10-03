from __future__ import annotations

from types import SimpleNamespace

import pytest

from mypr_mcp.config import ConfigError, ConfigStore
from mypr_mcp.config_runtime import RuntimeConfig


def test_dependencies_default_and_layered_override(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    store = ConfigStore(workspace, global_path)

    initial = store.load()
    assert store.get("dependencies.auto_install", snapshot=initial) is True
    store.set(
        "dependencies.auto_install", False, scope="global",
        expected_revision=initial.revision,
    )
    snapshot = store.load()
    assert store.get("dependencies.auto_install", snapshot=snapshot) is False
    assert store.get("dependencies.auto_install", scope="global", snapshot=snapshot) is False
    assert store.get("dependencies.auto_install", scope="workspace", snapshot=snapshot) is None

    store.set(
        "dependencies.auto_install", True, scope="workspace",
        expected_revision=snapshot.revision,
    )
    snapshot = store.load()
    assert store.get("dependencies.auto_install", snapshot=snapshot) is True
    assert store.explain("dependencies.auto_install", snapshot=snapshot)["source"] == "workspace"


def test_dependencies_validation_and_public_filter(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    path = workspace / ".mypr" / "config.toml"
    path.parent.mkdir()
    path.write_text("[dependencies]\nauto_install = 'yes'\n")
    with pytest.raises(ConfigError, match="dependencies.auto_install"):
        ConfigStore(workspace, tmp_path / "global.toml").load()

    path.write_text("[dependencies]\nauto_install = false\nfuture = 'kept'\n")
    store = ConfigStore(workspace, tmp_path / "global.toml")
    snapshot = store.load()
    assert store.get(snapshot=snapshot)["dependencies"] == {"auto_install": False}
    assert snapshot.values["dependencies"]["future"] == "kept"
    with pytest.raises(ConfigError):
        store.set("dependencies.unknown", True, expected_revision=snapshot.revision)


@pytest.mark.parametrize("value", ["relative/cache", "", "cache\x00dir", "~other/cache", 42])
def test_uv_cache_dir_requires_absolute_or_home_path(tmp_path, value):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    path = workspace / ".mypr" / "config.toml"
    path.parent.mkdir()
    path.write_text(f"[dependencies]\nuv_cache_dir = {value!r}\n")
    with pytest.raises(ConfigError, match="dependencies.uv_cache_dir"):
        ConfigStore(workspace, tmp_path / "global.toml").load()


def test_dependency_cache_settings_are_layered_and_public(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = ConfigStore(workspace, tmp_path / "global.toml")
    initial = store.load()
    store.set(
        "dependencies.uv_cache_dir", "~/shared/uv", scope="global",
        expected_revision=initial.revision,
    )
    snapshot = store.load()
    assert store.get("dependencies.uv_cache_dir", snapshot=snapshot) == "~/shared/uv"
    store.set(
        "dependencies.uv_link_mode", "hardlink", scope="workspace",
        expected_revision=snapshot.revision,
    )
    snapshot = store.load()
    assert store.get(snapshot=snapshot)["dependencies"] == {
        "auto_install": True,
        "uv_cache_dir": "~/shared/uv",
        "uv_link_mode": "hardlink",
    }
    assert store.explain("dependencies.uv_link_mode", snapshot=snapshot)["source"] == "workspace"


@pytest.mark.parametrize("value", ["symlink", "HARDLINK", 1])
def test_uv_link_mode_is_restricted(tmp_path, value):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    path = workspace / ".mypr" / "config.toml"
    path.parent.mkdir()
    path.write_text(f"[dependencies]\nuv_link_mode = {value!r}\n")
    with pytest.raises(ConfigError, match="dependencies.uv_link_mode"):
        ConfigStore(workspace, tmp_path / "global.toml").load()


@pytest.mark.asyncio
async def test_runtime_dependency_policy_calls_sync_service_only_on_change(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    snapshot = store.load()
    calls = []

    class Service:
        config = {"auto_install": True}

        def apply_config(self, value):
            calls.append(value)
            self.config = dict(value)
            return None

    runtime = SimpleNamespace(dependencies=Service())
    settings = RuntimeConfig(runtime, store, snapshot)
    assert settings._apply_dependencies({"auto_install": True}) == {"applied": {}}
    assert calls == []
    assert settings._apply_dependencies({"auto_install": False})["applied"] is True
    assert calls == [{"auto_install": False}]
    assert settings.applied["dependencies"] == {"auto_install": False}


def test_runtime_missing_dependency_service_defers_only_changes(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    settings = RuntimeConfig(SimpleNamespace(dependencies=None), store, store.load())
    assert settings._apply_dependencies({"auto_install": True}) == {"applied": {}}
    assert settings._apply_dependencies({"auto_install": False}) == {
        "deferred": "Dependency service is unavailable"
    }


def test_runtime_dependency_cache_settings_are_forwarded(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    snapshot = store.load()
    calls = []

    class Service:
        config = {"auto_install": True}

        def apply_config(self, value):
            calls.append(value)
            self.config = dict(value)
            return {"applied": True, "applied_config": value}

    settings = RuntimeConfig(SimpleNamespace(dependencies=Service()), store, snapshot)
    result = settings._apply_dependencies({
        "auto_install": True,
        "uv_cache_dir": "~/shared/uv",
        "uv_link_mode": "clone",
    })
    assert result["applied"] is True
    assert calls == [{
        "auto_install": True,
        "uv_cache_dir": "~/shared/uv",
        "uv_link_mode": "clone",
    }]
    assert settings.applied["dependencies"]["uv_link_mode"] == "clone"
