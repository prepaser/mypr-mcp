from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from mypr_mcp.config import ConfigError, ConfigStore, MCPConfig, global_config_path, parse_path
from mypr_mcp.lsp_config import LSPConfig


def test_named_server_with_unicode_name_round_trips(tmp_path):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    saved = store.save_server("mcp", "보고", {"command": "reports"})
    assert store.get('mcp.servers."보고"', snapshot=saved) == {"command": "reports"}
    assert store.load().values["mcp"]["servers"] == {"보고": {"command": "reports"}}


def test_oversized_write_does_not_replace_valid_config(tmp_path, monkeypatch):
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    store.set("limits.response_bytes", 4096)
    previous = store.workspace_path.read_bytes()
    monkeypatch.setattr("mypr_mcp.config._MAX_CONFIG_BYTES", 128)
    with pytest.raises(ConfigError, match="size limit"):
        store.set("mcp.servers.demo", {"command": "x" * 200})
    assert store.workspace_path.read_bytes() == previous


def _mcp(command: str) -> dict[str, object]:
    return {"command": command}


def test_global_path_is_resolved_once_and_supports_override(tmp_path: Path, monkeypatch):
    xdg = tmp_path / "xdg"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    monkeypatch.delenv("MYPR_GLOBAL_CONFIG", raising=False)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = ConfigStore(workspace)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "other"))

    assert global_config_path() == (tmp_path / "other" / "mypr" / "config.toml").resolve()
    assert store.global_path == (xdg / "mypr" / "config.toml").resolve()

    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "explicit.toml"))
    assert global_config_path() == (tmp_path / "explicit.toml").resolve()


def test_config_reads_do_not_create_lock_files(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text("[limits]\nresponse_bytes = 4096\n")
    store = ConfigStore(workspace, global_path)

    store.load()
    store.get()

    assert not global_path.with_suffix(".lock").exists()
    assert not store.workspace_path.with_suffix(".lock").exists()
    assert not store._profile_lock_path().exists()


def test_initial_global_and_workspace_writes_share_profile_lock(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    stores = (ConfigStore(workspace, global_path), ConfigStore(workspace, global_path))
    active = 0
    maximum = 0
    guard = threading.Lock()
    original_write = ConfigStore._write

    def tracked_write(path, data):
        nonlocal active, maximum
        with guard:
            active += 1
            maximum = max(maximum, active)
        try:
            time.sleep(0.02)
            original_write(path, data)
        finally:
            with guard:
                active -= 1

    monkeypatch.setattr(ConfigStore, "_write", staticmethod(tracked_write))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(stores[0].set, "limits.response_bytes", 4096, "global"),
            pool.submit(stores[1].set, "limits.output_bytes", 2048, "workspace"),
        ]
        [future.result() for future in futures]

    assert maximum == 1
    snapshot = ConfigStore(workspace, global_path).load()
    assert snapshot.values["limits"]["response_bytes"] == 4096
    assert snapshot.values["limits"]["output_bytes"] == 2048


def test_global_only_store_ignores_invalid_workspace_layer(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    workspace_config = workspace / ".mypr" / "config.toml"
    workspace_config.parent.mkdir()
    workspace_config.write_text("[limits\ninvalid = true\n")
    global_path = tmp_path / "global.toml"
    global_path.write_text("[limits]\nresponse_bytes = 4096\n")
    store = ConfigStore(None, global_path)

    snapshot = store.load()
    updated = store.set("limits.response_bytes", 8192, scope="global")

    assert snapshot.values["limits"]["response_bytes"] == 4096
    assert updated.values["limits"]["response_bytes"] == 8192
    assert snapshot.paths["workspace"] is None
    with pytest.raises(ConfigError, match="global-only"):
        store.set("limits.output_bytes", 2048)


def test_load_merges_layers_defaults_and_server_tombstones(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text(
        "# global comment\n"
        "[limits]\nresponse_bytes = 4096\n"
        "[mcp.servers.shared]\nurl = 'https://global.example'\n"
        "[mcp.servers.keep]\ncommand = 'keep'\n"
    )
    workspace_config = workspace / ".mypr" / "config.toml"
    workspace_config.parent.mkdir()
    workspace_config.write_text(
        "[limits]\noutput_bytes = 2048\n"
        "[mcp.servers.shared]\ncommand = 'local'\n"
        "[mcp.servers.removed]\nenabled = false\n"
        "[future]\nvalue = 'preserved'\n"
    )

    snapshot = ConfigStore(workspace, global_path).load()

    assert snapshot.values["limits"]["response_bytes"] == 4096
    assert snapshot.values["limits"]["output_bytes"] == 2048
    assert snapshot.values["mcp"]["servers"] == {
        "shared": {"command": "local"},
        "keep": {"command": "keep"},
    }
    assert snapshot.layers["global"]["limits"] == {"response_bytes": 4096}
    assert snapshot.layers["workspace"]["limits"] == {"output_bytes": 2048}
    assert snapshot.layers["workspace"]["mcp"]["servers"]["removed"] == {"enabled": False}
    assert snapshot.values["future"] == {"value": "preserved"}
    assert snapshot.revision == (
        f"global={snapshot.revisions['global']};workspace={snapshot.revisions['workspace']}"
    )


def test_wait_limits_default_and_layered_override(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text(
        "[limits]\nexecute_wait_ms = 0\npoll_wait_ms = 2500\n",
        encoding="utf-8",
    )
    store = ConfigStore(workspace, global_path)

    initial = store.load()
    assert initial.values["limits"]["execute_wait_ms"] == 0
    assert initial.values["limits"]["poll_wait_ms"] == 2500
    assert store.get("limits.execute_wait_ms", snapshot=initial) == 0
    assert store.explain("limits.execute_wait_ms", initial)["source"] == "global"

    workspace_config = workspace / ".mypr" / "config.toml"
    workspace_config.parent.mkdir()
    workspace_config.write_text("[limits]\npoll_wait_ms = 30000\n", encoding="utf-8")
    snapshot = store.load()
    assert snapshot.values["limits"]["execute_wait_ms"] == 0
    assert snapshot.values["limits"]["poll_wait_ms"] == 30000
    assert store.explain("limits.poll_wait_ms", snapshot)["source"] == "workspace"


@pytest.mark.parametrize("field", ["execute_wait_ms", "poll_wait_ms"])
@pytest.mark.parametrize("value", [-1, 30001, True, 1.0, "1000"])
def test_wait_limits_reject_non_integer_or_out_of_range_values(tmp_path: Path, field, value):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    path = workspace / ".mypr" / "config.toml"
    path.parent.mkdir()
    if isinstance(value, str):
        toml_value = repr(value)
    elif isinstance(value, bool):
        toml_value = str(value).lower()
    else:
        toml_value = str(value)
    path.write_text(f"[limits]\n{field} = {toml_value}\n", encoding="utf-8")

    with pytest.raises(ConfigError, match=f"limits\\.{field}"):
        ConfigStore(workspace, tmp_path / "global.toml").load()


@pytest.mark.parametrize(
    ("global_text", "workspace_text", "expected"),
    [
        ("[mcp.servers.demo]\nenabled = false\n", "", {}),
        ("", "[mcp.servers.demo]\nenabled = false\n", {}),
        (
            "[mcp.servers.demo]\nenabled = false\n",
            "[mcp.servers.demo]\ncommand = 'workspace'\n",
            {"demo": {"command": "workspace"}},
        ),
    ],
)
def test_server_tombstones_are_filtered_before_effective_validation(
    tmp_path: Path, global_text: str, workspace_text: str, expected: dict
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    if global_text:
        global_path.write_text(global_text)
    if workspace_text:
        path = workspace / ".mypr" / "config.toml"
        path.parent.mkdir()
        path.write_text(workspace_text)

    snapshot = ConfigStore(workspace, global_path).load()

    assert snapshot.values["mcp"]["servers"] == expected


def test_config_store_path_api_explain_and_cas(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = ConfigStore(workspace, tmp_path / "global.toml")
    first = store.set("limits.response_bytes", 4096)
    assert store.get("limits.response_bytes", snapshot=first) == 4096
    assert store.explain("limits.response_bytes", first)["source"] == "workspace"
    assert store.explain("limits.output_bytes", first)["source"] == "default"
    assert parse_path('mcp.servers."name.with.dots".command') == (
        "mcp", "servers", "name.with.dots", "command"
    )

    with pytest.raises(RuntimeError, match="changed"):
        store.set("limits.completed_tasks", 10, expected_revision="stale")

    second = store.set("limits.completed_tasks", 10, expected_revision=first.revision)
    assert store.get("limits.completed_tasks", scope="workspace", snapshot=second) == 10
    third = store.unset("limits.completed_tasks", expected_revision=second.revision)
    assert store.get("limits.completed_tasks", scope="workspace", snapshot=third) is None


def test_public_config_reads_filter_future_toml_values_and_reject_typos(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    config_path = workspace / ".mypr" / "config.toml"
    config_path.parent.mkdir()
    config_path.write_text(
        "[limits]\nresponse_bytes = 4096\n"
        "[future]\nwhen = 2025-01-01T00:00:00Z\n"
    )
    store = ConfigStore(workspace, tmp_path / "global.toml")

    public = store.get()
    explained = store.explain("limits")

    json.dumps(public)
    json.dumps(explained)
    assert "future" not in public
    assert "when" not in explained["value"]
    with pytest.raises(ConfigError, match="unknown"):
        store.set("limits.response_btyes", 4096)


def test_server_replacements_do_not_mix_global_definition_fields(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text("[mcp.servers.demo]\nurl = 'https://example.test'\n")
    store = ConfigStore(workspace, global_path)
    snapshot = store.load()

    with pytest.raises(ConfigError, match="whole"):
        store.set("mcp.servers.demo.command", "local", expected_revision=snapshot.revision)
    with pytest.raises(ConfigError, match="whole"):
        store.unset("mcp.servers.demo.command", expected_revision=snapshot.revision)
    assert not (workspace / ".mypr" / "config.toml").exists()

    updated = store.set(
        "mcp.servers.demo", _mcp("local"), expected_revision=snapshot.revision
    )
    assert updated.values["mcp"]["servers"]["demo"] == _mcp("local")


def test_unset_inherited_server_restores_global_definition(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text("[mcp.servers.demo]\ncommand = 'global'\n")
    store = ConfigStore(workspace, global_path)

    snapshot = store.unset("mcp.servers.demo")

    assert snapshot.values["mcp"]["servers"] == {"demo": {"command": "global"}}
    assert not (workspace / ".mypr" / "config.toml").exists()
    assert store.explain("mcp.servers.demo", snapshot)["source"] == "global"


def test_named_server_removal_writes_tombstone_for_inherited_server(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text("[mcp.servers.demo]\ncommand = 'global'\n")
    store = ConfigStore(workspace, global_path)

    snapshot = store.save_server("mcp", "demo", None)

    assert snapshot.values["mcp"]["servers"] == {}
    assert "enabled = false" in (workspace / ".mypr" / "config.toml").read_text()


def test_save_server_facades_diff_effective_map_and_preserve_comments(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text("[mcp.servers.inherited]\ncommand = 'global'\n")
    config_path = workspace / ".mypr" / "config.toml"
    config_path.parent.mkdir()
    config_path.write_text("# keep\n[mcp.servers.local]\ncommand = 'local'\n")
    store = MCPConfig(workspace, global_path)
    servers, revision = store.load()
    assert set(servers) == {"inherited", "local"}

    new_revision = store.save(
        {"inherited": _mcp("global"), "new": _mcp("new")}, revision
    )
    assert new_revision != revision
    text = config_path.read_text()
    assert "# keep" in text
    assert "enabled = false" not in text
    assert "[mcp.servers.local]" not in text
    assert "[mcp.servers.new]" in text

    loaded, _ = store.load()
    assert loaded == {"inherited": _mcp("global"), "new": _mcp("new")}


def test_lsp_raw_layer_keeps_missing_defaults_but_effective_adds_them(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    global_path.write_text(
        "[lsp.servers.python]\ncommand = ['pylsp']\nlanguages = ['python']\n"
    )
    store = ConfigStore(workspace, global_path)
    snapshot = store.load()
    assert "timeout" not in snapshot.layers["global"]["lsp"]["servers"]["python"]
    assert snapshot.values["lsp"]["servers"]["python"]["timeout"] == 10.0
    assert store.explain("lsp.servers.python.timeout", snapshot)["value"] == 10.0
    assert store.explain("lsp.servers.python", snapshot)["value"] == store.get(
        "lsp.servers.python", snapshot=snapshot,
    )

    config = LSPConfig(workspace, global_path)
    definitions, revision = config.load()
    assert definitions["python"]["timeout"] == 10.0
    config.save({"python": {"command": ["pylsp"], "languages": ["python"], "timeout": 5}}, revision)
    assert (
        ConfigStore(workspace, global_path)
        .load()
        .values["lsp"]["servers"]["python"]["timeout"]
        == 5.0
    )
