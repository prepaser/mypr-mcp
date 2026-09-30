from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from mypr_mcp.code_tools import CodeTools
from mypr_mcp.lsp_config import LSPConfig
from mypr_mcp.lsp_edits import EditError, EditPlanStore, PlannedOperation

SERVER = r'''
import json
import sys

def recv():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        key, value = line.decode().split(":", 1)
        headers[key.lower()] = value.strip()
    return json.loads(sys.stdin.buffer.read(int(headers["content-length"])))

def send(value):
    body = json.dumps(value, separators=(",", ":")).encode()
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    sys.stdout.buffer.flush()

while True:
    message = recv()
    if message is None:
        break
    method = message.get("method")
    params = message.get("params", {})
    result = None
    if method == "initialize":
        result = {"capabilities": {
            "positionEncoding": "utf-16",
            "renameProvider": True,
            "codeActionProvider": {"codeActionKinds": ["quickfix"]},
            "diagnosticProvider": {"workspaceDiagnostics": True},
        }}
    elif method == "textDocument/rename":
        uri = params["textDocument"]["uri"]
        result = {
            "changes": {
                uri: [
                    {
                        "range": {
                            "start": {"line": 0, "character": 2},
                            "end": {"line": 0, "character": 5},
                        },
                        "newText": params["newName"],
                    }
                ]
            }
        }
    elif method == "textDocument/codeAction":
        result = [
            {
                "title": "Replace",
                "kind": "quickfix",
                "edit": {
                    "changes": {
                        params["textDocument"]["uri"]: [
                            {
                                "range": {
                                    "start": {"line": 0, "character": 0},
                                    "end": {"line": 0, "character": 3},
                                },
                                "newText": "bar",
                            }
                        ]
                    }
                },
            }
        ]
    elif method == "workspace/diagnostic":
        result = {
            "items": [
                {
                    "uri": params.get("uri", "file:///missing"),
                    "kind": "full",
                    "items": [],
                    "resultId": "1",
                }
            ]
        }
    elif method == "shutdown":
        result = None
    elif method == "exit":
        break
    if "id" in message:
        send({"jsonrpc": "2.0", "id": message["id"], "result": result})
'''


async def configured(tmp_path: Path) -> tuple[CodeTools, Path]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    script = tmp_path / "server.py"
    script.write_text(textwrap.dedent(SERVER), encoding="utf-8")
    code = CodeTools(workspace)
    await code.configure("fake", [sys.executable, str(script)], ["python"])
    return code, workspace


@pytest.mark.asyncio
async def test_rename_preview_and_apply_handles_utf16_and_crlf(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_bytes("😀foo\r\n".encode())
    try:
        preview = await code.rename("fake", path, 1, 2, "bar")
        assert preview["applicable"] is True
        assert preview["changes"][0]["operation"] == "update"
        await code.apply_edit(preview["plan_id"])
        assert path.read_bytes() == "😀bar\r\n".encode()
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_code_action_can_be_selected_and_applied(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    try:
        actions = await code.actions("fake", path, 1, 1)
        assert actions["actions"][0]["supported"] is True
        preview = await code.prepare_action(actions["actions"][0]["action_id"])
        await code.apply_edit(preview["plan_id"])
        assert path.read_text(encoding="utf-8") == "bar\n"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_code_action_rejects_disk_changes_since_request(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    try:
        actions = await code.actions("fake", path, 1, 1)
        path.write_text("qux\n", encoding="utf-8")
        with pytest.raises(EditError, match="document changed"):
            await code.prepare_action(actions["actions"][0]["action_id"])
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_code_action_rejects_open_document_version_changes(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    try:
        actions = await code.actions("fake", path, 1, 1)
        action_id = actions["actions"][0]["action_id"]
        path.write_text("qux\n", encoding="utf-8")
        await code.actions("fake", path, 1, 1)
        with pytest.raises(EditError, match="document changed"):
            await code.prepare_action(action_id)
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_rename_rechecks_document_snapshot_before_plan(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    original = code._workspace_edit_plan

    async def race(server, edit, **kwargs):
        path.write_text("qux\n", encoding="utf-8")
        document = server.documents[path.as_uri()]
        document.text = "qux\n"
        document.version += 1
        return await original(server, edit, **kwargs)

    code._workspace_edit_plan = race
    try:
        with pytest.raises(EditError, match="document changed"):
            await code.rename("fake", path, 1, 1, "bar")
    finally:
        await code.aclose()


def test_lsp_config_roundtrip_preserves_other_sections(tmp_path):
    config = LSPConfig(tmp_path)
    config.path.parent.mkdir(parents=True)
    config.path.write_text("[mcp]\nvalue = 1\n", encoding="utf-8")
    _, revision = config.load()
    config.save(
        {"fake": {"command": ["fake-lsp"], "languages": ["python"], "timeout": 4}},
        revision,
    )
    text = config.path.read_text(encoding="utf-8")
    assert "value = 1" in text
    assert config.load()[0]["fake"]["languages"] == ["python"]


@pytest.mark.asyncio
async def test_persist_conflict_keeps_running_binding(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    try:
        old_pid = code.status("fake")["pid"]
        config_path = workspace / ".mypr" / "config.toml"
        config_path.write_text(
            config_path.read_text(encoding="utf-8").replace("timeout = 10.0", "timeout = 11.0"),
            encoding="utf-8",
        )
        with pytest.raises(RuntimeError, match="changed on disk"):
            await code.configure("fake", [sys.executable, str(tmp_path / "server.py")], ["python"])
        assert code.status("fake")["pid"] == old_pid
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_resource_create_and_text_edit_are_one_plan(tmp_path):
    code, workspace = await configured(tmp_path)
    server = code._servers["fake"]
    target = workspace / "created.py"
    try:
        plan = await code._workspace_edit_plan(
            server,
            {
                "documentChanges": [
                    {"kind": "create", "uri": target.as_uri()},
                    {
                        "textDocument": {"uri": target.as_uri(), "version": None},
                        "edits": [
                            {
                                "range": {
                                    "start": {"line": 0, "character": 0},
                                    "end": {"line": 0, "character": 0},
                                },
                                "newText": "created\n",
                            }
                        ],
                    },
                ]
            },
            title="Create",
        )
        await code.apply_edit(plan.ident)
        assert target.read_text(encoding="utf-8") == "created\n"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_command_only_action_is_reported_but_never_applied(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    server = code._servers["fake"]

    async def request(method, _params, **_options):
        if method == "textDocument/codeAction":
            return [{"title": "Run", "command": {"title": "Run", "command": "run"}}]
        return None

    server._request = request
    try:
        actions = await code.actions("fake", path, 1, 1)
        assert actions["actions"][0]["supported"] is False
        preview = await code.prepare_action(actions["actions"][0]["action_id"])
        assert preview["applicable"] is False
        with pytest.raises(EditError, match="command execution"):
            await code.apply_edit(preview["plan_id"])
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_workspace_diagnostics_unchanged_reuses_snapshot(tmp_path):
    code, workspace = await configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    server = code._servers["fake"]
    server.capabilities["diagnosticProvider"] = {"workspaceDiagnostics": True}
    calls = 0

    async def request(method, _params, **_options):
        nonlocal calls
        if method == "workspace/diagnostic":
            calls += 1
            if calls == 1:
                return {
                    "items": [
                        {
                            "uri": path.as_uri(),
                            "kind": "full",
                            "items": [],
                            "resultId": "one",
                        }
                    ]
                }
            return {
                "items": [
                    {"uri": path.as_uri(), "kind": "unchanged", "resultId": "one"}
                ]
            }
        return None

    server._request = request
    try:
        first = await code.workspace_diagnostics("fake")
        second = await code.workspace_diagnostics("fake")
        assert first["reports"][0]["kind"] == "full"
        assert second["reports"][0]["kind"] == "unchanged"
        assert calls == 2
    finally:
        await code.aclose()


def test_lsp_edit_plan_survives_store_recreation(tmp_path):
    path = tmp_path / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    first = EditPlanStore(tmp_path)
    plan = first.create(
        tmp_path,
        [PlannedOperation("update", path, b"foo\n", b"bar\n", "old")],
        "generation",
        "Rename",
        server="fake",
    )
    second = EditPlanStore(tmp_path)
    loaded = second.get(plan.ident)
    assert loaded.server == "fake"
    assert loaded.operations[0].new == b"bar\n"
