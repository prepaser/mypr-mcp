from __future__ import annotations

import asyncio
import json
import sys
import textwrap
from pathlib import Path

import pytest

from mypr_mcp.code_tools import CodeError, CodeTools

FAKE_SERVER = r"""
import json
import sys

log_path = sys.argv[1]

def read_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        key, value = line.decode("ascii").split(":", 1)
        headers[key.lower()] = value.strip()
    return json.loads(sys.stdin.buffer.read(int(headers["content-length"])))

def send(message):
    body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    sys.stdout.buffer.flush()

def log(message):
    with open(log_path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(message, ensure_ascii=False) + "\n")

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    params = message.get("params", {})
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"capabilities": {
            "textDocumentSync": 2,
            "definitionProvider": {},
            "referencesProvider": {},
            "hoverProvider": {},
        }}})
    elif method in ("textDocument/didOpen", "textDocument/didChange"):
        document = params["textDocument"]
        if "contentChanges" in params:
            text = params["contentChanges"][-1]["text"]
        else:
            text = document["text"]
        log({"method": method, "version": document["version"], "text": text})
        diagnostics = []
        if "bad" in text:
            diagnostics = [{"range": {"start": {"line": 0, "character": 0},
                                      "end": {"line": 0, "character": 3}},
                            "severity": 1, "source": "fake", "message": "bad source"}]
        if "pending" not in text:
            diag_params = {"uri": document["uri"], "diagnostics": diagnostics}
            if "unknown" not in text:
                diag_params["version"] = document["version"]
            send({"jsonrpc": "2.0", "method": "textDocument/publishDiagnostics",
                  "params": diag_params})
    elif method in ("textDocument/definition", "textDocument/references"):
        log({"method": method, "position": params["position"]})
        uri = params["textDocument"]["uri"]
        result = [{"uri": uri, "range": {"start": {"line": 0, "character": 3},
                                           "end": {"line": 0, "character": 9}}}]
        send({"jsonrpc": "2.0", "id": message["id"], "result": result})
    elif method == "textDocument/hover":
        log({"method": method, "position": params["position"]})
        if params["position"]["line"] == 98:
            continue
        uri = params["textDocument"]["uri"]
        send({"jsonrpc": "2.0", "id": message["id"], "result": {
            "contents": {"kind": "markdown", "value": "symbol **docs**"},
            "range": {"start": {"line": 0, "character": 3},
                      "end": {"line": 0, "character": 9}},
        }})
    elif method == "$/cancelRequest":
        log(message)
    elif method == "shutdown":
        send({"jsonrpc": "2.0", "id": message["id"], "result": None})
    elif method == "exit":
        break
    elif "id" in message:
        send({"jsonrpc": "2.0", "id": message["id"], "result": None})
"""


async def _configured(tmp_path: Path) -> tuple[CodeTools, Path, Path]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    script = tmp_path / "fake-lsp.py"
    script.write_text(textwrap.dedent(FAKE_SERVER), encoding="utf-8")
    log = tmp_path / "messages.jsonl"
    code = CodeTools(workspace)
    await code.configure("fake", [sys.executable, str(script), str(log)], ["python"])
    return code, workspace, log


def _messages(log: Path) -> list[dict]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


@pytest.mark.asyncio
async def test_lsp_framing_unicode_positions_and_document_changes(tmp_path):
    code, workspace, log = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("α😀target\n", encoding="utf-8")
    try:
        hover = await code.hover("fake", path, line=1, character=3)
        assert hover == {
            "contents": "symbol **docs**",
            "range": {
                "start": {"line": 1, "character": 3},
                "end": {"line": 1, "character": 9},
            },
            "truncated": False,
        }
        definition = await code.definition("fake", path, line=1, character=3)
        assert definition["locations"][0]["range"]["start"] == {"line": 1, "character": 3}
        path.write_text("prefix\nα😀target\n", encoding="utf-8")
        await code.references("fake", path, line=2, character=3)
        messages = _messages(log)
        assert [
            item["method"] for item in messages if item["method"].endswith(("didOpen", "didChange"))
        ] == [
            "textDocument/didOpen",
            "textDocument/didChange",
        ]
        sync_messages = [item for item in messages if "version" in item]
        assert [item["version"] for item in sync_messages] == [1, 2]
        positions = [item["position"] for item in messages if "position" in item]
        assert positions == [{"line": 0, "character": 3}] * 2 + [{"line": 1, "character": 3}]
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_push_diagnostics_are_versioned_and_empty_is_ready(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("bad\n", encoding="utf-8")
    try:
        first = await code.diagnostics("fake", path, wait_ms=1000)
        assert first["ready"] is True
        assert first["version"] == 1
        assert first["diagnostics"][0]["message"] == "bad source"
        path.write_text("ok\n", encoding="utf-8")
        second = await code.diagnostics("fake", path, wait_ms=1000)
        assert second["ready"] is True
        assert second["version"] == 2
        assert second["diagnostics"] == []
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_push_diagnostics_pending_is_not_reported_as_empty(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("pending\n", encoding="utf-8")
    try:
        result = await code.diagnostics("fake", path, wait_ms=0)
        assert result["ready"] is False
        assert result["diagnostics"] is None
        assert result["state"] == "pending"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_unversioned_push_diagnostics_remain_uncertain(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("unknown\n", encoding="utf-8")
    try:
        result = await code.diagnostics("fake", path, wait_ms=1000)
        assert result["ready"] is False
        assert result["diagnostics"] is None
        assert result["state"] == "version_unknown"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_cancelled_lsp_request_sends_cancel_notification(tmp_path):
    code, workspace, log = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("\n" * 99, encoding="utf-8")
    try:
        task = asyncio.create_task(code.hover("fake", path, line=99, character=1))
        for _ in range(50):
            if any(item["method"] == "textDocument/hover" for item in _messages(log)):
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(50):
            if any(item.get("method") == "$/cancelRequest" for item in _messages(log)):
                break
            await asyncio.sleep(0.01)
        assert any(item.get("method") == "$/cancelRequest" for item in _messages(log))
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_code_paths_cannot_escape_workspace(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    outside = tmp_path / "outside.py"
    outside.write_text("name = 1\n", encoding="utf-8")
    try:
        with pytest.raises(ValueError, match="inside the workspace"):
            await code.hover("fake", outside, line=1, character=1)
    finally:
        await code.aclose()


@pytest.mark.parametrize("invalid", [{}, {"jsonrpc": "2.0", "id": [], "result": {}}])
async def test_malformed_live_server_fails_initialization_promptly(tmp_path, invalid):
    start = FAKE_SERVER.index('    if method == "initialize":')
    end = FAKE_SERVER.index('    elif method in ("textDocument/didOpen"')
    source = (
        FAKE_SERVER[:start]
        + f'    if method == "initialize":\n        send({invalid!r})\n'
        + FAKE_SERVER[end:]
    )
    server = tmp_path / "malformed.py"
    server.write_text(source)
    code = CodeTools(tmp_path)
    try:
        async with asyncio.timeout(5):
            with pytest.raises(CodeError, match="JSON-RPC"):
                await code.configure(
                    "broken", [sys.executable, str(server), str(tmp_path / "log")], ["python"]
                )
    finally:
        await code.aclose()
