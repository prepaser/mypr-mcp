from __future__ import annotations

import asyncio
import json
import sys
import textwrap
import time
from pathlib import Path

import pytest

from mypr_mcp.code_tools import MAX_RESULTS, CodeError, CodeTools

FAKE_SERVER = r"""
import json
import sys
import threading
import time

log_path = sys.argv[1]
sample_uri = __import__("pathlib").Path(sys.argv[2], "sample.py").as_uri()
lock = threading.Lock()

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
    with lock:
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
    result = None
    if method == "initialize":
        result = {"capabilities": {
            "textDocumentSync": 2,
            "documentSymbolProvider": True,
            "workspaceSymbolProvider": {"workDoneProgress": False},
            "callHierarchyProvider": True,
        }}
    elif method == "textDocument/didOpen":
        log(message)
    elif method == "textDocument/documentSymbol":
        result = [{"name": "outer", "kind": 5,
                   "range": {"start": {"line": 0, "character": 0},
                             "end": {"line": 1, "character": 0}},
                   "selectionRange": {"start": {"line": 0, "character": 0},
                                      "end": {"line": 0, "character": 5}},
                   "children": [{"name": "inner", "kind": 12,
                                 "range": {"start": {"line": 0, "character": 6},
                                           "end": {"line": 0, "character": 14}},
                                 "selectionRange": {"start": {"line": 0, "character": 6},
                                                    "end": {"line": 0, "character": 14}}}]}]
    elif method == "workspace/symbol":
        log(message)
        uri = sample_uri
        result = [{"name": "Outer", "kind": 5, "containerName": "module",
                   "location": {"uri": uri,
                                "range": {"start": {"line": 0, "character": 0},
                                          "end": {"line": 0, "character": 5}}}}]
    elif method == "textDocument/prepareCallHierarchy":
        log(message)
        uri = params["textDocument"]["uri"]
        base = {"kind": 12, "uri": uri,
                "range": {"start": {"line": 0, "character": 0},
                          "end": {"line": 0, "character": 14}}}
        first = {"start": {"line": 0, "character": 0},
                 "end": {"line": 0, "character": 6}}
        second = {"start": {"line": 0, "character": 7},
                  "end": {"line": 0, "character": 14}}
        result = [dict(base, name="target-A", selectionRange=first),
                  dict(base, name="target-B", selectionRange=second)]
    elif method in ("callHierarchy/incomingCalls", "callHierarchy/outgoingCalls"):
        item = params["item"]
        if len(sys.argv) > 3 and sys.argv[3] == "slow":
            time.sleep(0.65)
        log({"method": method, "name": item["name"]})
        other = item["uri"].replace("sample.py", "other.py")
        point_range = {"start": {"line": 0, "character": 0},
                       "end": {"line": 0, "character": 2}}
        if method.endswith("incomingCalls"):
            endpoint = {"name": "caller", "kind": 12, "uri": other,
                        "range": point_range, "selectionRange": point_range}
            result = [{"from": endpoint, "fromRanges": [point_range]}]
        else:
            endpoint = {"name": "callee", "kind": 12, "uri": other,
                        "range": point_range, "selectionRange": point_range}
            result = [{"to": endpoint, "fromRanges": [point_range]}]
    elif method == "$/cancelRequest":
        log(message)
    elif method in ("shutdown",):
        result = None
    elif method == "exit":
        break
    elif "id" in message:
        result = None
    if "id" in message:
        send({"jsonrpc": "2.0", "id": message["id"], "result": result})
"""


async def _configured(tmp_path: Path, *, slow: bool = False) -> tuple[CodeTools, Path, Path]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    script = tmp_path / "fake-lsp.py"
    script.write_text(textwrap.dedent(FAKE_SERVER), encoding="utf-8")
    log = tmp_path / "messages.jsonl"
    code = CodeTools(workspace)
    command = [sys.executable, str(script), str(log), str(workspace)]
    if slow:
        command.append("slow")
    await code.configure("fake", command, ["python"])
    return code, workspace, log


def _messages(log: Path) -> list[dict]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


@pytest.mark.asyncio
async def test_document_and_workspace_symbols_preserve_structure_and_coordinates(tmp_path):
    code, workspace, log = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("outer α😀inner\n", encoding="utf-8")
    try:
        document = await code.document_symbols("fake", path)
        assert document["truncated"] is False
        assert document["document_version"] == 1
        assert document["coordinate_system"] == "one_based_unicode_code_points"
        assert document["symbols"][0]["name"] == "outer"
        assert document["symbols"][0]["coordinate_source"] == "open_document"
        assert document["symbols"][0]["children"][0]["name"] == "inner"
        assert document["symbols"][0]["children"][0]["range"]["start"] == {
            "line": 1,
            "character": 7,
        }

        workspace_symbols = await code.workspace_symbols("fake", "Outer")
        assert workspace_symbols["query"] == "Outer"
        symbol = workspace_symbols["symbols"][0]
        assert symbol["name"] == "Outer"
        assert symbol["path"] == str(path)
        assert symbol["document_version"] == 1
        assert symbol["range_status"] == "converted"
        assert symbol["range"]["start"] == {"line": 1, "character": 1}
        assert symbol["range"]["end"] == {"line": 1, "character": 6}
        assert workspace_symbols["coordinate_system"] == "one_based_unicode_code_points"
        requests = [item for item in _messages(log) if item.get("method") == "workspace/symbol"]
        assert requests[0]["params"]["query"] == "Outer"
        assert "documentSymbolProvider" in code.status("fake")["capabilities"]
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_symbol_output_limit_and_missing_source_are_explicit(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("outer α😀inner\n", encoding="utf-8")
    try:
        result = await code.document_symbols("fake", path, max_bytes=512)
        assert result["truncated"] is True
        assert len(json.dumps(result, ensure_ascii=False).encode()) <= 512
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_document_symbol_depth_limit_marks_omitted_children(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("x\n", encoding="utf-8")
    point = {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}}
    tree = {
        "name": "n32",
        "kind": 12,
        "range": point,
        "selectionRange": point,
    }
    for depth in reversed(range(32)):
        tree = {
            "name": f"n{depth}",
            "kind": 12,
            "range": point,
            "selectionRange": point,
            "children": [tree],
        }

    async def document_symbols(_method, _params):
        return [tree]

    code._servers["fake"]._request = document_symbols
    try:
        result = await code.document_symbols("fake", path)
        assert result["truncated"] is True
        current = result["symbols"][0]
        included = 1
        while current.get("children"):
            current = current["children"][0]
            included += 1
        assert included == 32
    finally:
        await code.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["incoming", "outgoing"])
async def test_call_hierarchy_groups_ambiguous_candidates_and_converts_ranges(tmp_path, direction):
    code, workspace, log = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("target-A α😀target-B\n", encoding="utf-8")
    try:
        result = await code.calls("fake", path, 1, 10, direction=direction)
        assert result["direction"] == direction
        assert result["ambiguous"] is True
        assert result["truncated"] is False
        assert [group["item"]["name"] for group in result["groups"]] == ["target-A", "target-B"]
        assert [group["calls"][0]["item"]["name"] for group in result["groups"]] == [
            "caller" if direction == "incoming" else "callee",
            "caller" if direction == "incoming" else "callee",
        ]
        call = result["groups"][0]["calls"][0]
        assert call["item"]["range"] is None
        assert call["item"]["range_status"] == "source_unavailable"
        assert result["document_version"] == 1
        assert result["coordinate_system"] == "one_based_unicode_code_points"
        first_range = call["ranges"][0]
        if direction == "incoming":
            assert first_range["range"] is None
            assert first_range["coordinate_status"] == "source_unavailable"
        else:
            assert first_range["range"]["end"] == {"line": 1, "character": 3}
            assert first_range["coordinate_status"] == "converted"
        methods = [
            item for item in _messages(log) if item.get("method", "").startswith("callHierarchy/")
        ]
        assert len(methods) == 2
        assert {item["method"] for item in methods} == {f"callHierarchy/{direction}Calls"}
        prepare = next(
            item
            for item in _messages(log)
            if item.get("method") == "textDocument/prepareCallHierarchy"
        )
        assert prepare["params"]["position"] == {"line": 0, "character": 9}
    finally:
        await code.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["incoming", "outgoing"])
async def test_call_hierarchy_marks_truncated_range_lists(tmp_path, direction):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("f()\n", encoding="utf-8")
    uri = path.as_uri()
    point = {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}}
    item = {
        "name": "function",
        "kind": 12,
        "uri": uri,
        "range": point,
        "selectionRange": point,
    }
    ranges = [point] * (MAX_RESULTS + 1)

    async def call_hierarchy(method, _params, **_options):
        if method == "textDocument/prepareCallHierarchy":
            return [item]
        target_key = "from" if direction == "incoming" else "to"
        return [{target_key: item, "fromRanges": ranges}]

    code._servers["fake"]._request = call_hierarchy
    try:
        result = await code.calls(
            "fake", path, 1, 1, direction=direction, max_bytes=1024 * 1024
        )
        assert len(result["groups"][0]["calls"][0]["ranges"]) == MAX_RESULTS
        assert result["truncated"] is True
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_structure_capability_absence_is_an_error_not_an_empty_result(tmp_path):
    code, workspace, _ = await _configured(tmp_path)
    path = workspace / "sample.py"
    path.write_text("symbol\n", encoding="utf-8")
    try:
        code._servers["fake"].capabilities.pop("documentSymbolProvider")
        with pytest.raises(CodeError, match="does not support documentSymbolProvider"):
            await code.document_symbols("fake", path)
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_call_hierarchy_shares_one_timeout_across_candidates(tmp_path):
    code, workspace, log = await _configured(tmp_path, slow=True)
    path = workspace / "sample.py"
    path.write_text("target-A α😀target-B\n", encoding="utf-8")
    code._servers["fake"].timeout = 1
    started = time.monotonic()
    try:
        result = await code.calls("fake", path, 1, 10, direction="outgoing")
        elapsed = time.monotonic() - started
        assert elapsed < 1.7
        assert result["truncated"] is True
        assert len(result["groups"]) == 1
    finally:
        await code.aclose()
    assert any(item.get("method") == "$/cancelRequest" for item in _messages(log))


@pytest.mark.asyncio
async def test_cancelling_call_hierarchy_sends_bounded_cancel_notification(tmp_path):
    code, workspace, log = await _configured(tmp_path, slow=True)
    path = workspace / "sample.py"
    path.write_text("target-A α😀target-B\n", encoding="utf-8")
    task = asyncio.create_task(code.calls("fake", path, 1, 10, direction="outgoing"))
    try:
        await asyncio.sleep(0.1)
        started = time.monotonic()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < 0.3
        for _ in range(100):
            if any(item.get("method") == "$/cancelRequest" for item in _messages(log)):
                break
            await asyncio.sleep(0.02)
        assert any(item.get("method") == "$/cancelRequest" for item in _messages(log))
    finally:
        await code.aclose()
