from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from mypr_mcp.code_tools import (
    CodeError,
    CodeTools,
    _DiagnosticOutputLimitError,
    _json_size,
    _LanguageServer,
)


def _server(root: Path, generation: str = "generation") -> _LanguageServer:
    server = object.__new__(_LanguageServer)
    server.root = root
    server.name = "demo"
    server.generation = generation
    server.capabilities = {"workspaceSymbolProvider": True}
    server.initialized = True
    server.timeout = 1
    server.process = SimpleNamespace(returncode=None)
    server._failure = None
    server._operation_lock = asyncio.Lock()
    server._closed = False
    server._closing = False
    return server


def _navigation_server(
    root: Path, capabilities: dict[str, object], request
) -> _LanguageServer:
    server = _LanguageServer(root, "demo", ("demo",), frozenset({"python"}), 1)
    server.capabilities = capabilities
    server.initialized = True

    async def notify(*_args, **_kwargs):
        return False

    server._notify_committed = notify
    server._request = request
    return server


def _long_document(root: Path) -> Path:
    directory = root / ("a" * 200) / ("b" * 200)
    directory.mkdir(parents=True)
    path = directory / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    return path


def _wrapper(server: _LanguageServer) -> CodeTools:
    code = object.__new__(CodeTools)

    async def get_server(_name):
        return server

    code._get_server = get_server
    return code


@pytest.mark.asyncio
async def test_workspace_symbols_rejects_metadata_larger_than_budget(tmp_path: Path):
    server = _server(tmp_path)

    async def request(*_args, **_kwargs):
        return []

    server._request = request
    code = object.__new__(CodeTools)

    async def get_server(_name):
        return server

    code._get_server = get_server
    with pytest.raises(ValueError, match="workspace symbol metadata"):
        await code.workspace_symbols("demo", "x" * 4096, max_bytes=512)


@pytest.mark.asyncio
async def test_document_symbols_rejects_metadata_larger_than_budget(tmp_path: Path):
    path = _long_document(tmp_path)

    async def request(*_args, **_kwargs):
        return []

    server = _navigation_server(
        tmp_path, {"documentSymbolProvider": True}, request
    )
    code = _wrapper(server)
    try:
        with pytest.raises(ValueError, match="document symbol metadata"):
            await code.document_symbols("demo", path, max_bytes=512)
    finally:
        await server.aclose()


@pytest.mark.asyncio
async def test_calls_rejects_metadata_larger_than_budget(tmp_path: Path):
    path = _long_document(tmp_path)

    async def request(*_args, **_kwargs):
        return []

    server = _navigation_server(
        tmp_path, {"callHierarchyProvider": True}, request
    )
    code = _wrapper(server)
    try:
        with pytest.raises(ValueError, match="call hierarchy metadata"):
            await code.calls("demo", path, 1, 1, direction="outgoing", max_bytes=512)
    finally:
        await server.aclose()


@pytest.mark.asyncio
async def test_timed_out_calls_reject_metadata_larger_than_budget(tmp_path: Path):
    path = _long_document(tmp_path)

    async def request(method, *_args, **_kwargs):
        if method == "textDocument/prepareCallHierarchy":
            await asyncio.sleep(0.01)
            raise CodeError("LSP request textDocument/prepareCallHierarchy timed out")
        return []

    server = _navigation_server(
        tmp_path, {"callHierarchyProvider": True}, request
    )
    server.timeout = 0.001
    code = _wrapper(server)
    try:
        with pytest.raises(ValueError, match="call hierarchy metadata"):
            await code.calls("demo", path, 1, 1, direction="outgoing", max_bytes=512)
    finally:
        await server.aclose()


@pytest.mark.asyncio
async def test_workspace_diagnostics_advances_unchanged_cache_metadata(tmp_path: Path):
    path = tmp_path / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    server = _server(tmp_path)
    server.capabilities = {"diagnosticProvider": {"workspaceDiagnostics": True}}
    calls: list[list[dict[str, str]] | None] = []

    async def workspace_diagnostics(previous):
        calls.append(previous)
        if len(calls) == 1:
            return {
                "items": [{
                    "uri": path.as_uri(),
                    "kind": "full",
                    "version": 1,
                    "resultId": "one",
                    "items": [],
                }]
            }
        version = None if len(calls) == 3 else len(calls)
        return {
            "items": [{
                "uri": path.as_uri(),
                "kind": "unchanged",
                "version": version,
                "resultId": f"result-{len(calls)}",
            }]
        }

    server.workspace_diagnostics = workspace_diagnostics
    server._text_for_uri = _text_for_uri
    server.aclose = _async_noop
    code = CodeTools(tmp_path)
    code._servers["demo"] = server
    code._definitions = {"demo": {"command": ["demo"], "languages": ["python"], "timeout": 1}}
    try:
        first = await code.workspace_diagnostics("demo")
        second = await code.workspace_diagnostics("demo")
        third = await code.workspace_diagnostics("demo")
        assert first["reports"][0]["result_id"] == "one"
        assert second["reports"][0]["result_id"] == "result-2"
        assert second["reports"][0]["version"] == 2
        assert third["reports"][0]["result_id"] == "result-3"
        assert third["reports"][0]["version"] is None
        fourth = await code.workspace_diagnostics("demo")
        assert fourth["reports"][0]["result_id"] == "result-4"
        assert fourth["reports"][0]["version"] == 4
        assert calls[1] == [{"uri": path.as_uri(), "value": "one"}]
        assert calls[2] == [{"uri": path.as_uri(), "value": "result-2"}]
        assert calls[3] == [{"uri": path.as_uri(), "value": "result-3"}]
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_workspace_diagnostic_cache_is_bounded_by_count_and_bytes(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr("mypr_mcp.code_tools.MAX_WORKSPACE_DIAGNOSTIC_REPORTS", 2)
    monkeypatch.setattr("mypr_mcp.code_tools.MAX_WORKSPACE_DIAGNOSTIC_BYTES", 1_200)
    path = tmp_path / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    server = _server(tmp_path)
    server.capabilities = {"diagnosticProvider": {"workspaceDiagnostics": True}}
    calls = 0

    async def workspace_diagnostics(_previous):
        nonlocal calls
        calls += 1
        current = tmp_path / f"sample-{calls}.py"
        return {
            "items": [{
                "uri": current.as_uri(),
                "kind": "full",
                "resultId": str(calls),
                "items": [],
            }]
        }

    server.workspace_diagnostics = workspace_diagnostics
    server._text_for_uri = _text_for_uri
    server.aclose = _async_noop
    code = CodeTools(tmp_path)
    code._servers["demo"] = server
    try:
        for _ in range(5):
            await code.workspace_diagnostics("demo")
        cache = code._workspace_diag_results["demo"]
        assert len(cache) <= 2
        assert sum(_json_size(item) for item in cache.values()) <= 1_200
    finally:
        await code.aclose()


async def _async_noop():
    return None


@pytest.mark.asyncio
async def test_obsolete_workspace_diagnostics_response_does_not_repopulate_cache(
    tmp_path: Path,
):
    path = tmp_path / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    server = _server(tmp_path, "old")
    entered = asyncio.Event()
    release = asyncio.Event()

    async def workspace_diagnostics(_previous):
        entered.set()
        await release.wait()
        return {
            "items": [{
                "uri": path.as_uri(),
                "kind": "full",
                "version": 1,
                "resultId": "old",
                "items": [],
            }]
        }

    server.workspace_diagnostics = workspace_diagnostics
    server._text_for_uri = _text_for_uri
    server.aclose = _async_noop
    replacement = _server(tmp_path, "new")
    replacement.workspace_diagnostics = lambda _previous: _empty_report(path)
    replacement._text_for_uri = _text_for_uri
    replacement.aclose = _async_noop
    code = CodeTools(tmp_path)
    code._servers["demo"] = server
    code._definitions = {"demo": {"command": ["demo"], "languages": ["python"], "timeout": 1}}
    task = asyncio.create_task(code.workspace_diagnostics("demo"))
    await entered.wait()
    code._servers["demo"] = replacement
    code._clear_workspace_diagnostics("demo")
    release.set()
    with pytest.raises(CodeError, match="changed"):
        await task
    assert "demo" not in code._workspace_diag_results
    await code.aclose()


@pytest.mark.asyncio
async def test_workspace_diagnostics_rechecks_generation_after_source_read(
    tmp_path: Path,
):
    path = tmp_path / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    server = _server(tmp_path, "old")
    entered = asyncio.Event()
    release = asyncio.Event()

    async def workspace_diagnostics(_previous):
        return {
            "items": [{
                "uri": path.as_uri(),
                "kind": "full",
                "version": 1,
                "resultId": "old",
                "items": [],
            }]
        }

    async def read_source(_uri, _cache):
        entered.set()
        await release.wait()
        return "value = 1\n"

    server.workspace_diagnostics = workspace_diagnostics
    server._text_for_uri = read_source
    server.aclose = _async_noop
    replacement = _server(tmp_path, "new")
    replacement.aclose = _async_noop
    code = CodeTools(tmp_path)
    code._servers["demo"] = server
    code._definitions = {"demo": {"command": ["demo"], "languages": ["python"], "timeout": 1}}
    task = asyncio.create_task(code.workspace_diagnostics("demo"))
    await entered.wait()
    code._servers["demo"] = replacement
    code._clear_workspace_diagnostics("demo")
    release.set()
    with pytest.raises(CodeError, match="changed"):
        await task
    assert "demo" not in code._workspace_diag_results
    await code.aclose()


async def _empty_report(path: Path):
    return {
        "items": [{
            "uri": path.as_uri(),
            "kind": "full",
            "version": 1,
            "resultId": "new",
            "items": [],
        }]
    }


async def _text_for_uri(_uri, _cache):
    return "value = 1\n"


@pytest.mark.asyncio
async def test_workspace_diagnostics_rejects_non_progressing_oversized_item(tmp_path: Path):
    workspace = tmp_path / ("x" * 190)
    workspace.mkdir()
    path = workspace / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    server = _server(workspace)
    server.capabilities = {"diagnosticProvider": {"workspaceDiagnostics": True}}
    server.workspace_diagnostics = lambda _previous: _empty_report(path)
    server._text_for_uri = _text_for_uri
    server.aclose = _async_noop
    code = CodeTools(workspace)
    code._servers["demo"] = server
    code._definitions = {"demo": {"command": ["demo"], "languages": ["python"], "timeout": 1}}
    try:
        with pytest.raises(_DiagnosticOutputLimitError) as failure:
            await code.workspace_diagnostics("demo", max_bytes=512)
        assert failure.value.next_cursor == failure.value.details["page_cursor"]
        page = await code.workspace_diagnostics(
            "demo", cursor=failure.value.next_cursor, max_bytes=32768
        )
        assert page["reports"][0]["path"] == str(path)
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_code_actions_rejects_oversized_empty_result_metadata(tmp_path: Path):
    workspace = tmp_path
    for _index in range(18):
        workspace = workspace / ("x" * 90)
        workspace.mkdir()
    path = workspace / "sample.py"
    path.write_text("value = 1\n", encoding="utf-8")
    server = _server(workspace)
    document = SimpleNamespace(
        path=path,
        uri=path.as_uri(),
        version=1,
        text="value = 1\n",
    )
    server.documents = {document.uri: document}

    async def code_actions(*_args, **_kwargs):
        return document, []

    server.code_actions = code_actions
    server.aclose = _async_noop
    code = CodeTools(workspace)
    code._servers["demo"] = server
    try:
        with pytest.raises(ValueError, match="code action metadata"):
            await code.actions("demo", path, 1, 1, max_bytes=512)
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_workspace_diagnostic_cursor_survives_server_generation_change(tmp_path: Path):
    code = CodeTools(tmp_path)
    server = _server(tmp_path, "new")
    server.aclose = _async_noop
    code._servers["demo"] = server
    item = {"uri": "file:///tmp/sample.py", "path": "/tmp/sample.py", "diagnostics": []}
    ident = code._diagnostic_snapshots.create(
        {"server": "demo"}, [item], kind="workspace-diagnostic"
    )
    cursor = code._diagnostic_snapshots.cursor(ident, 0, "workspace-diagnostic")
    try:
        result = await code.workspace_diagnostics("demo", cursor=cursor)
        assert result["reports"] == [item]
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_apply_definitions_clears_only_changed_diagnostic_caches(tmp_path: Path):
    code = CodeTools(tmp_path)
    old = {
        "keep": {"command": ["keep"], "languages": ["python"], "timeout": 1.0},
        "change": {"command": ["old"], "languages": ["python"], "timeout": 1.0},
    }
    new = {
        **old,
        "change": {"command": ["new"], "languages": ["python"], "timeout": 1.0},
    }
    code._definitions = old
    code._workspace_diag_results = {"keep": {"uri": {}}, "change": {"uri": {}}}
    code._workspace_diag_generations = {"keep": "keep", "change": "change"}
    try:
        result = await code.apply_definitions(new, "new")
        assert result["changed"] == ["change"]
        assert "keep" in code._workspace_diag_results
        assert "change" not in code._workspace_diag_results
    finally:
        await code.aclose()
