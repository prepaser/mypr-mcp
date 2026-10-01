from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from mypr_mcp.code_tools import CodeError, CodeTools, _Document, _LanguageServer
from mypr_mcp.runtime import Runtime
from mypr_mcp.services import MCPBridge


@pytest.mark.asyncio
async def test_applied_lsp_reports_preserve_completion_order(tmp_path):
    code = CodeTools(tmp_path)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    received = []

    async def report(method, definitions=None, **fields):
        assert method == "applied_lsp"
        name = definitions["demo"]["command"][0]
        if name == "A":
            first_entered.set()
            await release_first.wait()
        received.append((name, fields["revision"]))

    code._config_rpc = report
    code._definitions = {"demo": {"command": ["A"], "languages": ["python"]}}
    code._config_revision = "rev-A"
    first = asyncio.create_task(code._report_applied_lsp())
    await first_entered.wait()
    code._definitions = {"demo": {"command": ["B"], "languages": ["python"]}}
    code._config_revision = "rev-B"
    second = asyncio.create_task(code._report_applied_lsp())
    await asyncio.sleep(0)
    release_first.set()
    await asyncio.gather(first, second)
    await code.aclose()

    assert received == [("A", "rev-A"), ("B", "rev-B")]


@pytest.mark.asyncio
async def test_stale_applied_lsp_report_does_not_overwrite_runtime_cache(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.mcp = SimpleNamespace()
    runtime.settings.reset_lsp_generation(runtime.generation)
    first = {"demo": {"command": ["A"], "languages": ["python"]}}
    second = {"demo": {"command": ["B"], "languages": ["python"]}}
    accepted = await runtime.dispatch({
        "op": "code_config", "generation": runtime.generation,
        "method": "applied_lsp", "definitions": first, "sequence": 2,
        "revision": "old",
    })
    stale = await runtime.dispatch({
        "op": "code_config", "generation": runtime.generation,
        "method": "applied_lsp", "definitions": second, "sequence": 1,
        "revision": "newer-on-disk",
    })

    assert accepted["recorded"] is True
    assert stale["recorded"] is False
    assert runtime.settings.applied["lsp"]["servers"]["demo"]["command"] == ["A"]


@pytest.mark.asyncio
async def test_applied_lsp_uses_kernel_sequence_when_other_config_changes(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.mcp = MCPBridge(tmp_path, global_path=runtime.config_store.global_path)
    runtime.settings.reset_lsp_generation(runtime.generation)
    runtime.config_store.set("limits.response_bytes", runtime.response_limit + 1024)
    definitions = {"demo": {"command": ["B"], "languages": ["python"]}}

    result = await runtime.code_config({
        "method": "applied_lsp",
        "definitions": definitions,
        "revision": "revision-from-before-unrelated-limit-change",
        "sequence": 1,
        "generation": runtime.generation,
    })

    assert result["recorded"] is True
    assert runtime.settings.applied["lsp"]["servers"]["demo"]["command"] == ["B"]


@pytest.mark.asyncio
async def test_lsp_control_result_cannot_overwrite_newer_report(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.mcp = MCPBridge(tmp_path, global_path=runtime.config_store.global_path)
    runtime.settings.reset_lsp_generation(runtime.generation)
    runtime.healthy = True
    old = {"demo": {"command": ["old"], "languages": ["python"]}}
    newer = {"demo": {"command": ["new"], "languages": ["python"]}}
    runtime.config_store.set("lsp.servers.demo", old["demo"])
    entered = asyncio.Event()
    release = asyncio.Event()

    async def apply(_snapshot, _generation, _force):
        entered.set()
        await release.wait()
        return {
            "applied": True,
            "changed": ["demo"],
            "deferred": [],
            "sequence": 2,
            "generation": runtime.generation,
        }

    runtime.settings._apply_lsp = apply
    reloading = asyncio.create_task(runtime.settings.reload())
    await entered.wait()
    report = await runtime.code_config({
        "method": "applied_lsp", "definitions": newer, "sequence": 3,
        "generation": runtime.generation,
    })
    release.set()
    await reloading

    assert report["recorded"] is True
    assert runtime.settings.applied["lsp"]["servers"]["demo"]["command"] == ["new"]


@pytest.mark.asyncio
async def test_lsp_sequence_resets_for_a_new_kernel_generation(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.mcp = SimpleNamespace()
    old_generation = runtime.generation
    runtime.settings.reset_lsp_generation(old_generation)
    assert runtime.settings.record_lsp({}, sequence=4, generation=old_generation)

    runtime.generation = "new-generation"
    runtime.settings.reset_lsp_generation(runtime.generation)
    assert runtime.settings.record_lsp({}, sequence=1, generation=runtime.generation)
    stale = await runtime.code_config({
        "method": "applied_lsp",
        "definitions": {},
        "sequence": 5,
        "generation": old_generation,
    })
    assert stale["recorded"] is False
    assert runtime.settings.lsp_sequence == 1


class _FailingStdin:
    def __init__(self):
        self.frames = []

    def write(self, frame):
        self.frames.append(frame)

    async def drain(self):
        raise TimeoutError


@pytest.mark.asyncio
@pytest.mark.parametrize("already_open", [False, True])
async def test_post_write_timeout_poisoned_server_does_not_retry_or_rollback(
    tmp_path, already_open
):
    path = Path(tmp_path) / "sample.py"
    text = "new\n" if already_open else "old\n"
    path.write_text(text, encoding="utf-8")
    stdin = _FailingStdin()
    server = _LanguageServer(
        Path(tmp_path), "demo", ("demo-lsp",), frozenset({"python"}), timeout=1
    )
    server.process = SimpleNamespace(stdin=stdin, returncode=None)
    server._terminate_group = lambda _signal: None
    if already_open:
        server.documents[path.as_uri()] = _Document(path, path.as_uri(), "python", "old\n", 1)
    with pytest.raises(CodeError, match="timed out"):
        await server._document(path)
    assert server._write_uncertain is True
    assert path.as_uri() in server.documents
    assert server.documents[path.as_uri()].text == text
    assert server.documents[path.as_uri()].version == (2 if already_open else 1)
    assert len(stdin.frames) == 1

    path.write_text("later\n", encoding="utf-8")
    with pytest.raises(CodeError, match="uncertain"):
        await server._document(path)
    assert server.documents[path.as_uri()].text == text
    assert len(stdin.frames) == 1


@pytest.mark.asyncio
async def test_post_write_timeout_drains_request_future(tmp_path):
    stdin = _FailingStdin()
    server = _LanguageServer(
        Path(tmp_path), "demo", ("demo-lsp",), frozenset({"python"}), timeout=1
    )
    server.process = SimpleNamespace(stdin=stdin, returncode=None)
    server._terminate_group = lambda _signal: None
    errors = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        with pytest.raises(CodeError, match="timed out"):
            await server._request("textDocument/hover", {})
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)
    assert server._pending == {}
    assert errors == []
