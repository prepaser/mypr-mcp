from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from mypr_mcp.cli import stop_runtime
from mypr_mcp.transport import socket_path


@pytest.fixture(scope="session", autouse=True)
def test_runtime_environment(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    runtime = tmp_path_factory.mktemp("xdg-runtime")
    previous = os.environ.get("XDG_RUNTIME_DIR")
    cache_previous = os.environ.get("UV_CACHE_DIR")
    os.environ["XDG_RUNTIME_DIR"] = str(runtime)
    os.environ["UV_CACHE_DIR"] = "/tmp/mypr-uv-cache"
    Path(os.environ["UV_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("XDG_RUNTIME_DIR", None)
        else:
            os.environ["XDG_RUNTIME_DIR"] = previous
        if cache_previous is None:
            os.environ.pop("UV_CACHE_DIR", None)
        else:
            os.environ["UV_CACHE_DIR"] = cache_previous


@pytest_asyncio.fixture
async def workspace(tmp_path: Path) -> AsyncIterator[Path]:
    path = tmp_path / "workspace"
    path.mkdir()
    try:
        yield path
    finally:
        await stop_manager(path)
        await asyncio.to_thread(shutil.rmtree, path / ".mypr" / "venv", True)


@asynccontextmanager
async def mcp_session(
    workspace: Path,
    *,
    initialize_client: bool = True,
    client_id: str | None = None,
) -> AsyncIterator[ClientSession]:
    args = ["-m", "mypr_mcp.cli", "serve"]
    params = StdioServerParameters(
        command=sys.executable,
        args=args,
        cwd=str(workspace),
        env=dict(os.environ),
    )
    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            if initialize_client:
                arguments = {} if client_id is None else {"client_id": client_id}
                result = await session.call_tool("init", arguments)
                assert not result.is_error, result
            yield session


async def stop_manager(workspace: Path) -> None:
    with contextlib.suppress(Exception):
        await asyncio.wait_for(stop_runtime(socket_path(workspace), force=True), 35)
    for _ in range(100):
        if not socket_path(workspace).exists():
            return
        await asyncio.sleep(0.05)


def decode_result(result) -> dict:
    blocks = [block.text for block in result.content if hasattr(block, "text")]
    assert blocks, result
    return json.loads(blocks[0])


def result_text(payload: dict) -> str:
    return "".join(event.get("text", "") for event in payload.get("output", []))


async def execute(
    session: ClientSession,
    code: str,
    wait_ms: int = 10_000,
    request_id: str | None = None,
) -> dict:
    arguments = {"code": code, "wait_ms": wait_ms}
    if request_id is not None:
        arguments["request_id"] = request_id
    payload = decode_result(await session.call_tool("execute", arguments))
    if payload["state"] not in {"succeeded", "failed", "cancelled", "lost"}:
        payload = await poll_until_done(session, payload["exec_id"], initial=payload)
    return payload


async def poll_until_done(
    session: ClientSession,
    exec_id: str,
    *,
    initial: dict | None = None,
    deadline_seconds: float = 20,
) -> dict:
    deadline = asyncio.get_running_loop().time() + deadline_seconds
    cursor = (initial or {}).get("cursor", 0)
    payload = initial or {}
    events = list(payload.get("output", []))
    while asyncio.get_running_loop().time() < deadline:
        payload = decode_result(
            await session.call_tool("poll", {"exec_id": exec_id, "cursor": cursor, "wait_ms": 1000})
        )
        events.extend(payload.get("output", []))
        cursor = payload["cursor"]
        if payload["state"] in {"succeeded", "failed", "cancelled", "lost"}:
            payload["output"] = events
            payload["cursor"] = len(events)
            return payload
    raise AssertionError(f"execution did not finish: {payload}")
