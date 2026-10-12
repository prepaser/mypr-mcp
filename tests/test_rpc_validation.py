from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from mypr_mcp.diagnostics import RPCError
from mypr_mcp.kernel_api import _rpc
from mypr_mcp.transport import attachment, rpc

INVALID_RESPONSES = (b"\xff\n", b"{\n", b"[]\n")


@asynccontextmanager
async def fake_peer(
    tmp_path: Path,
    response: bytes,
    *,
    wait_for_eof: bool = False,
):
    path = tmp_path / "rpc.sock"
    handlers: set[asyncio.Task[None]] = set()
    request_received = asyncio.Event()
    peer_closed = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            await reader.readline()
            request_received.set()
            writer.write(response)
            await writer.drain()
            if wait_for_eof:
                await reader.read()
                peer_closed.set()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_unix_server(handle, path=str(path))
    try:
        yield path, request_received, peer_closed
    finally:
        server.close()
        await server.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)


@pytest.mark.parametrize("response", INVALID_RESPONSES)
async def test_rpc_rejects_invalid_envelopes(tmp_path: Path, response: bytes) -> None:
    async with fake_peer(tmp_path, response) as (path, received, _):
        with pytest.raises(RPCError) as caught:
            await rpc(path, op="status")

    assert received.is_set()
    assert str(caught.value) == "Workspace manager returned invalid RPC data"
    assert caught.value.code == "protocol_error"


@pytest.mark.parametrize("response", INVALID_RESPONSES)
async def test_attachment_rejects_invalid_envelopes(tmp_path: Path, response: bytes) -> None:
    async with fake_peer(tmp_path, response) as (path, received, _):
        with pytest.raises(RPCError) as caught:
            async with attachment(path, "connection"):
                raise AssertionError("invalid attach response was accepted")

    assert received.is_set()
    assert str(caught.value) == "Workspace manager returned invalid attach data"
    assert caught.value.code == "protocol_error"


@pytest.mark.parametrize("response", INVALID_RESPONSES)
async def test_kernel_rpc_rejects_invalid_envelopes(
    tmp_path: Path,
    response: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with fake_peer(tmp_path, response) as (path, received, _):
        monkeypatch.setenv("MYPR_SOCKET", str(path))
        with pytest.raises(RPCError) as caught:
            await _rpc("status")

    assert received.is_set()
    assert str(caught.value) == "invalid response from workspace manager"
    assert caught.value.code == "protocol_error"


async def test_rpc_requires_result_for_success(tmp_path: Path) -> None:
    async with fake_peer(tmp_path, b'{"ok":true}\n') as (path, _, _):
        with pytest.raises(RPCError) as caught:
            await rpc(path, op="status")

    assert str(caught.value) == "Workspace manager returned an RPC response without a result"
    assert caught.value.code == "protocol_error"


async def test_kernel_rpc_preserves_optional_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with fake_peer(tmp_path, b'{"ok":true}\n') as (path, _, _):
        monkeypatch.setenv("MYPR_SOCKET", str(path))
        assert await _rpc("status") is None


async def test_attachment_preserves_optional_result(tmp_path: Path) -> None:
    async with fake_peer(tmp_path, b'{"ok":true}\n') as (path, _, _):
        async with attachment(path, "connection") as attached:
            assert attached.status is None


@pytest.mark.parametrize("client", ("transport", "kernel"))
async def test_one_shot_rpc_cancellation_closes_pending_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, client: str
) -> None:
    async with fake_peer(tmp_path, b"", wait_for_eof=True) as (path, received, peer_closed):
        if client == "transport":
            call = rpc(path, op="status")
        else:
            monkeypatch.setenv("MYPR_SOCKET", str(path))
            call = _rpc("status")
        task = asyncio.create_task(call)
        try:
            await asyncio.wait_for(received.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(peer_closed.wait(), timeout=1)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_attachment_closes_handshake_socket_on_invalid_response(tmp_path: Path) -> None:
    async with fake_peer(tmp_path, b"{\n", wait_for_eof=True) as (
        path,
        _,
        peer_closed,
    ):
        with pytest.raises(RPCError):
            async with attachment(path, "connection"):
                raise AssertionError("invalid attach response was accepted")
        await asyncio.wait_for(peer_closed.wait(), timeout=1)


@pytest.mark.parametrize("client", ("transport", "kernel"))
async def test_error_details_must_be_an_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client: str,
) -> None:
    response = json.dumps(
        {
            "ok": False,
            "error": "remote failure",
            "error_info": {"code": "remote_failure", "details": ["ignored"]},
        }
    ).encode() + b"\n"
    async with fake_peer(tmp_path, response) as (path, _, _):
        if client == "transport":
            call = rpc(path, op="status")
        else:
            monkeypatch.setenv("MYPR_SOCKET", str(path))
            call = _rpc("status")
        with pytest.raises(RPCError) as caught:
            await call

    assert str(caught.value) == "remote failure"
    assert caught.value.code == "remote_failure"
    assert caught.value.details == {}
    if client == "kernel":
        assert caught.value.operation == "status"
