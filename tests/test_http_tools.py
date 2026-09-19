from __future__ import annotations

import asyncio
import gzip
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from mypr_mcp.http_tools import BodyTooLarge, HTTPTools


async def _serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        raw = await reader.readuntil(b"\r\n\r\n")
        lines = raw[:-4].decode().split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
        length = int(headers.get("Content-Length", "0"))
        body = await reader.readexactly(length)
        path = urlsplit(target).path

        response_headers = {"Content-Type": "text/plain"}
        if path == "/set-cookie":
            payload = b"ok"
            response_headers["Set-Cookie"] = "session=one; Path=/"
        elif path == "/cookie":
            payload = headers.get("Cookie", "").encode()
        elif path == "/redirect":
            payload = b""
            response_headers.update({"Location": "/set-cookie", "Content-Length": "0"})
            await _write_response(writer, 302, response_headers, payload)
            return
        elif path == "/auth-upload":
            if headers.get("Authorization") != "Bearer token":
                await _write_response(writer, 401, response_headers, b"unauthorized")
                return
            payload = body
        elif path == "/large":
            payload = b"x" * 64
        elif path == "/gzip":
            payload = gzip.compress(b"decoded gzip body")
            response_headers["Content-Encoding"] = "gzip"
        elif path == "/slow":
            response_headers.update({"Content-Length": "64", "Connection": "close"})
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + b"\r\n".join(
                    f"{key}: {value}".encode() for key, value in response_headers.items()
                )
                + b"\r\n\r\n"
                + b"x" * 16
            )
            await writer.drain()
            for _ in range(3):
                await asyncio.sleep(0.1)
                writer.write(b"x" * 16)
                await writer.drain()
            return
        else:
            payload = f"{method} {path}".encode()
        await _write_response(writer, 200, response_headers, payload)
    except asyncio.IncompleteReadError, ConnectionError, BrokenPipeError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()


async def _write_response(
    writer: asyncio.StreamWriter, status: int, headers: dict[str, str], payload: bytes
) -> None:
    headers = {**headers, "Content-Length": str(len(payload)), "Connection": "close"}
    reason = {200: "OK", 302: "Found", 401: "Unauthorized"}[status]
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\n".encode()
        + b"\r\n".join(f"{key}: {value}".encode() for key, value in headers.items())
        + b"\r\n\r\n"
        + payload
    )
    await writer.drain()


@pytest.fixture
async def http_server():
    server = await asyncio.start_server(_serve, "127.0.0.1", 0)
    address = server.sockets[0].getsockname()
    yield f"http://{address[0]}:{address[1]}"
    server.close()
    await server.wait_closed()


@pytest.fixture
def tools(tmp_path: Path):
    identity = {"id": "first"}
    instance = HTTPTools(tmp_path, lambda: identity["id"])
    instance._test_identity = identity
    yield instance
    if instance._clients:
        raise AssertionError("HTTP clients must be closed by each test")


async def test_clients_isolate_cookies_and_follow_redirects(tools: HTTPTools, http_server: str):
    response = await tools.get(f"{http_server}/redirect", follow_redirects=True)
    assert response.status_code == 200
    assert (await tools.get(f"{http_server}/cookie")).text == "session=one"

    tools._identity = lambda: "second"
    assert (await tools.get(f"{http_server}/cookie")).text == ""
    await tools.aclose()


async def test_request_upload_auth_and_body_limit(tools: HTTPTools, http_server: str):
    response = await tools.post(
        f"{http_server}/auth-upload",
        headers={"Authorization": "Bearer token"},
        files={"file": ("payload.txt", b"payload")},
    )
    assert response.status_code == 200
    assert b"payload" in response.content

    with pytest.raises(BodyTooLarge):
        await tools.get(f"{http_server}/large", max_bytes=32)
    await tools.aclose()


async def test_request_preserves_native_gzip_metadata(tools: HTTPTools, http_server: str):
    response = await tools.get(f"{http_server}/gzip")
    assert response.headers["Content-Encoding"] == "gzip"
    assert response.text == "decoded gzip body"
    await tools.aclose()


async def test_shared_namespace_and_reconfiguration(tools: HTTPTools, http_server: str):
    tools._identity = lambda: "shared"
    client = tools.client()
    shared = tools.client(shared=True)
    assert client is not shared
    with pytest.raises(RuntimeError, match="await ws.http.close"):
        tools.client(timeout=1)
    await tools.close()
    replacement = tools.client(timeout=1)
    assert replacement is not client
    await tools.aclose()


async def test_aclose_prevents_late_client_creation(tools: HTTPTools):
    await tools.aclose()
    await tools.aclose()
    with pytest.raises(RuntimeError, match="HTTP service is closed"):
        tools.client()


async def test_stream_is_native_response(tools: HTTPTools, http_server: str):
    async with tools.stream("GET", f"{http_server}/large") as response:
        assert response.__class__.__module__.startswith("httpx2")
        assert len(await response.aread()) == 64
    await tools.aclose()


async def test_download_is_atomic_on_cancellation(
    tools: HTTPTools, http_server: str, tmp_path: Path
):
    target = tmp_path / "result.bin"
    target.write_bytes(b"old")
    task = asyncio.create_task(tools.download(f"{http_server}/slow", target, overwrite=True))
    await asyncio.sleep(0.12)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert target.read_bytes() == b"old"
    assert await asyncio.to_thread(lambda: list(tmp_path.glob(".result.bin.*"))) == []
    await tools.aclose()


async def test_download_refuses_existing_target(tools: HTTPTools, http_server: str, tmp_path: Path):
    target = tmp_path / "result.bin"
    target.write_bytes(b"old")
    with pytest.raises(FileExistsError):
        await tools.download(f"{http_server}/large", target)
    assert target.read_bytes() == b"old"
    await tools.aclose()


async def test_manually_closed_native_client_is_replaced(tools: HTTPTools):
    old = tools.client()
    await old.aclose()
    replacement = tools.client()
    assert replacement is not old
    assert not replacement.is_closed
    await tools.aclose()


async def test_named_close_finishes_after_repeated_cancellation(tools: HTTPTools):
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockingClient:
        is_closed = False

        async def aclose(self):
            started.set()
            await release.wait()
            self.is_closed = True

    client = BlockingClient()
    key = ("client", "first", "default")
    tools._clients[key] = client
    tools._options[key] = {}

    task = asyncio.create_task(tools.close())
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.is_closed
    assert key in tools._clients

    await tools.close()
    assert key not in tools._clients
    await tools.aclose()
