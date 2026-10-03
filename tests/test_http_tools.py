from __future__ import annotations

import asyncio
import gzip
import os
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


async def test_client_dependency_preparation_follows_options(tools: HTTPTools):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append((names, automatic))

    tools._ensure_dependencies = ensure
    await tools._prepare_client_dependencies(
        {"http2": True, "proxy": "socks5://127.0.0.1:1080"}
    )
    assert calls == [(('httpx2', 'h2', 'socksio'), True)]


def test_socks_proxy_detection_includes_environment(monkeypatch):
    monkeypatch.setattr(
        "urllib.request.getproxies",
        lambda: {"https": "socks5://127.0.0.1:1080", "no": "socks5://ignored"},
    )
    assert HTTPTools._needs_socksio({"trust_env": True})
    assert not HTTPTools._needs_socksio({"trust_env": False})
    assert not HTTPTools._needs_socksio({"trust_env": True, "proxy": "http://proxy"})
    assert HTTPTools._needs_socksio({"trust_env": True, "proxy": None})
    assert HTTPTools._needs_socksio({"trust_env": True, "mounts": {"all://": object()}})
    assert not HTTPTools._needs_socksio({"trust_env": True, "transport": object()})


def test_socks_proxy_detection_ignores_no_proxy_environment(monkeypatch):
    monkeypatch.setattr(
        "urllib.request.getproxies", lambda: {"no": "socks5://127.0.0.1:1080"}
    )
    assert not HTTPTools._needs_socksio({"trust_env": True})


async def test_concurrent_client_creation_is_singleflight_without_a_lock(tools: HTTPTools):
    calls = 0
    entered = asyncio.Event()
    all_entered = asyncio.Event()
    release = asyncio.Event()

    async def ensure(*names, automatic=False):
        nonlocal calls
        calls += 1
        entered.set()
        if calls == 2:
            all_entered.set()
        await release.wait()

    tools._ensure_dependencies = ensure
    first = asyncio.create_task(tools.client("race", trust_env=False))
    await entered.wait()
    second = asyncio.create_task(tools.client("race", trust_env=False))
    await all_entered.wait()
    release.set()
    left, right = await asyncio.gather(first, second)
    assert left is right
    assert len(tools._clients) == 1
    await tools.aclose()


async def test_concurrent_client_creation_reports_option_conflict(tools: HTTPTools):
    entered = asyncio.Event()
    all_entered = asyncio.Event()
    release = asyncio.Event()

    calls = 0

    async def ensure(*names, automatic=False):
        nonlocal calls
        calls += 1
        entered.set()
        if calls == 2:
            all_entered.set()
        await release.wait()

    tools._ensure_dependencies = ensure
    first = asyncio.create_task(tools.client("race", trust_env=False, timeout=1))
    await entered.wait()
    second = asyncio.create_task(tools.client("race", trust_env=False, timeout=2))
    await all_entered.wait()
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert sum(isinstance(result, RuntimeError) for result in results) == 1
    assert sum(not isinstance(result, BaseException) for result in results) == 1
    await tools.aclose()


async def test_client_creation_after_close_during_dependency_prep_fails(tools: HTTPTools):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def ensure(*names, automatic=False):
        entered.set()
        await release.wait()

    tools._ensure_dependencies = ensure
    task = asyncio.create_task(tools.client(trust_env=False))
    await entered.wait()
    await tools.aclose()
    release.set()
    with pytest.raises(RuntimeError, match="HTTP service is closed"):
        await task


async def test_invalid_url_does_not_prepare_dependencies(tmp_path):
    async def forbidden(*names, automatic=False):
        raise AssertionError("invalid URL must not prepare dependencies")

    tools = HTTPTools(tmp_path, ensure_dependencies=forbidden)
    with pytest.raises(ValueError, match="non-empty string"):
        await tools.get(" ")


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
    client = await tools.client()
    shared = await tools.client(shared=True)
    assert client is not shared
    with pytest.raises(RuntimeError, match="await ws.http.close"):
        await tools.client(timeout=1)
    await tools.close()
    replacement = await tools.client(timeout=1)
    assert replacement is not client
    await tools.aclose()


async def test_aclose_prevents_late_client_creation(tools: HTTPTools):
    await tools.aclose()
    await tools.aclose()
    with pytest.raises(RuntimeError, match="HTTP service is closed"):
        await tools.client()


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


async def test_download_reports_cleanup_failure_after_target_commit(
    tools: HTTPTools, http_server: str, tmp_path: Path, monkeypatch
):
    target = tmp_path / "result.bin"
    original_unlink = os.unlink

    def fail_temporary(path):
        if str(path).startswith(str(tmp_path / ".result.bin.")):
            raise OSError("temporary cleanup failed")
        return original_unlink(path)

    monkeypatch.setattr("mypr_mcp.http_tools.os.unlink", fail_temporary)
    result = await tools.download(f"{http_server}/large", target)
    assert result == target
    assert target.read_bytes() == b"x" * 64
    assert tools.last_warnings == [
        {"code": "download_cleanup_failed", "text": "temporary cleanup failed"}
    ]
    await tools.aclose()


async def test_download_warnings_are_scoped_to_overlapping_clients(
    tools: HTTPTools, http_server: str, tmp_path: Path, monkeypatch
):
    original_unlink = os.unlink

    def fail_first_temporary(path):
        if str(path).startswith(str(tmp_path / ".first.bin.")):
            raise OSError("first cleanup failed")
        return original_unlink(path)

    monkeypatch.setattr("mypr_mcp.http_tools.os.unlink", fail_first_temporary)
    first = asyncio.create_task(tools.download(f"{http_server}/slow", "first.bin"))
    await asyncio.sleep(0)
    tools._test_identity["id"] = "second"
    second = asyncio.create_task(tools.download(f"{http_server}/slow", "second.bin"))
    await asyncio.gather(first, second)

    tools._test_identity["id"] = "first"
    assert tools.last_warnings == [
        {"code": "download_cleanup_failed", "text": "first cleanup failed"}
    ]
    tools._test_identity["id"] = "second"
    assert tools.last_warnings == []
    await tools.aclose()


async def test_manually_closed_native_client_is_replaced(tools: HTTPTools):
    old = await tools.client()
    await old.aclose()
    replacement = await tools.client()
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
