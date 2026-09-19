from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest_asyncio
from conftest import execute, mcp_session, result_text, stop_manager


async def _write_response(
    writer: asyncio.StreamWriter,
    status: int,
    headers: dict[str, str],
    payload: bytes,
) -> None:
    reasons = {200: "OK", 302: "Found", 401: "Unauthorized"}
    response_headers = {
        **headers,
        "Content-Length": str(len(payload)),
        "Connection": "close",
    }
    writer.write(
        f"HTTP/1.1 {status} {reasons[status]}\r\n".encode()
        + b"\r\n".join(f"{key}: {value}".encode() for key, value in response_headers.items())
        + b"\r\n\r\n"
        + payload
    )
    await writer.drain()


async def _http_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        raw = await reader.readuntil(b"\r\n\r\n")
        lines = raw[:-4].decode("latin-1").split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = {
            key.lower(): value.strip()
            for line in lines[1:]
            if ":" in line
            for key, value in [line.split(":", 1)]
        }
        length = int(headers.get("content-length", "0"))
        body = await reader.readexactly(length)
        path = urlsplit(target).path
        response_headers = {"Content-Type": "text/plain"}

        if path == "/set-cookie":
            payload = b"cookie-set"
            response_headers["Set-Cookie"] = "session=one; Path=/"
        elif path == "/cookie":
            payload = headers.get("cookie", "").encode()
        elif path == "/stream":
            response_headers.update({"Content-Type": "application/octet-stream"})
            chunks = (b"one\n", b"two\n", b"three\n")
            response_headers["Content-Length"] = str(sum(map(len, chunks)))
            response_headers["Connection"] = "close"
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + b"\r\n".join(
                    f"{key}: {value}".encode() for key, value in response_headers.items()
                )
                + b"\r\n\r\n"
            )
            await writer.drain()
            for chunk in chunks:
                writer.write(chunk)
                await writer.drain()
                await asyncio.sleep(0)
            return
        elif path == "/download":
            payload = b"downloaded-content"
            response_headers["Content-Type"] = "application/octet-stream"
        elif path == "/upload":
            payload = body
        elif path == "/too-large":
            payload = b"0123456789abcdef"
        else:
            payload = f"{method} {path}".encode()
        await _write_response(writer, 200, response_headers, payload)
    except asyncio.IncompleteReadError, ConnectionError, BrokenPipeError:
        pass
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


@pytest_asyncio.fixture
async def http_server():
    server = await asyncio.start_server(_http_handler, "127.0.0.1", 0)
    address = server.sockets[0].getsockname()
    try:
        yield f"http://{address[0]}:{address[1]}"
    finally:
        server.close()
        await server.wait_closed()


@pytest_asyncio.fixture
async def tcp_server():
    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    address = server.sockets[0].getsockname()
    try:
        yield address[1]
    finally:
        server.close()
        await server.wait_closed()


async def _json_cell(session, code: str) -> dict:
    payload = await execute(session, "import asyncio, json\n" + code)
    assert payload["state"] == "succeeded", payload.get("error") or result_text(payload)
    return json.loads(result_text(payload))


async def test_http_clients_are_exposed_and_isolated_per_client(workspace: Path, http_server: str):
    async with mcp_session(workspace) as first:
        own = await _json_cell(
            first,
            f"response = await ws.http.get({http_server + '/set-cookie'!r})\n"
            f"cookie = (await ws.http.get({http_server + '/cookie'!r})).text\n"
            "print(json.dumps({'status': response.status_code, 'cookie': cookie}))",
        )
        assert own == {"status": 200, "cookie": "session=one"}

        async with mcp_session(workspace) as second:
            isolated = await _json_cell(
                second,
                f"cookie = (await ws.http.get({http_server + '/cookie'!r})).text\n"
                "print(json.dumps({'cookie': cookie}))",
            )
            assert isolated["cookie"] == ""

            shared = await _json_cell(
                second,
                f"await ws.http.get({http_server + '/set-cookie'!r}, shared=True, name='shared')\n"
                f"cookie = (await ws.http.get({http_server + '/cookie'!r}, "
                "shared=True, name='shared')).text\n"
                "print(json.dumps({'cookie': cookie}))",
            )
            assert shared["cookie"] == "session=one"

        shared_again = await _json_cell(
            first,
            f"cookie = (await ws.http.get({http_server + '/cookie'!r}, "
            "shared=True, name='shared')).text\n"
            "print(json.dumps({'cookie': cookie}))",
        )
        assert shared_again["cookie"] == "session=one"


async def test_http_stream_upload_download_and_reset_cleanup(workspace: Path, http_server: str):
    async with mcp_session(workspace) as session:
        result = await _json_cell(
            session,
            f"async with ws.http.stream('GET', {http_server + '/stream'!r}) as response:\n"
            "    chunks = [chunk async for chunk in response.aiter_bytes()]\n"
            f"uploaded = await ws.http.post({http_server + '/upload'!r}, content=b'payload')\n"
            f"path = await ws.http.download({http_server + '/download'!r}, 'download.bin')\n"
            "print(json.dumps({'stream': b''.join(chunks).decode(), "
            "'upload': uploaded.text, 'download': path.read_bytes().decode()}))",
        )
        assert result == {
            "stream": "one\ntwo\nthree\n",
            "upload": "payload",
            "download": "downloaded-content",
        }
        assert (workspace / "download.bin").read_bytes() == b"downloaded-content"

        preserved = await _json_cell(
            session,
            "path = __import__('pathlib').Path('download.bin')\n"
            "path.write_bytes(b'old')\n"
            f"try:\n    await ws.http.download({http_server + '/too-large'!r}, path, "
            "overwrite=True, max_bytes=4)\n"
            "except Exception as error:\n    error_type = type(error).__name__\n"
            "else:\n    error_type = ''\n"
            "print(json.dumps({'error': error_type, 'content': path.read_bytes().decode()}))",
        )
        assert preserved == {"error": "BodyTooLarge", "content": "old"}

        rejected = await _json_cell(
            session,
            f"await ws.http.get({http_server + '/set-cookie'!r})\n"
            "blocker = ws.tasks.start(asyncio.sleep(10))\n"
            "try:\n"
            "    await ws.reset()\n"
            "except RuntimeError as error:\n"
            "    rejected = str(error)\n"
            "else:\n"
            "    rejected = ''\n"
            f"cookie = (await ws.http.get({http_server + '/cookie'!r})).text\n"
            "await blocker.cancel()\n"
            "print(json.dumps({'rejected': rejected, 'cookie': cookie}))",
        )
        assert "active" in rejected["rejected"]
        assert rejected["cookie"] == "session=one"

        reset = await execute(session, "await ws.reset()", wait_ms=15_000)
        assert reset["state"] == "succeeded", reset
        fresh = await _json_cell(
            session,
            f"cookie = (await ws.http.get({http_server + '/cookie'!r})).text\n"
            "print(json.dumps({'cookie': cookie}))",
        )
        assert fresh["cookie"] == ""


async def test_network_diagnostics_and_scan_attach_survive_reset_and_restart(
    workspace: Path, tcp_server: int
):
    async with mcp_session(workspace) as session:
        diagnostics = await _json_cell(
            session,
            f"resolved = await ws.net.resolve('127.0.0.1')\n"
            f"open_result = await ws.net.connect('127.0.0.1', {tcp_server})\n"
            f"closed_result = await ws.net.connect('127.0.0.1', {tcp_server + 1}, timeout=0.5)\n"
            "print(json.dumps({'resolved': resolved, 'open': open_result, "
            "'closed': closed_result}))",
        )
        assert diagnostics["resolved"]["addresses"]
        assert diagnostics["open"].get("open", diagnostics["open"].get("state") == "open")
        assert not diagnostics["closed"].get("open", diagnostics["closed"].get("state") == "open")

        scan_id = await _json_cell(
            session,
            f"scan = await ws.net.scan('127.0.0.1', ports=[{tcp_server}, {tcp_server + 1}], "
            "concurrency=2, rate=10000, timeout=0.5)\n"
            "summary = await scan\n"
            "page = await scan.results(max_entries=10)\n"
            "print(json.dumps({'id': scan.id, 'summary': summary, 'page': page}))",
        )
        assert scan_id["summary"]["state"] == "succeeded"
        assert scan_id["page"]["results"]
        assert {row["state"] for row in scan_id["page"]["results"]} <= {
            "open",
            "closed",
            "timeout",
            "unreachable",
        }

        reset = await execute(session, "await ws.reset()", wait_ms=15_000)
        assert reset["state"] == "succeeded", reset
        attached = await _json_cell(
            session,
            f"task = await ws.tasks.attach({scan_id['id']!r})\n"
            "summary = await task\n"
            "page = await task.results(max_entries=10)\n"
            "print(json.dumps({'summary': summary, 'page': page}))",
        )
        assert attached["summary"]["state"] == "succeeded"
        assert attached["page"]["results"]

    await stop_manager(workspace)
    async with mcp_session(workspace) as restarted:
        attached = await _json_cell(
            restarted,
            f"task = await ws.tasks.attach({scan_id['id']!r})\n"
            "print(json.dumps({'summary': await task, "
            "'page': await task.results(max_entries=10)}))",
        )
        assert attached["summary"]["state"] == "succeeded"
        assert attached["page"]["results"]
