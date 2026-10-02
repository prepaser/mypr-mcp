from __future__ import annotations

import asyncio
import base64
import socket

import pytest_asyncio
from conftest import execute, mcp_session, stop_manager
from test_network_integration import _json_cell


@pytest_asyncio.fixture
async def udp_echo():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.setblocking(False)
    loop = asyncio.get_running_loop()

    async def serve():
        while True:
            data, peer = await loop.sock_recvfrom(sock, 65535)
            await loop.sock_sendto(sock, data, peer)

    task = asyncio.create_task(serve())
    try:
        yield sock.getsockname()[1]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        sock.close()


@pytest_asyncio.fixture
async def udp_silent():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    try:
        yield sock.getsockname()[1]
    finally:
        sock.close()


async def test_udp_binary_results_survive_reset_and_restart(workspace, udp_echo):
    payload = b"\x00\xffmypr-udp"
    async with mcp_session(workspace) as session:
        data = await _json_cell(
            session,
            f"scan = await ws.net.scan('localhost', protocol='udp', ports=[{udp_echo}], "
            f"family='ipv4', payload={payload!r}, capture_response=True, "
            "rate=1000, per_host_rate=1000, retries=0)\n"
            "summary = await scan\n"
            "page = await scan.results(max_entries=1)\n"
            "print(json.dumps({'id': scan.id, 'summary': summary, 'page': page}))",
        )
        assert data["summary"]["state"] == "succeeded"
        assert data["summary"]["mode"] == "udp" and data["summary"]["complete"] is True
        assert data["summary"]["attempts"] == data["summary"]["completed"] == 1
        result = data["page"]["results"][0]
        assert result["state"] == "open" and result["protocol"] == "udp"
        assert base64.b64decode(result["response_b64"]) == payload
        assert data["page"]["complete"] is True

        reset = await execute(session, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded", reset
        restored = await _json_cell(
            session,
            f"scan = await ws.tasks.attach({data['id']!r})\n"
            "print(json.dumps({'summary': await scan, 'page': await scan.results()}))",
        )
        assert restored["summary"]["complete"] is True
        assert restored["page"]["results"] == data["page"]["results"]

    await stop_manager(workspace)
    async with mcp_session(workspace) as session:
        restored = await _json_cell(
            session,
            f"scan = await ws.tasks.attach({data['id']!r})\n"
            "print(json.dumps({'summary': await scan, 'page': await scan.results()}))",
        )
        assert restored["summary"]["state"] == "succeeded"
        assert restored["summary"]["complete"] is True
        assert restored["page"]["results"] == data["page"]["results"]


async def test_udp_cancellation_remains_incomplete_over_mcp(workspace, udp_silent):
    async with mcp_session(workspace) as session:
        data = await _json_cell(
            session,
            f"scan = await ws.net.scan('127.0.0.1', protocol='udp', ports=[{udp_silent}], "
            "per_host_rate=0.01, max_probes=100, retries=3)\n"
            "await scan.cancel()\n"
            "print(json.dumps(await scan.summary(wait_ms=1000)))",
        )
        assert data["state"] in {"cancelled", "lost"}
        assert data["complete"] is False
