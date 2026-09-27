from __future__ import annotations

import asyncio
import json
import os
import socket
from types import SimpleNamespace

import pytest

from mypr_mcp import system_base
from mypr_mcp.network_tools import NetworkTools
from mypr_mcp.system_tools import SystemTools


async def test_socket_filters_and_paging_use_a_stable_snapshot(tmp_path, monkeypatch):
    tools = NetworkTools(tmp_path)
    source = [{"pid": 12, "local_port": port} for port in range(6)]
    calls = 0

    async def collect(sections, *, timeout, output_limit, **options):  # noqa: ASYNC109
        nonlocal calls
        calls += 1
        return {"sockets": list(source), "total": len(source), "truncated": False, "warnings": []}

    monkeypatch.setattr(tools._system, "_collect", collect)
    first = await tools.sockets(protocol="tcp", local_port=42, limit=2)
    source.clear()
    assert first["cursor"] == first["next_cursor"]
    second = await tools.sockets(cursor=first["next_cursor"], limit=2)
    retry = await tools.sockets(cursor=first["next_cursor"], limit=2)
    third = await tools.sockets(cursor=second["next_cursor"], limit=2)
    ports = [item["local_port"] for item in first["sockets"] + second["sockets"] + third["sockets"]]
    assert ports == list(range(6))
    assert first["total"] == 6
    assert calls == 1
    assert retry == second
    second["sockets"][0]["local_port"] = -1
    replay = await tools.sockets(cursor=first["next_cursor"], limit=2)
    assert replay["sockets"][0]["local_port"] == 2


async def test_oversized_socket_record_is_skipped_without_repeating_cursor(tmp_path):
    tools = SystemTools(tmp_path)
    first = tools._socket_snapshots.page([{"path": "x" * 40000}, {"path": "small"}], limit=1)
    assert first["sockets"] == []
    assert first["omitted_by_output_limit"] == 1
    assert first["next_cursor"]
    second = tools._socket_snapshots.page(cursor=first["next_cursor"], limit=1)
    assert second["sockets"] == [{"path": "small"}]
    assert second["next_cursor"] is None
    assert len(json.dumps(first).encode()) <= 32768
    assert len(json.dumps(first).encode()) <= 32768


@pytest.mark.asyncio
async def test_local_tcp_udp_and_unix_sockets_are_discoverable(tmp_path):
    server = await asyncio.start_server(lambda _r, _w: None, "127.0.0.1", 0)
    tcp_port = server.sockets[0].getsockname()[1]
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("127.0.0.1", 0))
    udp_port = udp.getsockname()[1]
    unix_path = tmp_path / "diagnostic.sock"
    unix = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix.bind(str(unix_path))
    unix.listen()
    try:
        tools = NetworkTools(tmp_path)
        tcp = await tools.sockets(protocol="tcp", local_port=tcp_port, pid=os.getpid())
        udp_page = await tools.sockets(protocol="udp", local_port=udp_port, pid=os.getpid())
        unix_page = await tools.sockets(
            protocol="unix", local_address=str(unix_path), pid=os.getpid()
        )
        assert tcp["sockets"][0]["state"] == "LISTEN"
        assert udp_page["sockets"][0]["protocol"] == "udp"
        assert unix_page["sockets"][0]["family"] == "af_unix"
    finally:
        server.close()
        await server.wait_closed()
        udp.close()
        unix.close()


async def test_process_detail_reports_parent_and_optional_resources(tmp_path):
    tools = SystemTools(tmp_path)
    result = await tools.process(os.getpid(), children=True, open_files=True, sockets=True)
    assert result["status"] in {"ok", "partial"}
    assert result["process"]["pid"] == os.getpid()
    assert "parent" in result["process"]
    assert "children" in result["process"]
    assert "open_files" in result["process"]
    assert "sockets" in result["process"]


def test_process_detail_distinguishes_reused_pid(monkeypatch):
    class NoSuchProcess(Exception):
        pass

    observations = iter((100.0, 101.0))

    class Process:
        pid = 91

        def create_time(self):
            return next(observations)

        def __getattr__(self, name):
            return lambda: None

    fake = SimpleNamespace(Process=lambda _pid: Process(), NoSuchProcess=NoSuchProcess)
    monkeypatch.setattr(system_base, "_psutil", lambda: fake)
    result = system_base.collect("process_detail", {"pid": 91})
    assert result["status"] == "reused"
    assert result["process"] is None
    assert "changed identity" in result["warnings"][-1]


def test_process_detail_keeps_partial_fields_when_access_is_denied(monkeypatch):
    class NoSuchProcess(Exception):
        pass

    class AccessDenied(Exception):
        pass

    class Process:
        pid = 91

        def create_time(self):
            return 100.0

        def name(self):
            return "test"

        def username(self):
            return "user"

        def status(self):
            return "running"

        def exe(self):
            return "/tmp/test"

        def cwd(self):
            raise AccessDenied("permission denied")

        def parent(self):
            return None

    fake = SimpleNamespace(Process=lambda _pid: Process(), NoSuchProcess=NoSuchProcess)
    monkeypatch.setattr(system_base, "_psutil", lambda: fake)
    result = system_base.collect("process_detail", {"pid": 91})
    assert result["status"] == "partial"
    assert result["process"]["name"] == "test"
    assert result["process"]["cwd"] is None
    assert any("permission denied" in warning for warning in result["warnings"])


def test_socket_collection_reports_partial_access(monkeypatch):
    class FakePsutil:
        @staticmethod
        def net_connections(kind):
            if kind == "unix":
                raise PermissionError("permission denied")
            return [
                SimpleNamespace(
                    family=socket.AF_INET,
                    type=socket.SOCK_STREAM,
                    laddr=("127.0.0.1", 8000),
                    raddr=(),
                    status="LISTEN",
                    pid=77,
                )
            ]

    monkeypatch.setattr(system_base, "_psutil", lambda: FakePsutil)
    result = system_base.collect("sockets", {"local_port": 8000, "protocol": "tcp"})
    assert result["sockets"][0]["pid"] == 77
    assert result["sockets"][0]["family"] == "af_inet"
    assert result["warnings"]


async def test_process_and_socket_arguments_are_validated_before_probe(tmp_path, monkeypatch):
    tools = SystemTools(tmp_path)

    async def fail(*_args, **_kwargs):
        raise AssertionError("invalid input must not launch a probe")

    monkeypatch.setattr(tools, "_collect", fail)
    with pytest.raises(ValueError, match="pid"):
        await tools.process(0)
    with pytest.raises(ValueError, match="protocol"):
        await tools.sockets(protocol="icmp")
    with pytest.raises(ValueError, match="local_port"):
        await tools.sockets(local_port=65536)
