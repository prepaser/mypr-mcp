from __future__ import annotations

import asyncio
import json
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from mypr_mcp import scan_worker
from mypr_mcp.scan_api import Net
from mypr_mcp.scan_service import ScanService
from mypr_mcp.services import Shells


@pytest.mark.asyncio
async def test_tcp_scan_persists_rows_and_pages(tmp_path: Path):
    async def handler(reader, writer):
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    shells = Shells(tmp_path)
    try:
        service = ScanService(tmp_path, shells)
        launched = await service.start(
            "tcp", targets=["127.0.0.1"], ports=[port, port + 1], concurrency=2, rate=10000
        )
        summary = await service.wait(launched["id"])
        assert summary["state"] == "succeeded"
        page = await service.results(launched["id"], max_entries=1, max_bytes=4096)
        assert page["results"]
        assert page["has_more"]
        next_page = await service.results(
            launched["id"], cursor=page["next_cursor"], max_entries=10, max_bytes=4096
        )
        assert len(page["results"]) + len(next_page["results"]) == 2
        assert {item["state"] for item in page["results"] + next_page["results"]} <= {
            "open",
            "closed",
            "timeout",
            "unreachable",
        }
    finally:
        await shells.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_tcp_scan_stops_at_probe_limit(tmp_path: Path):
    async def handler(reader, writer):
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    shells = Shells(tmp_path)
    try:
        service = ScanService(tmp_path, shells)
        launched = await service.start(
            "tcp",
            targets="127.0.0.1",
            ports=[port, port + 1],
            concurrency=2,
            rate=10000,
            max_probes=1,
        )
        summary = await service.wait(launched["id"])
        assert summary["state"] == "succeeded"
        assert summary["attempts"] == 1
        assert summary["stop_reason"] == "probe_limit"
        assert summary["estimate"] == 2
    finally:
        await shells.close()
        server.close()
        await server.wait_closed()


def test_estimate_probes_matches_ipaddress_hosts_for_ipv6():
    assert scan_worker.estimate_probes(["2001:db8::/126"], [443]) == 3
    assert scan_worker.estimate_probes(["2001:db8::/127"], [443]) == 2
    assert scan_worker.estimate_probes(["2001:db8::/128"], [443]) == 1


@pytest.mark.asyncio
async def test_nmap_result_limit_keeps_partial_hosts_successful(tmp_path: Path, monkeypatch):
    fake = tmp_path / "nmap"
    script = """#!/usr/bin/env python3
print('<nmaprun><host><status state="up"/><address addr="127.0.0.1"/></host></nmaprun>')
"""
    await asyncio.to_thread(fake.write_text, script, encoding="utf-8")
    await asyncio.to_thread(fake.chmod, 0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setattr(scan_worker, "_RESULT_LIMIT", 1)
    config = {
        "mode": "nmap",
        "targets": ["127.0.0.1"],
        "args": [],
        "result_path": str(tmp_path / "results.jsonl"),
        "artifact_path": str(tmp_path / "artifact.xml"),
        "summary_path": str(tmp_path / "summary.json"),
        "max_duration": 30,
    }
    assert await scan_worker.run(config) == 0
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["stop_reason"] == "result_size_limit"
    assert summary["error"] is None
    assert summary["artifact_truncated"]


@pytest.mark.asyncio
async def test_scan_attach_after_service_restart(tmp_path: Path):
    shells = Shells(tmp_path)
    try:
        service = ScanService(tmp_path, shells)
        launched = await service.start("tcp", targets="127.0.0.1", ports=[9], rate=10000)
        await service.wait(launched["id"])
        await shells.close()
        restored = ScanService(tmp_path, Shells(tmp_path))
        summary = await restored.summary(launched["id"])
        assert summary["state"] == "succeeded"
        assert (await restored.results(launched["id"]))["state"] == "succeeded"
        await restored.shells.close()
    finally:
        if not shells._closed:
            await shells.close()


@pytest.mark.asyncio
async def test_scan_tracking_failure_persists_terminal_record(tmp_path: Path):
    class FakeShells:
        completed_records = 10

        def __init__(self):
            self.cancelled = []

        async def start(self, *args, **kwargs):
            return {"id": "a" * 32}

        async def cancel(self, ident):
            self.cancelled.append(ident)
            return {"id": ident, "state": "cancelled"}

    shells = FakeShells()

    def fail_tracking(*args, **kwargs):
        raise RuntimeError("tracking unavailable")

    service = ScanService(tmp_path, shells, track=fail_tracking)
    with pytest.raises(RuntimeError, match="tracking unavailable"):
        await service.start("tcp", targets="127.0.0.1", ports=[1])

    record_path = tmp_path / ".mypr" / "scans" / f"{'a' * 32}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["state"] == "failed"
    assert record["stop_reason"] == "tracking_failed"
    assert "tracking unavailable" in record["error"]
    assert shells.cancelled == ["a" * 32]
    assert not list((tmp_path / ".mypr" / "scans").glob("*.request.json"))


@pytest.mark.asyncio
async def test_net_diagnostics_and_scan_task_facade(tmp_path: Path):
    shells = Shells(tmp_path)
    service = ScanService(tmp_path, shells)

    async def rpc(op, **args):
        if op == "scan_start":
            return await service.start(**args)
        if op == "scan_summary":
            return await service.summary(args["id"], wait_ms=args["wait_ms"])
        if op == "scan_results":
            return await service.results(
                args["id"],
                cursor=args["cursor"],
                max_entries=args["max_entries"],
                max_bytes=args["max_bytes"],
            )
        if op == "scan_cancel":
            return await service.cancel(args["id"])
        raise AssertionError(op)

    class FakeTask:
        def __init__(self, ident, manager, callback):
            self.id = ident
            self._callback = callback

        async def summary(self):
            return await self._callback("scan_summary", id=self.id, wait_ms=0)

    try:
        net = Net(
            rpc,
            object(),
            task_factory=lambda ident, manager, callback: FakeTask(ident, manager, callback),
        )
        task = await net.scan("127.0.0.1", ports=[9], rate=10000)
        await service.wait(task.id)
        summary = await task.summary()
        assert summary["state"] == "succeeded"
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_nmap_xml_results_are_saved_when_available(tmp_path: Path):
    if shutil.which("nmap") is None:
        pytest.skip("nmap is not installed")
    shells = Shells(tmp_path)
    service = ScanService(tmp_path, shells)
    try:
        launched = await service.start("nmap", targets="127.0.0.1", args=["-sn"])
        summary = await service.wait(launched["id"])
        assert summary["state"] == "succeeded"
        assert summary["result_count"] == 1
        assert await asyncio.to_thread(Path(summary["artifact"]).is_file)
        page = await service.results(launched["id"])
        assert page["results"][0]["host"] == "127.0.0.1"
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_nmap_streams_xml_beyond_shell_output_limit(tmp_path: Path, monkeypatch):
    fake = tmp_path / "nmap"
    script = """#!/usr/bin/env python3
import sys
filler = "x" * (17 * 1024 * 1024)
print("<nmaprun>")
for host in ("127.0.0.1", "127.0.0.2"):
    print(f'<host><status state="up"/><address addr="{host}"/>'
          f'<script id="f"><output>{filler}</output></script></host>')
print("</nmaprun>")
"""
    await asyncio.to_thread(fake.write_text, script, encoding="utf-8")
    await asyncio.to_thread(fake.chmod, 0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    shells = Shells(tmp_path)
    service = ScanService(tmp_path, shells)
    try:
        launched = await service.start("nmap", targets="127.0.0.1")
        summary = await service.wait(launched["id"])
        assert summary["state"] == "succeeded"
        assert summary["result_count"] == 2
        assert summary["artifact_truncated"]
        assert (await service.results(launched["id"]))["results"][1]["host"] == "127.0.0.2"
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_cancelled_nmap_preserves_completed_hosts(tmp_path: Path, monkeypatch):
    fake = tmp_path / "nmap"
    script = """#!/usr/bin/env python3
import time
print("<nmaprun><host><status state=\\"up\\"/><address addr=\\"127.0.0.1\\"/></host>", flush=True)
time.sleep(30)
"""
    await asyncio.to_thread(fake.write_text, script, encoding="utf-8")
    await asyncio.to_thread(fake.chmod, 0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    shells = Shells(tmp_path)
    service = ScanService(tmp_path, shells)
    try:
        launched = await service.start("nmap", targets="127.0.0.1")
        for _ in range(50):
            if (await service.summary(launched["id"]))["result_count"]:
                break
            await asyncio.sleep(0.02)
        await service.cancel(launched["id"])
        summary = await service.wait(launched["id"])
        assert summary["state"] == "cancelled"
        assert summary["result_count"] == 1
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_worker_summary_is_written_after_consumer_failure(tmp_path: Path, monkeypatch):
    result_path = tmp_path / "results.jsonl"
    summary_path = tmp_path / "summary.json"
    config = {
        "mode": "tcp",
        "targets": ["127.0.0.1"],
        "ports": [1, 2],
        "concurrency": 2,
        "rate": 10000,
        "timeout": 1,
        "result_path": str(result_path),
        "summary_path": str(summary_path),
    }

    def fail(self, row):
        raise OSError("simulated result storage failure")

    monkeypatch.setattr(scan_worker._Results, "append", fail)
    code = await scan_worker.run(config)
    assert code == 2
    summary = await asyncio.to_thread(summary_path.read_text, encoding="utf-8")
    assert "simulated result storage failure" in summary


@pytest.mark.asyncio
async def test_worker_preserves_unicode_xml_split_across_bytes(tmp_path: Path, monkeypatch):
    fake = tmp_path / "nmap"
    script = """#!/usr/bin/env python3
import os
xml = '<nmaprun><host><status state="up"/><address addr="例え.テスト"/></host></nmaprun>'
data = xml.encode()
os.write(1, data[:31])
os.write(1, data[31:])
"""
    await asyncio.to_thread(fake.write_text, script, encoding="utf-8")
    await asyncio.to_thread(fake.chmod, 0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    config = {
        "mode": "nmap",
        "targets": ["127.0.0.1"],
        "args": [],
        "result_path": str(tmp_path / "results.jsonl"),
        "artifact_path": str(tmp_path / "artifact.xml"),
        "summary_path": str(tmp_path / "summary.json"),
    }
    assert await scan_worker.run(config) == 0
    row = json.loads((tmp_path / "results.jsonl").read_text(encoding="utf-8"))
    assert row["host"] == "例え.テスト"


@pytest.mark.asyncio
async def test_worker_stderr_tail_is_bounded():
    stream = asyncio.StreamReader()
    stream.feed_data(b"e" * 100_000)
    stream.feed_eof()
    tail = await scan_worker._read_tail(stream)
    assert len(tail) == 64 * 1024
    assert tail == b"e" * (64 * 1024)


def test_nmap_row_preserves_service_and_script_details():
    host = ET.fromstring(
        """<host><status state="up"/><address addr="127.0.0.1"/>
        <ports><port protocol="tcp" portid="443"><state state="open"/>
        <service name="https" product="Example" version="1.2" extrainfo="TLS">
        <cpe>cpe:/a:example:server:1.2</cpe></service>
        <script id="ssl-cert" output="certificate details"/></port></ports>
        <hostscript><script id="banner" output="hello"/></hostscript></host>"""
    )
    row = scan_worker._nmap_row(host)
    service = row["ports"][0]["service"]
    assert service["product"] == "Example"
    assert service["version"] == "1.2"
    assert service["cpe"]
    assert row["ports"][0]["scripts"][0]["id"] == "ssl-cert"
    assert row["scripts"][0]["output"] == "hello"
