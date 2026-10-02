from __future__ import annotations

import asyncio
import json
import socket
import time

from mypr_mcp import scan_worker, udp_scan
from mypr_mcp.scan_config import UDP_STATES


async def run_scan(tmp_path, **options):
    config = {
        "mode": "udp", "targets": ["127.0.0.1"], "ports": [12345],
        "rate": 10000, "per_host_rate": 10000, "retries": 0,
        "result_path": str(tmp_path / "results.jsonl"),
        "summary_path": str(tmp_path / "summary.json"), **options,
    }
    code = await asyncio.wait_for(scan_worker.run(config), 3)
    summary = json.loads((tmp_path / "summary.json").read_text())
    rows = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    return code, summary, rows


def row(host, port, state):
    return {"host": host, "address": host, "port": port, "protocol": "udp", "state": state}


async def test_udp_counts_all_states_and_filters_only_stored_rows(tmp_path, monkeypatch):
    async def probe(host, port, *args, **kwargs):
        return row(host, port, UDP_STATES[port - 20])

    monkeypatch.setattr(udp_scan, "probe", probe)
    code, summary, rows = await run_scan(tmp_path, ports="20-24", open_only=True)
    assert code == 0
    assert summary["complete"] is True
    assert summary["completed"] == summary["attempts"] == summary["estimate"] == 5
    assert summary["state_counts"] == dict.fromkeys(UDP_STATES, 1)
    assert [entry["state"] for entry in rows] == ["open"]


async def test_udp_retries_only_unanswered_probes(tmp_path, monkeypatch):
    calls = []

    async def probe(host, port, *args, **kwargs):
        calls.append(port)
        return row(host, port, "open|filtered" if len(calls) == 1 else "open")

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, rows = await run_scan(tmp_path, retries=1)
    assert len(calls) == summary["attempts"] == rows[0]["attempts"] == 2
    assert summary["completed"] == 1 and summary["complete"] is True


async def test_probe_limit_finishes_reserved_udp_attempt(tmp_path, monkeypatch):
    async def probe(host, port, *args, **kwargs):
        await asyncio.sleep(0.01)
        return row(host, port, "open")

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, rows = await run_scan(tmp_path, ports=[80, 81], max_probes=1)
    assert summary["attempts"] == summary["completed"] == len(rows) == 1
    assert summary["stop_reason"] == "probe_limit" and not summary["complete"]


async def test_exact_probe_budget_can_complete_udp_scope(tmp_path, monkeypatch):
    async def probe(host, port, *args, **kwargs):
        return row(host, port, "closed")

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, _ = await run_scan(tmp_path, max_probes=1)
    assert summary["attempts"] == 1 and summary["complete"] is True
    assert summary["stop_reason"] is None


async def test_cut_retry_is_not_final_udp_no_response(tmp_path, monkeypatch):
    async def probe(host, port, *args, **kwargs):
        return row(host, port, "open|filtered")

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, rows = await run_scan(tmp_path, retries=1, max_probes=1)
    assert summary["attempts"] == 1 and summary["completed"] == 0
    assert summary["stop_reason"] == "probe_limit" and not summary["complete"]
    assert rows == []


async def test_udp_overall_deadline_cancels_probe_without_inventing_state(tmp_path, monkeypatch):
    ended = asyncio.Event()

    async def probe(*args, **kwargs):
        try:
            await asyncio.sleep(30)
        finally:
            ended.set()

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, rows = await run_scan(tmp_path, max_duration=0.02)
    assert ended.is_set()
    assert summary["attempts"] == 1 and summary["completed"] == 0
    assert summary["stop_reason"] == "time_limit" and rows == []


async def test_udp_limits_share_numeric_address_between_aliases(tmp_path, monkeypatch):
    times = []

    async def resolve(host, family, protocol):
        assert protocol == "udp"
        return [(socket.AF_INET, ("127.0.0.1", 0))]

    async def probe(host, port, *args, **kwargs):
        times.append(time.monotonic())
        return row(host, port, "open")

    monkeypatch.setattr(scan_worker, "_resolve", resolve)
    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, _ = await run_scan(
        tmp_path, targets=["one.test", "two.test"], per_host_rate=20
    )
    assert summary["complete"] is True
    assert times[1] - times[0] >= 0.045


async def test_udp_host_wait_does_not_block_other_addresses(tmp_path, monkeypatch):
    calls = []

    async def probe(host, port, *args, **kwargs):
        calls.append((host, port))
        return row(host, port, "open")

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, _ = await run_scan(
        tmp_path, targets=["127.0.0.1", "127.0.0.2"], ports=[80, 81], per_host_rate=20
    )
    assert summary["complete"] is True
    assert {host for host, _ in calls[:2]} == {"127.0.0.1", "127.0.0.2"}


async def test_udp_output_limit_preserves_actual_counts(tmp_path, monkeypatch):
    async def probe(host, port, *args, **kwargs):
        return row(host, port, "open")

    monkeypatch.setattr(udp_scan, "probe", probe)
    monkeypatch.setattr(scan_worker, "_RESULT_LIMIT", 1)
    _, summary, rows = await run_scan(tmp_path)
    assert summary["completed"] == summary["discarded_results"] == 1
    assert summary["stop_reason"] == "result_size_limit" and rows == []


async def test_udp_scheduler_bounds_active_sockets(tmp_path, monkeypatch):
    running = maximum = 0

    async def probe(host, port, *args, **kwargs):
        nonlocal running, maximum
        running += 1
        maximum = max(maximum, running)
        try:
            await asyncio.sleep(0.005)
            return row(host, port, "open")
        finally:
            running -= 1

    monkeypatch.setattr(udp_scan, "probe", probe)
    _, summary, _ = await run_scan(tmp_path, ports="20-29", concurrency=2)
    assert summary["complete"] is True and maximum == 2
    assert running == 0
