from __future__ import annotations

import asyncio
import errno
import json
import socket
from contextlib import asynccontextmanager, suppress

import pytest

from mypr_mcp import network_tools, scan_worker


@pytest.mark.parametrize("scope", ["lo", "7"])
def test_ipv6_cidr_preserves_zone(scope):
    assert list(scan_worker._targets(f"fe80::1%{scope}/127")) == [
        f"fe80::%{scope}", f"fe80::1%{scope}",
    ]


async def run_scan(tmp_path, **options):
    config = {
        "mode": "tcp", "targets": ["127.0.0.1"], "ports": [9], "rate": 10000,
        "result_path": str(tmp_path / "results.jsonl"),
        "summary_path": str(tmp_path / "summary.json"),
        **options,
    }
    code = await asyncio.wait_for(scan_worker.run(config), 3)
    summary = json.loads((tmp_path / "summary.json").read_text())
    rows = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    return code, summary, rows


@asynccontextmanager
async def listener(host="127.0.0.1", data=None):
    handlers = set()

    async def handle(reader, writer):
        handlers.add(asyncio.current_task())
        try:
            if data is not None:
                writer.write(data)
                await writer.drain()
            await reader.read()
        except ConnectionError:
            pass
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()
            handlers.discard(asyncio.current_task())

    server = await asyncio.start_server(handle, host, 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()
        if handlers:
            await asyncio.gather(*list(handlers))


async def test_tcp_resolves_hostname_once_and_scans_each_unique_address(tmp_path, monkeypatch):
    calls = []

    async def resolve(host, port, family, *args):
        calls.append((host, family))
        return [
            {"family": socket.AF_INET, "sockaddr": [address, 0]}
            for address in ("127.0.0.1", "127.0.0.2", "127.0.0.1")
        ]

    monkeypatch.setattr(network_tools, "_resolve_worker", resolve)
    async with listener("0.0.0.0") as port:
        code, summary, rows = await run_scan(
            tmp_path, targets=["multi.test"], ports=[port], family="ipv4"
        )
    assert code == 0
    assert calls == [("multi.test", socket.AF_INET)]
    assert {row["address"] for row in rows} == {"127.0.0.1", "127.0.0.2"}
    assert all(row["host"] == "multi.test" and row["state"] == "open" for row in rows)
    assert summary["estimate"] == summary["completed"] == summary["attempts"] == 2
    assert summary["complete"] is True


async def test_tcp_ipv6_literal(tmp_path):
    try:
        async with listener("::1") as port:
            code, summary, rows = await run_scan(
                tmp_path, targets=["::1"], ports=[port], family="ipv6"
            )
    except OSError as exc:
        pytest.skip(f"IPv6 loopback unavailable: {exc}")
    assert code == 0 and summary["complete"]
    assert rows[0]["family"] == "ipv6"
    assert rows[0]["address"] == "::1"
    assert rows[0]["state"] == "open"


async def test_tcp_hostname_uses_guarded_dns_worker(tmp_path):
    async with listener() as port:
        code, summary, rows = await run_scan(
            tmp_path, targets=["localhost"], ports=[port], family="ipv4"
        )
    assert code == 0 and summary["complete"] is True
    assert any(row["address"] == "127.0.0.1" and row["state"] == "open" for row in rows)
    assert summary["resolve_errors"] == 0


async def test_family_filter_excludes_other_literal_version(tmp_path):
    _, summary, rows = await run_scan(tmp_path, family="ipv6")
    assert rows == []
    assert summary["attempts"] == summary["estimate"] == 0
    assert summary["complete"] is True


async def test_tcp_banner_is_bounded_and_binary_tolerant(tmp_path):
    async with listener(data=b"\xffSSH-2.0-test\r\n" * 10) as port:
        _, summary, rows = await run_scan(
            tmp_path, ports=[port], banner=True, banner_bytes=8
        )
    row = rows[0]
    assert row["state"] == "open"
    assert row["banner"] == "\ufffdSSH-2.0"
    assert row["banner_bytes"] == 8 and row["banner_truncated"] is True
    assert row["banner_complete"] is False
    assert summary["state_counts"]["open"] == 1


async def test_tcp_banner_timeout_preserves_open_state(tmp_path):
    async with listener() as port:
        _, summary, rows = await run_scan(
            tmp_path, ports=[port], banner=True, banner_timeout=0.01
        )
    assert rows[0]["state"] == "open"
    assert rows[0]["banner_error"] == "read_timeout"
    assert rows[0]["banner_complete"] is False
    assert summary["complete"] is True


async def test_timeout_retry_counts_actual_connections(tmp_path, monkeypatch):
    calls = 0

    async def probe(*args, **kwargs):
        nonlocal calls
        calls += 1
        return {"host": args[0], "port": args[1], "state": "timeout" if calls == 1 else "open"}

    monkeypatch.setattr(scan_worker, "_probe", probe)
    _, summary, rows = await run_scan(tmp_path, retries=2)
    assert calls == summary["attempts"] == rows[0]["attempts"] == 2
    assert summary["completed"] == 1
    assert summary["state_counts"]["open"] == 1
    assert summary["complete"] is True


async def test_probe_budget_includes_retries(tmp_path, monkeypatch):
    async def probe(*args, **kwargs):
        return {"host": args[0], "port": args[1], "state": "timeout"}

    monkeypatch.setattr(scan_worker, "_probe", probe)
    _, summary, rows = await run_scan(tmp_path, retries=3, max_probes=1)
    assert summary["attempts"] == rows[0]["attempts"] == 1
    assert summary["stop_reason"] == "probe_limit"
    assert summary["complete"] is False


async def test_open_only_retains_state_counts(tmp_path, monkeypatch):
    async def probe(*args, **kwargs):
        return {"host": args[0], "port": args[1], "state": "open" if args[1] == 22 else "closed"}

    monkeypatch.setattr(scan_worker, "_probe", probe)
    _, summary, rows = await run_scan(tmp_path, ports=[22, 23, 24], open_only=True)
    assert [row["port"] for row in rows] == [22]
    assert summary["completed"] == 3
    assert summary["state_counts"] == {"open": 1, "closed": 2, "timeout": 0, "unreachable": 0}
    assert summary["discarded_results"] == 0
    assert summary["complete"] is True


async def test_resolution_failure_does_not_fake_port_probes(tmp_path, monkeypatch):
    calls = 0

    async def resolve(*args):
        nonlocal calls
        calls += 1
        raise socket.gaierror(socket.EAI_NONAME, "not found")

    monkeypatch.setattr(network_tools, "_resolve_worker", resolve)
    _, summary, rows = await run_scan(tmp_path, targets=["missing.test"] * 2, ports=[22, 80])
    assert calls == 1
    assert summary["attempts"] == summary["completed"] == 0
    assert summary["resolve_errors"] == 2
    assert summary["complete"] is False
    assert all(row["port"] is None and row["phase"] == "resolve" for row in rows)


async def test_scan_deadline_covers_dns(tmp_path, monkeypatch):
    cancelled = asyncio.Event()

    async def resolve(*args):
        try:
            await asyncio.sleep(30)
        finally:
            cancelled.set()

    monkeypatch.setattr(network_tools, "_resolve_worker", resolve)
    _, summary, rows = await run_scan(tmp_path, targets=["slow.test"], max_duration=0.01)
    assert cancelled.is_set()
    assert summary["stop_reason"] == "time_limit"
    assert summary["attempts"] == 0 and not summary["complete"]
    assert rows == []


async def test_scoped_ipv6_resolution_failure_is_reported(tmp_path, monkeypatch):
    async def resolve(*args):
        raise socket.gaierror(socket.EAI_NONAME, "unknown interface")

    monkeypatch.setattr(network_tools, "_resolve_worker", resolve)
    _, summary, rows = await run_scan(tmp_path, targets=["fe80::1%missing"], family="ipv6")
    assert summary["resolve_errors"] == 1
    assert summary["attempts"] == 0 and not summary["complete"]
    assert rows[0]["reason_code"] == "resolve_failed"


async def test_banner_collects_multiple_fragments_until_eof(tmp_path):
    async def handle(reader, writer):
        try:
            writer.write(b"first-")
            await writer.drain()
            await asyncio.sleep(0.01)
            writer.write(b"second")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        _, _, rows = await run_scan(tmp_path, ports=[port], banner=True)
    finally:
        server.close()
        await server.wait_closed()
    assert rows[0]["banner"] == "first-second"
    assert rows[0]["banner_complete"] is True
    assert rows[0]["banner_truncated"] is False


async def test_local_socket_exhaustion_fails_scan(tmp_path, monkeypatch):
    def socket_error(*args, **kwargs):
        raise OSError(errno.EMFILE, "too many open files")

    monkeypatch.setattr(scan_worker.socket, "socket", socket_error)
    code, summary, rows = await run_scan(tmp_path)
    assert code == 2
    assert summary["stop_reason"] == "local_resource_error"
    assert summary["error"] and not summary["complete"]
    assert summary["attempts"] == 1 and summary["completed"] == 0
    assert rows == []


async def test_dns_worker_resource_exhaustion_fails_scan(tmp_path, monkeypatch):
    async def resolve(*args):
        raise OSError(errno.EMFILE, "too many open files")

    monkeypatch.setattr(network_tools, "_resolve_worker", resolve)
    code, summary, rows = await run_scan(tmp_path, targets=["localhost"])
    assert code == 2
    assert summary["stop_reason"] == "local_resource_error"
    assert summary["resolve_errors"] == 0 and summary["complete"] is False
    assert rows == []


async def test_output_loss_is_separate_from_completed_probes(tmp_path, monkeypatch):
    monkeypatch.setattr(scan_worker, "_RESULT_LIMIT", 1)
    async with listener() as port:
        _, summary, rows = await run_scan(
            tmp_path, ports=[port], continue_after_output_limit=True
        )
    assert rows == []
    assert summary["completed"] == summary["discarded_results"] == 1
    assert summary["truncated"] is True and summary["complete"] is False


async def test_cancel_preserves_inflight_attempt_count(tmp_path, monkeypatch):
    entered = asyncio.Event()

    async def probe(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(scan_worker, "_probe", probe)
    config = {
        "targets": ["127.0.0.1"], "ports": [9], "rate": 10000,
        "result_path": str(tmp_path / "results.jsonl"),
        "summary_path": str(tmp_path / "summary.json"),
    }
    task = asyncio.create_task(scan_worker.run(config))
    try:
        await asyncio.wait_for(entered.wait(), 1)
    finally:
        task.cancel()
        code = await asyncio.wait_for(task, 1)
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert code == 143 and summary["cancelled"] is True
    assert summary["attempts"] == 1 and summary["completed"] == 0
    assert summary["complete"] is False


async def test_rate_limiter_has_no_initial_delay_or_catchup_burst(monkeypatch):
    now = [0.0]
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(scan_worker.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(scan_worker.asyncio, "sleep", sleep)
    rate = scan_worker._Rate(10)
    await rate.wait()
    assert sleeps == []
    now[0] = 10
    await rate.wait()
    await rate.wait()
    assert sleeps == pytest.approx([0.1])


@pytest.mark.parametrize("options", [
    {"rate": float("nan")}, {"rate": float("inf")}, {"timeout": float("nan")},
    {"concurrency": True}, {"banner": 1}, {"open_only": "yes"}, {"family": []},
    {"retries": True}, {"retries": 4}, {"banner_bytes": 0}, {"banner_timeout": float("inf")},
    {"continue_after_output_limit": 1},
    {"ports": []}, {"ports": [True]},
])
def test_worker_rejects_invalid_tcp_options(options):
    with pytest.raises((TypeError, ValueError)):
        scan_worker.validate_tcp_config(options)
