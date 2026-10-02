from __future__ import annotations

import json

import pytest

from mypr_mcp.scan_api import Net
from mypr_mcp.scan_service import ScanService


@pytest.mark.asyncio
async def test_net_scan_forwards_tcp_options():
    calls = []

    async def rpc(operation, **kwargs):
        calls.append((operation, kwargs))
        return {"id": "a" * 32}

    net = Net(rpc, object(), task_factory=lambda ident, tasks, callback: ident)
    assert await net.scan(
        "127.0.0.1",
        ports=[443],
        family="ipv6",
        retries=2,
        banner=True,
        banner_timeout=0.25,
        banner_bytes=2048,
        open_only=True,
    ) == "a" * 32
    operation, kwargs = calls[0]
    assert operation == "scan_start"
    assert {key: kwargs[key] for key in (
        "family", "retries", "banner", "banner_timeout", "banner_bytes", "open_only"
    )} == {
        "family": "ipv6",
        "retries": 2,
        "banner": True,
        "banner_timeout": 0.25,
        "banner_bytes": 2048,
        "open_only": True,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("ports", "expected"), [
    ({443, 80}, [80, 443]),
    (frozenset({443, 80}), [80, 443]),
    ({443, "80-81"}, [80, 81, 443]),
])
async def test_net_scan_normalizes_port_sets_before_rpc(ports, expected):
    calls = []

    async def rpc(operation, **kwargs):
        calls.append((operation, kwargs))
        json.dumps(kwargs)
        return {"id": "a" * 32}

    net = Net(rpc, object(), task_factory=lambda ident, tasks, callback: ident)
    await net.scan("127.0.0.1", ports=ports)
    assert calls[0][1]["ports"] == expected


class _NoLaunchShells:
    completed_records = 10

    async def start(self, *_args, **_kwargs):
        raise AssertionError("invalid scans must be rejected before launch")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("family", "bogus"),
        ("retries", 4),
        ("banner", 1),
        ("banner_timeout", 0),
        ("banner_bytes", 0),
        ("open_only", "yes"),
    ],
)
async def test_invalid_tcp_option_is_rejected_before_launch(tmp_path, option, value):
    service = ScanService(tmp_path, _NoLaunchShells())
    options = {option: value}
    with pytest.raises((TypeError, ValueError)):
        await service.start("tcp", targets="127.0.0.1", ports=[443], **options)


@pytest.mark.parametrize(("state", "complete"), [("running", False), ("succeeded", True)])
async def test_restored_scan_completion_matches_terminal_state(tmp_path, state, complete):
    root = tmp_path / ".mypr" / "scans"
    root.mkdir(parents=True)
    ident = "a" * 32
    (root / f"{ident}.json").write_text(json.dumps({
        "id": ident, "mode": "tcp", "state": state, "complete": True,
    }))
    service = ScanService(tmp_path, _NoLaunchShells())
    restored = await service.summary(ident)
    assert restored["complete"] is complete
    assert restored["state"] == ("lost" if state == "running" else state)


async def test_cancel_clears_pending_completion(tmp_path):
    class Shells(_NoLaunchShells):
        async def cancel(self, ident):
            return {"state": "cancelled", "result": {"returncode": 143}}

    service = ScanService(tmp_path, Shells())
    record = {
        "id": "a" * 32, "mode": "tcp", "state": "running", "complete": True,
        "shell_id": "a" * 32,
    }
    service._write_record(record)
    service._cache(record)
    result = await service.cancel(record["id"])
    assert result["state"] == "cancelled" and result["complete"] is False


def test_cancelled_scan_ignores_late_complete_progress(tmp_path):
    service = ScanService(tmp_path, _NoLaunchShells())
    record = {"id": "a" * 32, "mode": "tcp", "state": "cancelled", "complete": False}
    service._write_record(record)
    service._consume_line(
        record,
        '{"type":"progress","complete":true,"completed":1}',
    )
    assert record["complete"] is False


@pytest.mark.asyncio
async def test_service_normalizes_port_sets_before_persisting(tmp_path, monkeypatch):
    captured = {}

    def capture(_path, value):
        captured.update(value)
        raise RuntimeError("stop before launch")

    monkeypatch.setattr(ScanService, "_atomic_json", staticmethod(capture))
    service = ScanService(tmp_path, _NoLaunchShells())
    with pytest.raises(RuntimeError, match="stop before launch"):
        await service.start("tcp", targets="127.0.0.1", ports={443, 80})
    assert captured["ports"] == [80, 443]


@pytest.mark.asyncio
async def test_finish_publishes_completion_only_after_terminal_state(tmp_path, monkeypatch):
    service = ScanService(tmp_path, _NoLaunchShells())
    result_path = tmp_path / "results.jsonl"
    summary_path = tmp_path / "summary.json"
    result_path.write_text("", encoding="utf-8")
    summary_path.write_text(json.dumps({"complete": True}), encoding="utf-8")
    record = {
        "id": "a" * 32,
        "mode": "tcp",
        "state": "running",
        "complete": False,
        "result_path": str(result_path),
        "summary_path": str(summary_path),
        "config": str(tmp_path / "request.json"),
        "warnings": [],
    }
    seen = {}

    def stats(_path):
        seen["complete"] = record["complete"]
        record["state"] = "cancelled"
        return 0, 0

    monkeypatch.setattr(ScanService, "_result_stats", staticmethod(stats))
    await service._finish(record, {"state": "succeeded", "result": {}})
    assert seen["complete"] is False
    assert record["complete"] is False
