from __future__ import annotations

import json

import pytest

from mypr_mcp.network_tools import NetworkTools
from mypr_mcp.scan_api import Net
from mypr_mcp.scan_service import ScanService


@pytest.mark.asyncio
async def test_net_udp_scan_encodes_binary_payload_and_resolves_defaults():
    calls = []

    async def rpc(operation, **kwargs):
        calls.append((operation, kwargs))
        return {"id": "a" * 32}

    net = Net(rpc, object(), task_factory=lambda ident, tasks, callback: ident)
    assert await net.scan(
        "127.0.0.1", protocol="UDP", ports=[53], payload=b"\x00\xff"
    ) == "a" * 32
    operation, request = calls[0]
    assert operation == "scan_start"
    assert request["mode"] == "udp"
    assert "protocol" not in request
    assert request["payload_b64"] == "AP8="
    assert request["concurrency"] == 16
    assert request["rate"] == 20
    assert request["timeout"] == 2.0
    assert request["retries"] == 1
    assert request["per_host_rate"] == 1.0
    json.dumps(request)


@pytest.mark.asyncio
async def test_net_rejects_invalid_udp_arguments_before_rpc():
    calls = []

    async def rpc(*args, **kwargs):
        calls.append((args, kwargs))
        return {"id": "a" * 32}

    net = Net(rpc, object(), task_factory=lambda ident, tasks, callback: ident)
    with pytest.raises(ValueError, match="protocol"):
        await net.scan("127.0.0.1", protocol="sctp")
    with pytest.raises(TypeError, match="payload"):
        await net.scan("127.0.0.1", protocol="udp", payload="dns")
    assert not calls


@pytest.mark.asyncio
async def test_network_tools_forwards_udp_options(tmp_path, monkeypatch):
    calls = []

    async def delegate(kind, **kwargs):
        calls.append((kind, kwargs))
        return "scan-id"

    net = NetworkTools(tmp_path)
    monkeypatch.setattr(net, "_delegate", delegate)
    assert await net.scan(
        "127.0.0.1",
        53,
        protocol="udp",
        probe="dns",
        per_host_rate=2,
        capture_response=True,
        response_bytes=128,
    ) == "scan-id"
    assert calls == [
        (
            "scan",
            {
                "targets": "127.0.0.1",
                "ports": 53,
                "concurrency": None,
                "rate": None,
                "timeout": None,
                "max_probes": 1_000_000,
                "max_duration": 3600.0,
                "continue_after_output_limit": False,
                "family": "any",
                "retries": None,
                "banner": False,
                "banner_timeout": 0.5,
                "banner_bytes": 1024,
                "open_only": False,
                "protocol": "udp",
                "per_host_rate": 2,
                "probe": "dns",
                "payload": None,
                "capture_response": True,
                "response_bytes": 128,
            },
        )
    ]


class _NoLaunchShells:
    completed_records = 10

    async def start(self, *_args, **_kwargs):
        raise AssertionError("invalid scans must be rejected before launch")


@pytest.mark.asyncio
async def test_service_persists_resolved_udp_config_without_raw_payload(tmp_path, monkeypatch):
    captured = {}

    def capture(_path, value):
        captured.update(value)
        raise RuntimeError("stop before launch")

    monkeypatch.setattr(ScanService, "_atomic_json", staticmethod(capture))
    service = ScanService(tmp_path, _NoLaunchShells())
    with pytest.raises(RuntimeError, match="stop before launch"):
        await service.start(
            "udp", targets="127.0.0.1", ports=[53], payload_b64="AP8=", capture_response=True
        )
    assert captured["mode"] == "udp"
    assert captured["concurrency"] == 16
    assert captured["rate"] == 20
    assert captured["timeout"] == 2.0
    assert captured["retries"] == 1
    assert captured["payload_b64"] == "AP8="


@pytest.mark.asyncio
async def test_udp_finish_sets_complete_only_after_success(tmp_path):
    service = ScanService(tmp_path, _NoLaunchShells())
    result_path = tmp_path / "results.jsonl"
    summary_path = tmp_path / "summary.json"
    result_path.write_text("", encoding="utf-8")
    summary_path.write_text(json.dumps({"complete": True}), encoding="utf-8")
    record = {
        "id": "a" * 32,
        "mode": "udp",
        "state": "running",
        "complete": False,
        "result_path": str(result_path),
        "summary_path": str(summary_path),
        "config": str(tmp_path / "request.json"),
        "warnings": [],
    }
    await service._finish(record, {"state": "succeeded", "result": {}})
    assert record["complete"] is True
