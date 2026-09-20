from __future__ import annotations

import time

import pytest

import mypr_mcp.system_intel as intel


def _device(index: int, card: str) -> dict:
    return {
        "vendor": "intel",
        "source": "sysfs",
        "id": f"0000:0{index + 1}:00.0",
        "name": None,
        "model": None,
        "driver": None,
        "pci": f"0000:0{index + 1}:00.0",
        "uuid": None,
        "utilization": {},
        "memory": {},
        "temperature_c": None,
        "power": {},
        "clocks": {},
        "processes": [],
        "metrics": {"card": card},
        "warnings": [],
    }


def test_xpu_discovery_and_per_device_stats_are_merged_once(monkeypatch):
    calls: list[list[str]] = []

    def run(argv, _timeout):
        calls.append(argv)
        if argv[1] == "discovery":
            return (
                {
                    "device_list": [
                        {"device_id": 0, "device_name": "Arc A", "pci_bdf": "00000000:01:00.0"},
                        {"device_id": 1, "device_name": "Arc B", "pci_bdf": "00000000:02:00.0"},
                    ]
                },
                None,
            )
        identity = argv[3]
        return (
            {
                "device_list": [
                    {
                        "device_id": int(identity),
                        "device_level": [
                            {
                                "metric_type": "GPU_UTILIZATION",
                                "value": {"value": 20 + int(identity), "unit": "%"},
                            }
                        ],
                    }
                ]
            },
            None,
        )

    monkeypatch.setattr(intel, "_run_json", run)
    monkeypatch.setattr(intel, "_stream_json", lambda *_: ([], None))
    devices = [_device(0, "card0"), _device(1, "card1")]
    result = intel.collect({"timeout": 2, "interval": 0.1}, devices, [])
    assert len(result) == 2
    assert [device["utilization"]["gpu_percent"] for device in result] == [20, 21]
    assert [device["source"] for device in result] == ["xpu-smi+sysfs", "xpu-smi+sysfs"]
    assert [call[3] for call in calls if call[1] == "stats"] == ["0", "1"]


def test_xpu_json_payload_accepts_root_array():
    assert intel._json_payload(b'[{"device_id": 0}]') == [{"device_id": 0}]
    raw = b'[{"engines":{"Render":{"busy":1}}},{"engines":{"Render":'
    assert intel._complete_objects(raw) == [{"engines": {"Render": {"busy": 1}}}]


def test_engine_utilization_does_not_become_global_gpu_utilization():
    record = intel._normalize_metric(
        {
            "device_level": [
                {"metric_type": "RENDER_UTILIZATION", "value": 88},
                {"metric_type": "GPU_UTILIZATION", "value": 22},
            ]
        }
    )
    device = _device(0, "card0")
    intel._merge_record(device, record)
    assert device["utilization"]["gpu_percent"] == 22
    assert device["metrics"]["engine_metrics"]["RENDER_UTILIZATION"] == 88


def test_memory_stats_numeric_values_use_documented_mib_unit_and_average_shapes():
    record = intel._normalize_metric(
        {
            "device_level": [
                {"metric_type": "GPU_MEMORY_USED", "avg": 256},
                {"metric_type": "GPU_MEMORY_TOTAL", "value": {"current": 1024}},
            ]
        }
    )
    device = _device(0, "card0")
    intel._merge_record(device, record)
    assert device["memory"]["used_bytes"] == 256 * 1024**2
    assert device["memory"]["total_bytes"] == 1024 * 1024**2


def test_stat_without_device_id_stays_on_queried_device(monkeypatch):
    def run(argv, _timeout):
        if argv[1] == "discovery":
            return (
                {
                    "device_list": [
                        {"device_id": 0, "pci_bdf": "0000:01:00.0"},
                        {"device_id": 1, "pci_bdf": "0000:02:00.0"},
                    ]
                },
                None,
            )
        if argv[3] == "1":
            return (
                {"device_level": [{"metric_type": "GPU_UTILIZATION", "value": 77}]},
                None,
            )
        return {"device_level": []}, None

    monkeypatch.setattr(intel, "_run_json", run)
    monkeypatch.setattr(intel, "_stream_json", lambda *_: ([], None))
    result = intel.collect(
        {"timeout": 2, "interval": 0.1}, [_device(0, "card0"), _device(1, "card1")], []
    )
    assert result[0]["utilization"] == {}
    assert result[1]["utilization"]["gpu_percent"] == 77


def test_intel_gpu_top_is_sampled_per_card_and_engines_stay_separate(monkeypatch):
    streams = []

    def stream(argv, budget):
        streams.append((argv, budget))
        card = argv[2].rsplit("/", 1)[-1]
        return (
            [
                {
                    "period": {"duration": 500, "unit": "ms"},
                    "engines": {f"Render/{card}": {"busy": 12 if card == "card0" else 34}},
                }
            ],
            None,
        )

    monkeypatch.setattr(intel, "_run_json", lambda *_: ({"device_list": []}, None))
    monkeypatch.setattr(intel, "_stream_json", stream)
    devices = [_device(0, "card0"), _device(1, "card1")]
    result = intel.collect({"timeout": 3, "interval": 0.5}, devices, [])
    assert len(streams) == 2
    assert result[0]["metrics"]["intel_gpu_top"]["engines"]["Render/card0"]["busy_percent"] == 12
    assert result[1]["metrics"]["intel_gpu_top"]["engines"]["Render/card1"]["busy_percent"] == 34
    assert result[0]["utilization"] == {}


def test_stream_parser_salvages_complete_samples_before_budget(monkeypatch, tmp_path):
    script = tmp_path / "intel_gpu_top"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import json, time\n"
        "print(json.dumps({'period': {'duration': 100}, 'engines': {}}), flush=True)\n"
        "time.sleep(2)\n"
    )
    script.chmod(0o755)
    started = time.monotonic()
    samples, error = intel._stream_json([str(script), "-J", "-o", "-"], 0.2)
    assert time.monotonic() - started < 1
    assert samples and samples[-1]["period"]["duration"] == 100
    assert error and "budget" in error


@pytest.mark.parametrize(
    "message", ["xpu-smi: [Errno 2] No such file", "intel_gpu_top: Permission denied"]
)
def test_missing_or_permission_diagnostics_are_bounded(monkeypatch, message):
    monkeypatch.setattr(intel, "_run_json", lambda *_: (None, message))
    monkeypatch.setattr(intel, "_stream_json", lambda *_: ([], message))
    warnings: list[str] = []
    result = intel.collect({"timeout": 1, "interval": 0.1}, [_device(0, "card0")], warnings)
    assert result
    if "No such file" in message:
        assert warnings == []
    else:
        assert warnings and len(warnings[0]) < 512


def test_native_stats_memory_units_and_engine_scope_are_preserved():
    from mypr_mcp.system_gpu import _base

    normalized = intel._normalize_metric(
        {
            "device_level": [
                {"metric_type": "XPUM_STATS_MEMORY_USED", "value": {"value": 1234, "unit": "B"}},
                {"metric_type": "GPU_MEMORY_TOTAL", "avg": 8192},
                {"metric_type": "XPUM_STATS_GPU_UTILIZATION", "value": 70},
                {"metric_type": "GPU_RENDER_UTILIZATION", "value": 90},
                {"metric_type": "XPUM_STATS_POWER", "current": 25},
            ]
        }
    )
    device = _base("intel", "xpu-smi", "0")
    intel._merge_record(device, normalized)
    assert device["memory"]["used_bytes"] == 1234
    assert device["memory"]["total_bytes"] == 8192 * 1024**2
    assert device["utilization"]["gpu_percent"] == 70
    assert device["power"]["draw_w"] == 25
    assert device["metrics"]["engine_metrics"]["GPU_RENDER_UTILIZATION"] == 90


def test_native_discovery_address_reuses_sysfs_device():
    device = _device(0, "card0")
    record = {"device_id": 0, "pci_bdf_address": "0000:01:00.0"}
    assert intel._find_device([device], {}, record, "0") is device
