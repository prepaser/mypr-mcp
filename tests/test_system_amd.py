from __future__ import annotations

import json

import mypr_mcp.system_amd as amd
from mypr_mcp.system_gpu import _base


def _devices() -> list[dict]:
    values = []
    for index, pci in enumerate(("0000:01:00.0", "0000:02:00.0")):
        device = _base("amd", "sysfs", pci)
        device["pci"] = pci
        device["id"] = pci
        device["name"] = f"sysfs-{index}"
        values.append(device)
    return values


def test_nested_static_metric_and_process_json_merge_by_gpu_identity(monkeypatch):
    calls: list[list[str]] = []

    def command(argv, _timeout):
        calls.append(argv)
        if argv[1] == "static":
            value = {
                "gpu_data": [
                    {
                        "gpu": 0,
                        "bus": {"bdf": "0000:01:00.0"},
                        "asic": {"market_name": "Radeon Alpha"},
                        "vram": {"total": {"value": 8, "unit": "GiB"}},
                    },
                    {
                        "gpu": 1,
                        "pci": {"bus_id": "0000:02:00.0"},
                        "asic": {"market_name": "Radeon Beta"},
                        "vram": {"total": {"value": 16, "unit": "GiB"}},
                    },
                ]
            }
        elif argv[1] == "metric":
            value = {
                "gpu_data": [
                    {
                        "gpu": 0,
                        "gpu_metrics": {
                            "average_gfx_activity": {"value": 77, "unit": "%"},
                            "temperature_hotspot": {"value": 72, "unit": "C"},
                            "average_socket_power": {"value": 44, "unit": "W"},
                            "gfxclk": {"value": 1700, "unit": "MHz"},
                        },
                    },
                    {
                        "gpu": 1,
                        "gpu_metrics": {
                            "average_gfx_activity": {"value": 12, "unit": "%"},
                            "temperature_edge": {"value": 55, "unit": "C"},
                        },
                    },
                ]
            }
        else:
            value = {
                "gpu_data": [
                    {
                        "gpu": 0,
                        "processes": [
                            {
                                "pid": 42,
                                "name": "render",
                                "memory_usage": {
                                    "vram_mem": {"value": 4, "unit": "GiB"},
                                    "gtt_mem": {"value": 128, "unit": "MiB"},
                                    "cpu_mem": {"value": 2, "unit": "MiB"},
                                },
                                "usage": {"gfx": {"value": 100, "unit": "ns"}},
                            }
                        ],
                    }
                ]
            }
        return 0, json.dumps(value).encode(), b"", None

    monkeypatch.setattr(amd, "_bounded_command", command)
    warnings: list[str] = []
    result = amd.collect(
        {"timeout": 2, "processes": True, "reference_pid": 42, "limit": 20},
        _devices(),
        warnings,
    )

    assert not warnings
    assert [device["name"] for device in result] == ["Radeon Alpha", "Radeon Beta"]
    assert result[0]["memory"]["total_bytes"] == 8 * 1024**3
    assert result[1]["memory"]["total_bytes"] == 16 * 1024**3
    assert result[0]["memory"]["kind"] == "vram"
    assert result[0]["utilization"]["gpu_percent"] == 77
    assert result[0]["temperature"]["hotspot_c"] == 72
    assert result[0]["power"]["draw_w"] == 44
    assert result[0]["clocks"]["graphics_mhz"] == 1700
    process = result[0]["processes"][0]
    assert process["pid"] == 42
    assert process["used_memory_bytes"] == 4 * 1024**3
    assert process["gtt_bytes"] == 128 * 1024**2
    assert process["pid_namespace"] == "driver"
    assert not any("process" in call for call in calls if call[1] != "process")


def test_process_command_is_skipped_when_processes_false(monkeypatch):
    calls: list[list[str]] = []

    def command(argv, _timeout):
        calls.append(argv)
        return 0, b'{"gpu_data":[{"gpu":0,"asic":{"market_name":"Radeon"}}]}', b"", None

    monkeypatch.setattr(amd, "_bounded_command", command)
    result = amd.collect({"timeout": 2, "processes": False}, _devices(), [])

    assert result[0]["name"] == "Radeon"
    assert not any("process" in call for call in calls)
    assert result[0]["processes"] == []


def test_missing_metric_keeps_static_and_reports_invalid_metric(monkeypatch):
    def command(argv, _timeout):
        if argv[1] == "static":
            return 0, b'{"gpu_data":[{"gpu":0,"asic":{"market_name":"Radeon"}}]}', b"", None
        return 0, b"not-json", b"", None

    monkeypatch.setattr(amd, "_bounded_command", command)
    warnings: list[str] = []
    result = amd.collect({"timeout": 2, "processes": False}, _devices(), warnings)

    assert result[0]["name"] == "Radeon"
    assert any("metric" in warning and "invalid JSON" in warning for warning in warnings)


def test_missing_amd_tool_with_sysfs_devices_is_a_warning(monkeypatch):
    monkeypatch.setattr(
        amd,
        "_bounded_command",
        lambda *_: (None, b"", b"", "amd-smi: No such file or directory"),
    )
    warnings: list[str] = []
    result = amd.collect({"timeout": 2, "processes": False}, _devices(), warnings)

    assert len(result) == 2
    assert any("AMD SMI is unavailable" in warning for warning in warnings)


def test_missing_amd_tool_without_sysfs_devices_is_quiet(monkeypatch):
    monkeypatch.setattr(
        amd,
        "_bounded_command",
        lambda *_: (None, b"", b"", "amd-smi: No such file or directory"),
    )
    warnings: list[str] = []

    assert amd.collect({"timeout": 2, "processes": False}, [], warnings) == []
    assert warnings == []


def test_vendor_identity_does_not_overwrite_reordered_sysfs_devices():
    devices = _devices()
    static = {
        "gpu_data": [
            {"gpu": 0, "bus": {"bdf": "0000:01:00.0"}, "asic": {"market_name": "Alpha"}},
            {"gpu": 1, "pci": {"bus_id": "0000:02:00.0"}, "asic": {"market_name": "Beta"}},
        ]
    }

    original = amd._bounded_command
    amd._bounded_command = lambda argv, _timeout: (
        0,
        json.dumps(static).encode(),
        b"",
        None,
    )
    try:
        result = amd.collect({"timeout": 2, "processes": False}, list(reversed(devices)), [])
    finally:
        amd._bounded_command = original

    assert {device["pci"]: device["name"] for device in result} == {
        "0000:01:00.0": "Alpha",
        "0000:02:00.0": "Beta",
    }


def test_command_budget_reserves_process_sampling_time(monkeypatch):
    budgets: list[float] = []

    def command(_argv, timeout):
        budgets.append(timeout)
        return 0, b'{"gpu_data":[]}', b"", None

    monkeypatch.setattr(amd, "_bounded_command", command)
    amd.collect({"timeout": 1, "interval": 0.5, "processes": True}, [], [])

    assert budgets
    assert max(budgets) <= 0.35


def test_process_limit_marks_device_truncated(monkeypatch):
    process_data = {
        "gpu_data": [
            {
                "gpu": 0,
                "processes": [{"pid": pid, "name": f"p{pid}"} for pid in range(3)],
            }
        ]
    }

    def command(argv, _timeout):
        if argv[1] == "process":
            return 0, json.dumps(process_data).encode(), b"", None
        return 0, b'{"gpu_data":[]}', b"", None

    monkeypatch.setattr(amd, "_bounded_command", command)
    warnings: list[str] = []
    result = amd.collect(
        {"timeout": 2, "processes": True, "interval": 0.1, "limit": 2},
        _devices(),
        warnings,
    )

    assert len(result[0]["processes"]) == 2
    assert result[0]["truncated"]
    assert "truncated" in result[0]["warnings"][0]


def test_clock_groups_and_driver_do_not_mix_unrelated_totals():
    device = _base("amd", "sysfs", "0")
    amd._merge_metric(
        device,
        {
            "driver": {"driver_version": "test-driver"},
            "clock": {
                "gfx_0": {"clk": {"value": 1700, "unit": "MHz"}},
                "mem_0": {"clk": {"value": 900, "unit": "MHz"}},
            },
            "unrelated": {"total": 999},
        },
    )
    assert device["driver"] == "test-driver"
    assert device["clocks"]["graphics_mhz"] == 1700
    assert device["clocks"]["memory_mhz"] == 900
    assert device["memory"]["total_bytes"] is None
