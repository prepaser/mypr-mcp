from __future__ import annotations

import sys

import pytest

import mypr_mcp.system_gpu as gpu


def test_byte_units_preserve_decimal_values_and_units():
    assert gpu._bytes("8.0 GiB") == 8 * 1024**3
    assert gpu._bytes({"value": 512, "unit": "MiB"}) == 512 * 1024**2
    assert gpu._bytes("N/A") is None


def test_nvidia_xml_is_normalized_and_processes_are_optional(monkeypatch):
    monkeypatch.setattr(gpu.shutil, "which", lambda name: f"/fake/{name}")
    xml = b"""
    <nvidia_smi_log>
      <driver_version>555.42</driver_version>
      <gpu id="0">
        <product_name>RTX Test</product_name>
        <uuid>GPU-test</uuid>
        <pci><pci_bus_id>0000:01:00.0</pci_bus_id></pci>
        <utilization><gpu_util>42 %</gpu_util><memory_util>10 %</memory_util></utilization>
        <fb_memory_usage><total>8 GiB</total><used>1 GiB</used><free>7 GiB</free></fb_memory_usage>
        <temperature><gpu_temp>55 C</gpu_temp></temperature>
        <gpu_power_readings><instant_power_draw>40.0 W</instant_power_draw>
        <average_power_draw>35.0 W</average_power_draw>
        <gpu_ceiling_power_limit><current_power_limit>200 W</current_power_limit>
        </gpu_ceiling_power_limit></gpu_power_readings>
        <clocks><graphics_clock>1800 MHz</graphics_clock><sm_clock>1700 MHz</sm_clock></clocks>
        <processes><process_info><pid>42</pid><process_name>worker</process_name>
        <used_memory>512 MiB</used_memory><type>C</type></process_info></processes>
      </gpu>
    </nvidia_smi_log>
    """
    monkeypatch.setattr(gpu, "_bounded_command", lambda *_: (0, xml, b"", None))
    result = gpu.collect({"vendor": "nvidia", "processes": True, "reference_pid": 42})
    device = result["devices"][0]
    assert device["driver"] == "555.42"
    assert device["pci"] == "0000:01:00.0"
    assert device["memory"]["total_bytes"] == 8 * 1024**3
    assert device["utilization"]["gpu_percent"] == 42
    assert device["power"] == {"draw_w": 40, "average_w": 35, "limit_w": 200}
    assert "reference" not in device["processes"][0]
    result = gpu.collect({"vendor": "nvidia", "processes": False})
    assert result["devices"][0]["processes"] == []


def test_external_command_output_and_timeout_are_bounded():
    code = "print('ok')"
    rc, stdout, stderr, error = gpu._bounded_command([sys.executable, "-c", code], 2)
    assert rc == 0
    assert stdout == b"ok\n"
    assert stderr == b""
    assert error is None
    _rc, _stdout, _stderr, error = gpu._bounded_command(
        [sys.executable, "-c", "import time; time.sleep(2)"], 0.05
    )
    assert error and "timed out" in error


@pytest.mark.parametrize("options", [{"vendor": "wat"}, {"limit": 0}, {"interval": 11}])
def test_invalid_requests_fail_fast(options):
    with pytest.raises((TypeError, ValueError)):
        gpu.collect(options)


def test_sysfs_fallback_normalizes_metrics_and_ignores_connectors(tmp_path, monkeypatch):
    root = tmp_path / "drm"
    device = tmp_path / "devices" / "0000:03:00.0"
    device.mkdir(parents=True)
    (device / "vendor").write_text("0x1002")
    for name, value in {
        "mem_info_vram_total": 1000,
        "mem_info_vram_used": 200,
        "mem_info_gtt_used": 300,
        "gpu_busy_percent": 42,
    }.items():
        (device / name).write_text(str(value))
    (device / "pp_dpm_sclk").write_text("0: 500Mhz\n1: 1500Mhz *\n")
    for name in ("card0", "card0-DP-1"):
        (root / name).mkdir(parents=True)
        (root / name / "device").symlink_to(device)
    monkeypatch.setattr(gpu, "_SYSFS", root)
    values = gpu._sysfs_devices("amd")
    assert len(values) == 1
    assert values[0]["pci"] == "0000:03:00.0"
    assert values[0]["memory"]["kind"] == "vram"
    assert values[0]["memory"]["free_bytes"] == 800
    assert values[0]["memory"]["gtt_used_bytes"] == 300
    assert values[0]["clocks"]["graphics_mhz"] == 1500
