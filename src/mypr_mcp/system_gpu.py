"""Bounded GPU discovery and telemetry helpers.

The module deliberately has no vendor Python dependencies.  Vendor utilities and
the Linux DRM/proc files are optional and failures are returned as warnings.
"""

from __future__ import annotations

import math
import os
import re
import selectors
import shutil
import signal
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .system_drm import canonical_pci as _canonical_pci

_MAX_OUTPUT = 4 * 1024 * 1024
_VENDORS = {"all", "nvidia", "amd", "intel"}
_SYSFS = Path("/sys/class/drm")


def _number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("value", value.get("val"))
        if value is None:
            return None
    match = re.fullmatch(
        r"\s*([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)\s*[%A-Za-z°/]*\s*",
        str(value).replace(",", ""),
    )
    if not match:
        return None
    number = float(match[1])
    if not math.isfinite(number):
        return None
    return int(number) if number.is_integer() else number


def _bytes(value: Any) -> int | None:
    if value is None:
        return None
    unit = ""
    if isinstance(value, dict):
        unit = str(value.get("unit", ""))
        value = value.get("value", value.get("val"))
    text = f"{value} {unit}".strip().replace(",", "")
    match = re.fullmatch(r"(\d+(?:\.\d*)?|\.\d+)\s*([A-Za-z]+)?", text)
    if not match:
        return None
    number = float(match[1])
    suffix = (match[2] or "").lower()
    factors = {
        "": 1,
        "b": 1,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
    }
    factor = factors.get(suffix)
    return int(number * factor) if factor is not None and math.isfinite(number) else None


def _key(mapping: dict[str, Any], *names: str) -> Any:
    lowered = {str(k).lower().replace("_", " "): v for k, v in mapping.items()}
    for name in names:
        value = lowered.get(name.lower().replace("_", " "))
        if value is not None:
            return value
    return None


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError, UnicodeError:
        return None


def _bounded_command(
    argv: list[str], timeout: float
) -> tuple[int | None, bytes, bytes, str | None]:
    """Run a vendor utility with a bounded pipe and process-group cleanup."""
    try:
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except (OSError, ValueError) as exc:
        return None, b"", b"", f"{argv[0]}: {exc}"
    selector = selectors.DefaultSelector()
    assert process.stdout is not None and process.stderr is not None
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    output = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout
    error: str | None = None
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                error = f"{argv[0]} timed out after {timeout:g}s"
                break
            for key, _ in selector.select(min(remaining, 0.1)):
                try:
                    chunk = os.read(key.fileobj.fileno(), 65536)
                except OSError:
                    chunk = b""
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                stream = output[key.data]
                if len(stream) + len(chunk) > _MAX_OUTPUT:
                    error = f"{argv[0]} output exceeded {_MAX_OUTPUT} bytes"
                    break
                stream.extend(chunk)
            if error:
                break
            if process.poll() is not None and not selector.get_map():
                break
    finally:
        if error:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except OSError:
                pass
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except OSError:
                pass
            process.wait()
        selector.close()
        for stream in (process.stdout, process.stderr):
            if stream and not stream.closed:
                stream.close()
    return process.returncode, bytes(output["stdout"]), bytes(output["stderr"]), error


def _base(vendor: str, source: str, index: Any = None) -> dict[str, Any]:
    value: dict[str, Any] = {
        "vendor": vendor,
        "source": source,
        "id": str(index) if index is not None else None,
        "name": None,
        "model": None,
        "driver": None,
        "pci": None,
        "uuid": None,
        "utilization": {
            key: None
            for key in ("gpu_percent", "memory_percent", "encoder_percent", "decoder_percent")
        },
        "memory": {"kind": "unknown", "total_bytes": None, "used_bytes": None, "free_bytes": None},
        "temperature_c": None,
        "power": {"draw_w": None, "average_w": None, "limit_w": None},
        "clocks": {key: None for key in ("graphics_mhz", "sm_mhz", "memory_mhz", "video_mhz")},
        "processes": [],
        "metrics": {},
        "warnings": [],
    }
    return value


def _xml_text(node: ET.Element, *paths: str) -> str | None:
    for path in paths:
        found = node.find(path)
        if found is not None and found.text:
            return found.text.strip()
    return None


def _nvidia(request: dict[str, Any], warnings: list[str]) -> list[dict[str, Any]]:
    if not shutil.which("nvidia-smi") and not _sysfs_devices("nvidia"):
        return []
    timeout = float(request["timeout"])
    argv = ["nvidia-smi", "-q", "-x"]
    rc, raw, stderr, error = _bounded_command(argv, timeout)
    if error:
        warnings.append(error)
        return _sysfs_devices("nvidia")
    if rc != 0:
        detail = (stderr.decode("utf-8", "replace") + raw.decode("utf-8", "replace")).strip()[:512]
        warnings.append(f"nvidia-smi failed ({rc}): {detail}".rstrip())
        return _sysfs_devices("nvidia")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        warnings.append(f"nvidia-smi returned invalid XML: {exc}")
        return _sysfs_devices("nvidia")
    driver = _xml_text(root, "driver_version")
    result: list[dict[str, Any]] = []
    for index, gpu in enumerate(root.findall(".//gpu")):
        item = _base("nvidia", "nvidia-smi", index)
        item.update(
            {
                "name": _xml_text(gpu, "product_name"),
                "model": _xml_text(gpu, "product_name"),
                "driver": driver,
                "pci": _canonical_pci(_xml_text(gpu, "pci/pci_bus_id")),
                "uuid": _xml_text(gpu, "uuid"),
            }
        )
        item["id"] = item["uuid"] or item["pci"] or str(index)
        util = item["utilization"]
        util.update(
            {
                "gpu_percent": _number(_xml_text(gpu, "utilization/gpu_util")),
                "memory_percent": _number(_xml_text(gpu, "utilization/memory_util")),
                "encoder_percent": _number(_xml_text(gpu, "utilization/encoder_util")),
                "decoder_percent": _number(_xml_text(gpu, "utilization/decoder_util")),
            }
        )
        item["memory"].update(
            {
                "kind": "dedicated_vram",
                "total_bytes": _bytes(_xml_text(gpu, "fb_memory_usage/total")),
                "used_bytes": _bytes(_xml_text(gpu, "fb_memory_usage/used")),
                "free_bytes": _bytes(_xml_text(gpu, "fb_memory_usage/free")),
            }
        )
        item["temperature_c"] = _number(_xml_text(gpu, "temperature/gpu_temp"))
        item["power"].update(
            {
                "draw_w": _number(
                    _xml_text(
                        gpu,
                        "gpu_power_readings/instant_power_draw",
                        "gpu_power_readings/power_draw",
                        "power_readings/power_draw",
                    )
                ),
                "average_w": _number(_xml_text(gpu, "gpu_power_readings/average_power_draw")),
                "limit_w": _number(
                    _xml_text(
                        gpu,
                        "gpu_power_readings/current_power_limit",
                        "gpu_power_readings/gpu_ceiling_power_limit/current_power_limit",
                        "gpu_power_readings/power_limit",
                        "power_readings/power_limit",
                    )
                ),
            }
        )
        item["clocks"].update(
            {
                "graphics_mhz": _number(_xml_text(gpu, "clocks/graphics_clock")),
                "sm_mhz": _number(_xml_text(gpu, "clocks/sm_clock")),
                "memory_mhz": _number(_xml_text(gpu, "clocks/mem_clock")),
                "video_mhz": _number(_xml_text(gpu, "clocks/video_clock")),
            }
        )
        processes: list[dict[str, Any]] = []
        for proc in gpu.findall(".//processes/process_info") if request.get("processes") else []:
            pid = _number(_xml_text(proc, "pid"))
            name = _xml_text(proc, "process_name")
            if pid is None and not name:
                continue
            processes.append(
                {
                    "pid": pid,
                    "name": name,
                    "type": _xml_text(proc, "type") or "unknown",
                    "pid_namespace": "driver",
                    "used_memory_bytes": _bytes(_xml_text(proc, "used_memory")),
                }
            )
        if len(processes) > int(request["limit"]):
            item["truncated"] = True
            warnings.append(f"nvidia-smi process list truncated to {request['limit']} entries")
        item["processes"] = processes[: int(request["limit"])]
        result.append(item)
    if not result:
        result = _sysfs_devices("nvidia")
    return result


def _sysfs_devices(vendor: str) -> list[dict[str, Any]]:
    expected = {"nvidia": "0x10de", "amd": "0x1002", "intel": "0x8086"}[vendor]
    devices: list[dict[str, Any]] = []
    for card in sorted(_SYSFS.glob("card[0-9]*")):
        if not re.fullmatch(r"card\d+", card.name):
            continue
        device = card / "device"
        if _read(device / "vendor") != expected:
            continue
        pci = _canonical_pci(device.resolve().name)
        item = _base(vendor, "sysfs", pci)
        item.update(
            {
                "pci": pci,
                "driver": (device / "driver").resolve().name
                if (device / "driver").exists()
                else None,
                "name": f"{vendor.upper()} GPU",
                "model": _read(device / "product_name") or _read(device / "name"),
            }
        )
        item["metrics"]["card"] = card.name
        item["metrics"]["device_path"] = str(device)
        for path, key, scale in (
            ("gpu_busy_percent", "gpu_busy_percent", 1),
            ("mem_info_vram_total", "vram_total_bytes", 1),
            ("mem_info_vram_used", "vram_used_bytes", 1),
        ):
            value = _number(_read(device / path))
            if value is not None:
                item["metrics"][key] = int(value * scale)
                if key == "gpu_busy_percent":
                    item["utilization"]["gpu_percent"] = value
                elif key == "vram_total_bytes":
                    item["memory"]["total_bytes"] = int(value * scale)
                    item["memory"]["kind"] = "vram"
                elif key == "vram_used_bytes":
                    item["memory"]["used_bytes"] = int(value * scale)
        total, used = item["memory"]["total_bytes"], item["memory"]["used_bytes"]
        if total is not None and used is not None and total >= used:
            item["memory"]["free_bytes"] = total - used
        gtt = _number(_read(device / "mem_info_gtt_used"))
        if gtt is not None:
            item["memory"]["gtt_used_bytes"] = gtt
        for name, key in (("pp_dpm_sclk", "graphics_mhz"), ("pp_dpm_mclk", "memory_mhz")):
            text = _read(device / name) or ""
            active = re.search(r":\s*(\d+)\s*MHz\s*\*", text, re.I)
            if active:
                item["clocks"][key] = int(active[1])
        frequencies = {}
        for path in [
            card / "gt_act_freq_mhz",
            *sorted(device.glob("gt/gt*/rps_act_freq_mhz")),
            *sorted(device.glob("tile*/gt*/freq*/act_freq")),
        ]:
            value = _number(_read(path))
            if value is not None:
                frequencies[str(path.relative_to(card))] = value
        if frequencies:
            item["metrics"]["frequencies_mhz"] = frequencies
            if len(frequencies) == 1:
                item["clocks"]["graphics_mhz"] = next(iter(frequencies.values()))
        hwmons = sorted((device / "hwmon").glob("hwmon*"))
        for hwmon in hwmons:
            temp = _number(_read(hwmon / "temp1_input"))
            power = _number(_read(hwmon / "power1_average") or _read(hwmon / "power1_input"))
            if temp is not None:
                item["temperature_c"] = temp / 1000
            if power is not None:
                item["power"]["draw_w"] = power / 1_000_000
        devices.append(item)
    return devices


def _attach_drm(
    devices: list[dict[str, Any]], request: dict[str, Any], warnings: list[str]
) -> None:
    devices = [device for device in devices if device["vendor"] in {"amd", "intel"}]
    if not request.get("processes") or not devices:
        return
    try:
        from .system_drm import sample
    except ImportError:
        return
    pcis = {device.get("pci") for device in devices if device.get("pci")}
    if not pcis:
        return
    try:
        result = sample(
            sorted(pcis),
            interval=float(request["interval"]),
            reference_pid=request.get("reference_pid"),
            limit=int(request["limit"]),
        )
    except Exception as exc:
        warnings.append(f"DRM process metrics unavailable: {type(exc).__name__}: {exc}")
        return
    for device in devices:
        pci = device.get("pci")
        if pci in result:
            device["drm"] = result[pci]
    for value in result.values():
        for warning in value.get("warnings", []):
            warnings.append(str(warning))


def _validate(request: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise TypeError("GPU request must be a mapping")
    value = dict(request)
    vendor = value.get("vendor", "all")
    if vendor not in _VENDORS:
        raise ValueError(f"vendor must be one of {sorted(_VENDORS)}")
    interval = value.get("interval", 0.5)
    if (
        isinstance(interval, bool)
        or not isinstance(interval, (int, float))
        or not math.isfinite(interval)
        or not 0 <= interval <= 10
    ):
        raise ValueError("interval must be between 0 and 10 seconds")
    timeout = value.get("timeout", max(5.0, float(interval) + 1))
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be positive")
    limit = value.get("limit", 20)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ValueError("limit must be between 1 and 200")
    reference = value.get("reference_pid")
    if reference is not None and (
        isinstance(reference, bool) or not isinstance(reference, int) or reference < 1
    ):
        raise ValueError("reference_pid must be a positive integer")
    processes = value.get("processes", False)
    if not isinstance(processes, bool):
        raise TypeError("processes must be a boolean")
    value.update(
        {
            "vendor": vendor,
            "interval": float(interval),
            "timeout": float(timeout),
            "limit": limit,
            "processes": processes,
            "reference_pid": reference,
        }
    )
    return value


def collect(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Collect normalized GPU information without requiring any vendor software."""
    request = _validate({} if request is None else request)
    warnings: list[str] = []
    vendors = [request["vendor"]] if request["vendor"] != "all" else ["nvidia", "amd", "intel"]
    devices: list[dict[str, Any]] = []
    for vendor in vendors:
        if vendor == "nvidia":
            devices.extend(_nvidia(request, warnings))
        else:
            if vendor == "amd":
                from .system_amd import collect as collect_vendor
            else:
                from .system_intel import collect as collect_vendor
            devices.extend(collect_vendor(request, _sysfs_devices(vendor), warnings))
    _attach_drm(devices, request, warnings)
    for device in devices:
        device["measurement"] = {
            "interval_seconds": request["interval"],
            "period": "vendor_window",
            "provider_period": "reported_by_vendor",
        }
        missing = [
            f"{section}.{key}"
            for section, keys in {
                "utilization": ("gpu_percent",),
                "memory": ("total_bytes", "used_bytes"),
                "power": ("draw_w",),
                "clocks": ("graphics_mhz",),
            }.items()
            for key in keys
            if device[section].get(key) is None
        ]
        if device["temperature_c"] is None:
            missing.append("temperature_c")
        if missing:
            message = f"{device['id']}: provider did not report {', '.join(missing)}"
            device["warnings"].append(message)
            warnings.append(message)
    bounded_warnings = [
        {"code": "gpu_provider", "message": message[:512]} for message in warnings[:8]
    ]
    if len(warnings) > 8:
        bounded_warnings[-1] = {
            "code": "warnings_truncated",
            "message": f"{len(warnings) - 7} additional GPU diagnostics omitted",
        }
    truncated = any(
        device.get("truncated") or device.get("drm", {}).get("truncated") for device in devices
    )
    return {"devices": devices, "warnings": bounded_warnings, "truncated": truncated}


__all__ = ["collect"]
