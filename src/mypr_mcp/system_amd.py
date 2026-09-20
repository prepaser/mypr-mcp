"""AMD SMI JSON adapter for the workstation resource collector."""

from __future__ import annotations

import json
import time
from typing import Any

from .system_gpu import _base, _bounded_command, _bytes, _canonical_pci, _number


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not isinstance(value, dict):
        return []
    for name in ("gpu_data", "gpus", "devices", "data", "gpu"):
        child = value.get(name)
        if isinstance(child, list):
            return [item for item in child if isinstance(item, dict)]
    return [value]


def _json(raw: bytes) -> Any:
    text = raw.decode("utf-8", "replace").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as first:
        starts = [index for index in (text.find("{"), text.find("[")) if index >= 0]
        if not starts:
            raise first
        return json.loads(text[min(starts) :])


def _find(value: dict[str, Any], *names: str) -> Any:
    wanted = {name.lower().replace("_", " ") for name in names}
    queue: list[Any] = [value]
    while queue:
        current = queue.pop(0)
        if isinstance(current, list):
            queue.extend(child for child in current if isinstance(child, (dict, list)))
            continue
        if not isinstance(current, dict):
            continue
        for key, child in current.items():
            if str(key).lower().replace("_", " ") in wanted and child is not None:
                return child
        queue.extend(child for child in current.values() if isinstance(child, (dict, list)))
    return None


def _scalar(value: dict[str, Any], *names: str) -> Any:
    wanted = {name.lower().replace("_", " ") for name in names}
    queue: list[Any] = [value]
    while queue:
        current = queue.pop(0)
        if isinstance(current, list):
            queue.extend(child for child in current if isinstance(child, (dict, list)))
            continue
        if not isinstance(current, dict):
            continue
        for key, child in current.items():
            if str(key).lower().replace("_", " ") not in wanted:
                continue
            if not isinstance(child, dict) or "value" in child or "val" in child:
                return child
            queue.append(child)
        queue.extend(child for child in current.values() if isinstance(child, (dict, list)))
    return None


def _value(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("value", value.get("val"))
    return value


def _pci_value(record: dict[str, Any]) -> Any:
    wanted = {"bdf", "pci bdf", "pci bus id", "bus id", "pci", "bus", "location"}
    queue: list[Any] = [record]
    while queue:
        current = queue.pop(0)
        if isinstance(current, list):
            queue.extend(child for child in current if isinstance(child, (dict, list)))
            continue
        if not isinstance(current, dict):
            continue
        for key, child in current.items():
            if str(key).lower().replace("_", " ") not in wanted:
                continue
            candidate = child
            if isinstance(candidate, dict):
                for nested in ("bdf", "bus_id", "pci_bus_id", "address", "value"):
                    if nested in candidate:
                        candidate = candidate[nested]
                        break
            if _canonical_pci(candidate) and ":" in str(candidate):
                return candidate
        queue.extend(child for child in current.values() if isinstance(child, (dict, list)))
    return None


def _identity(record: dict[str, Any], fallback: int | None = None) -> tuple[str | None, str | None]:
    pci = _pci_value(record)
    gpu = _find(record, "gpu", "gpu_id", "gpu_index", "device_id", "index")
    if isinstance(gpu, dict):
        gpu = _value(gpu)
    if gpu is None and fallback is not None:
        gpu = fallback
    return (_canonical_pci(pci), str(gpu) if gpu is not None else None)


def _device_for(
    devices: list[dict[str, Any]],
    identities: dict[str, int],
    record: dict[str, Any],
    fallback: int,
) -> dict[str, Any]:
    pci, gpu = _identity(record, fallback)
    if pci:
        for device in devices:
            if _canonical_pci(device.get("pci")) == pci:
                if gpu:
                    identities.setdefault(f"gpu:{gpu}", devices.index(device))
                return device
        item = _base("amd", "amd-smi", gpu or fallback)
        item["pci"] = pci
        item["id"] = pci
        devices.append(item)
        identities[f"pci:{pci}"] = len(devices) - 1
        if gpu:
            identities[f"gpu:{gpu}"] = len(devices) - 1
        return item
    if gpu and f"gpu:{gpu}" in identities:
        return devices[identities[f"gpu:{gpu}"]]
    if gpu:
        try:
            index = int(gpu)
        except ValueError:
            index = fallback
        if 0 <= index < len(devices):
            identities[f"gpu:{gpu}"] = index
            return devices[index]
    if fallback < len(devices):
        return devices[fallback]
    item = _base("amd", "amd-smi", gpu or fallback)
    devices.append(item)
    if gpu:
        identities[f"gpu:{gpu}"] = len(devices) - 1
    return item


def _set_name(device: dict[str, Any], record: dict[str, Any]) -> None:
    value = _find(record, "market_name", "market", "product_name", "device_name", "name")
    if value is not None and not isinstance(value, (dict, list)):
        device["name"] = device["model"] = str(_value(value))
    value = _scalar(record, "driver", "driver_version", "amdgpu_version")
    if value is not None and not isinstance(value, (dict, list)):
        device["driver"] = str(_value(value))
    value = _find(record, "uuid", "gpu_uuid", "unique_id")
    if value is not None and not isinstance(value, (dict, list)):
        device["uuid"] = str(_value(value))
    value = _pci_value(record)
    if value is not None:
        device["pci"] = _canonical_pci(_value(value))
    if device.get("uuid") or device.get("pci"):
        device["id"] = device.get("uuid") or device.get("pci")


def _merge_memory(device: dict[str, Any], record: dict[str, Any]) -> None:
    memory = device["memory"]
    memory["kind"] = "vram"
    values = {
        "total_bytes": _find(record, "vram_total", "memory_total", "total_vram"),
        "used_bytes": _find(record, "vram_used", "memory_used", "used_vram"),
        "free_bytes": _find(record, "vram_free", "memory_free", "free_vram"),
    }
    for key, value in values.items():
        if value is not None:
            parsed = _bytes(value)
            if parsed is not None:
                memory[key] = parsed
    vram = _find(record, "vram", "vram_info", "vram_memory")
    if isinstance(vram, dict):
        memory_type = _find(vram, "type", "memory_type", "vram_type")
        if memory_type is not None and not isinstance(memory_type, (dict, list)):
            memory["type"] = str(_value(memory_type))
        for key, aliases in {
            "total_bytes": ("total", "size", "capacity"),
            "used_bytes": ("used", "usage"),
            "free_bytes": ("free",),
        }.items():
            value = _find(vram, *aliases)
            parsed = _bytes(value) if value is not None else None
            if parsed is not None:
                memory[key] = parsed
    usage = _find(record, "mem_usage", "memory_usage")
    if isinstance(usage, dict):
        for key, aliases in {
            "total_bytes": ("vram_total", "total"),
            "used_bytes": ("vram_used", "used", "vram_mem"),
            "free_bytes": ("vram_free", "free"),
        }.items():
            value = _find(usage, *aliases)
            parsed = _bytes(value) if value is not None else None
            if parsed is not None:
                memory[key] = parsed


def _merge_metric(device: dict[str, Any], record: dict[str, Any]) -> None:
    _set_name(device, record)
    _merge_memory(device, record)
    util = device["utilization"]
    for key, aliases in {
        "gpu_percent": (
            "average_gfx_activity",
            "gfx_activity",
            "gfx_usage",
            "gpu_busy_percent",
            "gpu_utilization",
            "gpu_busy",
            "usage",
        ),
        "memory_percent": (
            "average_umc_activity",
            "umc_usage",
            "memory_activity",
            "memory_utilization",
            "mem_usage",
        ),
    }.items():
        value = _scalar(record, *aliases)
        parsed = _number(_value(value)) if value is not None else None
        if parsed is not None:
            util[key] = parsed
    temperature = device.setdefault("temperature", {})
    for key, aliases in {
        "edge_c": ("temperature_edge", "edge_temperature", "edge", "temperature"),
        "hotspot_c": ("temperature_hotspot", "hotspot_temperature", "hotspot"),
        "vram_c": ("temperature_vram", "vram_temperature", "vram"),
    }.items():
        value = _scalar(record, *aliases)
        parsed = _number(_value(value)) if value is not None else None
        if parsed is not None:
            temperature[key] = parsed
    if temperature:
        device["temperature_c"] = temperature.get("hotspot_c", temperature.get("edge_c"))
        device["metrics"].update(
            {f"temperature_{key}": value for key, value in temperature.items()}
        )
    power = device["power"]
    for key, aliases in {
        "draw_w": ("average_socket_power", "socket_power", "power", "power_usage"),
        "limit_w": ("power_limit", "socket_power_limit"),
    }.items():
        value = _scalar(record, *aliases)
        parsed = _number(_value(value)) if value is not None else None
        if parsed is not None:
            power[key] = parsed
    clocks = device["clocks"]
    for key, aliases in {
        "graphics_mhz": (
            "gfxclk",
            "gfx_clock",
            "graphics_clock",
            "sclk",
        ),
        "memory_mhz": ("memclk", "mem_clock", "memory_clock", "mclk"),
    }.items():
        value = _scalar(record, *aliases)
        parsed = _number(_value(value)) if value is not None else None
        if parsed is not None:
            clocks[key] = parsed
    clock_tree = _find(record, "clock", "clocks")
    if isinstance(clock_tree, dict):
        for key, aliases in {
            "graphics_mhz": ("gfx_0", "gfx", "sclk"),
            "memory_mhz": ("mem_0", "mem", "mclk"),
        }.items():
            group = next((clock_tree[name] for name in aliases if name in clock_tree), None)
            value = (
                _scalar(group, "clk", "current", "frequency") if isinstance(group, dict) else group
            )
            parsed = _number(value)
            if parsed is not None:
                clocks[key] = parsed


def _process_rows(record: dict[str, Any]) -> list[dict[str, Any]]:
    values = _find(record, "processes", "process_list", "process_info", "processes_info")
    if isinstance(values, dict):
        values = [values]
    if not isinstance(values, list):
        values = [record] if _find(record, "pid", "process_id") is not None else []
    result = []
    for process in values:
        if not isinstance(process, dict):
            continue
        pid = _number(_find(process, "pid", "process_id"))
        if pid is None:
            continue
        memory_usage = _find(process, "memory_usage")
        if not isinstance(memory_usage, dict):
            memory_usage = {}
        vram = _find(memory_usage, "vram_mem", "vram", "vram_memory")
        gtt = _find(memory_usage, "gtt_mem", "gtt", "gtt_memory")
        cpu = _find(memory_usage, "cpu_mem", "cpu", "cpu_memory")
        used = _find(process, "mem_usage", "memory", "used_memory") or vram
        row = {
            "pid": int(pid),
            "name": _value(_find(process, "name", "process_name")),
            "used_memory_bytes": _bytes(used),
            "vram_bytes": _bytes(vram),
            "gtt_bytes": _bytes(gtt),
            "cpu_memory_bytes": _bytes(cpu),
            "type": _value(_find(process, "type", "engine")) or "unknown",
            "pid_namespace": "driver",
        }
        usage = _find(process, "usage")
        if isinstance(usage, dict):
            row["engine_time_ns"] = {
                str(key): _number(_value(value)) for key, value in usage.items()
            }
        result.append(row)
    return result


def _run(argv: tuple[str, ...], request: dict[str, Any], warnings: list[str], deadline: float):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None
    rc, raw, stderr, error = _bounded_command(list(argv), remaining)
    if error:
        if "No such file" not in error and "not found" not in error:
            warnings.append(error)
        return None
    if rc:
        detail = (stderr or raw).decode("utf-8", "replace").strip()[:256]
        if detail:
            warnings.append(f"{' '.join(argv)} failed ({rc}): {detail}")
        return None
    try:
        return _json(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        warnings.append(f"{' '.join(argv)} returned invalid JSON: {exc}")
        return None


def collect(
    request: dict[str, Any], devices: list[dict[str, Any]], warnings: list[str]
) -> list[dict[str, Any]]:
    """Merge AMD SMI static, metric, and optional process JSON into devices."""

    timeout = float(request.get("timeout", 5))
    interval = float(request.get("interval", 0))
    reserved = interval + 0.15 if request.get("processes") else 0.15
    deadline = time.monotonic() + max(0.01, timeout - reserved)
    identities: dict[str, int] = {}
    for index, device in enumerate(devices):
        pci = _canonical_pci(device.get("pci"))
        if pci:
            identities[f"pci:{pci}"] = index
        device.setdefault("memory", {}).setdefault("kind", "unknown")
        device.setdefault("processes", [])
    source_available = False
    source_missing = True
    for argv in (("amd-smi", "static", "--json"), ("amd-smi", "metric", "--json")):
        data = _run(argv, request, warnings, deadline)
        if data is None:
            continue
        source_available = True
        source_missing = False
        records = _records(data)
        for index, record in enumerate(records):
            device = _device_for(devices, identities, record, index)
            _set_name(device, record)
            if argv[1] == "static":
                _merge_memory(device, record)
            else:
                _merge_metric(device, record)
            device["source"] = "amd-smi+sysfs" if "sysfs" in device.get("source", "") else "amd-smi"
    if request.get("processes"):
        data = _run(("amd-smi", "process", "--json"), request, warnings, deadline)
        if data is not None:
            source_available = True
            source_missing = False
            for index, record in enumerate(_records(data)):
                device = _device_for(devices, identities, record, index)
                rows = _process_rows(record)
                device["processes"].extend(rows)
                if len(device["processes"]) > int(request.get("limit", 20)):
                    device["truncated"] = True
                    device["warnings"].append(
                        f"amd-smi process list truncated to {request.get('limit', 20)} entries"
                    )
                    device["processes"] = device["processes"][: int(request.get("limit", 20))]
    if not devices and source_missing:
        return []
    if not source_available and devices:
        warnings.append("AMD SMI is unavailable; using Linux sysfs data")
    for device in devices:
        device.setdefault("vendor", "amd")
        device.setdefault("source", "sysfs")
        device.setdefault("memory", {}).setdefault("kind", "unknown")
    return devices
