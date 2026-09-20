"""Intel GPU adapters used by the isolated workstation collector."""

from __future__ import annotations

import json
import os
import selectors
import signal
import subprocess
import time
from typing import Any

from .system_gpu import _base, _bounded_command, _bytes, _canonical_pci, _key, _number

_MAX_OUTPUT = 1024 * 1024


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not isinstance(value, dict):
        return []
    for name in ("device_list", "devices", "gpus", "data"):
        child = value.get(name)
        if isinstance(child, list):
            return [item for item in child if isinstance(item, dict)]
    return [value]


def _json_documents(raw: bytes) -> list[dict[str, Any]]:
    text = raw.decode("utf-8", "replace")
    decoder = json.JSONDecoder()
    result: list[dict[str, Any]] = []
    position = 0
    while position < len(text):
        starts = [
            index for index in (text.find("{", position), text.find("[", position)) if index >= 0
        ]
        if not starts:
            break
        position = min(starts)
        try:
            value, end = decoder.raw_decode(text, position)
        except json.JSONDecodeError:
            position += 1
            continue
        if isinstance(value, dict):
            result.append(value)
        position = end
    return result


def _complete_objects(raw: bytes) -> list[dict[str, Any]]:
    """Extract only complete top-level JSON objects from a stream."""
    text = raw.decode("utf-8", "replace")
    result: list[dict[str, Any]] = []
    start = None
    depth = 0
    quoted = False
    escaped = False
    for index, char in enumerate(text):
        if start is None:
            if char == "{":
                start = index
                depth = 1
            continue
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    value = json.loads(text[start : index + 1])
                except json.JSONDecodeError:
                    value = None
                if isinstance(value, dict):
                    result.append(value)
                start = None
    return result


def _json_payload(raw: bytes) -> Any:
    text = raw.decode("utf-8", "replace").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for marker in ("{", "["):
            position = text.find(marker)
            if position < 0:
                continue
            try:
                return decoder.raw_decode(text, position)[0]
            except json.JSONDecodeError:
                continue
        raise ValueError("utility returned no complete JSON value") from None


def _metric_value(value: Any) -> Any:
    if isinstance(value, dict):
        for name in ("value", "val", "current", "average", "avg", "mean"):
            if value.get(name) is not None:
                return value[name]
    return value


def _metric_measurement(metric: dict[str, Any]) -> Any:
    value = _key(metric, "value", "metric_value", "data")
    if value is None:
        value = _key(metric, "current", "average", "avg", "mean")
    if isinstance(value, dict):
        measured = _metric_value(value)
        unit = value.get("unit", metric.get("unit"))
    else:
        measured, unit = value, metric.get("unit")
    return {"value": measured, "unit": unit} if unit else measured


def _normalize_metric(record: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    levels = _key(record, "device_level", "metrics", "metric_list")
    if not isinstance(levels, list):
        return result
    aliases = {
        "GPU_UTILIZATION": "gpu_utilization",
        "GPU_MEMORY_UTILIZATION": "memory_utilization",
        "MEMORY_UTILIZATION": "memory_utilization",
        "GPU_CORE_TEMPERATURE": "temperature",
        "GPU_TEMPERATURE": "temperature",
        "POWER": "power",
        "GPU_POWER": "power",
        "GPU_FREQUENCY": "clock",
        "GPU_ACTUAL_FREQUENCY": "clock",
        "GPU_MEMORY_TOTAL": "memory_total",
        "MEMORY_TOTAL": "memory_total",
        "GPU_MEMORY_USED": "memory_used",
        "MEMORY_USED": "memory_used",
    }
    for metric in levels:
        if not isinstance(metric, dict):
            continue
        kind = str(_key(metric, "metric_type", "type", "name") or "").upper()
        measured = _metric_measurement(metric)
        if measured is None:
            continue
        field = aliases.get(kind.removeprefix("XPUM_STATS_"))
        if field in {"memory_total", "memory_used"} and isinstance(measured, (int, float)):
            measured = {"value": measured, "unit": "MiB"}
        if field:
            result[field] = measured
        else:
            result.setdefault("engine_metrics", {})[kind] = measured
    return result


def _identity(record: dict[str, Any], fallback: int) -> str:
    value = _key(record, "device_id", "gpu", "id", "index")
    return str(value if value is not None else fallback)


def _merge_record(device: dict[str, Any], record: dict[str, Any]) -> None:
    device["source"] = "xpu-smi+sysfs" if "sysfs" in str(device.get("source", "")) else "xpu-smi"
    name = _key(record, "device_name", "name", "model", "product_name")
    if name is not None:
        device["name"] = device["model"] = str(name)
    driver = _key(record, "driver", "driver_version")
    if driver is not None:
        device["driver"] = str(driver)
    pci = _key(record, "pci_bdf", "pci_bdf_address", "pci_bus_id", "bdf", "pci", "bus")
    if pci is not None:
        device["pci"] = _canonical_pci(pci)
    uuid = _key(record, "device_uuid", "uuid")
    if uuid is not None:
        device["uuid"] = str(uuid)
    if device.get("uuid"):
        device["id"] = device["uuid"]
    elif device.get("pci"):
        device["id"] = device["pci"]
    drm_device = _key(record, "drm_device", "drm")
    if drm_device is not None:
        device.setdefault("metrics", {})["drm_device"] = str(drm_device)
    util = _metric_value(_key(record, "gpu_utilization", "utilization"))
    if util is not None:
        device["utilization"]["gpu_percent"] = _number(util)
    memory_util = _metric_value(_key(record, "memory_utilization"))
    if memory_util is not None:
        device["utilization"]["memory_percent"] = _number(memory_util)
    temperature = _metric_value(_key(record, "temperature", "temperature_c"))
    if temperature is not None:
        device["temperature_c"] = _number(temperature)
    power = _metric_value(_key(record, "power"))
    if power is not None:
        device["power"]["draw_w"] = _number(power)
    clock = _metric_value(_key(record, "clock", "clocks"))
    if clock is not None:
        device["clocks"]["graphics_mhz"] = _number(clock)
    engine_metrics = _key(record, "engine_metrics")
    if isinstance(engine_metrics, dict):
        device.setdefault("metrics", {})["engine_metrics"] = engine_metrics
    total = _key(record, "memory_total", "memory_physical_size_byte")
    used = _key(record, "memory_used", "memory_used_size_byte")
    free = _key(record, "memory_free", "memory_free_size_byte")
    if total is not None:
        device["memory"]["total_bytes"] = _bytes(total)
    if used is not None:
        device["memory"]["used_bytes"] = _bytes(used)
    if free is not None:
        device["memory"]["free_bytes"] = _bytes(free)


def _find_device(
    devices: list[dict[str, Any]],
    by_id: dict[str, dict[str, Any]],
    record: dict[str, Any],
    identity: str,
) -> dict[str, Any] | None:
    device = by_id.get(identity)
    if device is not None:
        return device
    pci = _key(record, "pci_bdf", "pci_bdf_address", "pci_bus_id", "bdf", "pci", "bus")
    canonical = _canonical_pci(pci) if pci is not None else None
    if canonical is not None:
        return next((item for item in devices if item.get("pci") == canonical), None)
    return None


def _run_json(argv: list[str], timeout: float) -> tuple[Any | None, str | None]:
    rc, raw, stderr, error = _bounded_command(argv, timeout)
    if error:
        return None, error
    if rc:
        detail = stderr.decode("utf-8", "replace") + raw.decode("utf-8", "replace")
        return None, f"{' '.join(argv)} failed ({rc}): {detail.strip()[:256]}"
    try:
        return _json_payload(raw), None
    except (ValueError, UnicodeDecodeError) as exc:
        return None, f"{' '.join(argv)} returned invalid JSON: {exc}"


def _stream_json(argv: list[str], budget: float) -> tuple[list[dict[str, Any]], str | None]:
    """Collect complete JSON samples from intel_gpu_top and terminate it cleanly."""
    try:
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except (OSError, ValueError) as exc:
        return [], f"{argv[0]}: {exc}"
    assert process.stdout is not None and process.stderr is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    output = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + max(0.05, budget)
    timed_out = False
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            for key, _ in selector.select(min(remaining, 0.05)):
                try:
                    chunk = os.read(key.fileobj.fileno(), 65536)
                except OSError:
                    chunk = b""
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                stream = output[key.data]
                if len(stream) + len(chunk) <= _MAX_OUTPUT:
                    stream.extend(chunk)
                else:
                    timed_out = True
                    break
            if timed_out:
                break
            if process.poll() is not None and not selector.get_map():
                break
    finally:
        if process.poll() is None:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
            except OSError:
                pass
        try:
            process.wait(timeout=0.25)
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
    documents = _complete_objects(bytes(output["stdout"]))
    if timed_out:
        return documents, f"{argv[0]} sample budget expired"
    if process.returncode:
        detail = bytes(output["stderr"]).decode("utf-8", "replace").strip()[:256]
        return documents, f"{argv[0]} failed ({process.returncode}): {detail}".rstrip()
    return documents, None


def _top_metrics(sample: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, Any] = {"source": "intel_gpu_top"}
    period = sample.get("period")
    if isinstance(period, dict):
        duration = _number(period.get("duration"))
        unit = str(period.get("unit", "ms")).lower()
        if duration is not None:
            metrics["period_ms"] = duration * (1000 if unit.startswith("s") else 1)
    engines: dict[str, dict[str, Any]] = {}
    for name, values in (sample.get("engines") or {}).items():
        if not isinstance(values, dict):
            continue
        engine: dict[str, Any] = {}
        busy = _number(values.get("busy"))
        if busy is not None:
            engine["busy_percent"] = busy
        for field in ("sema", "wait", "total"):
            number = _number(values.get(field))
            if number is not None:
                engine[field] = number
        if engine:
            engines[str(name)] = engine
    metrics["engines"] = engines
    frequency = sample.get("frequency")
    if isinstance(frequency, dict):
        actual = _number(frequency.get("actual"))
        if actual is not None:
            metrics["frequency_mhz"] = actual
    power = sample.get("power")
    if isinstance(power, dict):
        gpu = _number(power.get("GPU"))
        if gpu is not None:
            metrics["power_w"] = gpu
    return metrics


def _card(device: dict[str, Any], index: int) -> str | None:
    metrics = device.get("metrics")
    value = metrics.get("card") if isinstance(metrics, dict) else None
    if value is None:
        value = device.get("card")
    if value is None:
        return None
    value = str(value)
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    return value if value.startswith("card") and value[4:].isdigit() else None


def collect(
    request: dict[str, Any], devices: list[dict[str, Any]], warnings: list[str]
) -> list[dict[str, Any]]:
    """Merge Intel XPU-SMI and intel_gpu_top data into supplied devices."""
    timeout = float(request.get("timeout", 5))
    interval = float(request.get("interval", 0.5))
    reserve = interval + 0.15 if request.get("processes") else 0.05
    deadline = time.monotonic() + max(0.05, timeout - reserve)
    by_id: dict[str, dict[str, Any]] = {}
    for device in devices:
        if device.get("vendor") == "intel":
            by_id[str(device.get("id"))] = device

    discovery, error = _run_json(
        ["xpu-smi", "discovery", "-j"], max(0.05, deadline - time.monotonic())
    )
    records = []
    if error:
        if "No such file" not in error and "not found" not in error:
            warnings.append(error)
    else:
        records = [_normalize_metric(record) for record in _records(discovery)]
    for index, record in enumerate(records):
        identity = _identity(record, index)
        device = _find_device(devices, by_id, record, identity)
        if device is None:
            device = _base("intel", "xpu-smi", identity)
            devices.append(device)
            by_id[identity] = device
        _merge_record(device, record)
        by_id[identity] = device
    stats_ids = list(
        dict.fromkeys(_identity(record, index) for index, record in enumerate(records))
    )
    for identity in stats_ids:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            warnings.append("intel xpu-smi collection deadline expired")
            break
        stats, error = _run_json(["xpu-smi", "stats", "-d", identity, "-j"], remaining)
        if error:
            if "No such file" not in error and "not found" not in error:
                warnings.append(error)
            continue
        for record in [_normalize_metric(item) for item in _records(stats)]:
            record_identity = _key(record, "device_id", "gpu", "id", "index")
            target_identity = identity if record_identity is None else str(record_identity)
            target = _find_device(devices, by_id, record, target_identity)
            if target is None:
                target = by_id.get(identity)
            if target is not None:
                _merge_record(target, record)
    for index, device in enumerate([item for item in devices if item.get("vendor") == "intel"]):
        card = _card(device, index)
        if card is None:
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            warnings.append("intel_gpu_top collection deadline expired")
            break
        budget = min(max(0.15, interval + 0.2), max(0.05, remaining - 0.1))
        samples, error = _stream_json(
            [
                "intel_gpu_top",
                "-d",
                f"drm:/dev/dri/{card}",
                "-J",
                "-o",
                "-",
                "-s",
                str(max(100, int(interval * 1000))),
            ],
            budget,
        )
        if error and not samples and "No such file" not in error and "not found" not in error:
            warnings.append(error)
        if samples:
            device.setdefault("metrics", {})["intel_gpu_top"] = _top_metrics(samples[-1])
    return devices


__all__ = ["collect"]
