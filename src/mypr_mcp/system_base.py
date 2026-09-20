"""Synchronous, bounded workstation resource collectors.

The runtime calls this module in a helper process so a slow filesystem or
vendor driver cannot block the persistent Python kernel.
"""

from __future__ import annotations

import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any

from .system_limits import collect_limits

_SYSFS = Path("/sys")
_PROC = Path("/proc")
_NETWORK_FS = {
    "9p",
    "afs",
    "cifs",
    "ncp",
    "nfs",
    "nfs4",
    "smbfs",
    "sshfs",
    "ceph",
    "glusterfs",
    "fuse.sshfs",
}
_MAX_TEXT = 4096


def _psutil():
    import psutil

    return psutil


def _text(value: Any, limit: int = _MAX_TEXT) -> str | None:
    if value is None:
        return None
    try:
        raw = str(value).encode("utf-8", "replace")
    except Exception:
        return None
    return raw[:limit].decode("utf-8", "ignore")


def _base() -> dict[str, Any]:
    return {"available": True, "warnings": []}


def _error(result: dict[str, Any], message: str) -> None:
    result["available"] = False
    result.setdefault("warnings", []).append(message)


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None


def _cpu_model() -> str | None:
    value = None
    cpuinfo = _read(_PROC / "cpuinfo")
    if cpuinfo:
        for line in cpuinfo.splitlines():
            key, _, item = line.partition(":")
            if key.strip().lower() in {"model name", "hardware"} and item.strip():
                value = item.strip()
                break
    return value or platform.processor() or None


def _physical_storage() -> list[dict[str, Any]]:
    result = []
    block = _SYSFS / "block"
    try:
        entries = sorted(block.iterdir(), key=lambda item: item.name)
    except OSError:
        return result
    for device in entries:
        size = _read(device / "size")
        sectors = None
        try:
            sectors = int(size) if size is not None else None
        except ValueError:
            pass
        model = _read(device / "device/model") or _read(device / "model")
        rotational = _read(device / "queue/rotational")
        result.append(
            {
                "name": device.name,
                "size_bytes": sectors * 512 if sectors is not None else None,
                "model": _text(model),
                "rotational": rotational in {"1", "y", "Y"} if rotational is not None else None,
            }
        )
    return result


def _memory(value: Any) -> dict[str, Any]:
    return {
        "total_bytes": getattr(value, "total", None),
        "available_bytes": getattr(value, "available", None),
        "used_bytes": getattr(value, "used", None),
        "free_bytes": getattr(value, "free", None),
        "percent": getattr(value, "percent", None),
    }


def _load_average(psutil) -> list[float] | None:
    try:
        values = psutil.getloadavg()
    except AttributeError, OSError:
        return None
    return [float(value) for value in values]


def _counter(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    return {
        key: getattr(value, key, None)
        for key in (
            "bytes_sent",
            "bytes_recv",
            "packets_sent",
            "packets_recv",
            "errin",
            "errout",
            "dropin",
            "dropout",
        )
        if hasattr(value, key)
    }


def _delta(current: Any, previous: Any, elapsed: float, fields: tuple[str, ...]) -> dict[str, Any]:
    result = _counter(current)
    for field in fields:
        now = getattr(current, field, None)
        before = getattr(previous, field, None) if previous is not None else None
        result[field] = now
        result[f"{field}_per_sec"] = (
            (now - before) / elapsed
            if isinstance(now, (int, float))
            and isinstance(before, (int, float))
            and now >= before
            and elapsed > 0
            else None
        )
    return result


def _snapshot_cpu(psutil) -> Any:
    return psutil.cpu_times(percpu=False)


def _cpu_usage(before: Any, after: Any, elapsed: float) -> dict[str, Any]:
    fields = [
        name
        for name in dir(after)
        if not name.startswith("_") and isinstance(getattr(after, name), (int, float))
    ]
    deltas = {
        name: max(0.0, float(getattr(after, name)) - float(getattr(before, name, 0)))
        for name in fields
    }
    deltas.pop("guest", None)
    deltas.pop("guest_nice", None)
    total = sum(deltas.values())
    idle = deltas.get("idle", 0.0) + deltas.get("iowait", 0.0)
    return {
        "percent": ((total - idle) / total * 100) if total else None,
        "iowait_percent": (deltas.get("iowait", 0.0) / total * 100) if total else None,
        "busy_seconds": total - idle,
        "total_seconds": total,
        "fields": deltas,
        "interval_seconds": elapsed,
    }


def _cpu_seconds(value: Any) -> float | None:
    values = [getattr(value, name, None) for name in ("user", "system")]
    numeric = [float(item) for item in values if isinstance(item, (int, float))]
    return sum(numeric) if numeric else None


def _io_rate(
    current: Any, previous: Any, field: str, same_process: bool, elapsed: float
) -> float | None:
    now = getattr(current, field, None)
    before = getattr(previous, field, None) if previous is not None else None
    if (
        not same_process
        or not isinstance(now, (int, float))
        or not isinstance(before, (int, float))
    ):
        return None
    return (now - before) / elapsed if now >= before and elapsed > 0 else None


def _info(request: dict[str, Any]) -> dict[str, Any]:
    result = _base()
    try:
        psutil = _psutil()
    except Exception as exc:
        _error(result, f"system information unavailable: {type(exc).__name__}: {_text(exc, 512)}")
        return result
    result.update(
        {
            "os": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "machine": platform.machine(),
                "architecture": platform.architecture()[0],
            },
            "python": {
                "version": platform.python_version(),
                "implementation": platform.python_implementation(),
                "executable": sys.executable,
            },
        }
    )
    try:
        freq = psutil.cpu_freq()
    except Exception as exc:
        freq = None
        result["warnings"].append(f"CPU frequency unavailable: {_text(exc, 256)}")
    try:
        result["cpu"] = {
            "model": _cpu_model(),
            "physical_cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
            "load_average": _load_average(psutil),
            "frequency_mhz": getattr(freq, "current", None) if freq else None,
            "min_frequency_mhz": getattr(freq, "min", None) if freq else None,
            "max_frequency_mhz": getattr(freq, "max", None) if freq else None,
        }
    except Exception as exc:
        result["cpu"] = None
        result["warnings"].append(f"CPU information unavailable: {_text(exc, 256)}")
    try:
        result["memory"] = _memory(psutil.virtual_memory())
    except Exception as exc:
        result["memory"] = None
        result["warnings"].append(f"memory information unavailable: {_text(exc, 256)}")
    result["storage"] = _physical_storage()
    try:
        result["limits"] = collect_limits(request.get("reference_pid"))
    except Exception as exc:
        result["limits"] = None
        result["warnings"].append(f"resource limits unavailable: {_text(exc, 256)}")
    return result


def _base_usage(request: dict[str, Any]) -> dict[str, Any]:
    result = _base()
    interval = _interval(request)
    psutil = _psutil()
    before_time = time.monotonic()
    try:
        before_cpu = _snapshot_cpu(psutil)
    except Exception as exc:
        before_cpu = None
        result["warnings"].append(f"CPU counters unavailable: {_text(exc, 256)}")
    try:
        before_network = psutil.net_io_counters(pernic=True) or {}
    except Exception as exc:
        before_network = {}
        result["warnings"].append(f"network counters unavailable: {_text(exc, 256)}")
    try:
        before_disk = psutil.disk_io_counters(perdisk=True) or {}
    except Exception as exc:
        before_disk = {}
        result["warnings"].append(f"disk I/O counters unavailable: {_text(exc, 256)}")
    time.sleep(interval)
    elapsed = max(time.monotonic() - before_time, 1e-9)
    try:
        after_cpu = _snapshot_cpu(psutil)
    except Exception as exc:
        after_cpu = None
        result["warnings"].append(f"CPU counters unavailable: {_text(exc, 256)}")
    try:
        after_network = psutil.net_io_counters(pernic=True) or {}
    except Exception as exc:
        after_network = {}
        result["warnings"].append(f"network counters unavailable: {_text(exc, 256)}")
    try:
        after_disk = psutil.disk_io_counters(perdisk=True) or {}
    except Exception as exc:
        after_disk = {}
        result["warnings"].append(f"disk I/O counters unavailable: {_text(exc, 256)}")
    try:
        memory = _memory(psutil.virtual_memory())
    except Exception as exc:
        memory = None
        result["warnings"].append(f"memory information unavailable: {_text(exc, 256)}")
    try:
        swap = _memory(psutil.swap_memory())
    except Exception as exc:
        swap = None
        result["warnings"].append(f"swap information unavailable: {_text(exc, 256)}")
    cpu = (
        _cpu_usage(before_cpu, after_cpu, elapsed)
        if before_cpu is not None and after_cpu is not None
        else None
    )
    result.update(
        {
            "sample_seconds": elapsed,
            "cpu": {**cpu, "load_average": _load_average(psutil)} if cpu else None,
            "memory": memory,
            "swap": swap,
            "network": {
                name: _delta(
                    after_network.get(name),
                    before_network.get(name),
                    elapsed,
                    ("bytes_sent", "bytes_recv"),
                )
                for name in sorted(set(before_network) | set(after_network))
                if after_network.get(name) is not None
            },
            "disk_io": {
                name: _delta(
                    after_disk.get(name),
                    before_disk.get(name),
                    elapsed,
                    ("read_bytes", "write_bytes"),
                )
                for name in sorted(set(before_disk) | set(after_disk))
                if after_disk.get(name) is not None
            },
        }
    )
    try:
        result["limits"] = collect_limits(request.get("reference_pid"))
    except Exception as exc:
        result["limits"] = None
        result["warnings"].append(f"resource limits unavailable: {_text(exc, 256)}")
    return result


def _process_snapshot(
    psutil, pids: set[int] | None, user: str | None
) -> tuple[dict[int, Any], list[str]]:
    processes: dict[int, Any] = {}
    warnings: list[str] = []
    try:
        clear_cache = getattr(psutil.process_iter, "cache_clear", None)
        if callable(clear_cache):
            clear_cache()
        iterator = psutil.process_iter()
    except Exception as exc:
        return {}, [f"process enumeration failed: {_text(exc, 256)}"]
    for process in iterator:
        try:
            pid = int(process.pid)
            if pids is not None and pid not in pids:
                continue
            if user is not None and process.username() != user:
                continue
            create_time = float(process.create_time())
            cpu_times = process.cpu_times()
            try:
                memory = process.memory_info()
            except Exception as exc:
                memory = None
                warnings.append(f"memory unavailable for pid {pid}: {_text(exc, 160)}")
            try:
                io = process.io_counters()
            except Exception as exc:
                io = None
                warnings.append(f"I/O unavailable for pid {pid}: {_text(exc, 160)}")
            identity = (
                create_time,
                cpu_times,
                memory,
                io,
                time.monotonic(),
            )
            processes[pid] = (process, identity)
        except Exception as exc:
            warnings.append(f"process unavailable: {_text(exc, 160)}")
    return processes, warnings


def _processes(request: dict[str, Any]) -> dict[str, Any]:
    result = _base()
    psutil = _psutil()
    interval = _interval(request)
    limit = min(max(int(request.get("limit", 20)), 1), 200)
    sort = str(request.get("sort", "cpu"))
    if sort not in {"cpu", "rss", "read", "write"}:
        raise ValueError("sort must be cpu, rss, read, or write")
    raw_pids = request.get("pids")
    pids = {int(pid) for pid in raw_pids} if raw_pids is not None else None
    user = request.get("user")
    if user is not None:
        user = str(user)
    started = time.monotonic()
    before, warnings = _process_snapshot(psutil, pids, user)
    time.sleep(interval)
    after, after_warnings = _process_snapshot(psutil, pids, user)
    elapsed = max(time.monotonic() - started, 1e-9)
    warnings.extend(after_warnings)
    rows = []
    for pid, (process, current) in after.items():
        previous = before.get(pid)
        create_time, cpu_times, memory, io, sampled = current
        span = max(sampled - previous[1][4], 1e-9) if previous else elapsed
        old_create = previous[1][0] if previous else None
        old_cpu = previous[1][1] if previous else None
        old_io = previous[1][3] if previous else None
        cpu_now = _cpu_seconds(cpu_times)
        cpu_old = _cpu_seconds(old_cpu)
        try:
            row = {
                "pid": pid,
                "create_time": create_time,
                "name": _text(process.name()),
                "username": _text(process.username()),
                "status": _text(process.status()),
                "cpu_percent": (
                    (cpu_now - cpu_old) / span * 100
                    if old_create == create_time
                    and cpu_now is not None
                    and cpu_old is not None
                    and cpu_now >= cpu_old
                    else None
                ),
                "rss_bytes": getattr(memory, "rss", None),
                "read_bytes": getattr(io, "read_bytes", None),
                "write_bytes": getattr(io, "write_bytes", None),
                "read_bytes_per_sec": _io_rate(
                    io, old_io, "read_bytes", old_create == create_time, span
                ),
                "write_bytes_per_sec": _io_rate(
                    io, old_io, "write_bytes", old_create == create_time, span
                ),
            }
            if request.get("cmdline", False):
                try:
                    row["cmdline"] = [_text(item, 1024) for item in process.cmdline()][:128]
                except Exception as exc:
                    row["cmdline"] = None
                    warnings.append(f"cmdline unavailable for pid {pid}: {_text(exc, 160)}")
            rows.append(row)
        except Exception as exc:
            warnings.append(f"process {pid} unavailable: {_text(exc, 160)}")
    key = {
        "cpu": "cpu_percent",
        "rss": "rss_bytes",
        "read": "read_bytes_per_sec",
        "write": "write_bytes_per_sec",
    }[sort]
    rows.sort(
        key=lambda item: item[key] if isinstance(item[key], (int, float)) else -1, reverse=True
    )
    result.update(
        {
            "sample_seconds": elapsed,
            "sort": sort,
            "processes": rows[:limit],
            "total": len(rows),
            "truncated": len(rows) > limit,
            "warnings": warnings,
        }
    )
    return result


def _mounts(psutil, path: str | None, workspace: str | None) -> tuple[list[Any], list[str]]:
    warnings: list[str] = []
    if path is not None:
        target = Path(path)
        if not target.is_absolute():
            target = Path(workspace or os.getcwd()) / target
        try:
            target = target.resolve()
        except OSError:
            target = target.absolute()
        return [target], warnings
    try:
        return [
            item for item in psutil.disk_partitions(all=False) if item.fstype not in _NETWORK_FS
        ], warnings
    except Exception as exc:
        return [], [f"filesystem enumeration failed: {_text(exc, 256)}"]


def _disks(request: dict[str, Any]) -> dict[str, Any]:
    result = _base()
    psutil = _psutil()
    entries, warnings = _mounts(psutil, request.get("path"), request.get("workspace"))
    filesystems = []
    for entry in entries:
        if isinstance(entry, Path):
            mount = str(entry)
            device = None
            fstype = None
        else:
            mount = str(entry.mountpoint)
            device = _text(getattr(entry, "device", None))
            fstype = _text(getattr(entry, "fstype", None))
        try:
            usage = psutil.disk_usage(mount)
            filesystems.append(
                {
                    "mountpoint": mount,
                    "device": device,
                    "fstype": fstype,
                    "total_bytes": usage.total,
                    "used_bytes": usage.used,
                    "free_bytes": usage.free,
                    "percent": usage.percent,
                }
            )
        except Exception as exc:
            warnings.append(f"disk usage unavailable for {mount}: {_text(exc, 160)}")
    result.update({"disks": filesystems, "storage": _physical_storage(), "warnings": warnings})
    return result


def _interval(request: dict[str, Any]) -> float:
    try:
        interval = float(request.get("interval", 0.5))
    except (TypeError, ValueError) as exc:
        raise ValueError("interval must be a number") from exc
    if not 0.1 <= interval <= 10:
        raise ValueError("interval must be between 0.1 and 10 seconds")
    return interval


def collect(section: str, request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Collect one system section and return only JSON-compatible values."""

    request = dict(request or {})
    collectors = {
        "info": _info,
        "base_usage": _base_usage,
        "processes": _processes,
        "disks": _disks,
    }
    try:
        result = collectors[section](request)
    except KeyError as exc:
        raise ValueError(f"unknown system section: {section}") from exc
    except ValueError:
        raise
    except Exception as exc:
        result = _base()
        _error(result, f"{section} collection failed: {type(exc).__name__}: {_text(exc, 512)}")
    warnings = result.get("warnings")
    if isinstance(warnings, list):
        bounded = [_text(item, 512) or "collector warning" for item in warnings[:8]]
        if len(warnings) > 8:
            bounded[-1] = f"{len(warnings) - 7} additional warnings omitted"
        result["warnings"] = bounded
    return json.loads(json.dumps(result, default=str))
