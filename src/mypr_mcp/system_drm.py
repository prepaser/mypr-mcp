"""Sample visible DRM clients without double-counting shared file descriptors."""

from __future__ import annotations

import re
import time
from pathlib import Path

_PROC = Path("/proc")
_PCI = re.compile(r"(?:([0-9a-f]{4,8}):)?([0-9a-f]{2}:[0-9a-f]{2}\.[0-7])", re.I)


def canonical_pci(value):
    match = _PCI.fullmatch(str(value))
    if not match:
        return None
    return f"{int(match[1] or '0', 16):04x}:{match[2].lower()}"


def _read(path):
    try:
        with path.open(encoding="utf-8", errors="replace") as stream:
            return stream.read(65536)
    except OSError:
        return None


def _start(directory):
    text = _read(directory / "stat")
    try:
        return int(text.rsplit(") ", 1)[1].split()[19])
    except AttributeError, IndexError, ValueError:
        return None


def _uint(value, unit=None):
    match = re.fullmatch(r"(\d+)(?:\s+(\w+))?", value.strip())
    if not match or (unit and match[2] not in {None, unit}):
        return None
    return int(match[1])


def _memory(value):
    match = re.fullmatch(r"(\d+)(?:\s+(B|KiB|MiB))?", value.strip())
    if not match:
        return None
    return int(match[1]) * {None: 1, "B": 1, "KiB": 1024, "MiB": 1024**2}[match[2]]


def _fields(text):
    return dict(
        line.split(":", 1) for line in text.splitlines() if line.startswith("drm-") and ":" in line
    )


def _snapshot(pcis):
    clients = {}
    inaccessible = 0
    unidentified = 0
    for directory in _PROC.glob("[0-9]*"):
        if not directory.name.isdecimal():
            continue
        start = _start(directory)
        if start is None:
            continue
        pid = int(directory.name)
        try:
            paths = list((directory / "fdinfo").iterdir())
        except OSError:
            inaccessible += 1
            continue
        pending = []
        for path in paths:
            text = _read(path)
            if text is None:
                inaccessible += 1
                continue
            if "drm-client-id:" not in text:
                continue
            fields = _fields(text)
            pci = canonical_pci(fields.get("drm-pdev", "").strip())
            client = _uint(fields.get("drm-client-id", ""))
            if pci is None or client is None:
                unidentified += 1
                continue
            if pci not in pcis:
                continue
            engines, capacity, memory, cycles, total_cycles = {}, {}, {}, {}, {}
            for name, value in fields.items():
                if name.startswith("drm-engine-capacity-"):
                    count = _uint(value)
                    if count:
                        capacity[name.removeprefix("drm-engine-capacity-")] = count
                elif name.startswith("drm-engine-"):
                    engines[name.removeprefix("drm-engine-")] = _uint(value, "ns")
                elif name.startswith("drm-cycles-"):
                    cycles[name.removeprefix("drm-cycles-")] = _uint(value)
                elif name.startswith("drm-total-cycles-"):
                    total_cycles[name.removeprefix("drm-total-cycles-")] = _uint(value)
                else:
                    match = re.fullmatch(
                        r"drm-(total|shared|resident|memory|purgeable|active)-(.+)", name
                    )
                    if match and not name.startswith("drm-total-cycles-"):
                        kind = "resident" if match[1] == "memory" else match[1]
                        memory.setdefault(match[2], {})[f"{kind}_bytes"] = _memory(value)
            pending.append(
                (
                    (pci, client),
                    {
                        "engines": engines,
                        "cycles": cycles,
                        "total_cycles": total_cycles,
                        "capacity": capacity,
                        "memory_regions": memory,
                        "sample_time": time.monotonic(),
                    },
                )
            )
        if _start(directory) != start:
            continue
        name = (_read(directory / "comm") or "").strip()[:256] or None
        for key, entry in pending:
            old = clients.get(key)
            owners = old["owners"] if old else {}
            owners[(pid, start)] = {"pid": pid, "start_ticks": start, "name": name}
            # A shared descriptor is one client even when inherited by another PID.
            clients[key] = {**entry, "owners": owners}
    return clients, inaccessible, unidentified


def sample(pcis, *, interval, reference_pid, limit=20):
    pcis = {pci for value in pcis if (pci := canonical_pci(value)) is not None}
    if not pcis:
        return {}
    started = time.monotonic()
    before, denied_before, unknown_before = _snapshot(pcis)
    time.sleep(interval)
    after, denied_after, unknown_after = _snapshot(pcis)
    elapsed = time.monotonic() - started
    warnings = []
    if denied_before or denied_after:
        warnings.append(
            "Some process fdinfo files were inaccessible; DRM client coverage is partial"
        )
    if unknown_before or unknown_after:
        warnings.append("DRM clients without a usable PCI/client identity were omitted")
    result = {
        pci: {
            "clients": [],
            "sample_seconds": elapsed,
            "truncated": False,
            "warnings": list(warnings),
            "scope": "visible_drm_clients",
        }
        for pci in sorted(pcis)
    }
    for (pci, client), current in sorted(after.items()):
        previous = before.get((pci, client))
        same = previous is not None and bool(current["owners"].keys() & previous["owners"].keys())
        span = current["sample_time"] - previous["sample_time"] if same else None
        engine_usage = {}
        for engine, now in current["engines"].items():
            old = previous["engines"].get(engine) if same else None
            delta = max(0, now - old) if now is not None and old is not None else None
            capacity = current["capacity"].get(engine, 1)
            engine_usage[engine] = {
                "busy_ns": delta,
                "capacity": capacity,
                "percent": delta / (span * 1e9 * capacity) * 100
                if delta is not None and span and span > 0
                else None,
            }
        for engine, now in current["cycles"].items():
            old = previous["cycles"].get(engine) if same else None
            total = current["total_cycles"].get(engine)
            old_total = previous["total_cycles"].get(engine) if same else None
            delta = max(0, now - old) if now is not None and old is not None else None
            total_delta = total - old_total if total is not None and old_total is not None else None
            capacity = current["capacity"].get(engine, 1)
            item = engine_usage.setdefault(engine, {"capacity": capacity, "percent": None})
            item.update({"busy_cycles": delta, "total_cycles": total_delta})
            if total_delta is not None and total_delta > 0 and delta is not None:
                item["percent"] = delta / (total_delta * capacity) * 100
        owners = sorted(current["owners"].values(), key=lambda owner: owner["pid"])
        item = result[pci]
        if len(item["clients"]) >= limit:
            item["truncated"] = True
            continue
        item["clients"].append(
            {
                "client_id": client,
                "owners": owners[:limit],
                "owner_count": len(owners),
                "reference": any(owner["pid"] == reference_pid for owner in owners),
                "engines": engine_usage,
                "memory_regions": current["memory_regions"],
                "sample_seconds": span,
            }
        )
        item["truncated"] |= len(owners) > limit
    return result
