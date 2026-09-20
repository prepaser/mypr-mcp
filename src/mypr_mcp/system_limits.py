"""Best-effort resource limits for the process hosting a workspace kernel."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


def _number(value: str) -> int | None:
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError, UnicodeError:
        return None


def _unescape_mount(value: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), value)


def _mounts(proc_root: Path) -> list[tuple[Path, Path]]:
    result = []
    try:
        lines = (proc_root / "self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError, UnicodeError:
        return result
    for line in lines:
        left, _, right = line.partition(" - ")
        fields = left.split()
        values = right.split()
        if len(fields) < 5 or not values or values[0] != "cgroup2":
            continue
        result.append((Path(_unescape_mount(fields[4])), Path(_unescape_mount(fields[3]))))
    return result


def _cgroup_path(pid: int, proc_root: Path) -> str | None:
    try:
        lines = (proc_root / str(pid) / "cgroup").read_text(encoding="utf-8").splitlines()
    except OSError, UnicodeError:
        return None
    for line in lines:
        fields = line.split(":", 2)
        if len(fields) == 3 and not fields[1]:
            return fields[2] or "/"
    return None


def _cgroup_directories(pid: int, proc_root: Path) -> list[Path]:
    relative = _cgroup_path(pid, proc_root)
    if relative is None:
        return []
    mounts = _mounts(proc_root)
    if not mounts:
        return []
    selected: tuple[Path, Path, Path] | None = None
    for candidate_mount, candidate_root in sorted(
        mounts, key=lambda item: len(str(item[1])), reverse=True
    ):
        relative_path = Path(relative.lstrip("/"))
        root_path = Path(str(candidate_root).lstrip("/"))
        if root_path != Path("."):
            try:
                relative_path = relative_path.relative_to(root_path)
            except ValueError:
                continue
        selected = (candidate_mount, candidate_root, relative_path)
        break
    if selected is None:
        return []
    mount, _, relative_path = selected
    path = (mount / relative_path).resolve()
    values = []
    while path == mount or mount in path.parents:
        values.append(path)
        if path == mount:
            break
        path = path.parent
    return values


def _limit(value: str | None) -> int | None:
    if value is None or value == "max":
        return None
    number = _number(value)
    return number if number is not None and number >= 0 else None


def collect_limits(
    reference_pid: int | None = None,
    *,
    proc_root: Path | str = "/proc",
) -> dict[str, Any]:
    """Return limits visible to *reference_pid*.

    ``None`` means that a limit could not be established or is unlimited.  The
    accompanying ``known`` fields distinguish the two cases where useful.
    """

    root = Path(proc_root)
    pid = int(reference_pid or os.getpid())
    warnings: list[str] = []
    affinity: list[int] | None = None
    try:
        import psutil

        affinity = sorted(psutil.Process(pid).cpu_affinity())
    except Exception:
        warnings.append("CPU affinity is unavailable")

    directories = _cgroup_directories(pid, root)
    cpu_quota: float | None = None
    memory_limit: int | None = None
    memory_current: int | None = None
    cpuset: str | None = None
    mounts = _mounts(root)
    hierarchy_roots = {mount for mount, mount_root in mounts if mount_root == Path("/")}
    cpu_known = bool(directories)
    memory_known = bool(directories)
    cpu_missing = False
    memory_missing = False
    memory_headrooms: list[int] = []
    if not directories:
        warnings.append("cgroup v2 limits are unavailable")
    for directory in directories:
        cpu = _read(directory / "cpu.max")
        if cpu:
            fields = cpu.split()
            if len(fields) >= 2:
                quota = _limit(fields[0])
                period = _number(fields[1])
                quota_valid = fields[0] == "max" or quota is not None
                if quota_valid and period and period > 0:
                    if quota is not None:
                        value = quota / period
                        cpu_quota = value if cpu_quota is None else min(cpu_quota, value)
                else:
                    cpu_known = False
                    warnings.append("CPU quota value is invalid")
            else:
                cpu_known = False
                warnings.append("CPU quota value is invalid")
        elif directory not in hierarchy_roots:
            cpu_missing = True
        memory_raw = _read(directory / "memory.max")
        memory_valid = memory_raw == "max" or _limit(memory_raw) is not None
        if not memory_valid and directory not in hierarchy_roots:
            memory_missing = True
        if not memory_valid and memory_raw is not None:
            memory_known = False
            warnings.append("memory limit value is invalid")
        memory = _limit(memory_raw)
        if memory is not None:
            memory_limit = memory if memory_limit is None else min(memory_limit, memory)
        current = _number(_read(directory / "memory.current") or "")
        if current is not None and current < 0:
            current = None
        if current is not None and memory_current is None:
            memory_current = current
        if memory is not None and current is not None:
            memory_headrooms.append(max(0, memory - current))
        elif memory is not None:
            memory_known = False
            warnings.append("memory usage is unavailable for one or more cgroup ancestors")
        effective = _read(directory / "cpuset.cpus.effective")
        if effective and cpuset is None:
            cpuset = effective

    memory_headroom = min(memory_headrooms) if memory_headrooms else None
    if cpu_missing:
        warnings.append("CPU quota is unavailable for one or more cgroup ancestors")
    if memory_missing:
        warnings.append("memory limit is unavailable for one or more cgroup ancestors")
    if len(warnings) > 8:
        warnings = warnings[:7] + [f"{len(warnings) - 7} additional limit warnings omitted"]
    return {
        "available": True,
        "warnings": warnings,
        "pid": pid,
        "cpu_affinity": affinity,
        "cpuset": cpuset,
        "cpu_quota_cpus": cpu_quota,
        "cpu_quota_known": cpu_known and not cpu_missing,
        "memory_limit_bytes": memory_limit,
        "memory_current_bytes": memory_current,
        "memory_headroom_bytes": memory_headroom,
        "memory_limit_known": memory_known and not memory_missing,
        "cgroup": {
            "version": 2 if directories else None,
            "path": _cgroup_path(pid, root),
            "mount_roots": [str(mount) for mount, _ in _mounts(root)],
            "mounts": [
                {"mountpoint": str(mount), "root": str(mount_root)}
                for mount, mount_root in _mounts(root)
            ],
            "ancestors": [str(directory) for directory in directories],
        },
    }
