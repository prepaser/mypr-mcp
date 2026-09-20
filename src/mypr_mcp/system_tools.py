"""Bounded workstation inspection without blocking the shared Python kernel."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

_MAX_OUTPUT = 32768
_PROBE_OUTPUT = 1024 * 1024
_GPUS = ("gpu:nvidia", "gpu:amd", "gpu:intel")


def _number(value, name, *, minimum=0, maximum=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    if value <= minimum or (maximum is not None and value > maximum):
        raise ValueError(f"{name} is outside its supported range")
    return float(value)


def _size(value):
    return len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode())


def _bounded(result):
    if _size(result) <= _MAX_OUTPUT:
        return result
    result["truncated"] = True
    result["warnings"].append(
        {"code": "output_limit", "message": "Some details were omitted to fit 32 KiB"}
    )
    while _size(result) > _MAX_OUTPUT:
        candidates = []

        def visit(value, path, candidates=candidates):
            if isinstance(value, list) and value:
                candidates.append((_size(value), path, value))
                for index, child in enumerate(value):
                    visit(child, f"{path}.{index}")
            elif isinstance(value, dict):
                for key, child in value.items():
                    visit(child, f"{path}.{key}")

        for key, value in result.items():
            if key not in {"warnings", "sources", "scope"}:
                visit(value, key)
        if not candidates:
            # Collector diagnostics and optional strings are also bounded.
            for key in list(result):
                if key not in {
                    "collected_at",
                    "duration_seconds",
                    "scope",
                    "sources",
                    "warnings",
                    "truncated",
                }:
                    result[key] = None
            result["warnings"] = result["warnings"][:8]
            break
        _, path, values = max(candidates, key=lambda item: item[0])
        removed = max(1, len(values) // 2)
        del values[-removed:]
        result.setdefault("omitted", {})[path] = result.get("omitted", {}).get(path, 0) + removed
    return result


async def _finish(task):
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


class SystemTools:
    def __init__(self, workspace):
        self.workspace = Path(workspace).resolve()
        self.reference_pid = os.getpid()
        self._slots = asyncio.Semaphore(4)

    @staticmethod
    def _validate(interval, timeout):
        timeout = _number(timeout, "timeout")
        if interval is not None:
            interval = _number(interval, "interval", minimum=0, maximum=10)
            if interval < 0.1:
                raise ValueError("interval must be between 0.1 and 10 seconds")
            if timeout <= interval:
                raise ValueError("timeout must be greater than interval")
        return interval, timeout

    async def info(self, *, timeout=5):  # noqa: ASYNC109
        return await self._collect(["info", *_GPUS], timeout=timeout, mode="info")

    async def usage(self, interval=0.5, *, timeout=5):  # noqa: ASYNC109
        self._validate(interval, timeout)
        return await self._collect(
            ["base_usage", "disks", *_GPUS], timeout=timeout, interval=interval
        )

    async def processes(
        self,
        *,
        sort="cpu",
        limit=20,
        interval=0.5,
        pids=None,
        user=None,
        cmdline=False,
        timeout=5,  # noqa: ASYNC109
    ):  # noqa: ASYNC109
        self._validate(interval, timeout)
        if sort not in {"cpu", "rss", "read", "write"}:
            raise ValueError("sort must be cpu, rss, read, or write")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        if pids is not None:
            if (
                not isinstance(pids, (list, tuple))
                or len(pids) > 200
                or any(type(pid) is not int or pid <= 0 for pid in pids)
            ):
                raise ValueError("pids must contain at most 200 positive integers")
            pids = list(dict.fromkeys(pids))
        if user is not None and (not isinstance(user, str) or not user):
            raise ValueError("user must be a non-empty username")
        if type(cmdline) is not bool:
            raise TypeError("cmdline must be a boolean")
        return await self._collect(
            ["processes"],
            timeout=timeout,
            interval=interval,
            sort=sort,
            limit=limit,
            pids=pids,
            user=user,
            cmdline=cmdline,
        )

    async def gpus(self, interval=0.5, *, processes=False, timeout=5):  # noqa: ASYNC109
        self._validate(interval, timeout)
        if type(processes) is not bool:
            raise TypeError("processes must be a boolean")
        return await self._collect(
            _GPUS, timeout=timeout, interval=interval, processes=processes, limit=20
        )

    async def disks(self, path=None, *, timeout=5):  # noqa: ASYNC109
        if path is not None:
            path = await asyncio.to_thread(Path(path).expanduser)
            if not path.is_absolute():
                path = self.workspace / path
            path = str(path.absolute())
        return await self._collect(["disks"], timeout=timeout, path=path)

    async def _collect(self, sections, *, timeout, **options):  # noqa: ASYNC109
        _, timeout = self._validate(None, timeout)
        started = time.monotonic()
        deadline = started + timeout
        result = {
            "collected_at": datetime.now(UTC).isoformat(),
            "scope": {
                "pid": self.reference_pid,
                "visibility": "kernel",
                "workspace": str(self.workspace),
            },
            "sources": {},
            "warnings": [],
            "truncated": False,
        }
        request = {"workspace": str(self.workspace), "reference_pid": self.reference_pid, **options}

        async def collect(section):
            before = time.monotonic()
            try:
                async with asyncio.timeout_at(deadline):
                    async with self._slots:
                        data = await self._probe(
                            section, {**request, "timeout": max(0.01, deadline - time.monotonic())}
                        )
                return (
                    section,
                    data,
                    {"status": "ok", "duration_seconds": round(time.monotonic() - before, 6)},
                )
            except Exception as exc:
                code = "timeout" if isinstance(exc, TimeoutError) else "unavailable"
                return (
                    section,
                    {},
                    {"status": code, "error": f"{type(exc).__name__}: {str(exc)[:256]}"},
                )

        jobs = [asyncio.create_task(collect(section)) for section in sections]
        try:
            collected = await asyncio.gather(*jobs)
        finally:
            for job in jobs:
                if not job.done():
                    job.cancel()
            await _finish(asyncio.create_task(self._join(jobs)))
        for section, data, source in collected:
            result["sources"][section] = source
            if source["status"] != "ok":
                result["warnings"].append(
                    {"source": section, "code": source["status"], "message": source["error"]}
                )
            warnings = data.pop("warnings", [])
            available = data.pop("available", True)
            if source["status"] == "ok":
                if not available:
                    source["status"] = "unavailable"
                elif warnings:
                    source["status"] = "partial"
            for warning in warnings[:8]:
                if not isinstance(warning, dict):
                    warning = {"message": str(warning)}
                result["warnings"].append(
                    {
                        "source": section,
                        "code": str(warning.get("code", "partial"))[:80],
                        "message": str(warning.get("message", warning.get("error", warning)))[:512],
                    }
                )
            result["truncated"] |= bool(data.pop("truncated", False))
            omitted = data.pop("omitted", {})
            if omitted:
                result.setdefault("omitted", {}).update(
                    {f"{section}.{key}": value for key, value in omitted.items()}
                )
            if section.startswith("gpu:"):
                result.setdefault("gpus", []).extend(data.get("devices", []))
            else:
                result.update(data)
        if len(result["warnings"]) > 16:
            result["warnings"] = result["warnings"][:15] + [
                {"code": "warnings_truncated", "message": "Additional diagnostics omitted"}
            ]
        result["duration_seconds"] = round(time.monotonic() - started, 6)
        return _bounded(result)

    @staticmethod
    async def _join(jobs):
        await asyncio.gather(*jobs, return_exceptions=True)

    async def _probe(self, section, request):
        guard = Path(__file__).with_name("process_guard.py")
        worker = Path(__file__).with_name("system_probe.py")
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                str(guard),
                "--parent-pid",
                str(os.getpid()),
                "--tree",
                "--",
                sys.executable,
                str(worker),
                cwd=self.workspace,
                start_new_session=True,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        )
        proc = None
        readers = []
        try:
            proc = await asyncio.shield(launch)
            readers = [
                asyncio.create_task(self._read(proc.stdout, _PROBE_OUTPUT)),
                asyncio.create_task(self._read(proc.stderr, 65536)),
            ]
            payload = json.dumps({"section": section, **request}).encode()
            proc.stdin.write(payload)
            await proc.stdin.drain()
            proc.stdin.close()
            stdout, stderr = await asyncio.gather(*readers)
            await proc.wait()
            try:
                response = json.loads(stdout)
            except (ValueError, UnicodeError) as exc:
                raise RuntimeError(
                    f"Invalid collector response: {stderr.decode(errors='replace')[-256:]}"
                ) from exc
            if (
                not isinstance(response, dict)
                or not response.get("ok")
                or not isinstance(response.get("data"), dict)
            ):
                raise RuntimeError(
                    response.get("error", "Collector failed")
                    if isinstance(response, dict)
                    else "Collector failed"
                )
            if proc.returncode:
                raise RuntimeError(f"Collector exited with status {proc.returncode}")
            return response["data"]
        finally:
            if proc is None:
                with contextlib.suppress(Exception):
                    proc = await _finish(launch)
            if proc is not None and proc.returncode is None:
                await _finish(asyncio.create_task(self._stop(proc)))
            for reader in readers:
                reader.cancel()
            if readers:
                await _finish(asyncio.create_task(self._join(readers)))

    @staticmethod
    async def _read(stream, limit):
        output = bytearray()
        while chunk := await stream.read(65536):
            if len(output) + len(chunk) > limit:
                raise RuntimeError("Collector output exceeded its byte limit")
            output.extend(chunk)
        return bytes(output)

    @staticmethod
    async def _stop(proc):
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
