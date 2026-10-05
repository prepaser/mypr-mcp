"""Manager-owned network scans and durable scan result pages."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import math
import os
import secrets
import shutil
import sys
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .async_utils import finish_owned, wait_owned
from .file_io import open_regular, read_bytes
from .services import Shells

_RESULT_LIMIT = 16 * 1024 * 1024
_DEFAULT_PAGE_ENTRIES = 100
_DEFAULT_PAGE_BYTES = 32768
_DEFAULT_MAX_PROBES = 1_000_000
_DEFAULT_MAX_DURATION = 3600.0
_PERSISTED_JSON_LIMIT = 1 * 1024 * 1024
_TERMINAL = {"succeeded", "failed", "cancelled", "lost"}


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class ScanService:
    """Start, supervise, and page scans without owning a kernel task."""

    def __init__(self, workspace: str | os.PathLike[str], shells: Shells, track=None):
        self.workspace = Path(workspace).expanduser().resolve()
        self.shells = shells
        self.root = self.workspace / ".mypr" / "scans"
        self.root.mkdir(parents=True, exist_ok=True)
        self._records: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._monitors: dict[str, asyncio.Task[None]] = {}
        self._state_lock = asyncio.Lock()
        self._load_records()
        self._track = track

    def _load_records(self) -> None:
        for path in self.root.glob("*.json"):
            if path.name.endswith((".request.json", ".summary.json")):
                continue
            try:
                record = json.loads(read_bytes(path, max_bytes=_PERSISTED_JSON_LIMIT))
            except OSError, ValueError:
                continue
            if not isinstance(record, dict) or record.get("id") != path.stem:
                continue
            if record.get("state") not in _TERMINAL:
                record.update(state="lost", error="scan manager restarted before completion")
                if record.get("mode") in {"tcp", "udp"}:
                    record["complete"] = False
                self._write_record(record)
                self._remove_request(record)
        for path in self.root.glob("*.request.json"):
            with contextlib.suppress(OSError):
                path.unlink()

    def _remove_request(self, record: dict[str, Any]) -> None:
        config = record.get("config")
        if not isinstance(config, str):
            return
        request = Path(config)
        try:
            if request.parent.resolve(strict=False) != self.root.resolve(strict=False):
                return
        except (OSError, RuntimeError):
            return
        name = request.name.removesuffix(".request.json")
        if len(name) != 32 or any(char not in "0123456789abcdef" for char in name):
            return
        with contextlib.suppress(OSError):
            request.unlink()

    def _cache(self, record: dict[str, Any]) -> dict[str, Any]:
        ident = str(record["id"])
        self._records.pop(ident, None)
        self._records[ident] = record
        completed = [key for key, value in self._records.items() if value.get("state") in _TERMINAL]
        limit = self.shells.completed_records
        for key in completed[: max(0, len(completed) - limit)]:
            self._records.pop(key, None)
        return record

    async def start(
        self,
        mode: str,
        *,
        targets: Any,
        ports: Any = None,
        concurrency: int | None = None,
        rate: float | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
        max_probes: int | None = _DEFAULT_MAX_PROBES,
        max_duration: float | None = _DEFAULT_MAX_DURATION,
        continue_after_output_limit: bool = False,
        family: str = "any",
        retries: int | None = None,
        banner: bool = False,
        banner_timeout: float = 0.5,
        banner_bytes: int = 1024,
        open_only: bool = False,
        per_host_rate: float | None = None,
        probe: str | None = None,
        payload_b64: str | None = None,
        capture_response: bool = False,
        response_bytes: int = 1024,
        args: list[str] | None = None,
        client_id: str | None = None,
        connection_id: str | None = None,
        exec_id: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"tcp", "udp", "nmap"}:
            raise ValueError("mode must be 'tcp', 'udp', or 'nmap'")
        target_list = self._validate_targets(targets)
        self._validate_limits(max_probes, max_duration, continue_after_output_limit)
        estimate = None
        if mode == "udp" and banner:
            raise ValueError("banner is only available for TCP scans")
        request_id = secrets.token_hex(16)
        result_path = self.root / f"{request_id}.jsonl"
        artifact_path = self.root / f"{request_id}.xml"
        summary_path = self.root / f"{request_id}.summary.json"
        if mode in {"tcp", "udp"}:
            from .scan_config import normalize_native_config
            from .scan_worker import _ports

            config = normalize_native_config({
                "mode": mode,
                "targets": target_list,
                "ports": _ports(ports),
                "concurrency": concurrency,
                "rate": rate,
                "timeout": timeout,
                "max_probes": max_probes,
                "max_duration": max_duration,
                "continue_after_output_limit": continue_after_output_limit,
                "family": family,
                "retries": retries,
                "banner": banner,
                "banner_timeout": banner_timeout,
                "banner_bytes": banner_bytes,
                "open_only": open_only,
                "per_host_rate": per_host_rate,
                "probe": probe,
                "payload_b64": payload_b64,
                "capture_response": capture_response,
                "response_bytes": response_bytes,
            })
            config.update(
                result_path=str(result_path),
                artifact_path=str(artifact_path),
                summary_path=str(summary_path),
            )
            from .scan_worker import estimate_probes

            estimate = estimate_probes(target_list, config["ports"], family=config["family"])
        else:
            config = {
                "mode": mode,
                "targets": target_list,
                "args": self._validate_nmap_args(args),
                "max_duration": max_duration,
                "continue_after_output_limit": continue_after_output_limit,
                "result_path": str(result_path),
                "artifact_path": str(artifact_path),
                "summary_path": str(summary_path),
            }
        if mode == "nmap" and shutil.which("nmap") is None:
            raise RuntimeError("nmap is not installed")
        config_path = self.root / f"{request_id}.request.json"
        self._atomic_json(config_path, config)
        command = [
            sys.executable,
            str(Path(__file__).with_name("scan_worker.py")),
            "--config",
            str(config_path),
        ]
        launch = asyncio.create_task(self.shells.start(command, cwd=str(self.workspace)))
        try:
            job = await asyncio.shield(launch)
        except asyncio.CancelledError:
            try:
                job = await self._finish_cancelled_launch(launch)
                with contextlib.suppress(BaseException):
                    await wait_owned(
                        asyncio.create_task(self.shells.cancel(job["id"])),
                        propagate=False,
                    )
            finally:
                with contextlib.suppress(OSError):
                    config_path.unlink()
            raise
        except Exception:
            with contextlib.suppress(OSError):
                config_path.unlink()
            raise
        ident = str(job["id"])
        record = {
            "id": ident,
            "mode": mode,
            "state": "running",
            "created": time.time(),
            "targets": target_list,
            "config": str(config_path),
            "result_path": str(config["result_path"]),
            "summary_path": str(summary_path),
            "artifact": str(config["artifact_path"]) if mode == "nmap" else None,
            "result_count": 0,
            "result_bytes": 0,
            "attempts": 0,
            "estimate": estimate,
            "max_probes": max_probes,
            "max_duration": max_duration,
            "truncated": False,
            "stop_reason": None,
            **(
                self._native_record_fields(mode, config)
                if mode in {"tcp", "udp"}
                else {}
            ),
            "warnings": [],
            "client_id": client_id,
            "connection_id": connection_id,
            "exec_id": exec_id,
            "shell_id": ident,
        }
        try:
            self._cache(record)
            self._write_record(record)
            if self._track is not None:
                self._track(
                    ident,
                    client_id,
                    connection_id,
                    exec_id,
                    kind="scan",
                    mode=mode,
                    shell_id=ident,
                )
        except BaseException as tracking_error:
            cleanup_error = None
            try:
                await wait_owned(asyncio.create_task(self.shells.cancel(ident)), propagate=False)
            except BaseException as exc:
                cleanup_error = exc
            record["state"] = "failed"
            record["finished"] = time.time()
            record["error"] = f"scan tracking failed: {tracking_error}"
            record["stop_reason"] = "tracking_failed"
            if cleanup_error is not None:
                record["warnings"] = [
                    {
                        "code": "scan_cleanup_failed",
                        "text": str(cleanup_error)[:256],
                    }
                ]
            with contextlib.suppress(Exception):
                self._write_record(record)
            self._cache(record)
            with contextlib.suppress(OSError):
                config_path.unlink()
            raise
        monitor = asyncio.create_task(self._monitor(record), name=f"mypr:scan:{ident}")
        self._monitors[ident] = monitor
        monitor.add_done_callback(lambda _: self._monitors.pop(ident, None))
        return {
            "id": ident,
            "state": record["state"],
            "mode": mode,
            "estimate": estimate,
            "max_probes": max_probes,
            "max_duration": max_duration,
        }

    @staticmethod
    async def _finish_cancelled_launch(launch: asyncio.Task[Any]) -> dict[str, Any]:
        return await wait_owned(launch, propagate=False)

    async def results(
        self,
        scan_id: str,
        *,
        cursor: str | None = None,
        max_entries: int = _DEFAULT_PAGE_ENTRIES,
        max_bytes: int = _DEFAULT_PAGE_BYTES,
    ) -> dict[str, Any]:
        record = self._record(scan_id)
        if type(max_entries) is not int or not 1 <= max_entries <= 1000:
            raise ValueError("max_entries must be between 1 and 1000")
        if type(max_bytes) is not int or not 1 <= max_bytes <= _RESULT_LIMIT:
            raise ValueError(f"max_bytes must be between 1 and {_RESULT_LIMIT}")
        offset = self._decode_cursor(cursor, scan_id) if cursor is not None else 0
        output_missing = False
        try:
            rows, next_offset, more = await asyncio.to_thread(
                self._read_results_page,
                Path(record["result_path"]),
                offset,
                max_entries,
                max_bytes,
                record.get("state") not in _TERMINAL,
            )
        except FileNotFoundError:
            rows, next_offset, more = [], offset, False
            output_missing = True
        next_cursor = self._encode_cursor(scan_id, next_offset) if more else None
        result = {
            "id": scan_id,
            "results": rows,
            "cursor": next_cursor,
            "next_cursor": next_cursor,
            "has_more": more,
            "state": record.get("state", "unknown"),
            "truncated": bool(record.get("truncated")),
            "warnings": list(record.get("warnings", [])),
            "stop_reason": record.get("stop_reason"),
        }
        if record.get("mode") in {"tcp", "udp"}:
            result["complete"] = bool(record.get("complete", False))
        if output_missing:
            result.update(output_unavailable=True, truncated=True, complete=False)
            result["warnings"].append({
                "code": "scan_output_unavailable",
                "text": "Saved scan output is missing; an empty page is not a complete result.",
            })
        return result

    @staticmethod
    def _read_results_page(
        path: Path, offset: int, max_entries: int, max_bytes: int, running: bool
    ) -> tuple[list[dict[str, Any]], int, bool]:
        rows: list[dict[str, Any]] = []
        used = 0
        next_offset = offset
        budget_more = False
        eof = False
        more_data = False
        try:
            source = open_regular(path)
        except FileNotFoundError:
            if running:
                return rows, offset, True
            raise
        with source as stream:
            if offset > os.fstat(stream.fileno()).st_size:
                raise ValueError("scan cursor is beyond the retained results")
            stream.seek(offset)
            while len(rows) < max_entries:
                line = stream.readline()
                if not line:
                    eof = True
                    break
                next_offset = stream.tell()
                if not line.endswith(b"\n"):
                    if running:
                        return rows, next_offset - len(line), True
                    raise RuntimeError("scan results contain an incomplete final record")
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except (UnicodeDecodeError, ValueError) as exc:
                    raise RuntimeError("scan results contain a damaged record") from exc
                cost = len(_json_bytes(row))
                if not rows and cost > max_bytes:
                    raise ValueError("max_bytes is too small for the next scan result")
                if rows and used + cost > max_bytes:
                    budget_more = True
                    next_offset -= len(line)
                    break
                rows.append(row)
                used += cost
            if not eof and not budget_more:
                more_data = stream.peek(1) if hasattr(stream, "peek") else bool(stream.read(1))
            else:
                more_data = False
        return rows, next_offset, bool(running or budget_more or more_data)

    async def summary(self, scan_id: str, *, wait_ms: int = 0) -> dict[str, Any]:
        record = self._record(scan_id)
        if type(wait_ms) is not int or not 0 <= wait_ms <= 30000:
            raise ValueError("wait_ms must be between 0 and 30000")
        monitor = self._monitors.get(scan_id)
        if wait_ms and monitor is not None and not monitor.done():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(monitor), wait_ms / 1000)
        return dict(record)

    async def wait(self, scan_id: str) -> dict[str, Any]:
        monitor = self._monitors.get(scan_id)
        if monitor is not None:
            await asyncio.shield(monitor)
        return await self.summary(scan_id)

    async def cancel(self, scan_id: str) -> dict[str, Any]:
        record = self._record(scan_id)
        if record.get("state") in _TERMINAL:
            return dict(record)
        shell_id = record.get("shell_id")
        shell_result: dict[str, Any] = {}
        cancelled = False
        if shell_id:
            shell_result, cancelled = await finish_owned(
                asyncio.create_task(self.shells.cancel(shell_id))
            )
        async with self._state_lock:
            if record.get("state") not in _TERMINAL:
                state = shell_result.get("state")
                if state not in _TERMINAL:
                    state = "cancelled"
                record["state"] = state
                if record.get("mode") in {"tcp", "udp"} and state != "succeeded":
                    record["complete"] = False
                result = shell_result.get("result")
                if isinstance(result, dict):
                    record["returncode"] = result.get("returncode")
                if shell_result.get("error"):
                    record["error"] = shell_result["error"]
                record["finished"] = shell_result.get("finished_at") or time.time()
                self._write_record(record)
                config = record.get("config")
                if isinstance(config, str):
                    with contextlib.suppress(OSError):
                        await wait_owned(
                            asyncio.to_thread(Path(config).unlink), propagate=False
                        )
            result = dict(record)
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def close(self) -> None:
        active = [
            ident for ident, record in self._records.items() if record.get("state") not in _TERMINAL
        ]
        await asyncio.gather(*(self.cancel(ident) for ident in active), return_exceptions=True)
        monitors = list(self._monitors.values())
        if monitors:
            await asyncio.gather(*monitors, return_exceptions=True)

    def _record(self, scan_id: str) -> dict[str, Any]:
        if (
            not isinstance(scan_id, str)
            or len(scan_id) != 32
            or any(char not in "0123456789abcdef" for char in scan_id)
            or not (self.root / f"{scan_id}.json").is_file()
        ):
            raise ValueError("unknown scan")
        current = self._records.get(scan_id)
        if current is not None:
            self._records.move_to_end(scan_id)
            return current
        try:
            record = json.loads(
                read_bytes(self.root / f"{scan_id}.json", max_bytes=_PERSISTED_JSON_LIMIT)
            )
        except (OSError, ValueError) as exc:
            raise ValueError("invalid persisted scan") from exc
        if not isinstance(record, dict) or record.get("id") != scan_id:
            raise ValueError("invalid persisted scan")
        return self._cache(record)

    async def _monitor(self, record: dict[str, Any]) -> None:
        cursor = None
        pending = ""
        try:
            while True:
                page = await self.shells.read(
                    record["shell_id"], cursor=cursor, max_bytes=32768, wait_ms=1000
                )
                cursor = page.get("cursor", cursor)
                events = page.get("output", page.get("events", []))
                if isinstance(events, str):
                    events = [{"text": events}]
                for event in events if isinstance(events, list) else []:
                    if not isinstance(event, dict):
                        continue
                    text = str(event.get("text", ""))
                    if event.get("stream") != "stdout":
                        continue
                    pending += text
                    lines = pending.splitlines(keepends=True)
                    pending = "" if not lines or lines[-1].endswith(("\n", "\r")) else lines.pop()
                    for line in lines:
                        self._consume_line(record, line)
                terminal = page.get("state") not in {"running", "cancelling", "queued"}
                if terminal and not page.get("has_more"):
                    if pending.strip():
                        self._consume_line(record, pending)
                    await self._finish(record, page)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with contextlib.suppress(BaseException):
                await wait_owned(
                    asyncio.create_task(self.shells.cancel(record["shell_id"])),
                    propagate=False,
                )
            await self._finish(record, {"state": "failed", "error": str(exc), "result": None})

    def _consume_line(self, record: dict[str, Any], line: str) -> None:
        try:
            message = json.loads(line)
        except ValueError:
            self._warning(record, "invalid_scan_output", line.strip()[:256])
            return
        if not isinstance(message, dict):
            return
        if message.get("type") == "progress":
            for key in (
                "count",
                "bytes",
                "attempts",
                "truncated",
                "stop_reason",
                "artifact_bytes",
                "artifact_truncated",
                "returncode",
                "estimate",
                "completed",
                "state_counts",
                "discarded_results",
                "resolve_errors",
            ):
                if key in message:
                    target = {
                        "count": "result_count",
                        "bytes": "result_bytes",
                    }.get(key, key)
                    record[target] = message[key]
            if message.get("stderr"):
                self._warning(record, "nmap_stderr", str(message["stderr"])[-256:])
            self._write_record(record)
        elif message.get("type") == "error":
            record["error"] = str(message.get("error", "scan failed"))

    async def _finish(self, record: dict[str, Any], page: dict[str, Any]) -> None:
        complete = False
        summary_path = record.get("summary_path")
        if summary_path:
            try:
                summary = json.loads(
                    await asyncio.to_thread(
                        read_bytes, Path(summary_path), max_bytes=_PERSISTED_JSON_LIMIT
                    )
                )
                if isinstance(summary, dict):
                    for key in (
                        "truncated",
                        "attempts",
                        "stop_reason",
                        "duration_seconds",
                        "artifact_bytes",
                        "artifact_truncated",
                        "returncode",
                        "error",
                        "estimate",
                        "completed",
                        "state_counts",
                        "discarded_results",
                        "resolve_errors",
                    ):
                        if key in summary:
                            record[key] = summary[key]
                    complete = summary.get("complete") is True
            except (OSError, ValueError) as exc:
                self._warning(record, "summary_unavailable", str(exc)[:256])
        state = str(page.get("state", "failed"))
        if state not in _TERMINAL:
            state = "failed"
        result = page.get("result") or {}
        returncode = record.get("returncode", result.get("returncode"))
        if state == "succeeded" and returncode not in (None, 0):
            state = "failed"
        result_bytes, result_count = await asyncio.to_thread(
            self._result_stats, Path(record["result_path"])
        )
        if result_bytes is not None:
            record["result_bytes"] = result_bytes
            record["result_count"] = result_count
        async with self._state_lock:
            if record.get("state") in _TERMINAL:
                state = record["state"]
            if record.get("mode") in {"tcp", "udp"}:
                record["complete"] = complete if state == "succeeded" else False
            record.update(
                state=state,
                finished=time.time(),
                returncode=returncode,
                error=record.get("error") or page.get("error"),
                shell_truncated=bool(page.get("truncated")),
            )
            if page.get("warnings"):
                for warning in page["warnings"]:
                    if warning not in record["warnings"] and len(record["warnings"]) < 4:
                        record["warnings"].append(warning)
            with contextlib.suppress(OSError):
                await asyncio.to_thread(Path(record["config"]).unlink)
            self._write_record(record)
            self._cache(record)

    @staticmethod
    def _result_stats(path: Path) -> tuple[int | None, int]:
        try:
            with open_regular(path) as stream:
                return path.stat().st_size, sum(1 for line in stream if line.strip())
        except OSError:
            return None, 0

    def _warning(self, record: dict[str, Any], code: str, text: str) -> None:
        warnings = record.setdefault("warnings", [])
        item = {"code": code, "text": text[:256]}
        if item not in warnings and len(warnings) < 4:
            warnings.append(item)
        self._write_record(record)

    def _results_path(self, ident: str) -> Path:
        return self.root / f"{ident}.jsonl"

    def _write_record(self, record: dict[str, Any]) -> None:
        self._atomic_json(self.root / f"{record['id']}.json", record)

    @staticmethod
    def _atomic_json(path: Path, value: Any) -> None:
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, ensure_ascii=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)

    @staticmethod
    def _validate_targets(targets: Any) -> list[str]:
        values = list(targets) if isinstance(targets, (list, tuple)) else [targets]
        if not values or len(values) > 1024:
            raise ValueError("targets must contain between 1 and 1024 entries")
        result = []
        for target in values:
            if not isinstance(target, str) or not target.strip() or target.startswith("-"):
                raise ValueError("targets must contain non-empty host names or addresses")
            result.append(target.strip())
        return result

    @staticmethod
    def _validate_tcp(config: dict[str, Any]) -> None:
        from .scan_worker import validate_tcp_config

        validate_tcp_config(config)

    @staticmethod
    def _native_record_fields(mode: str, config: dict[str, Any]) -> dict[str, Any]:
        from .scan_config import TCP_STATES, UDP_STATES

        states = TCP_STATES if mode == "tcp" else UDP_STATES
        return {
            "protocol": mode,
            "family": config["family"],
            "concurrency": config["concurrency"],
            "rate": config["rate"],
            "timeout": config["timeout"],
            "retries": config["retries"],
            "per_host_rate": config["per_host_rate"],
            "banner": config["banner"],
            "banner_timeout": config["banner_timeout"],
            "banner_bytes": config["banner_bytes"],
            "open_only": config["open_only"],
            "probe": config["probe"],
            "capture_response": config["capture_response"],
            "response_bytes": config["response_bytes"],
            "completed": 0,
            "state_counts": {state: 0 for state in states},
            "discarded_results": 0,
            "resolve_errors": 0,
            "complete": False,
        }

    @staticmethod
    def _validate_limits(
        max_probes: int | None, max_duration: float | None, continue_after_output_limit: bool
    ) -> None:
        if max_probes is not None and (
            isinstance(max_probes, bool) or not isinstance(max_probes, int) or max_probes < 1
        ):
            raise ValueError("max_probes must be a positive integer or None")
        if max_duration is not None and (
            isinstance(max_duration, bool)
            or not isinstance(max_duration, (int, float))
            or not math.isfinite(float(max_duration))
            or max_duration <= 0
        ):
            raise ValueError("max_duration must be positive and finite or None")
        if type(continue_after_output_limit) is not bool:
            raise TypeError("continue_after_output_limit must be a boolean")

    @staticmethod
    def _validate_nmap_args(args: list[str] | None) -> list[str]:
        if args is None:
            return []
        if not isinstance(args, list) or any(not isinstance(item, str) for item in args):
            raise TypeError("nmap args must be a list of strings")
        if "--" in args:
            raise ValueError("nmap args cannot contain '--'")
        if any(
            item in {"-oX", "--xml", "-oA", "-oN", "-oG"} or item.startswith("-o") for item in args
        ):
            raise ValueError("nmap output options are managed by mypr")
        return list(args)

    @staticmethod
    def _encode_cursor(ident: str, index: int) -> str:
        raw = _json_bytes({"v": 1, "id": ident, "index": index})
        return "mypr-scan1." + base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str, ident: str) -> int:
        if not isinstance(cursor, str) or not cursor.startswith("mypr-scan1."):
            raise ValueError("invalid scan cursor")
        try:
            encoded = cursor.split(".", 1)[1]
            value = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid scan cursor") from exc
        if (
            not isinstance(value, dict)
            or value.get("v") != 1
            or value.get("id") != ident
            or type(value.get("index")) is not int
            or value["index"] < 0
        ):
            raise ValueError("invalid scan cursor")
        return value["index"]


__all__ = ["ScanService"]
