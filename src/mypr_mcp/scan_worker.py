"""Manager-launched network scanner worker."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import os
import signal
import socket
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path
from typing import Any

_RESULT_LIMIT = 16 * 1024 * 1024
_DEFAULT_MAX_PROBES = 1_000_000
_DEFAULT_MAX_DURATION = 3600.0


def _targets(value: Any) -> Iterator[str]:
    values = value if isinstance(value, (list, tuple, set)) else [value]
    produced = 0
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("targets must contain non-empty strings")
        target = item.strip()
        try:
            network = ipaddress.ip_network(target, strict=False)
        except ValueError:
            yield target
        else:
            for address in network.hosts():
                produced += 1
                if produced > 1_000_000:
                    raise ValueError("target network expands to more than 1000000 hosts")
                yield str(address)


def _ports(value: Any) -> list[int]:
    if value is None:
        value = "1-1024"
    values = value if isinstance(value, (list, tuple, set)) else [value]
    result: set[int] = set()
    for item in values:
        if isinstance(item, int) and not isinstance(item, bool):
            starts = ends = item
        elif isinstance(item, str):
            for part in item.split(","):
                part = part.strip()
                if not part:
                    continue
                if "-" in part:
                    left, right = part.split("-", 1)
                    starts, ends = int(left), int(right)
                else:
                    starts = ends = int(part)
                if starts > ends:
                    starts, ends = ends, starts
                if starts < 1 or ends > 65535:
                    raise ValueError("ports must be between 1 and 65535")
                result.update(range(starts, ends + 1))
            continue
        else:
            raise TypeError("ports must be integers or ranges")
        if starts < 1 or ends > 65535:
            raise ValueError("ports must be between 1 and 65535")
        result.add(starts)
    if not result:
        raise ValueError("ports must not be empty")
    return sorted(result)


class _Results:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = path.open("ab")
        self.count = 0
        self.bytes = 0
        self.truncated = False
        self.stop_reason: str | None = None

    def append(self, row: dict[str, Any]) -> bool:
        encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        if self.bytes + len(encoded) > _RESULT_LIMIT:
            self.truncated = True
            self.stop_reason = self.stop_reason or "result_size_limit"
            return False
        self.file.write(encoded)
        self.file.flush()
        self.count += 1
        self.bytes += len(encoded)
        return True

    def close(self) -> None:
        self.file.close()


def _progress(**fields: Any) -> None:
    print(json.dumps({"type": "progress", **fields}, separators=(",", ":")), flush=True)


class _Rate:
    def __init__(self, rate: float | None):
        self.delay = 0.0 if rate is None or rate <= 0 else 1.0 / rate
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        if not self.delay:
            return
        async with self.lock:
            now = time.monotonic()
            self.next_at = max(now, self.next_at) + self.delay
            pause = self.next_at - now
        if pause > 0:
            await asyncio.sleep(pause)


async def _probe(
    host: str,
    port: int,
    timeout: float,  # noqa: ASYNC109
    rate: _Rate,
) -> dict[str, Any]:
    await rate.wait()
    started = time.monotonic()
    state = "closed"
    reason = None
    address = None
    writer = None
    try:
        async with asyncio.timeout(timeout):
            _, writer = await asyncio.open_connection(host, port)
            address = writer.get_extra_info("peername")
            state = "open"
    except TimeoutError:
        state, reason = "timeout", "connect_timeout"
    except socket.gaierror as exc:
        state, reason = "unreachable", str(exc)
    except ConnectionRefusedError as exc:
        state, reason = "closed", str(exc)
    except OSError as exc:
        state, reason = "unreachable", str(exc)
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
    result: dict[str, Any] = {
        "host": host,
        "port": port,
        "state": state,
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
    }
    if address:
        result["address"] = address[0] if isinstance(address, tuple) else str(address)
    if reason:
        result["reason"] = reason
    return result


async def _tcp(config: dict[str, Any], results: _Results) -> int:
    targets = _targets(config.get("targets"))
    ports = _ports(config.get("ports"))
    workers = int(config.get("concurrency", 64))
    timeout = float(config.get("timeout", 1.0))
    rate = _Rate(float(config.get("rate", 200)))
    max_probes = config.get("max_probes", _DEFAULT_MAX_PROBES)
    max_duration = config.get("max_duration", _DEFAULT_MAX_DURATION)
    continue_after_output_limit = bool(config.get("continue_after_output_limit", False))
    if workers < 1 or workers > 1024:
        raise ValueError("concurrency must be between 1 and 1024")
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if max_probes is not None and (isinstance(max_probes, bool) or not isinstance(max_probes, int)):
        raise ValueError("max_probes must be a positive integer or None")
    if max_probes is not None and max_probes < 1:
        raise ValueError("max_probes must be a positive integer or None")
    if max_duration is not None and (
        isinstance(max_duration, bool)
        or not isinstance(max_duration, (int, float))
        or not 0 < float(max_duration) < float("inf")
    ):
        raise ValueError("max_duration must be positive and finite or None")
    queue: asyncio.Queue[tuple[str, int] | None] = asyncio.Queue(maxsize=workers * 2)
    stop = asyncio.Event()
    attempts = 0
    attempt_lock = asyncio.Lock()
    started = time.monotonic()
    deadline = started + float(max_duration) if max_duration is not None else None

    async def stop_for(reason: str) -> None:
        if not stop.is_set():
            config["stop_reason"] = reason
            stop.set()

    async def reserve() -> bool:
        nonlocal attempts
        async with attempt_lock:
            if stop.is_set():
                return False
            if deadline is not None and time.monotonic() >= deadline:
                await stop_for("time_limit")
                return False
            if max_probes is not None and attempts >= max_probes:
                await stop_for("probe_limit")
                return False
            attempts += 1
            return True

    async def produce() -> None:
        for host in targets:
            for port in ports:
                if stop.is_set():
                    return
                await queue.put((host, port))
            if stop.is_set():
                return
        for _ in range(workers):
            if stop.is_set():
                return
            await queue.put(None)

    async def consume() -> None:
        while True:
            pair = await queue.get()
            if pair is None:
                return
            if not await reserve():
                return
            try:
                if deadline is None:
                    row = await _probe(*pair, timeout, rate)
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        await stop_for("time_limit")
                        return
                    row = await asyncio.wait_for(_probe(*pair, timeout, rate), remaining)
            except TimeoutError:
                await stop_for("time_limit")
                return
            stored = results.append(row)
            if not stored and not continue_after_output_limit:
                await stop_for("result_size_limit")
            if results.count % 100 == 0:
                _progress(count=results.count, bytes=results.bytes, attempts=attempts)

    async with asyncio.TaskGroup() as group:
        group.create_task(produce())
        for _ in range(workers):
            group.create_task(consume())
    config["attempts"] = attempts
    config.setdefault("stop_reason", results.stop_reason)
    return 0


def estimate_probes(targets: list[str], ports: Any) -> int | None:
    """Return a cheap exact estimate for literal IP/CIDR targets."""

    try:
        port_count = len(_ports(ports))
    except (TypeError, ValueError):
        return None
    target_count = 0
    for value in targets:
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError:
            return None
        count = network.num_addresses
        if network.version == 4 and network.prefixlen < 31:
            count = max(0, count - 2)
        elif network.version == 6 and network.prefixlen < 127:
            count = max(0, count - 1)
        target_count += count
    return target_count * port_count


def _bounded_text(value: str | None, limit: int = 8192) -> str | None:
    if value is None:
        return None
    return value if len(value) <= limit else value[:limit] + "…"


def _script_row(script: ET.Element) -> dict[str, Any]:
    row: dict[str, Any] = {}
    if script.get("id"):
        row["id"] = script.get("id")
    output = _bounded_text(script.get("output"))
    if output is not None:
        row["output"] = output
    tables = []
    for table in script.findall("table"):
        item: dict[str, Any] = {}
        if table.get("key"):
            item["key"] = table.get("key")
        values = [
            _bounded_text(value.get("key") or value.text)
            for value in table.findall("elem")
            if value.get("key") or value.text
        ]
        if values:
            item["values"] = values[:64]
        if item:
            tables.append(item)
    if tables:
        row["tables"] = tables[:64]
    return row


def _nmap_row(host: ET.Element) -> dict[str, Any]:
    status = host.find("status")
    row: dict[str, Any] = {
        "host": next(
            (item.get("addr") for item in host.findall("address") if item.get("addr")), None
        ),
        "state": status.get("state") if status is not None else "unknown",
    }
    names = [item.get("name") for item in host.findall("hostnames/hostname") if item.get("name")]
    if names:
        row["hostnames"] = names
    ports = []
    for item in host.findall("ports/port"):
        state = item.find("state")
        entry: dict[str, Any] = {
            "port": int(item.get("portid", 0)),
            "protocol": item.get("protocol"),
        }
        if state is not None:
            entry["state"] = state.get("state")
            if state.get("reason"):
                entry["reason"] = state.get("reason")
        service = item.find("service")
        if service is not None:
            service_data = {
                key: _bounded_text(service.get(key))
                for key in ("name", "product", "version", "extrainfo", "tunnel", "method")
                if service.get(key)
            }
            cpes = [cpe.text for cpe in service.findall("cpe") if cpe.text]
            if cpes:
                service_data["cpe"] = cpes[:16]
            if service_data:
                entry["service"] = service_data
        scripts = [_script_row(script) for script in item.findall("script")]
        if scripts:
            entry["scripts"] = scripts[:64]
        ports.append(entry)
    if ports:
        row["ports"] = ports
    host_scripts = [_script_row(script) for script in host.findall("hostscript/script")]
    if host_scripts:
        row["scripts"] = host_scripts[:64]
    os_matches = []
    for match in host.findall("os/osmatch"):
        item = {
            key: _bounded_text(match.get(key))
            for key in ("name", "accuracy", "line")
            if match.get(key)
        }
        cpes = [cpe.text for cpe in match.findall("osclass/cpe") if cpe.text]
        if cpes:
            item["cpe"] = cpes[:16]
        if item:
            os_matches.append(item)
    if os_matches:
        row["os"] = os_matches[:64]
    return row


async def _read_tail(stream: asyncio.StreamReader, limit: int = 64 * 1024) -> bytes:
    tail = bytearray()
    while chunk := await stream.read(8192):
        tail.extend(chunk)
        if len(tail) > limit:
            del tail[: len(tail) - limit]
    return bytes(tail)


async def _nmap(config: dict[str, Any], results: _Results) -> int:
    command = ["nmap", *config.get("args", []), "-oX", "-", "--", *config["targets"]]
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    parser = ET.XMLPullParser(["start", "end"])
    stack: list[ET.Element] = []
    parse_error: str | None = None
    artifact_file = Path(config["artifact_path"]).open("wb")  # noqa: ASYNC230
    artifact_bytes = 0
    artifact_truncated = False
    stderr_task = asyncio.create_task(_read_tail(process.stderr))
    try:
        while True:
            chunk = await process.stdout.read(65536)
            if not chunk:
                break
            if not artifact_truncated:
                kept = chunk[: max(0, _RESULT_LIMIT - artifact_bytes)]
                artifact_file.write(kept)
                artifact_bytes += len(kept)
                artifact_truncated = len(kept) < len(chunk)
            if parser is not None:
                try:
                    parser.feed(chunk)
                    for event, element in parser.read_events():
                        if event == "start":
                            stack.append(element)
                            continue
                        if element.tag == "host":
                            stored = results.append(_nmap_row(element))
                            _progress(count=results.count, bytes=results.bytes)
                            if not stored and not config.get("continue_after_output_limit", False):
                                config["stop_reason"] = "result_size_limit"
                                process.terminate()
                                break
                            if len(stack) >= 2:
                                with contextlib.suppress(ValueError):
                                    stack[-2].remove(element)
                            element.clear()
                        if stack:
                            stack.pop()
                except ET.ParseError as exc:
                    parse_error = str(exc)
                    parser = None
                    stack.clear()
            if config.get("stop_reason") == "result_size_limit":
                break
        if config.get("stop_reason") == "result_size_limit" and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 2.0)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
        else:
            await process.wait()
        if parser is not None:
            try:
                parser.close()
            except ET.ParseError as exc:
                parse_error = str(exc)
        stderr = await stderr_task
        if stderr:
            _progress(stderr=stderr.decode("utf-8", "replace")[-1024:])
        _progress(
            count=results.count,
            bytes=results.bytes,
            artifact_bytes=artifact_bytes,
            artifact_truncated=artifact_truncated,
            returncode=process.returncode,
        )
        if parse_error and config.get("stop_reason") != "result_size_limit":
            config["error"] = f"invalid or incomplete Nmap XML: {parse_error}"
            return process.returncode or 2
        if config.get("stop_reason") == "result_size_limit":
            return 0
        return process.returncode or 0
    finally:
        artifact_file.close()
        config["artifact_bytes"] = artifact_bytes
        config["artifact_truncated"] = artifact_truncated
        if process.returncode is None:
            process.kill()
            await process.wait()
        if not stderr_task.done():
            stderr_task.cancel()
            await asyncio.gather(stderr_task, return_exceptions=True)


async def run(config: dict[str, Any]) -> int:
    results = _Results(Path(config["result_path"]))
    started = time.monotonic()
    returncode = 2
    error = None
    cancelled = False
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    signal_installed = False
    if current is not None and hasattr(loop, "add_signal_handler"):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal.SIGTERM, current.cancel)
            signal_installed = True
    try:
        if config.get("mode", "tcp") == "tcp":
            returncode = await _tcp(config, results)
        elif config.get("mode") == "nmap":
            max_duration = config.get("max_duration", _DEFAULT_MAX_DURATION)
            if max_duration is None:
                returncode = await _nmap(config, results)
            else:
                try:
                    async with asyncio.timeout(float(max_duration)):
                        returncode = await _nmap(config, results)
                except TimeoutError:
                    config["stop_reason"] = "time_limit"
                    returncode = 0
        else:
            raise ValueError("unknown scan mode")
        _progress(
            count=results.count,
            bytes=results.bytes,
            truncated=results.truncated,
            attempts=config.get("attempts", results.count),
            stop_reason=config.get("stop_reason") or results.stop_reason,
            returncode=returncode,
        )
        return returncode
    except asyncio.CancelledError:
        cancelled = True
        returncode = 143
        return returncode
    except Exception as exc:
        error = _exception_text(exc)
        _progress(error=error, count=results.count, bytes=results.bytes)
        return returncode
    finally:
        results.close()
        if signal_installed:
            loop.remove_signal_handler(signal.SIGTERM)
        summary_path = config.get("summary_path")
        if summary_path:
            summary = {
                "count": results.count,
                "bytes": results.bytes,
                "truncated": results.truncated,
                "attempts": config.get("attempts", results.count),
                "stop_reason": config.get("stop_reason") or results.stop_reason,
                "duration_seconds": round(time.monotonic() - started, 3),
                "artifact_bytes": config.get("artifact_bytes", 0),
                "artifact_truncated": config.get("artifact_truncated", False),
                "returncode": returncode,
                "cancelled": cancelled,
                "error": error or config.get("error"),
            }
            temporary = Path(summary_path).with_suffix(".tmp")
            try:
                temporary.write_text(json.dumps(summary, separators=(",", ":")), encoding="utf-8")
                os.replace(temporary, summary_path)
            finally:
                with contextlib.suppress(OSError):
                    temporary.unlink()


def _exception_text(exc: BaseException) -> str:
    if isinstance(exc, BaseExceptionGroup):
        details = [_exception_text(item) for item in exc.exceptions]
        return "; ".join(item for item in details if item) or str(exc)
    return str(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        return asyncio.run(run(config))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(json.dumps({"type": "error", "error": str(exc)}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
