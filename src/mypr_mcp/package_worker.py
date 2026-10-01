"""Serialize workspace package installs outside the persistent Python kernel."""

from __future__ import annotations

import argparse
import errno
import fcntl
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

_MAX_FREEZE_BYTES = 16 * 1024 * 1024


class _ManifestCommitError(OSError):
    """The manifest was replaced, but its directory durability is unknown."""

    def __init__(self, cause: OSError):
        super().__init__("manifest was replaced but directory durability is unknown")
        self.__cause__ = cause


def _emit(phase: str, **fields: Any) -> None:
    print(json.dumps({"phase": phase, **fields}, separators=(",", ":")), flush=True)


def _validate_specs(specs: list[str]) -> list[str]:
    if not isinstance(specs, list) or not specs:
        raise ValueError("at least one package requirement is required")
    if any(not isinstance(spec, str) for spec in specs):
        raise ValueError("package requirements must be strings")
    values = [spec.strip() for spec in specs]
    if any(not spec or spec.startswith("-") for spec in values):
        raise ValueError("package requirements must be non-empty and cannot be options")
    return values


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{path.name}.", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError as exc:
            raise _ManifestCommitError(exc) from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def install_packages(
    python: str | os.PathLike[str],
    root: str | os.PathLike[str],
    specs: list[str],
    *,
    uv: str = "uv",
    lock_timeout: float = 300,
) -> dict[str, Any]:
    """Install requirements under a workspace lock and atomically refresh freeze output."""

    python_path = Path(python).absolute()
    root_path = Path(root).resolve()
    if not python_path.is_file():
        raise FileNotFoundError(f"workspace Python does not exist: {python_path}")
    if not root_path.is_dir():
        raise NotADirectoryError(root_path)
    specs = _validate_specs(specs)
    executable = shutil.which(uv) if os.path.sep not in uv else uv
    if executable is None:
        raise FileNotFoundError(f"package installer not found: {uv}")
    lock_path = root_path / "packages.lock"
    manifest = root_path / "requirements.txt"
    root_path.mkdir(parents=True, exist_ok=True)
    with _open_lock(lock_path) as lock:
        try:
            _lock(lock, lock_timeout)
        except TimeoutError:
            raise TimeoutError("timed out waiting for workspace package lock") from None
        try:
            _emit("locked", root=str(root_path))
            _run(
                [executable, "pip", "install", "--python", str(python_path), *specs],
                "install",
                lock_fd=lock.fileno(),
                stream=True,
            )
            _emit("installed", specs=specs)
            freeze = _run(
                [executable, "--color", "never", "pip", "freeze", "--python", str(python_path)],
                "freeze",
                lock_fd=lock.fileno(),
            )
            if len(freeze) > _MAX_FREEZE_BYTES:
                raise RuntimeError("package freeze exceeds manifest size limit")
            try:
                _atomic_write(manifest, freeze)
            except _ManifestCommitError as exc:
                warning = {
                    "code": "manifest_durability_unknown",
                    "text": str(exc),
                }
                _emit("warning", **warning)
                _emit(
                    "saved",
                    manifest=str(manifest),
                    bytes=len(freeze),
                    durability="unknown",
                )
                return {
                    "manifest": str(manifest),
                    "bytes": len(freeze),
                    "specs": specs,
                    "durability": "unknown",
                    "warnings": [warning],
                }
            _emit("saved", manifest=str(manifest), bytes=len(freeze), durability="confirmed")
            return {
                "manifest": str(manifest),
                "bytes": len(freeze),
                "specs": specs,
                "durability": "confirmed",
            }
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _open_lock(path: Path):
    """Open a regular lock file without blocking on a replaced FIFO."""

    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError(errno.EINVAL, f"package lock is not a regular file: {path}")
        return os.fdopen(descriptor, "a+")
    except BaseException:
        os.close(descriptor)
        raise


def _lock(stream, timeout: float) -> None:
    if timeout <= 0:
        raise ValueError("lock_timeout must be positive")
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise TimeoutError from None
            time.sleep(min(0.1, max(0.001, deadline - time.monotonic())))


def _run(
    command: list[str],
    phase: str,
    *,
    lock_fd: int | None = None,
    stream: bool = False,
) -> bytes:
    kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "start_new_session": False,
    }
    if lock_fd is not None and os.name == "posix":
        kwargs["pass_fds"] = (lock_fd,)
    process = subprocess.Popen(
        command,
        **kwargs,
    )
    chunks: list[bytes] = []
    size = 0
    overflow = False
    assert process.stdout is not None
    while True:
        chunk = process.stdout.read(64 * 1024)
        if not chunk:
            break
        if stream:
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
        if phase == "freeze":
            available = max(0, _MAX_FREEZE_BYTES - size)
            if available:
                keep = chunk[:available]
                chunks.append(keep)
                size += len(keep)
            overflow |= len(chunk) > available
        else:
            if size < 4096:
                keep = chunk[: 4096 - size]
                chunks.append(keep)
                size += len(keep)
    process.wait()
    output = b"".join(chunks)
    if process.returncode:
        text = output.decode("utf-8", "replace")[-4096:]
        _emit("failed", step=phase, returncode=process.returncode, output=text)
        raise RuntimeError(f"package {phase} failed with exit code {process.returncode}: {text}")
    if phase == "freeze" and overflow:
        _emit("failed", step=phase, reason="manifest_too_large")
        raise RuntimeError("package freeze exceeds manifest size limit")
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install and freeze workspace Python packages")
    parser.add_argument("--python", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--lock-timeout", type=float, default=300)
    parser.add_argument("--spec", action="append", default=[])
    parser.add_argument("--spec-json", "--specs-json", dest="spec_json")
    parser.add_argument("specs", nargs="*")
    args = parser.parse_args(argv)
    specs = [*args.spec, *args.specs]
    try:
        if args.spec_json is not None:
            value = json.loads(args.spec_json)
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise ValueError("--spec-json must contain a JSON array of strings")
            specs = [*value, *specs]
        install_packages(
            args.python,
            args.root,
            specs,
            uv=args.uv,
            lock_timeout=args.lock_timeout,
        )
    except (OSError, RuntimeError, ValueError, TimeoutError) as exc:
        _emit("error", type=type(exc).__name__, message=str(exc)[:4096])
        return 1
    _emit("complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
