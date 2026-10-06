"""Serialize workspace package installs outside the persistent Python kernel."""

from __future__ import annotations

import argparse
import errno
import fcntl
import json
import math
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

try:
    from .python_dependencies import (
        CORE_PACKAGES,
        PYTHON_PACKAGE_REQUIREMENTS,
        PYTHON_PACKAGES,
        version_satisfies,
    )
except ImportError:
    import importlib.util

    _catalogue_path = Path(__file__).with_name("python_dependencies.py")
    _catalogue_spec = importlib.util.spec_from_file_location(
        "mypr_mcp_python_dependencies", _catalogue_path
    )
    if _catalogue_spec is None or _catalogue_spec.loader is None:
        raise ImportError(f"unable to load package catalogue: {_catalogue_path}") from None
    _catalogue = importlib.util.module_from_spec(_catalogue_spec)
    sys.modules[_catalogue_spec.name] = _catalogue
    _catalogue_spec.loader.exec_module(_catalogue)
    CORE_PACKAGES = _catalogue.CORE_PACKAGES
    PYTHON_PACKAGE_REQUIREMENTS = _catalogue.PYTHON_PACKAGE_REQUIREMENTS
    PYTHON_PACKAGES = _catalogue.PYTHON_PACKAGES
    version_satisfies = _catalogue.version_satisfies

_MAX_FREEZE_BYTES = 16 * 1024 * 1024
_PROBE_TIMEOUT = 30

# Automatic installation is deliberately limited to packages used by built-in
# features. Manual ``ws.packages.add`` remains the escape hatch for everything
# else. These aliases are kept for callers importing the old module constants.
AUTO_PACKAGE_MODULES = PYTHON_PACKAGES


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


def _canonical_name(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _validate_automatic_specs(specs: list[str]) -> list[str]:
    """Validate the restricted package names accepted by automatic installs."""

    normalized = []
    for spec in specs:
        key = _canonical_name(spec)
        if key != spec.lower() or key not in AUTO_PACKAGE_MODULES:
            raise ValueError(
                "automatic installation accepts only supported plain package names: "
                + ", ".join(sorted(AUTO_PACKAGE_MODULES))
            )
        normalized.append(key)
    return normalized


def _package_requirement(name: str) -> str:
    try:
        return PYTHON_PACKAGE_REQUIREMENTS[name]
    except KeyError as exc:
        raise ValueError(f"unknown automatic package: {name}") from exc


def _probe_environment(python: Path, modules: list[str]) -> dict[str, Any]:
    """Read distribution versions and import status from the workspace Python."""

    script = """
import contextlib
import importlib.metadata as metadata
import io
import json
import sys

class _Capture(io.StringIO):
    encoding = "utf-8"
    errors = "strict"

    def __init__(self, limit=512):
        super().__init__()
        self.limit = limit
        self.parts = []
        self.size = 0

    def write(self, value):
        if not isinstance(value, str):
            raise TypeError("write() argument must be str")
        length = len(value)
        if self.size < self.limit:
            value = value[: self.limit - self.size]
            self.parts.append(value)
            self.size += len(value)
        return length

    def text(self):
        return ''.join(self.parts)

requested = json.loads(sys.argv[1])
dist = {}
for item in metadata.distributions():
    name = item.metadata.get("Name")
    version = item.version
    if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
        continue
    dist[name.lower().replace("_", "-").replace(".", "-")] = {
        "name": name,
        "version": version,
    }
imports = {}
for name in requested:
    stdout = _Capture()
    stderr = _Capture()
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            __import__(name)
    except BaseException as exc:
        detail = f"{type(exc).__name__}: {exc}"
        diagnostic = (stdout.text() + stderr.text()).strip()
        if diagnostic:
            detail += f" [import output: {diagnostic}]"
        imports[name] = {"ok": False, "error": detail[:512]}
    else:
        imports[name] = {"ok": True}
print(json.dumps({"distributions": dist, "imports": imports}, separators=(",", ":")))
"""
    try:
        completed = subprocess.run(
            [str(python), "-I", "-c", script, json.dumps(modules)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("workspace package probe timed out") from exc
    if completed.returncode:
        output = (completed.stderr or completed.stdout)[-4096:]
        raise RuntimeError(
            f"workspace package probe failed with exit code {completed.returncode}: {output}"
        )
    try:
        result = json.loads(completed.stdout)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("workspace package probe returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise RuntimeError("workspace package probe returned invalid data")
    return result


def _constraint_lines(distributions: dict[str, Any]) -> str:
    lines = []
    for _key, value in sorted(distributions.items()):
        if not isinstance(value, dict):
            raise RuntimeError("workspace package probe returned invalid distribution data")
        name = value.get("name")
        version = value.get("version")
        if (
            not isinstance(name, str)
            or not name
            or any(char in name for char in "\r\n")
            or not isinstance(version, str)
            or not version
            or any(char.isspace() for char in version)
        ):
            raise RuntimeError("workspace package metadata contains an invalid distribution")
        lines.append(f"{name}=={version}\n")
    return "".join(lines)


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
    automatic: bool = False,
) -> dict[str, Any]:
    """Install requirements under a workspace lock and atomically refresh freeze output."""

    python_path = Path(python).absolute()
    root_path = Path(root).resolve()
    if not python_path.is_file():
        raise FileNotFoundError(f"workspace Python does not exist: {python_path}")
    if not root_path.is_dir():
        raise NotADirectoryError(root_path)
    specs = _validate_specs(specs)
    if automatic:
        specs = _validate_automatic_specs(specs)
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
            constraint_path = None
            already_satisfied = []
            missing = specs
            before = None
            requested_modules = []
            if automatic:
                requested_modules = [AUTO_PACKAGE_MODULES[spec] for spec in specs]
                before = _probe_environment(python_path, requested_modules)
                distributions = before.get("distributions", {})
                imports = before.get("imports", {})
                if not isinstance(distributions, dict) or not isinstance(imports, dict):
                    raise RuntimeError("workspace package probe returned invalid data")
                unusable = []
                repairs = []
                already_satisfied = []
                missing = []
                for spec, module in zip(specs, requested_modules, strict=True):
                    distribution = distributions.get(spec)
                    import_state = imports.get(module)
                    if distribution is not None and not isinstance(import_state, dict):
                        raise RuntimeError("workspace package probe returned invalid import data")
                    if distribution is not None and import_state.get("ok") is not True:
                        error = import_state.get("error", "import failed")
                        if spec in CORE_PACKAGES:
                            repairs.append(spec)
                            missing.append(spec)
                        else:
                            unusable.append(f"{spec} ({module}: {error})")
                    elif distribution is not None and import_state.get("ok") is True:
                        version = distribution.get("version")
                        requirement = _package_requirement(spec)
                        if not version_satisfies(version, requirement):
                            if spec in CORE_PACKAGES:
                                repairs.append(spec)
                                missing.append(spec)
                            else:
                                unusable.append(
                                    f"{spec} ({version!r} does not satisfy {requirement!r})"
                                )
                        else:
                            already_satisfied.append(spec)
                    else:
                        missing.append(spec)
                if unusable:
                    raise RuntimeError(
                        "automatic package is installed but unusable: " + ", ".join(unusable)
                    )
                if not missing:
                    _emit("already_satisfied", specs=already_satisfied)
                    return {
                        "manifest": str(manifest),
                        "bytes": manifest.stat().st_size if manifest.is_file() else 0,
                        "specs": [],
                        "already_satisfied": already_satisfied,
                        "automatic": True,
                        "durability": "unchanged",
                    }
                constrained = {
                    name: value
                    for name, value in distributions.items()
                    if name not in repairs
                }
                constraint = _constraint_lines(constrained)
                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", prefix=".constraints.", suffix=".txt",
                    dir=root_path, delete=False,
                ) as stream:
                    constraint_path = Path(stream.name)
                    stream.write(constraint)
                    stream.flush()
                    os.fsync(stream.fileno())
                install_command = [
                    executable, "pip", "install", "--python", str(python_path),
                    "--constraint", str(constraint_path),
                    *[argument for name in repairs for argument in ("--reinstall-package", name)],
                    *[_package_requirement(name) for name in missing],
                ]
            else:
                install_command = [
                    executable, "pip", "install", "--python", str(python_path), *specs,
                ]
            _run(
                install_command,
                "install",
                lock_fd=lock.fileno(),
                stream=True,
            )
            if automatic:
                after = _probe_environment(python_path, requested_modules)
                after_distributions = after.get("distributions", {})
                after_imports = after.get("imports", {})
                if not isinstance(after_distributions, dict) or not isinstance(after_imports, dict):
                    raise RuntimeError("workspace package probe returned invalid data")
                changed = []
                for name, value in before["distributions"].items():
                    if after_distributions.get(name) != value:
                        changed.append(name)
                unexpected = [name for name in changed if name not in repairs]
                if unexpected:
                    raise RuntimeError(
                        "automatic package installation changed existing distributions: "
                        + ", ".join(sorted(unexpected))
                    )
                unusable = []
                for spec, module in zip(specs, requested_modules, strict=True):
                    distribution = after_distributions.get(spec)
                    import_state = after_imports.get(module)
                    if (
                        distribution is None
                        or not isinstance(import_state, dict)
                        or import_state.get("ok") is not True
                    ):
                        detail = (
                            import_state.get("error", "import failed")
                            if isinstance(import_state, dict)
                            else "distribution or module is missing"
                        )
                        unusable.append(f"{spec} ({module}: {detail})")
                    elif not version_satisfies(
                        distribution.get("version"), _package_requirement(spec)
                    ):
                        unusable.append(
                            f"{spec} ({distribution.get('version')!r} does not satisfy "
                            f"{_package_requirement(spec)!r})"
                        )
                if unusable:
                    raise RuntimeError(
                        "automatic package could not be verified: " + ", ".join(unusable)
                    )
            _emit("installed", specs=missing)
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
                result = {
                    "manifest": str(manifest),
                    "bytes": len(freeze),
                    "specs": missing,
                    "durability": "unknown",
                    "warnings": [warning],
                }
                if automatic:
                    result["already_satisfied"] = already_satisfied
                    result["automatic"] = True
                return result
            _emit("saved", manifest=str(manifest), bytes=len(freeze), durability="confirmed")
            result = {
                "manifest": str(manifest),
                "bytes": len(freeze),
                "specs": missing,
                "durability": "confirmed",
            }
            if automatic:
                result["already_satisfied"] = already_satisfied
                result["automatic"] = True
            return result
        finally:
            if constraint_path is not None:
                constraint_path.unlink(missing_ok=True)
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
    if (
        not isinstance(timeout, (int, float))
        or isinstance(timeout, bool)
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("lock_timeout must be a finite positive number")
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
    parser.add_argument("--automatic", action="store_true")
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
            automatic=args.automatic,
        )
    except (OSError, RuntimeError, ValueError, TimeoutError) as exc:
        _emit("error", type=type(exc).__name__, message=str(exc)[:4096])
        return 1
    _emit("complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
