"""Offline workspace readiness diagnostics.

The doctor only inspects local files, executables, and already running services.
It never installs packages, contacts a network service, or starts a manager.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

from .config import ConfigError, ConfigStore
from .startup import read_startup_failure

_PYTHON_PACKAGES = {
    "ipykernel": "ipykernel",
    "pyyaml": "yaml",
    "tomlkit": "tomlkit",
    "httpx2": "httpx2",
    "h2": "h2",
    "socksio": "socksio",
    "playwright": "playwright",
    "psutil": "psutil",
    "trafilatura": "trafilatura",
    "cssselect": "cssselect",
    "python-docx": "docx",
    "openpyxl": "openpyxl",
    "python-pptx": "pptx",
    "pymupdf": "fitz",
    "pillow": "PIL",
}
_BINARIES = ("uv", "rg", "rga", "ast-grep", "sg", "tesseract", "pandoc", "pdftotext")
_SEARCH_BINARIES = ("rg", "rga", "ast-grep", "sg")
_MAX_PROBE_STDOUT = 64 * 1024
_MAX_PROBE_STDERR = 8 * 1024


def _python_path(root: Path) -> Path:
    if os.name == "nt":
        return root / "venv" / "Scripts" / "python.exe"
    return root / "venv" / "bin" / "python"


def _browser_cache_path() -> Path | None:
    value = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if value == "0":
        return None
    if value:
        return Path(value).expanduser().resolve()
    cache_root = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return (Path(cache_root) / "ms-playwright").expanduser().resolve()


async def _probe_python(python: Path) -> dict[str, Any]:
    available = await asyncio.to_thread(lambda: python.is_file() and os.access(python, os.X_OK))
    if not available:
        return {"path": str(python), "available": False, "packages": {}}
    script = """import importlib.metadata as m
import json
import platform
import sys
names = json.loads(sys.argv[1])
result = {}
for name in names:
    try:
        result[name] = m.version(name)
    except m.PackageNotFoundError:
        result[name] = None
print(json.dumps({'version': platform.python_version(), 'packages': result}))
"""
    process = await asyncio.create_subprocess_exec(
        str(python),
        "-I",
        "-c",
        script,
        json.dumps(list(_PYTHON_PACKAGES)),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        probe_stdout, probe_stderr, _ = await asyncio.wait_for(
            asyncio.gather(
                _read_bounded(process.stdout, _MAX_PROBE_STDOUT),
                _read_bounded(process.stderr, _MAX_PROBE_STDERR),
                process.wait(),
            ),
            5,
        )
    except asyncio.CancelledError:
        await _kill_and_reap(process)
        raise
    except TimeoutError:
        await _kill_and_reap(process)
        return {
            "path": str(python),
            "available": False,
            "error": "probe timeout",
            "packages": {},
        }
    stdout, stdout_truncated = probe_stdout
    stderr, stderr_truncated = probe_stderr
    if process.returncode != 0:
        return {
            "path": str(python),
            "available": False,
            "error": stderr.decode("utf-8", "replace")[:256] or "probe failed",
            "output_truncated": stderr_truncated,
            "packages": {},
        }
    try:
        result = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {
            "path": str(python),
            "available": False,
            "error": "invalid probe output",
            "output_truncated": stdout_truncated,
            "packages": {},
        }
    packages = result.get("packages") if isinstance(result, dict) else {}
    if not isinstance(packages, dict):
        packages = {}
    return {
        "path": str(python),
        "available": True,
        "version": result.get("version"),
        "output_truncated": stdout_truncated,
        "packages": {
            name: {"available": packages.get(name) is not None, "version": packages.get(name)}
            for name in _PYTHON_PACKAGES
        },
    }


async def _tesseract() -> dict[str, Any]:
    executable = shutil.which("tesseract")
    result = {"executable": executable, "available": executable is not None, "languages": []}
    if executable is None:
        return result
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            executable,
            "--list-langs",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_result, _, _ = await asyncio.wait_for(
            asyncio.gather(
                _read_bounded(process.stdout, _MAX_PROBE_STDOUT),
                _read_bounded(process.stderr, _MAX_PROBE_STDERR),
                process.wait(),
            ),
            3,
        )
        stdout, _ = stdout_result
        if process.returncode == 0:
            result["languages"] = [
                line.strip()
                for line in stdout.decode("utf-8", "replace").splitlines()[1:]
                if line.strip()
            ][:256]
    except asyncio.CancelledError:
        if process is not None:
            await _kill_and_reap(process)
        raise
    except (OSError, TimeoutError):
        if process is not None:
            await _kill_and_reap(process)
        result["warning"] = "unable to inspect language data"
    return result


async def _read_bounded(stream: asyncio.StreamReader, limit: int) -> tuple[bytes, bool]:
    chunks = []
    size = 0
    truncated = False
    while True:
        chunk = await stream.read(16 * 1024)
        if not chunk:
            break
        keep = chunk[: max(0, limit - size)]
        if keep:
            chunks.append(keep)
            size += len(keep)
        if len(chunk) > len(keep):
            truncated = True
    return b"".join(chunks), truncated


async def _kill_and_reap(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
    with contextlib.suppress(ProcessLookupError):
        await asyncio.shield(process.wait())


async def _service_status(ws: Any, attr: str, method: str) -> dict[str, Any]:
    if ws is None:
        return {"configured": False, "available": False}
    service = getattr(ws, attr, None)
    callback = getattr(service, method, None)
    if callback is None:
        return {"configured": False, "available": False}
    try:
        value = callback()
        if hasattr(value, "__await__"):
            value = await asyncio.wait_for(value, 5)
        return {"configured": True, "available": True, "value": value}
    except Exception as exc:
        return {
            "configured": True,
            "available": False,
            "error": f"{type(exc).__name__}: {exc}"[:512],
        }


def _mail_readiness(config: dict[str, Any]) -> dict[str, Any]:
    accounts = {}
    for name, account in config.get("accounts", {}).items():
        if not account.get("enabled", True):
            continue
        references = {}
        for protocol in ("imap", "smtp"):
            endpoint = account.get(protocol, {})
            variable = endpoint.get("password_from")
            if variable:
                references[protocol] = {
                    "source": variable,
                    "available": variable in os.environ,
                }
        accounts[name] = {
            "credentials": references,
            "ready": all(value["available"] for value in references.values()),
            "sent_mailbox": account.get("sent_mailbox"),
        }
    return {
        "configured": bool(accounts),
        "default_account": config.get("default_account") or None,
        "accounts": accounts,
        "network_checked": False,
    }


async def doctor_workspace(workspace: str | os.PathLike[str], ws: Any = None) -> dict[str, Any]:
    """Return local readiness information without repairing or starting anything."""

    path = await asyncio.to_thread(lambda: Path(workspace).resolve())
    root = path / ".mypr"
    result: dict[str, Any] = {
        "workspace": str(path),
        "mypr": {"path": str(root), "exists": await asyncio.to_thread(root.is_dir)},
        "config": {
            "path": str(root / "config.toml"),
            "valid": True,
            "paths": {"workspace": str(root / "config.toml")},
        },
        "python": {},
        "binaries": {},
        "search": {},
        "ocr": {},
        "browser": {},
        "lsp": {},
        "mcp": {},
        "mail": {},
        "warnings": [],
    }
    startup_failure = await asyncio.to_thread(read_startup_failure, root)
    if startup_failure is not None:
        result["startup_error"] = startup_failure
    config_store = ConfigStore(path)
    result["config"]["paths"]["global"] = str(config_store.global_path)
    try:
        snapshot = config_store.load()
        result["mail"] = _mail_readiness(snapshot.values.get("mail", {}))
        result["config"].update(
            valid=True,
            revision=snapshot.revision,
            sections=sorted(snapshot.values),
            revisions=dict(snapshot.revisions),
            sources={
                scope: {
                    "path": str(snapshot.paths[scope]),
                    "revision": snapshot.revisions.get(scope),
                }
                for scope in ("global", "workspace")
            },
        )
    except (OSError, ConfigError) as exc:
        result["config"].update(valid=False, error=f"{type(exc).__name__}: {exc}"[:512])
        result["warnings"].append("configuration is unavailable")
    python = _python_path(root)
    result["python"] = await _probe_python(python)
    for name in _BINARIES:
        found = shutil.which(name)
        result["binaries"][name] = {"available": found is not None, "path": found}
    result["search"] = {
        name: result["binaries"][name] for name in _SEARCH_BINARIES
    }
    browser_cache = _browser_cache_path()
    result["ocr"] = {
        "tesseract": await _tesseract(),
        "python": {
            name: result["python"].get("packages", {}).get(name, {"available": False})
            for name in ("pymupdf",)
        },
    }
    result["browser"] = {
        "python": result["python"].get("packages", {}).get("playwright", {"available": False}),
        "engines_path": str(browser_cache) if browser_cache else None,
        "engines_scope": "package" if browser_cache is None else "shared",
        "engines_path_exists": (
            await asyncio.to_thread(browser_cache.is_dir)
            if browser_cache is not None
            else None
        ),
    }
    result["lsp"] = await _service_status(ws, "code", "status")
    result["mcp"] = await _service_status(ws, "mcp", "list_servers")
    if ws is not None:
        result["mail"]["service"] = await _service_status(ws, "mail", "status")
    result["ready"] = bool(
        result["config"]["valid"]
        and result["python"].get("available")
        and result["python"].get("packages", {}).get("ipykernel", {}).get("available")
    )
    return result


__all__ = ["doctor_workspace"]
