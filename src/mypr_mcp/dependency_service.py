"""Prepare shared tools and workspace packages before optional feature work."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import signal
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any

from .dependency_store import DependencyStore
from .diagnostics import RPCError, safe_error
from .package_worker import PYTHON_PACKAGES

_KINDS = {"binary", "python", "model", "browser"}
_BROWSERS = ("chromium", "firefox", "webkit")
_PACKAGE_PROBE = """import contextlib, importlib, importlib.metadata as m, importlib.util
import json, os, sys
result = {}
for name, module in json.loads(sys.argv[1]).items():
    version = None
    try:
        version = m.version(name)
    except m.PackageNotFoundError:
        pass
    try:
        spec = importlib.util.find_spec(module)
        if spec is None:
            state = 'unusable' if version is not None else 'missing'
            reason = 'module is unavailable' if version is not None else None
        else:
            with (open(os.devnull, 'w') as out, contextlib.redirect_stdout(out),
                  contextlib.redirect_stderr(out)):
                imported = importlib.import_module(module)
            state, reason = 'installed', None
        result[name] = {'status': state, 'version': version, 'reason': reason}
    except Exception as exc:
        result[name] = {'status': 'unusable', 'version': version, 'reason': str(exc)[:512]}
print(json.dumps(result))
"""
_BROWSER_PROBE = """import importlib.metadata, json, os, sys
from playwright.sync_api import sync_playwright
result = {}
with sync_playwright() as p:
    for name in json.loads(sys.argv[1]):
        path = getattr(p, name).executable_path
        result[name] = {'path': path, 'status': 'installed' if os.path.isfile(path) else 'missing',
                        'version': importlib.metadata.version('playwright')}
print(json.dumps(result))
"""


class DependencyService:
    def __init__(
        self,
        workspace: Path,
        python: Path,
        config: Mapping[str, Any],
        install_packages: Callable[..., Awaitable[Any]],
        install_browser: Callable[..., Awaitable[Any]],
        record: Callable[..., Awaitable[Any]] | None = None,
        *,
        store: DependencyStore | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.python = Path(python)
        self.store = store if store is not None else DependencyStore()
        self.apply_config(config)
        self._install_packages = install_packages
        self._install_browser = install_browser
        self._record = record
        self._jobs: dict[str, asyncio.Task] = {}
        self._python_pending: dict[str, asyncio.Task] = {}
        self._probe_slots = asyncio.Semaphore(2)
        self._closed = False

    @property
    def active_count(self) -> int:
        return sum(not task.done() for task in self._jobs.values())

    def status(self) -> dict[str, Any]:
        return {
            **self.config,
            "active_count": self.active_count,
            "data_root": str(self.store.data_root),
            "bin_root": str(self.store.bin_root),
            "cache_root": str(self.store.cache_root),
        }

    def apply_config(self, config: Mapping[str, Any]) -> None:
        value = config.get("auto_install", True)
        if type(value) is not bool:
            raise ValueError("dependencies.auto_install must be a boolean")
        self.config = {"auto_install": value}

    async def close(self) -> None:
        self._closed = True
        tasks = tuple(self._jobs.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.store.close()

    def _catalog(self, kind: str | None = None) -> list[str]:
        names = []
        if kind in (None, "binary", "model"):
            names.extend(self.store.names(kind))
        if kind in (None, "python"):
            names.extend(PYTHON_PACKAGES)
        if kind in (None, "browser"):
            names.extend(f"browser:{name}" for name in _BROWSERS)
        return sorted(names)

    async def list(
        self, kind: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> dict[str, Any]:
        if kind is not None and (not isinstance(kind, str) or kind not in _KINDS):
            raise ValueError("kind must be binary, python, model, browser, or None")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")
        names = self._catalog(kind)
        fingerprint = hashlib.sha256(json.dumps(names).encode()).hexdigest()
        offset = 0
        if cursor is not None:
            try:
                if not isinstance(cursor, str) or len(cursor) > 512:
                    raise ValueError
                value = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
                offset = value["offset"]
                if (
                    value["catalog"] != fingerprint
                    or value["kind"] != kind
                    or type(offset) is not int
                    or not 0 <= offset <= len(names)
                ):
                    raise ValueError
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError("invalid dependency cursor") from exc
        selected = names[offset : offset + limit]
        packages = [name for name in selected if name in PYTHON_PACKAGES]
        browsers = [name.split(":", 1)[1] for name in selected if name.startswith("browser:")]
        inventory = await self._packages(packages) if packages else {}
        browser_inventory = await self._browsers(browsers) if browsers else {}
        items = []
        for name in selected:
            if name in inventory:
                item = inventory[name]
            elif name.startswith("browser:"):
                item = browser_inventory[name.split(":", 1)[1]]
            else:
                item = await self.store.inspect(name)
            items.append(item)
        end = offset + len(selected)
        has_more = end < len(names)
        next_cursor = None
        if has_more:
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(
                    {"catalog": fingerprint, "kind": kind, "offset": end},
                    separators=(",", ":"),
                ).encode()
            ).decode()
        return {
            "auto_install": self.config["auto_install"],
            "items": items,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }

    async def ensure(
        self, names: Sequence[str], *, automatic: bool = False, context: Mapping | None = None
    ) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("Dependency service is closed")
        if type(automatic) is not bool:
            raise ValueError("automatic must be a boolean")
        catalog = set(self._catalog())
        if (
            isinstance(names, (str, bytes))
            or not isinstance(names, Sequence)
            or not 1 <= len(names) <= 32
            or any(not isinstance(name, str) or name not in catalog for name in names)
        ):
            raise ValueError("names must contain 1..32 registered dependency names")
        names = list(dict.fromkeys(names))
        context = dict(context or {})
        packages = [name for name in names if name in PYTHON_PACKAGES]
        inventory = await self._packages(packages) if packages else {}
        pending = []
        missing = []
        for name, item in inventory.items():
            if item["status"] == "unusable":
                raise RPCError(
                    f"{name} is installed but unusable: {item.get('reason')}",
                    code="dependency_unusable",
                    details={"name": name},
                )
            if item["status"] != "installed":
                self._check_install(name, automatic)
                if name in self._python_pending:
                    pending.append(self._python_pending[name])
                else:
                    missing.append(name)
        if missing:
            key = "python:" + ",".join(sorted(missing))
            task = self._start(
                key, missing, context, lambda: self._install_packages(missing, context)
            )
            for name in missing:
                self._python_pending[name] = task
            task.add_done_callback(lambda task: self._release_packages(missing, task))
            pending.append(task)
        if pending:
            await asyncio.gather(*(asyncio.shield(task) for task in set(pending)))
            inventory = await self._packages(packages)
            for name, item in inventory.items():
                if item["status"] != "installed":
                    raise RPCError(
                        f"{name} is unavailable after installation",
                        code="dependency_unusable",
                        details={"name": name, "reason": item.get("reason")},
                    )
        items = []
        for name in names:
            if name in inventory:
                items.append(inventory[name])
                continue
            if name.startswith("browser:"):
                browser = name.split(":", 1)[1]
                item = (await self._browsers([browser]))[browser]
                if item["status"] != "installed":
                    self._check_install(name, automatic)
                    task = self._start(
                        name,
                        [name],
                        context,
                        lambda browser=browser: self._install_browser(browser, context),
                    )
                    await asyncio.shield(task)
                    item = (await self._browsers([browser]))[browser]
                if item["status"] != "installed":
                    raise RPCError(f"{name} is unavailable", code="dependency_unusable")
            else:
                item = await self.store.inspect(name)
                if item.get("status") != "installed":
                    self._check_install(name, automatic)
                    if item.get("status") == "unsupported":
                        raise RPCError(
                            f"{name}: {item.get('reason', 'unsupported platform')}",
                            code="dependency_unsupported",
                            details={"name": name},
                        )
                    task = self._start(
                        name, [name], context, lambda name=name: self.store.ensure(name)
                    )
                    item = await asyncio.shield(task)
                else:
                    item = await self.store.ensure(name)
            items.append(item)
        return {"items": items}

    def _check_install(self, name: str, automatic: bool) -> None:
        if automatic and not self.config["auto_install"]:
            raise RPCError(
                f"{name} is missing and dependencies.auto_install is false; "
                f"run await ws.dependencies.ensure({name!r}) to install it explicitly.",
                code="dependency_missing",
                details={"name": name, "auto_install": False},
            )

    def _start(self, key, names, context, action) -> asyncio.Task:
        if self._closed:
            raise RuntimeError("Dependency service is closed")
        task = self._jobs.get(key)
        if task is None:
            task = asyncio.create_task(self._install(names, context, action), name=f"mypr:{key}")
            self._jobs[key] = task
            task.add_done_callback(lambda task: self._release(key, task))
        return task

    def _release(self, key: str, task: asyncio.Task) -> None:
        if self._jobs.get(key) is task:
            self._jobs.pop(key, None)
        if not task.cancelled():
            task.exception()

    def _release_packages(self, names, task) -> None:
        for name in names:
            if self._python_pending.get(name) is task:
                self._python_pending.pop(name, None)

    async def _install(self, names, context, action):
        await self._event("started", names, context)
        try:
            result = await action()
        except BaseException as exc:
            await self._event(
                "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
                names,
                context,
                error=safe_error(exc),
            )
            raise
        await self._event("succeeded", names, context)
        return result

    async def _event(self, state, names, context, **fields):
        if self._record is not None:
            await self._record(state, {"names": names, **context, **fields})

    async def _packages(self, names) -> dict[str, dict[str, Any]]:
        values = await self._probe(_PACKAGE_PROBE, {name: PYTHON_PACKAGES[name] for name in names})
        return {
            name: {
                "name": name,
                "kind": "python",
                "scope": "workspace",
                "source": "workspace" if value["status"] != "missing" else None,
                "path": str(self.python),
                **value,
            }
            for name, value in values.items()
        }

    async def _browsers(self, names) -> dict[str, dict[str, Any]]:
        values = await self._probe(_BROWSER_PROBE, names)
        return {
            name: {
                "name": f"browser:{name}",
                "kind": "browser",
                "scope": "global",
                "source": "shared" if value["status"] == "installed" else None,
                **value,
            }
            for name, value in values.items()
        }

    async def _probe(self, script: str, arguments: Any) -> dict:
        async with self._probe_slots:
            process = await asyncio.create_subprocess_exec(
                str(self.python),
                "-I",
                "-c",
                script,
                json.dumps(arguments),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            try:
                async with asyncio.timeout(15):
                    out, err, _ = await asyncio.gather(
                        _read(process.stdout, 128 * 1024),
                        _read(process.stderr, 8 * 1024),
                        process.wait(),
                    )
                if process.returncode:
                    raise RPCError(
                        f"Dependency probe failed: {err.decode(errors='replace')[:512]}",
                        code="dependency_probe_failed",
                    )
                value = json.loads(out)
                if not isinstance(value, dict):
                    raise ValueError("invalid dependency probe output")
                return value
            finally:
                if process.returncode is None:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()


async def _read(stream: asyncio.StreamReader, limit: int) -> bytes:
    result = bytearray()
    while chunk := await stream.read(16 * 1024):
        if len(result) + len(chunk) > limit:
            raise RPCError(
                "Dependency probe output exceeded its limit", code="dependency_probe_failed"
            )
        result.extend(chunk)
    return bytes(result)
