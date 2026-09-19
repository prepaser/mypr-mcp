"""Workspace-local reusable Python module management."""

from __future__ import annotations

import base64
import difflib
import hashlib
import importlib
import importlib.util
import json
import os
import sys
import tokenize
from pathlib import Path
from types import ModuleType
from typing import Any


class ModuleManager:
    """Manage modules below ``.mypr/lib/ws_lib``.

    File operations are delegated to the workspace filesystem object so that
    revisions and atomic writes have the same semantics as other workspace
    files.  Loading is intentionally a separate operation from writing.
    """

    def __init__(self, workspace: str | os.PathLike[str], fs: Any, shell: Any) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.root = self.workspace / ".mypr" / "lib" / "ws_lib"
        self.fs = fs
        self.shell = shell

    def list(self) -> list[dict[str, Any]]:
        if not self.root.is_dir():
            return []
        found: list[dict[str, Any]] = []
        for path in sorted(self.root.rglob("*.py")):
            if path.name == "__init__.py" or not path.is_file():
                continue
            try:
                relative = path.resolve().relative_to(self.root.resolve())
            except ValueError:
                continue
            name = ".".join(relative.with_suffix("").parts)
            try:
                data = path.read_bytes()
            except (OSError, UnicodeError) as exc:
                found.append({"name": name, "path": self._display(path), "error": str(exc)})
                continue
            revision = hashlib.sha256(data).hexdigest()
            found.append(
                {
                    "name": name,
                    "path": self._display(path),
                    "revision": revision,
                    "size": len(data),
                }
            )
        return found

    async def read(self, name: str, *, max_bytes: int = 4 * 1024 * 1024) -> dict[str, Any]:
        path = self._module_path(name)
        result = await self.fs.read(self._relative(path), max_bytes=max_bytes)
        return {"name": name, **result}

    async def check(
        self,
        name: str,
        source: str | None = None,
        *,
        test_code: str | None = None,
        timeout: float | None = 30,  # noqa: ASYNC109
        max_bytes: int = 32 * 1024,
    ) -> dict[str, Any]:
        """Syntax-check and optionally execute a module in the workspace venv."""

        self._validate_source(source)
        if source is None:
            page = await self.read(name, max_bytes=4 * 1024 * 1024)
            if page.get("truncated"):
                raise ValueError("module source exceeds max_bytes; pass the full source explicitly")
            source = page["text"]
        _compile(source, self._module_path(name))
        if test_code is not None and not isinstance(test_code, str):
            raise TypeError("test_code must be a string or None")
        payload = {
            "name": self._qualified(name),
            "path": str(self._module_path(name)),
            "source": source,
            "test_code": test_code,
            "lib": str(self.root.parent),
        }
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        command = [str(self._python()), "-I", "-c", _CHECK_SCRIPT]
        result = await self.shell.run(
            command,
            cwd=self.workspace,
            input=encoded,
            timeout=timeout,
            check=False,
            max_bytes=max_bytes,
        )
        state = result.get("state", "failed")
        return {
            "name": name,
            "valid": state == "succeeded" and not result.get("timed_out"),
            "state": state,
            "returncode": result.get("returncode"),
            "timed_out": bool(result.get("timed_out")),
            "stdout": result.get("stdout", ""),
            "stderr": result.get("stderr", ""),
            "truncated": bool(result.get("truncated")),
            "error": result.get("error"),
        }

    async def write(
        self,
        name: str,
        source: str,
        *,
        expected_hash: str | None = None,
        dry_run: bool = False,
        max_diff_bytes: int = 32 * 1024,
    ) -> dict[str, Any]:
        """Validate and atomically save a module without activating it."""

        if not isinstance(source, str):
            raise TypeError("source must be a string")
        path = self._module_path(name)
        _compile(source, path)
        relative = self._relative(path)
        try:
            old_result = await self.fs.read(relative, max_bytes=16 * 1024 * 1024)
            if old_result.get("truncated"):
                raise ValueError("existing module exceeds max_bytes; refusing partial CAS")
            old = old_result["text"]
            old_revision = old_result["revision"]
        except FileNotFoundError:
            old = None
            old_revision = None
        if dry_run:
            _check_hash(old, expected_hash, old_revision)
            diff, diff_truncated = _diff(relative, old or "", source, max_diff_bytes)
            return {
                "name": name,
                "path": relative,
                "changed": old != source,
                "dry_run": True,
                "old_revision": old_revision,
                "revision": _sha256(source.encode()),
                "diff": diff,
                "diff_truncated": diff_truncated,
            }
        result = await self.fs.write(
            relative,
            source,
            expected_hash=expected_hash,
            overwrite=False,
            create_parents=True,
        )
        return {
            "name": name,
            "path": relative,
            "changed": old != source,
            "dry_run": False,
            "old_revision": old_revision,
            **result,
        }

    def load(self, name: str) -> ModuleType:
        return self._activate(name)

    def reload(self, name: str) -> ModuleType:
        return self._activate(name)

    def _activate(self, name: str) -> ModuleType:
        path = self._module_path(name)
        qualified = self._qualified(name)
        self._ensure_import_path()
        parent_name, _, child_name = qualified.rpartition(".")
        parent = importlib.import_module(parent_name)
        spec = importlib.util.spec_from_file_location(qualified, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load module: {name}")
        candidate = importlib.util.module_from_spec(spec)
        old_module = sys.modules.get(qualified)
        old_attribute = getattr(parent, child_name, _MISSING)
        sys.modules[qualified] = candidate
        try:
            with tokenize.open(path) as source_file:
                exec(compile(source_file.read(), str(path), "exec"), candidate.__dict__)
        except BaseException:
            if old_module is None:
                sys.modules.pop(qualified, None)
            else:
                sys.modules[qualified] = old_module
            if old_attribute is _MISSING:
                try:
                    delattr(parent, child_name)
                except AttributeError:
                    pass
            else:
                setattr(parent, child_name, old_attribute)
            raise
        setattr(parent, child_name, candidate)
        return candidate

    def _module_path(self, name: str) -> Path:
        _validate_name(name)
        path = self.root.joinpath(*name.split(".")).with_suffix(".py").resolve()
        try:
            path.relative_to(self.root.resolve())
        except ValueError as exc:
            raise ValueError("module path escapes workspace") from exc
        return path

    def _relative(self, path: Path) -> str:
        return str(path.relative_to(self.workspace))

    def _display(self, path: Path) -> str:
        return self._relative(path.resolve())

    def _qualified(self, name: str) -> str:
        return f"ws_lib.{name}"

    def _python(self) -> Path:
        path = self.workspace / ".mypr" / "venv" / "bin" / "python"
        if not path.is_file():
            raise RuntimeError(f"Workspace Python is unavailable: {path}")
        return path

    def _ensure_import_path(self) -> None:
        parent = str(self.root.parent)
        if parent not in sys.path:
            sys.path.insert(0, parent)

    @staticmethod
    def _validate_source(source: str | None) -> None:
        if source is not None and not isinstance(source, str):
            raise TypeError("source must be a string or None")


_MISSING = object()


def _validate_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("module name must be a non-empty dotted name")
    parts = name.split(".")
    if any(not part.isidentifier() or part.startswith("_") for part in parts):
        raise ValueError("module name must contain only public Python identifiers")


def _compile(source: str, path: Path) -> None:
    compile(source, str(path), "exec")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _check_hash(old: str | None, expected: str | None, actual_revision: str | None = None) -> None:
    if expected is None:
        if old is not None:
            raise FileExistsError("Module already exists; provide expected_hash")
        return
    actual = actual_revision if old is not None else None
    if actual != expected:
        raise ValueError(f"Revision mismatch: expected {expected}, got {actual}")


def _diff(path: str, old: str, new: str, limit: int) -> tuple[str, bool]:
    if type(limit) is not int or limit < 1:
        raise ValueError("max_diff_bytes must be a positive integer")
    output = bytearray()
    truncated = False
    lines = difflib.unified_diff(
        old.splitlines(keepends=True),
        new.splitlines(keepends=True),
        fromfile=path,
        tofile=path,
    )
    for line in lines:
        encoded = line.encode()
        if len(output) + len(encoded) > limit:
            truncated = True
            break
        output.extend(encoded)
    return output.decode("utf-8", "replace"), truncated


_CHECK_SCRIPT = r"""
import base64, json, pathlib, sys, types
payload = json.loads(base64.b64decode(sys.stdin.read()))
sys.path.insert(0, payload["lib"])
name = payload["name"]
module = types.ModuleType(name)
module.__file__ = payload["path"]
module.__package__ = name.rpartition(".")[0]
module.__path__ = []
sys.modules[name] = module
exec(compile(payload["source"], payload["path"], "exec"), module.__dict__)
if payload["test_code"] is not None:
    exec(compile(payload["test_code"], "<module-check>", "exec"), module.__dict__)
"""
