"""Safe persistence for workspace language-server definitions."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import tomlkit

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def _revision(raw: bytes | None) -> str | None:
    return hashlib.sha256(raw).hexdigest() if raw is not None else None


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def validate_servers(value: Any) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("lsp.servers must be a table")
    result: dict[str, dict[str, Any]] = {}
    for name, raw in value.items():
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ValueError("LSP server names must contain 1..64 characters")
        if not isinstance(raw, Mapping):
            raise ValueError(f"LSP server {name!r} must be a table")
        unknown = set(raw) - {"command", "languages", "timeout"}
        if unknown:
            raise ValueError(
                f"Unknown LSP configuration fields for {name!r}: {', '.join(sorted(unknown))}"
            )
        command = raw.get("command")
        languages = raw.get("languages")
        if not isinstance(command, (list, tuple)) or not command or not all(
            isinstance(item, str) and item and "\x00" not in item for item in command
        ):
            raise ValueError(f"LSP server {name!r} command must be a non-empty list of strings")
        if not isinstance(languages, (list, tuple)) or not languages or not all(
            isinstance(item, str) and item and "\x00" not in item for item in languages
        ):
            raise ValueError(f"LSP server {name!r} languages must be a non-empty list of strings")
        timeout = raw.get("timeout", 10.0)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 1 <= timeout <= 60
        ):
            raise ValueError(f"LSP server {name!r} timeout must be between 1 and 60 seconds")
        result[name] = {
            "command": list(command),
            "languages": list(dict.fromkeys(languages)),
            "timeout": float(timeout),
        }
    return result


class LSPConfig:
    """Read and atomically update only the ``lsp`` TOML section."""

    def __init__(self, workspace: str | os.PathLike[str]) -> None:
        self.path = Path(workspace).expanduser().resolve() / ".mypr" / "config.toml"

    def _raw(self) -> bytes | None:
        try:
            return self.path.read_bytes()
        except FileNotFoundError:
            return None

    @staticmethod
    def _parse(raw: bytes | None) -> tuple[Any, dict[str, dict[str, Any]]]:
        doc = tomlkit.parse(raw.decode("utf-8")) if raw is not None else tomlkit.document()
        plain = doc.unwrap()
        lsp = plain.get("lsp", {})
        if not isinstance(lsp, Mapping):
            raise ValueError("lsp must be a table")
        return doc, validate_servers(lsp.get("servers", {}))

    def load(self) -> tuple[dict[str, dict[str, Any]], str | None]:
        raw = self._raw()
        _, servers = self._parse(raw)
        return servers, _revision(raw)

    def save(
        self, servers: Mapping[str, Mapping[str, Any]], expected_revision: str | None
    ) -> str:
        value = validate_servers(servers)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            raw = self._raw()
            if _revision(raw) != expected_revision:
                raise RuntimeError("LSP configuration changed on disk; call ws.code.reload() first")
            doc, _ = self._parse(raw)
            lsp = doc.get("lsp")
            if lsp is None:
                lsp = tomlkit.table()
                doc["lsp"] = lsp
            elif not isinstance(lsp, Mapping):
                raise ValueError("lsp must be a table")
            table = lsp.get("servers")
            if table is None:
                table = tomlkit.table()
                lsp["servers"] = table
            elif not isinstance(table, Mapping):
                raise ValueError("lsp.servers must be a table")
            for name in list(table):
                if name not in value:
                    del table[name]
            for name, config in value.items():
                entry = table.get(name)
                if entry is None:
                    entry = tomlkit.table()
                    table[name] = entry
                entry["command"] = list(config["command"])
                entry["languages"] = list(config["languages"])
                entry["timeout"] = config["timeout"]
            data = tomlkit.dumps(doc).encode("utf-8")
            temporary: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=self.path.parent, prefix=".config-", delete=False
                ) as stream:
                    temporary = Path(stream.name)
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                if self.path.exists():
                    temporary.chmod(self.path.stat().st_mode & 0o777)
                if _revision(self._raw()) != expected_revision:
                    raise RuntimeError("LSP configuration changed during save; reload and retry")
                os.replace(temporary, self.path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        return _revision(data) or ""


__all__ = ["LSPConfig", "validate_servers"]
