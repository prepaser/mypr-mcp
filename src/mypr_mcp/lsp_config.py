"""Safe persistence for workspace language-server definitions."""

from __future__ import annotations

import copy
import hashlib
import os
import re
from collections.abc import Mapping
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
    """Compatibility facade over the layered configuration store."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        global_path: str | os.PathLike[str] | None = None,
    ) -> None:
        from .config import ConfigStore

        self.core = ConfigStore(workspace, global_path)
        self.path = self.core.workspace_path

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

    def load(self) -> tuple[dict[str, dict[str, Any]], str]:
        snapshot = self.core.load()
        return copy.deepcopy(snapshot.values["lsp"]["servers"]), snapshot.revision

    def save(
        self, servers: Mapping[str, Mapping[str, Any]], expected_revision: str | None
    ) -> str:
        return self.core.save_servers("lsp", servers, expected_revision).revision

    def save_server(
        self,
        section: str,
        name: str,
        definition: Mapping[str, Any] | None,
        expected_revision: str | None = None,
    ):
        return self.core.save_server(section, name, definition, expected_revision)


__all__ = ["LSPConfig", "validate_servers"]
