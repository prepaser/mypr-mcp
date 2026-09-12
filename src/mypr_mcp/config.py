import copy
import fcntl
import hashlib
import os
import tempfile
from collections.abc import MutableMapping
from pathlib import Path
from urllib.parse import urlsplit

import tomlkit


def validate_name(name):
    if not isinstance(name, str) or not name.strip() or len(name) > 128:
        raise ValueError("MCP server name must contain 1..128 characters")


def validate_servers(servers):
    if not isinstance(servers, dict):
        raise ValueError("mcp.servers must be a table")
    for name, config in servers.items():
        validate_name(name)
        if not isinstance(config, dict):
            raise ValueError(f"MCP server {name!r} must be a table")
        if ("url" in config) == ("command" in config):
            raise ValueError(f"MCP server {name!r} requires exactly one of command or url")
        http = "url" in config
        allowed = {"url", "headers_from"} if http else {"command", "args", "cwd", "env_from"}
        unknown = config.keys() - allowed
        if unknown:
            raise ValueError(f"Unknown MCP configuration fields: {', '.join(sorted(unknown))}")
        if http:
            url = config["url"]
            if not isinstance(url, str):
                raise ValueError("url must be an HTTP(S) URL")
            parts = urlsplit(url)
            if parts.scheme not in {"http", "https"} or not parts.hostname:
                raise ValueError("url must be an HTTP(S) URL")
            _ = parts.port
        else:
            command = config["command"]
            if not (
                (isinstance(command, str) and command.strip())
                or (
                    isinstance(command, list)
                    and command
                    and all(isinstance(item, str) and item for item in command)
                )
            ):
                raise ValueError("command must be a non-empty string or argument list")
            if "args" in config and not (
                isinstance(config["args"], list)
                and all(isinstance(item, str) for item in config["args"])
            ):
                raise ValueError("args must be a list of strings")
            if "cwd" in config and not (isinstance(config["cwd"], str) and config["cwd"]):
                raise ValueError("cwd must be a non-empty string")
        field = "headers_from" if http else "env_from"
        if field in config:
            mapping = config[field]
            if not isinstance(mapping, dict) or not all(
                isinstance(key, str) and key and isinstance(value, str) and value
                for key, value in mapping.items()
            ):
                raise ValueError(f"{field} must map names to non-empty environment variable names")
    return copy.deepcopy(servers)


def _revision(raw):
    return hashlib.sha256(raw).hexdigest() if raw is not None else None


def _update_table(table, values):
    for key in list(table):
        if key not in values:
            del table[key]
    for key, value in values.items():
        if isinstance(value, dict) and isinstance(table.get(key), MutableMapping):
            _update_table(table[key], value)
        elif table.get(key) != value:
            table[key] = value


class MCPConfig:
    def __init__(self, workspace):
        self.path = Path(workspace).resolve() / ".mypr" / "config.toml"

    def _raw(self):
        try:
            return self.path.read_bytes()
        except FileNotFoundError:
            return None

    @staticmethod
    def _parse(raw):
        doc = tomlkit.parse(raw.decode()) if raw is not None else tomlkit.document()
        plain = doc.unwrap()
        mcp = plain.get("mcp", {})
        if not isinstance(mcp, dict):
            raise ValueError("mcp must be a table")
        servers = validate_servers(mcp.get("servers", {}))
        return doc, servers

    def load(self):
        raw = self._raw()
        _, servers = self._parse(raw)
        return servers, _revision(raw)

    def save(self, servers, expected_revision):
        servers = validate_servers(servers)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            raw = self._raw()
            if _revision(raw) != expected_revision:
                raise RuntimeError("MCP configuration changed on disk; call ws.mcp.reload() first")
            doc, _ = self._parse(raw)
            if "mcp" not in doc:
                doc["mcp"] = tomlkit.table()
            if "servers" not in doc["mcp"]:
                doc["mcp"]["servers"] = tomlkit.table()
            _update_table(doc["mcp"]["servers"], servers)
            data = tomlkit.dumps(doc).encode()
            target = self.path.resolve()
            temp = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=target.parent, prefix=".config-", delete=False
                ) as file:
                    temp = Path(file.name)
                    file.write(data)
                    file.flush()
                    os.fsync(file.fileno())
                if target.exists():
                    temp.chmod(target.stat().st_mode & 0o777)
                if _revision(self._raw()) != expected_revision:
                    raise RuntimeError("MCP configuration changed during save; reload and retry")
                os.replace(temp, target)
            finally:
                if temp is not None:
                    temp.unlink(missing_ok=True)
        return _revision(data)
