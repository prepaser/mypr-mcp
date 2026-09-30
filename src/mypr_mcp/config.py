import copy
import fcntl
import hashlib
import os
import tempfile
from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import tomlkit

from .lsp_config import validate_servers as _validate_lsp_servers

CONFIG_VERSION = 1
MAX_RESPONSE_BYTES = 1024 * 1024
KNOWN_SECTIONS = {"mcp", "limits", "lsp", "storage"}


class ConfigError(ValueError):
    """A workspace configuration error with a stable TOML field path."""

    def __init__(
        self,
        message: str,
        *,
        path: str = "",
        line: int | None = None,
        column: int | None = None,
        code: str = "invalid_workspace_config",
    ) -> None:
        self.code = code
        self.path = str(path)[:512]
        self.line = line if type(line) is int and 1 <= line <= 1_000_000 else None
        self.column = column if type(column) is int and 1 <= column <= 1_000_000 else None
        self.message = str(message)[:1024]
        location = self.path
        if self.line is not None:
            location += f":{self.line}"
            if self.column is not None:
                location += f":{self.column}"
        super().__init__(f"{location}: {self.message}" if location else self.message)


@dataclass(frozen=True)
class ConfigSnapshot:
    """Parsed workspace configuration and the bytes revision it came from."""

    values: dict
    revision: str | None


DEFAULT_LIMITS = {
    "output_bytes": 16 * 1024 * 1024,
    "response_bytes": 32 * 1024,
    "completed_tasks": 128,
    "completed_records": 128,
    "cache_bytes": 32 * 1024 * 1024,
}
DEFAULT_STORAGE = {
    "enabled": True,
    "revision_keep": 50,
    "retention_days": 30,
    "max_bytes": 1024**3,
    "gc_interval_seconds": 300,
}


def validate_name(name):
    if not isinstance(name, str) or not name.strip() or len(name) > 128:
        raise ConfigError("MCP server name must contain 1..128 characters", path="mcp.servers")


def validate_servers(servers):
    if not isinstance(servers, dict):
        raise ConfigError("must be a table", path="mcp.servers")
    for name, config in servers.items():
        validate_name(name)
        if not isinstance(config, dict):
            raise ConfigError("must be a table", path=f"mcp.servers.{name}")
        if ("url" in config) == ("command" in config):
            raise ConfigError(
                "requires exactly one of command or url", path=f"mcp.servers.{name}"
            )
        http = "url" in config
        allowed = {"url", "headers_from"} if http else {"command", "args", "cwd", "env_from"}
        unknown = config.keys() - allowed
        if unknown:
            raise ConfigError(
                f"unknown fields: {', '.join(sorted(unknown))}",
                path=f"mcp.servers.{name}",
            )
        if http:
            url = config["url"]
            if not isinstance(url, str):
                raise ConfigError("must be an HTTP(S) URL", path=f"mcp.servers.{name}.url")
            parts = urlsplit(url)
            if parts.scheme not in {"http", "https"} or not parts.hostname:
                raise ConfigError("must be an HTTP(S) URL", path=f"mcp.servers.{name}.url")
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
                raise ConfigError(
                    "must be a non-empty string or argument list",
                    path=f"mcp.servers.{name}.command",
                )
            if "args" in config and not (
                isinstance(config["args"], list)
                and all(isinstance(item, str) for item in config["args"])
            ):
                raise ConfigError("must be a list of strings", path=f"mcp.servers.{name}.args")
            if "cwd" in config and not (isinstance(config["cwd"], str) and config["cwd"]):
                raise ConfigError("must be a non-empty string", path=f"mcp.servers.{name}.cwd")
        field = "headers_from" if http else "env_from"
        if field in config:
            mapping = config[field]
            if not isinstance(mapping, dict) or not all(
                isinstance(key, str) and key and isinstance(value, str) and value
                for key, value in mapping.items()
            ):
                raise ConfigError(
                    "must map names to non-empty environment variable names",
                    path=f"mcp.servers.{name}.{field}",
                )
    return copy.deepcopy(servers)


def _revision(raw):
    return hashlib.sha256(raw).hexdigest() if raw is not None else None


def _field_error(path: str, message: str) -> ConfigError:
    return ConfigError(message, path=path)


def _positive(value, path: str, *, minimum: int = 1, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum:
        raise _field_error(path, f"must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise _field_error(path, f"must be an integer <= {maximum}")
    return value


def _validate_limits(value):
    if value is None:
        return dict(DEFAULT_LIMITS)
    if not isinstance(value, dict):
        raise _field_error("limits", "must be a table")
    result = dict(DEFAULT_LIMITS)
    for key, item in value.items():
        if key in DEFAULT_LIMITS:
            minimum = 1024 if key in {"output_bytes", "response_bytes", "cache_bytes"} else 1
            maximum = MAX_RESPONSE_BYTES if key == "response_bytes" else None
            result[key] = _positive(item, f"limits.{key}", minimum=minimum, maximum=maximum)
        else:
            result[key] = copy.deepcopy(item)
    if result["response_bytes"] < 1024 or result["output_bytes"] < 1024:
        raise _field_error("limits", "output_bytes and response_bytes must be at least 1024")
    return result


def _validate_storage(value):
    if value is None:
        return dict(DEFAULT_STORAGE)
    if not isinstance(value, dict):
        raise _field_error("storage", "must be a table")
    result = dict(DEFAULT_STORAGE)
    for key, item in value.items():
        if key == "enabled":
            if type(item) is not bool:
                raise _field_error("storage.enabled", "must be a boolean")
            result[key] = item
        elif key in DEFAULT_STORAGE:
            result[key] = _positive(item, f"storage.{key}")
        else:
            result[key] = copy.deepcopy(item)
    return result


def _validate_lsp(value):
    if value is None:
        return {"servers": {}}
    if not isinstance(value, dict):
        raise _field_error("lsp", "must be a table")
    result = copy.deepcopy(value)
    try:
        result["servers"] = _validate_lsp_servers(value.get("servers", {}))
    except (TypeError, ValueError) as exc:
        raise _field_error("lsp.servers", str(exc)) from exc
    return result


def validate_config(values):
    """Validate known configuration sections while preserving future sections."""

    if not isinstance(values, dict):
        raise ConfigError("configuration must be a table")
    result = copy.deepcopy(values)
    version = result.get("version", CONFIG_VERSION)
    if type(version) is not int or version < 1 or version > CONFIG_VERSION:
        raise _field_error("version", f"must be an integer between 1 and {CONFIG_VERSION}")
    result["version"] = version
    result["limits"] = _validate_limits(result.get("limits"))
    result["storage"] = _validate_storage(result.get("storage"))
    result["lsp"] = _validate_lsp(result.get("lsp"))
    mcp = result.get("mcp")
    if mcp is None:
        result["mcp"] = {"servers": {}}
    elif not isinstance(mcp, dict):
        raise _field_error("mcp", "must be a table")
    else:
        result["mcp"] = copy.deepcopy(mcp)
        result["mcp"]["servers"] = validate_servers(mcp.get("servers", {}))
    return result


def parse_config(raw: bytes | str | None) -> dict:
    """Parse and validate a workspace config, retaining unrelated TOML sections."""

    if raw is None or raw == b"" or raw == "":
        return validate_config({})
    try:
        source = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        parsed = tomlkit.parse(source).unwrap()
    except (UnicodeDecodeError, tomlkit.exceptions.ParseError) as exc:
        raise ConfigError(
            f"invalid TOML: {exc}",
            line=getattr(exc, "line", None),
            column=getattr(exc, "col", None),
        ) from exc
    except Exception as exc:
        raise ConfigError(f"unable to parse configuration: {exc}") from exc
    return validate_config(parsed)


def load_workspace_config(workspace: str | os.PathLike[str]) -> ConfigSnapshot:
    path = Path(workspace).resolve() / ".mypr" / "config.toml"
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        raw = None
    try:
        values = parse_config(raw)
    except ConfigError as exc:
        if not exc.path:
            exc.path = str(path)
        raise
    return ConfigSnapshot(values, _revision(raw))


load_config = load_workspace_config


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
        self.workspace = Path(workspace).resolve()
        self.path = self.workspace / ".mypr" / "config.toml"

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

    def load_document(self):
        """Return the parsed document and its on-disk revision for coordinated writes."""

        raw = self._raw()
        try:
            document = tomlkit.parse(raw.decode("utf-8")) if raw is not None else tomlkit.document()
        except (UnicodeDecodeError, tomlkit.exceptions.ParseError) as exc:
            raise ConfigError(f"invalid TOML: {exc}", path=str(self.path)) from exc
        return document, _revision(raw)

    def load_all(self) -> ConfigSnapshot:
        return load_workspace_config(self.workspace)

    @property
    def revision(self) -> str | None:
        return _revision(self._raw())

    def save_section(self, section: str, values: dict, expected_revision: str | None) -> str:
        """Atomically update one top-level table while retaining comments and siblings."""

        if not isinstance(section, str) or not section or "." in section:
            raise ConfigError("must be a top-level table name", path="section")
        if not isinstance(values, dict):
            raise ConfigError("must be a table", path=section)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            raw = self._raw()
            current = _revision(raw)
            if current != expected_revision:
                raise RuntimeError("MCP configuration changed on disk; reload before saving")
            document, _ = self.load_document()
            table = document.get(section)
            if table is None:
                table = tomlkit.table()
                document[section] = table
            if not isinstance(table, MutableMapping):
                raise ConfigError("must be a table", path=section)
            _update_table(table, values)
            data = tomlkit.dumps(document).encode("utf-8")
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=self.path.parent, prefix=f".{self.path.name}.", delete=False
                ) as stream:
                    temporary = Path(stream.name)
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                if self.path.exists():
                    temporary.chmod(self.path.stat().st_mode & 0o777)
                if _revision(self._raw()) != expected_revision:
                    raise RuntimeError("MCP configuration changed during save; reload and retry")
                os.replace(temporary, self.path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        return _revision(data)

    def save_lsp(self, servers: dict, expected_revision: str | None) -> str:
        values = _validate_lsp({"servers": servers})
        return self.save_section("lsp", values, expected_revision)

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
