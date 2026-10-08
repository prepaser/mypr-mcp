import copy
import fcntl
import hashlib
import math
import os
import re
import stat
import tempfile
from collections.abc import Mapping, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from email.utils import parseaddr
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import tomlkit

from .file_io import open_regular
from .lsp_config import validate_servers as _validate_lsp_servers

CONFIG_VERSION = 1
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_WAIT_MS = 30_000
MANAGED_SECTIONS = frozenset(
    {"limits", "storage", "mcp", "lsp", "mail", "web", "dependencies"}
)
_SERVER_SECTIONS = frozenset({"mcp", "lsp"})
_NAMED_SECTIONS = frozenset({"mail", "web"})
_MAX_CONFIG_BYTES = 16 * 1024 * 1024


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
    layers: dict = field(default_factory=dict)
    revisions: dict = field(default_factory=dict)
    paths: dict = field(default_factory=dict)


DEFAULT_LIMITS = {
    "output_bytes": 16 * 1024 * 1024,
    "response_bytes": 32 * 1024,
    "execute_wait_ms": 1000,
    "poll_wait_ms": 1000,
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
DEFAULT_MAIL = {
    "default_account": "",
    "accounts": {},
}
DEFAULT_WEB = {
    "default_provider": "",
    "timeout_seconds": 30,
    "max_concurrency": 4,
    "providers": {},
}
DEFAULT_DEPENDENCIES = {
    "auto_install": True,
}
_DEPENDENCY_LINK_MODES = frozenset({"clone", "hardlink", "copy"})

_WEB_PROVIDERS = frozenset({"kagi", "brave", "tavily"})
_WEB_FIELDS = frozenset(
    {"default_provider", "timeout_seconds", "max_concurrency", "providers"}
)

_MAIL_SECURITY = frozenset({"ssl", "starttls", "plain"})
_MAIL_ACCOUNT_FIELDS = frozenset({"from", "imap", "smtp", "sent_mailbox"})
_MAIL_ENDPOINT_FIELDS = frozenset(
    {"host", "port", "security", "username", "password_from", "ca_file"}
)


def global_config_path() -> Path:
    override = os.environ.get("MYPR_GLOBAL_CONFIG")
    if override:
        return Path(override).expanduser().resolve()
    root = os.environ.get("XDG_CONFIG_HOME")
    base = Path(root).expanduser() if root else Path.home() / ".config"
    return (base / "mypr" / "config.toml").resolve()


def validate_name(name):
    if not isinstance(name, str) or not name.strip() or len(name) > 128 or "\x00" in name:
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
            if not isinstance(url, str) or "\x00" in url:
                raise ConfigError("must be an HTTP(S) URL", path=f"mcp.servers.{name}.url")
            try:
                parts = urlsplit(url)
                host = parts.hostname
                _ = parts.port
            except ValueError as exc:
                raise ConfigError(
                    f"invalid HTTP(S) URL: {exc}", path=f"mcp.servers.{name}.url"
                ) from exc
            if parts.scheme not in {"http", "https"} or not host:
                raise ConfigError("must be an HTTP(S) URL", path=f"mcp.servers.{name}.url")
        else:
            command = config["command"]
            if not (
                (isinstance(command, str) and command.strip() and "\x00" not in command)
                or (
                    isinstance(command, list)
                    and command
                    and all(
                        isinstance(item, str) and item and "\x00" not in item
                        for item in command
                    )
                )
            ):
                raise ConfigError(
                    "must be a non-empty string or argument list",
                    path=f"mcp.servers.{name}.command",
                )
            if "args" in config and not (
                isinstance(config["args"], list)
                and all(
                    isinstance(item, str) and "\x00" not in item for item in config["args"]
                )
            ):
                raise ConfigError("must be a list of strings", path=f"mcp.servers.{name}.args")
            if "cwd" in config and not (
                isinstance(config["cwd"], str)
                and config["cwd"]
                and "\x00" not in config["cwd"]
            ):
                raise ConfigError("must be a non-empty string", path=f"mcp.servers.{name}.cwd")
        field = "headers_from" if http else "env_from"
        if field in config:
            mapping = config[field]
            if not isinstance(mapping, dict) or not all(
                isinstance(key, str)
                and key
                and "\x00" not in key
                and isinstance(value, str)
                and value
                and "\x00" not in value
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


def _validate_wait_ms(value: Any, path: str) -> int:
    return _positive(value, path, minimum=0, maximum=MAX_WAIT_MS)


def _validate_limits(value):
    if value is None:
        return dict(DEFAULT_LIMITS)
    if not isinstance(value, dict):
        raise _field_error("limits", "must be a table")
    result = dict(DEFAULT_LIMITS)
    for key, item in value.items():
        if key in DEFAULT_LIMITS:
            if key in {"execute_wait_ms", "poll_wait_ms"}:
                result[key] = _validate_wait_ms(item, f"limits.{key}")
                continue
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


def _validate_dependencies(value):
    if value is None:
        return dict(DEFAULT_DEPENDENCIES)
    if not isinstance(value, dict):
        raise _field_error("dependencies", "must be a table")
    result = dict(DEFAULT_DEPENDENCIES)
    for key, item in value.items():
        if key == "auto_install":
            if type(item) is not bool:
                raise _field_error("dependencies.auto_install", "must be a boolean")
            result[key] = item
        elif key == "uv_cache_dir":
            if (
                not isinstance(item, str)
                or not item
                or "\x00" in item
                or not (item == "~" or item.startswith("~/") or Path(item).is_absolute())
            ):
                raise _field_error(
                    "dependencies.uv_cache_dir",
                    "must be an absolute path, ~, or a path beginning with ~/",
                )
            result[key] = item
        elif key == "uv_link_mode":
            if not isinstance(item, str) or item not in _DEPENDENCY_LINK_MODES:
                raise _field_error(
                    "dependencies.uv_link_mode",
                    "must be one of clone, hardlink, or copy",
                )
            result[key] = item
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


def _mail_text(value: Any, path: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(char in value for char in "\x00\r\n")
    ):
        raise _field_error(path, "must be a non-empty string")
    if len(value) > maximum:
        raise _field_error(path, f"must contain at most {maximum} characters")
    return value


def _mail_env_name(value: Any, path: str) -> str:
    value = _mail_text(value, path, maximum=256)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        raise _field_error(path, "must be a valid environment variable name")
    return value


def _mail_port(value: Any, path: str, default: int) -> int:
    if value is None:
        return default
    if type(value) is not int or not 1 <= value <= 65535:
        raise _field_error(path, "must be an integer between 1 and 65535")
    return value


def _validate_mail_endpoint(value: Any, path: str, *, kind: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _field_error(path, "must be a table")
    unknown = set(value) - _MAIL_ENDPOINT_FIELDS
    if unknown:
        raise _field_error(path, f"unknown fields: {', '.join(sorted(unknown))}")
    host = _mail_text(value.get("host"), f"{path}.host", maximum=253)
    security = value.get("security", "ssl")
    if not isinstance(security, str) or security not in _MAIL_SECURITY:
        raise _field_error(
            f"{path}.security", "must be one of ssl, starttls, or plain"
        )
    defaults = {
        "imap": {"ssl": 993, "starttls": 143, "plain": 143},
        "smtp": {"ssl": 465, "starttls": 587, "plain": 25},
    }[kind]
    result = {
        "host": host,
        "port": _mail_port(value.get("port"), f"{path}.port", defaults[security]),
        "security": security,
    }
    username = value.get("username")
    password_from = value.get("password_from")
    if kind == "imap":
        result["username"] = _mail_text(username, f"{path}.username", maximum=320)
        result["password_from"] = _mail_env_name(password_from, f"{path}.password_from")
    else:
        if username is not None:
            result["username"] = _mail_text(username, f"{path}.username", maximum=320)
            if password_from is None:
                raise _field_error(
                    f"{path}.password_from", "is required when username is configured"
                )
            result["password_from"] = _mail_env_name(
                password_from, f"{path}.password_from"
            )
        elif password_from is not None:
            raise _field_error(
                f"{path}.password_from", "requires a username for SMTP authentication"
            )
    if "ca_file" in value:
        result["ca_file"] = _mail_text(value["ca_file"], f"{path}.ca_file", maximum=4096)
    return result


def _validate_mail_account(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _field_error(path, "must be a table")
    unknown = set(value) - _MAIL_ACCOUNT_FIELDS
    if unknown:
        raise _field_error(path, f"unknown fields: {', '.join(sorted(unknown))}")
    sender = _mail_text(value.get("from"), f"{path}.from", maximum=320)
    address = parseaddr(sender)[1]
    if not address or "@" not in address or any(char.isspace() for char in address):
        raise _field_error(f"{path}.from", "must contain a valid email address")
    if "imap" not in value:
        raise _field_error(f"{path}.imap", "is required")
    if "smtp" not in value:
        raise _field_error(f"{path}.smtp", "is required")
    result = {
        "from": sender,
        "imap": _validate_mail_endpoint(value["imap"], f"{path}.imap", kind="imap"),
        "smtp": _validate_mail_endpoint(value["smtp"], f"{path}.smtp", kind="smtp"),
    }
    if "sent_mailbox" in value:
        result["sent_mailbox"] = _mail_text(
            value["sent_mailbox"], f"{path}.sent_mailbox", maximum=255
        )
    return result


def _validate_mail_name(name: Any, path: str) -> str:
    if (
        not isinstance(name, str)
        or not name.strip()
        or len(name) > 128
        or any(char in name for char in "\x00\r\n")
    ):
        raise _field_error(path, "must contain 1..128 characters")
    return name


def _validate_mail_accounts(value: Any, path: str, *, normalize: bool) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _field_error(path, "must be a table")
    result: dict[str, Any] = {}
    for name, definition in value.items():
        account_path = f"{path}.{name}"
        _validate_mail_name(name, path)
        if isinstance(definition, Mapping) and definition == {"enabled": False}:
            result[name] = {"enabled": False}
            continue
        validated = _validate_mail_account(definition, account_path)
        result[name] = validated if normalize else copy.deepcopy(definition)
    return result


def _validate_mail_layer(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise _field_error("mail", "must be a table")
    result = copy.deepcopy(dict(value))
    unknown = set(value) - {"default_account", "accounts"}
    if unknown:
        raise _field_error("mail", f"unknown fields: {', '.join(sorted(unknown))}")
    if "default_account" in value:
        default = value["default_account"]
        if (
            not isinstance(default, str)
            or len(default) > 128
            or any(char in default for char in "\x00\r\n")
        ):
            raise _field_error("mail.default_account", "must be an account name or empty")
    if "accounts" in value:
        result["accounts"] = _validate_mail_accounts(
            value["accounts"], "mail.accounts", normalize=False
        )
    return result


def validate_mail_config(value: Any) -> dict[str, Any]:
    """Validate and normalize the effective ``mail`` configuration."""

    if value is None:
        return copy.deepcopy(DEFAULT_MAIL)
    layer = _validate_mail_layer(value)
    result = copy.deepcopy(DEFAULT_MAIL)
    if "default_account" in layer:
        result["default_account"] = layer["default_account"]
    if "accounts" in layer:
        result["accounts"] = _validate_mail_accounts(
            layer["accounts"], "mail.accounts", normalize=True
        )
    default = result["default_account"]
    if default and (
        default not in result["accounts"]
        or result["accounts"][default].get("enabled", True) is False
    ):
        result["default_account"] = ""
    return result


def _validate_web_provider_name(name: Any, path: str) -> str:
    if not isinstance(name, str) or name not in _WEB_PROVIDERS:
        raise _field_error(path, "must be one of kagi, brave, or tavily")
    return name


def _validate_web_providers(value: Any, path: str, *, normalize: bool) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _field_error(path, "must be a table")
    result: dict[str, Any] = {}
    for name, definition in value.items():
        provider_path = f"{path}.{name}"
        _validate_web_provider_name(name, provider_path)
        if isinstance(definition, Mapping) and definition == {"enabled": False}:
            result[name] = {"enabled": False}
            continue
        if not isinstance(definition, Mapping):
            raise _field_error(provider_path, "must be a table")
        unknown = set(definition) - {"api_key_env"}
        if unknown:
            raise _field_error(
                provider_path, f"unknown fields: {', '.join(sorted(unknown))}"
            )
        if "api_key_env" not in definition:
            raise _field_error(f"{provider_path}.api_key_env", "is required")
        api_key_env = _mail_env_name(
            definition["api_key_env"], f"{provider_path}.api_key_env"
        )
        result[name] = {"api_key_env": api_key_env} if normalize else copy.deepcopy(definition)
    return result


def _validate_web_layer(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise _field_error("web", "must be a table")
    result = copy.deepcopy(dict(value))
    unknown = set(value) - _WEB_FIELDS
    if unknown:
        raise _field_error("web", f"unknown fields: {', '.join(sorted(unknown))}")
    if "default_provider" in value:
        default = value["default_provider"]
        if not isinstance(default, str) or default not in {"", *_WEB_PROVIDERS}:
            raise _field_error(
                "web.default_provider", "must be empty or one of kagi, brave, or tavily"
            )
    if "timeout_seconds" in value:
        timeout = value["timeout_seconds"]
        try:
            valid_timeout = (
                type(timeout) in {int, float}
                and math.isfinite(timeout)
                and 1 <= timeout <= 120
            )
        except (OverflowError, ValueError):
            valid_timeout = False
        if not valid_timeout:
            raise _field_error("web.timeout_seconds", "must be a finite number between 1 and 120")
    if "max_concurrency" in value:
        concurrency = value["max_concurrency"]
        if type(concurrency) is not int or not 1 <= concurrency <= 32:
            raise _field_error("web.max_concurrency", "must be an integer between 1 and 32")
    if "providers" in value:
        result["providers"] = _validate_web_providers(
            value["providers"], "web.providers", normalize=False
        )
    return result


def validate_web_config(value: Any) -> dict[str, Any]:
    """Validate and normalize the effective ``web`` configuration."""

    layer = _validate_web_layer(value)
    result = copy.deepcopy(DEFAULT_WEB)
    for key in ("default_provider", "timeout_seconds", "max_concurrency"):
        if key in layer:
            result[key] = layer[key]
    if "providers" in layer:
        result["providers"] = _validate_web_providers(
            layer["providers"], "web.providers", normalize=True
        )
    default = result["default_provider"]
    if default and (
        default not in result["providers"]
        or result["providers"][default] == {"enabled": False}
    ):
        raise _field_error(
            "web.default_provider", "must refer to an active configured provider"
        )
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
    result["dependencies"] = _validate_dependencies(result.get("dependencies"))
    result["lsp"] = _validate_lsp(result.get("lsp"))
    result["mail"] = validate_mail_config(result.get("mail"))
    result["web"] = validate_web_config(result.get("web"))
    mcp = result.get("mcp")
    if mcp is None:
        result["mcp"] = {"servers": {}}
    elif not isinstance(mcp, dict):
        raise _field_error("mcp", "must be a table")
    else:
        result["mcp"] = copy.deepcopy(mcp)
        result["mcp"]["servers"] = validate_servers(mcp.get("servers", {}))
    return result


def _validate_server_layer(section: str, value: Any) -> None:
    if not isinstance(value, Mapping):
        raise _field_error(f"{section}.servers", "must be a table")
    active = {}
    for name, definition in value.items():
        if isinstance(definition, Mapping) and "enabled" in definition:
            if definition != {"enabled": False}:
                raise _field_error(
                    f"{section}.servers.{name}",
                    "enabled=false is only valid as a server tombstone",
                )
            try:
                if section == "mcp":
                    validate_name(name)
                else:
                    _validate_lsp_servers(
                        {name: {"command": ["x"], "languages": ["x"]}}
                    )
            except (TypeError, ValueError) as exc:
                raise _field_error(f"{section}.servers", str(exc)) from exc
            continue
        active[name] = definition
    try:
        (validate_servers if section == "mcp" else _validate_lsp_servers)(active)
    except (TypeError, ValueError) as exc:
        raise _field_error(f"{section}.servers", str(exc)) from exc


def _validate_layer(values: Any, path: Path) -> dict:
    if not isinstance(values, Mapping):
        raise ConfigError("configuration must be a table", path=str(path))
    result = copy.deepcopy(dict(values))
    if "version" in result:
        version = result["version"]
        if type(version) is not int or version < 1 or version > CONFIG_VERSION:
            raise _field_error("version", f"must be an integer between 1 and {CONFIG_VERSION}")
    limits = result.get("limits")
    if limits is not None:
        if not isinstance(limits, Mapping):
            raise _field_error("limits", "must be a table")
        for key, value in limits.items():
            if key in DEFAULT_LIMITS:
                if key in {"execute_wait_ms", "poll_wait_ms"}:
                    _validate_wait_ms(value, f"limits.{key}")
                    continue
                minimum = 1024 if key in {"output_bytes", "response_bytes", "cache_bytes"} else 1
                maximum = MAX_RESPONSE_BYTES if key == "response_bytes" else None
                _positive(value, f"limits.{key}", minimum=minimum, maximum=maximum)
    storage = result.get("storage")
    if storage is not None:
        if not isinstance(storage, Mapping):
            raise _field_error("storage", "must be a table")
        for key, value in storage.items():
            if key == "enabled":
                if type(value) is not bool:
                    raise _field_error("storage.enabled", "must be a boolean")
            elif key in DEFAULT_STORAGE:
                _positive(value, f"storage.{key}")
    dependencies = result.get("dependencies")
    if dependencies is not None:
        if not isinstance(dependencies, Mapping):
            raise _field_error("dependencies", "must be a table")
        _validate_dependencies(dependencies)
    for section in _SERVER_SECTIONS:
        table = result.get(section)
        if table is None:
            continue
        if not isinstance(table, Mapping):
            raise _field_error(section, "must be a table")
        servers = table.get("servers")
        if servers is not None:
            _validate_server_layer(section, servers)
    if "mail" in result:
        _validate_mail_layer(result["mail"])
    if "web" in result:
        _validate_web_layer(result["web"])
    return result


def _merge_servers(global_value: Any, workspace_value: Any) -> dict:
    result: dict[str, Any] = {}
    for source in (global_value, workspace_value):
        if source is None:
            continue
        if not isinstance(source, Mapping):
            raise ConfigError("servers must be a table")
        for name, definition in source.items():
            if isinstance(definition, Mapping) and definition.get("enabled") is False:
                result.pop(name, None)
            else:
                result[name] = copy.deepcopy(definition)
    return result


def _merge_named(global_value: Any, workspace_value: Any) -> dict:
    """Merge named complete replacements with disabled-entry tombstones."""

    result: dict[str, Any] = {}
    for source in (global_value, workspace_value):
        if source is None:
            continue
        if not isinstance(source, Mapping):
            raise ConfigError("accounts must be a table")
        for name, definition in source.items():
            if isinstance(definition, Mapping) and definition.get("enabled") is False:
                result.pop(name, None)
            else:
                result[name] = copy.deepcopy(definition)
    return result


def _merge_layers(global_value: Mapping, workspace_value: Mapping) -> dict:
    def merge(left: Any, right: Any) -> Any:
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return copy.deepcopy(right if right is not None else left)
        merged = copy.deepcopy(dict(left))
        for key, value in right.items():
            if key in merged and isinstance(merged[key], Mapping) and isinstance(value, Mapping):
                merged[key] = merge(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged

    merged = merge(global_value, workspace_value)
    for section in _SERVER_SECTIONS:
        global_section = global_value.get(section, {})
        workspace_section = workspace_value.get(section, {})
        if not isinstance(global_section, Mapping) and not isinstance(workspace_section, Mapping):
            continue
        section_value = merged.get(section, {})
        if not isinstance(section_value, Mapping):
            section_value = {}
        section_value = copy.deepcopy(dict(section_value))
        if (
            isinstance(global_section, Mapping)
            and "servers" in global_section
        ) or (
            isinstance(workspace_section, Mapping)
            and "servers" in workspace_section
        ):
            section_value["servers"] = _merge_servers(
                global_section.get("servers") if isinstance(global_section, Mapping) else None,
                workspace_section.get("servers")
                if isinstance(workspace_section, Mapping)
                else None,
            )
        merged[section] = section_value
    global_mail = global_value.get("mail", {})
    workspace_mail = workspace_value.get("mail", {})
    mail_value = merged.get("mail", {})
    if not isinstance(mail_value, Mapping):
        mail_value = {}
    mail_value = copy.deepcopy(dict(mail_value))
    if (
        isinstance(global_mail, Mapping) and "accounts" in global_mail
    ) or (
        isinstance(workspace_mail, Mapping) and "accounts" in workspace_mail
    ):
        mail_value["accounts"] = _merge_named(
            global_mail.get("accounts") if isinstance(global_mail, Mapping) else None,
            workspace_mail.get("accounts") if isinstance(workspace_mail, Mapping) else None,
        )
    merged["mail"] = mail_value
    global_web = global_value.get("web", {})
    workspace_web = workspace_value.get("web", {})
    web_value = merged.get("web", {})
    if not isinstance(web_value, Mapping):
        web_value = {}
    web_value = copy.deepcopy(dict(web_value))
    if (
        isinstance(global_web, Mapping) and "providers" in global_web
    ) or (
        isinstance(workspace_web, Mapping) and "providers" in workspace_web
    ):
        web_value["providers"] = _merge_named(
            global_web.get("providers") if isinstance(global_web, Mapping) else None,
            workspace_web.get("providers") if isinstance(workspace_web, Mapping) else None,
        )
    merged["web"] = web_value
    return merged


def parse_path(path: str) -> tuple[str, ...]:
    """Parse a TOML dotted key, including quoted keys containing dots."""

    if not isinstance(path, str) or not path or len(path) > 512 or any(
        ord(char) < 0x20 for char in path
    ):
        raise ConfigError("must be a TOML dotted key", path="path")
    try:
        parsed = tomlkit.parse(f"{path} = 0\n").unwrap()
    except Exception as exc:
        raise ConfigError(f"must be a TOML dotted key: {exc}", path="path") from exc
    parts: list[str] = []
    current: Any = parsed
    while isinstance(current, dict) and len(current) == 1:
        key, value = next(iter(current.items()))
        if not isinstance(key, str) or not key or len(key) > 128:
            raise ConfigError("must contain non-empty key parts", path="path")
        parts.append(key)
        if isinstance(value, dict):
            current = value
        elif value == 0:
            break
        else:
            raise ConfigError("must be a TOML dotted key", path="path")
    if not parts or len(parts) > 16 or not isinstance(current, dict) or len(current) != 1:
        raise ConfigError("must be a TOML dotted key", path="path")
    if parts[0] not in MANAGED_SECTIONS:
        raise ConfigError("only managed configuration sections are supported", path=path)
    return tuple(parts)


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
    raw = ConfigStore._read(path)
    try:
        values = parse_config(raw)
    except ConfigError as exc:
        if not exc.path:
            exc.path = str(path)
        raise
    return ConfigSnapshot(values, _revision(raw))


load_config = load_workspace_config


def _composite_revision(revisions: Mapping[str, str | None]) -> str:
    return ";".join(
        f"{name}={revisions.get(name) or '-'}" for name in ("global", "workspace")
    )


def _lookup(values: Any, parts: tuple[str, ...]) -> tuple[bool, Any]:
    current = values
    for part in parts:
        if not isinstance(current, Mapping) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _public_values(values: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for section in ("limits", "storage", "dependencies", "mcp", "lsp", "mail", "web"):
        value = values.get(section)
        if not isinstance(value, Mapping):
            continue
        if section == "limits":
            result[section] = {
                key: copy.deepcopy(value[key]) for key in DEFAULT_LIMITS if key in value
            }
        elif section == "storage":
            result[section] = {
                key: copy.deepcopy(value[key]) for key in DEFAULT_STORAGE if key in value
            }
        elif section == "dependencies":
            result[section] = {
                key: copy.deepcopy(value[key])
                for key in (*DEFAULT_DEPENDENCIES, "uv_cache_dir", "uv_link_mode")
                if key in value
            }
        elif section in _SERVER_SECTIONS:
            result[section] = {"servers": copy.deepcopy(value.get("servers", {}))}
        elif section == "mail":
            result[section] = {
                "default_account": copy.deepcopy(value.get("default_account", "")),
                "accounts": copy.deepcopy(value.get("accounts", {})),
            }
        elif section == "web":
            result[section] = {
                "default_provider": copy.deepcopy(value.get("default_provider", "")),
                "timeout_seconds": copy.deepcopy(value.get("timeout_seconds", 30)),
                "max_concurrency": copy.deepcopy(value.get("max_concurrency", 4)),
                "providers": copy.deepcopy(value.get("providers", {})),
            }
    return result


def _validate_public_path(parts: tuple[str, ...]) -> None:
    section = parts[0]
    if len(parts) == 1 and section in MANAGED_SECTIONS:
        return
    if section in {"limits", "storage", "dependencies"}:
        allowed = {
            "limits": DEFAULT_LIMITS,
            "storage": DEFAULT_STORAGE,
            "dependencies": DEFAULT_DEPENDENCIES,
        }[section]
        dependency_fields = {"auto_install", "uv_cache_dir", "uv_link_mode"}
        if section == "dependencies":
            allowed = dependency_fields
        if len(parts) > 2 or (len(parts) == 2 and parts[1] not in allowed):
            raise ConfigError("unknown managed configuration field", path=".".join(parts))
        return
    if section in _SERVER_SECTIONS and len(parts) >= 2 and parts[1] == "servers":
        return
    if section == "mail":
        if len(parts) == 2 and parts[1] == "default_account":
            return
        if len(parts) == 2 and parts[1] == "accounts":
            return
        if len(parts) == 3 and parts[1] == "accounts":
            return
    if section == "web":
        if len(parts) == 2 and parts[1] in {
            "default_provider", "timeout_seconds", "max_concurrency", "providers",
        }:
            return
        if len(parts) == 3 and parts[1] == "providers":
            _validate_web_provider_name(parts[2], ".".join(parts))
            return
    raise ConfigError("unknown managed configuration field", path=".".join(parts))


def _server_tombstone(value: Any) -> bool:
    return isinstance(value, Mapping) and value == {"enabled": False}


def _quote_key(value: str) -> str:
    if value and all(char.isascii() and (char.isalnum() or char in "_-") for char in value):
        return value
    if any(ord(char) < 0x20 for char in value):
        raise ConfigError("configuration name contains a control character", path="mail.accounts")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _document_from_raw(raw: bytes | None) -> Any:
    if raw is None or raw == b"":
        return tomlkit.document()
    return tomlkit.parse(raw.decode("utf-8"))


def _parse_layer(raw: bytes | None, path: Path) -> dict:
    if raw is None or raw == b"":
        return {}
    try:
        parsed = tomlkit.parse(raw.decode("utf-8")).unwrap()
    except (UnicodeDecodeError, tomlkit.exceptions.ParseError) as exc:
        raise ConfigError(
            f"invalid TOML: {exc}",
            path=str(path),
            line=getattr(exc, "line", None),
            column=getattr(exc, "col", None),
        ) from exc
    return _validate_layer(parsed, path)


def _set_document(document: Any, parts: tuple[str, ...], value: Any) -> None:
    current = document
    for part in parts[:-1]:
        child = current.get(part)
        if child is None:
            child = tomlkit.table()
            current[part] = child
        elif not isinstance(child, MutableMapping):
            raise ConfigError("must be a table", path=".".join(parts))
        current = child
    current[parts[-1]] = copy.deepcopy(value)


def _unset_document(document: Any, parts: tuple[str, ...]) -> bool:
    current = document
    for part in parts[:-1]:
        current = current.get(part)
        if not isinstance(current, MutableMapping):
            return False
    if parts[-1] not in current:
        return False
    del current[parts[-1]]
    return True


class ConfigStore:
    """Coordinate the global and workspace TOML configuration layers."""

    def __init__(
        self,
        workspace: str | os.PathLike[str] | None = None,
        global_path: str | os.PathLike[str] | None = None,
        *,
        workspace_guard=None,
    ) -> None:
        self.workspace = (
            Path(workspace).expanduser().resolve() if workspace is not None else None
        )
        self.workspace_path = self.workspace / ".mypr" / "config.toml" if self.workspace else None
        self.global_path = (
            Path(global_path).expanduser().resolve()
            if global_path is not None
            else global_config_path()
        )
        if self.global_path == self.workspace_path:
            raise ValueError("global and workspace configuration paths must differ")
        self._workspace_guard = workspace_guard

    @property
    def paths(self) -> dict[str, Path]:
        return {"global": self.global_path, "workspace": self.workspace_path}

    def load(self) -> ConfigSnapshot:
        with self._profile_lock(exclusive=False, create=False) as locked:
            if locked:
                with self._locks(create=False):
                    return self._snapshot(
                        self._read(self.global_path), self._read(self.workspace_path)
                    )
            for _ in range(3):
                with self._locks(create=False):
                    first = self._read(self.global_path), self._read(self.workspace_path)
                with self._locks(create=False):
                    second = self._read(self.global_path), self._read(self.workspace_path)
                if first == second:
                    return self._snapshot(*first)
            raise RuntimeError("configuration changed while being read; retry")

    def get(
        self,
        path: str | None = None,
        scope: str = "effective",
        snapshot: ConfigSnapshot | None = None,
    ) -> Any:
        snapshot = snapshot or self.load()
        if scope not in {"effective", "global", "workspace"}:
            raise ConfigError("must be effective, global, or workspace", path="scope")
        values = snapshot.values if scope == "effective" else snapshot.layers.get(scope, {})
        if path is None:
            return _public_values(values)
        parts = parse_path(path)
        _validate_public_path(parts)
        values = _public_values(values)
        found, value = _lookup(values, parts)
        return copy.deepcopy(value) if found else None

    def explain(self, path: str, snapshot: ConfigSnapshot | None = None) -> dict[str, Any]:
        snapshot = snapshot or self.load()
        parts = parse_path(path)
        _validate_public_path(parts)
        public_values = _public_values(snapshot.values)
        public_layers = {
            scope: _public_values(snapshot.layers.get(scope, {}))
            for scope in ("global", "workspace")
        }
        found, value = _lookup(public_values, parts)
        if parts[0] in _SERVER_SECTIONS and len(parts) >= 3:
            for scope in ("workspace", "global"):
                server_found, definition = _lookup(
                    snapshot.layers.get(scope, {}), parts[:3]
                )
                if not server_found:
                    continue
                if _server_tombstone(definition):
                    return {
                        "value": None,
                        "source": scope,
                        "revisions": copy.deepcopy(snapshot.revisions),
                    }
                return {
                    "value": copy.deepcopy(value) if found else None,
                    "source": scope,
                    "revisions": copy.deepcopy(snapshot.revisions),
                }
        source = "default"
        if found:
            for scope in ("workspace", "global"):
                layer_found, layer_value = _lookup(public_layers[scope], parts)
                if layer_found:
                    source = scope
                    if parts[0] in _SERVER_SECTIONS and len(parts) >= 3:
                        if _server_tombstone(layer_value):
                            value = None
                        else:
                            value = layer_value
                    break
        else:
            for scope in ("workspace", "global"):
                layer_found, layer_value = _lookup(public_layers[scope], parts)
                if layer_found:
                    source = scope
                    value = None if _server_tombstone(layer_value) else layer_value
                    break
        return {
            "value": copy.deepcopy(value) if found or source != "default" else None,
            "source": source,
            "revisions": copy.deepcopy(snapshot.revisions),
        }

    def set(
        self,
        path: str,
        value: Any,
        scope: str = "workspace",
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        parts = parse_path(path)
        _validate_public_path(parts)
        if value is None:
            raise ConfigError("use unset() to remove a value", path=path)
        if parts[0] in _SERVER_SECTIONS and len(parts) >= 3 and len(parts) != 3:
            raise ConfigError("server definitions must be replaced as a whole", path=path)
        if parts[0] == "web" and len(parts) >= 4:
            raise ConfigError("web provider definitions must be replaced as a whole", path=path)
        return self._mutate(
            path,
            scope,
            expected_revision,
            lambda doc, _layers: _set_document(doc, parts, value),
        )

    def unset(
        self,
        path: str,
        scope: str = "workspace",
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        parts = parse_path(path)
        _validate_public_path(parts)
        if parts[0] in _SERVER_SECTIONS and len(parts) >= 3 and len(parts) != 3:
            raise ConfigError("server definitions must be removed as a whole", path=path)
        if parts[0] == "web" and len(parts) >= 4:
            raise ConfigError("web provider definitions must be removed as a whole", path=path)

        return self._mutate(
            path,
            scope,
            expected_revision,
            lambda document, _layers: _unset_document(document, parts),
        )

    def save_server(
        self,
        section: str,
        name: str,
        definition: Mapping[str, Any] | None,
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        if section not in _SERVER_SECTIONS or not isinstance(name, str) or not name:
            raise ConfigError("invalid server target", path=f"{section}.servers")
        path = f"{section}.servers.{_quote_key(name)}"
        if definition is None:
            parts = parse_path(path)

            def remove(document: Any, layers: dict[str, dict]) -> bool:
                if self.workspace_path is None:
                    return _unset_document(document, parts)
                global_servers = layers["global"].get(section, {}).get("servers", {})
                inherited = (
                    global_servers.get(name) if isinstance(global_servers, Mapping) else None
                )
                if inherited is not None and not _server_tombstone(inherited):
                    _set_document(document, parts, {"enabled": False})
                    return True
                return _unset_document(document, parts)

            return self._mutate(
                path,
                "global" if self.workspace_path is None else "workspace",
                expected_revision,
                remove,
            )
        if not isinstance(definition, Mapping):
            raise ConfigError("server definition must be a table", path=path)
        return self.set(
            path,
            dict(definition),
            scope="global" if self.workspace_path is None else "workspace",
            expected_revision=expected_revision,
        )

    def save_servers(
        self,
        section: str,
        definitions: Mapping[str, Mapping[str, Any]],
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        if section not in _SERVER_SECTIONS or not isinstance(definitions, Mapping):
            raise ConfigError("invalid server definitions", path=f"{section}.servers")
        validator = validate_servers if section == "mcp" else _validate_lsp_servers
        desired = validator(definitions)
        path_parts = (section, "servers")

        def change(document: Any, layers: dict[str, dict]) -> None:
            global_servers = layers["global"].get(section, {}).get("servers", {})
            workspace_servers = layers["workspace"].get(section, {}).get("servers", {})
            if not isinstance(global_servers, Mapping):
                global_servers = {}
            if not isinstance(workspace_servers, Mapping):
                workspace_servers = {}
            if self.workspace_path is None:
                names = set(desired) | set(global_servers)
                for name in sorted(names):
                    if name in desired:
                        _set_document(document, (*path_parts, name), desired[name])
                    else:
                        _unset_document(document, (*path_parts, name))
                return
            names = set(desired) | set(global_servers) | set(workspace_servers)
            for name in sorted(names):
                wanted = desired.get(name)
                inherited = global_servers.get(name)
                if wanted is None:
                    if name in global_servers and not _server_tombstone(inherited):
                        if not _server_tombstone(workspace_servers.get(name)):
                            _set_document(document, (*path_parts, name), {"enabled": False})
                    else:
                        _unset_document(document, (*path_parts, name))
                elif name in workspace_servers and not _server_tombstone(workspace_servers[name]):
                    workspace_effective = validator({name: workspace_servers[name]})[name]
                    if workspace_effective == wanted:
                        continue
                    _set_document(document, (*path_parts, name), wanted)
                elif inherited is not None and not _server_tombstone(inherited):
                    inherited_effective = validator({name: inherited})[name]
                    if inherited_effective == wanted:
                        _unset_document(document, (*path_parts, name))
                    else:
                        _set_document(document, (*path_parts, name), wanted)
                elif inherited == wanted:
                    _unset_document(document, (*path_parts, name))
                else:
                    _set_document(document, (*path_parts, name), wanted)

        return self._mutate(
            f"{section}.servers",
            "global" if self.workspace_path is None else "workspace",
            expected_revision,
            change,
        )

    def save_mail_account(
        self,
        name: str,
        definition: Mapping[str, Any] | None,
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        """Replace one workspace account, preserving inherited-account semantics."""

        _validate_mail_name(name, "mail.accounts")
        path = f"mail.accounts.{_quote_key(name)}"
        if definition is not None:
            if not isinstance(definition, Mapping):
                raise ConfigError("mail account definition must be a table", path=path)
            scope = "global" if self.workspace_path is None else "workspace"
            if isinstance(definition, Mapping) and definition == {"enabled": False}:
                return self.set(
                    path, {"enabled": False}, scope=scope,
                    expected_revision=expected_revision,
                )
            normalized = _validate_mail_account(definition, path)
            return self.set(path, normalized, scope=scope, expected_revision=expected_revision)

        parts = parse_path(path)

        def remove(document: Any, layers: dict[str, dict]) -> bool:
            if self.workspace_path is None:
                return _unset_document(document, parts)
            mail = layers["global"].get("mail", {})
            global_accounts = mail.get("accounts", {}) if isinstance(mail, Mapping) else {}
            inherited = global_accounts.get(name) if isinstance(global_accounts, Mapping) else None
            if inherited is not None and not _server_tombstone(inherited):
                _set_document(document, parts, {"enabled": False})
                return True
            return _unset_document(document, parts)

        return self._mutate(
            path,
            "global" if self.workspace_path is None else "workspace",
            expected_revision,
            remove,
        )

    def save_mail_accounts(
        self,
        definitions: Mapping[str, Mapping[str, Any]],
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        """Replace the effective account map while keeping global inheritance compact."""

        if not isinstance(definitions, Mapping):
            raise ConfigError("mail accounts must be a table", path="mail.accounts")
        desired = _validate_mail_accounts(definitions, "mail.accounts", normalize=True)
        path_parts = ("mail", "accounts")

        def change(document: Any, layers: dict[str, dict]) -> None:
            global_mail = layers["global"].get("mail", {})
            workspace_mail = layers["workspace"].get("mail", {})
            global_accounts = (
                global_mail.get("accounts", {}) if isinstance(global_mail, Mapping) else {}
            )
            workspace_accounts = (
                workspace_mail.get("accounts", {}) if isinstance(workspace_mail, Mapping) else {}
            )
            if not isinstance(global_accounts, Mapping):
                global_accounts = {}
            if not isinstance(workspace_accounts, Mapping):
                workspace_accounts = {}
            names = set(desired) | set(global_accounts) | set(workspace_accounts)
            for name in sorted(names):
                wanted = desired.get(name)
                if _server_tombstone(wanted):
                    wanted = None
                inherited = global_accounts.get(name)
                current = workspace_accounts.get(name)
                if wanted is None:
                    if (
                        self.workspace_path is not None
                        and name in global_accounts
                        and not _server_tombstone(inherited)
                    ):
                        if not _server_tombstone(current):
                            _set_document(document, (*path_parts, name), {"enabled": False})
                    else:
                        _unset_document(document, (*path_parts, name))
                    continue
                if (
                    self.workspace_path is not None
                    and current is not None
                    and not _server_tombstone(current)
                ):
                    if _validate_mail_account(current, f"mail.accounts.{name}") == wanted:
                        continue
                    _set_document(document, (*path_parts, name), wanted)
                elif (
                    self.workspace_path is not None
                    and inherited is not None
                    and not _server_tombstone(inherited)
                ):
                    if _validate_mail_account(inherited, f"mail.accounts.{name}") == wanted:
                        _unset_document(document, (*path_parts, name))
                    else:
                        _set_document(document, (*path_parts, name), wanted)
                elif inherited == wanted:
                    _unset_document(document, (*path_parts, name))
                else:
                    _set_document(document, (*path_parts, name), wanted)

        return self._mutate(
            "mail.accounts",
            "global" if self.workspace_path is None else "workspace",
            expected_revision,
            change,
        )

    def _snapshot(self, global_raw: bytes | None, workspace_raw: bytes | None) -> ConfigSnapshot:
        global_values = _parse_layer(global_raw, self.global_path)
        workspace_values = (
            _parse_layer(workspace_raw, self.workspace_path)
            if self.workspace_path is not None
            else {}
        )
        merged = _merge_layers(global_values, workspace_values)
        values = validate_config(merged)
        revisions = {"global": _revision(global_raw), "workspace": _revision(workspace_raw)}
        return ConfigSnapshot(
            values,
            _composite_revision(revisions),
            {"global": global_values, "workspace": workspace_values},
            revisions,
            self.paths,
        )

    def _mutate(
        self,
        path: str,
        scope: str,
        expected_revision: str | None,
        change: Any,
    ) -> ConfigSnapshot:
        if scope not in {"global", "workspace"}:
            raise ConfigError("writes require global or workspace scope", path="scope")
        if scope == "workspace" and self.workspace_path is None:
            raise ConfigError("workspace scope is unavailable in global-only mode", path="scope")
        if scope == "workspace" and self._workspace_guard is not None:
            self._workspace_guard()
        target = self.global_path if scope == "global" else self.workspace_path
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._profile_lock(exclusive=True, create=True):
            with self._locks(create=True, target=target):
                if scope == "workspace" and self._workspace_guard is not None:
                    self._workspace_guard()
                original_global = self._read(self.global_path)
                original_workspace = self._read(self.workspace_path)
                current = self._snapshot(original_global, original_workspace)
                if expected_revision is not None and current.revision != expected_revision:
                    raise RuntimeError("configuration changed on disk; reload before saving")
                layers = current.layers
                document = _document_from_raw(
                    original_global if scope == "global" else original_workspace
                )
                try:
                    changed = change(document, layers)
                    if changed is False:
                        return current
                    data = tomlkit.dumps(document).encode("utf-8")
                    if len(data) > _MAX_CONFIG_BYTES:
                        raise ConfigError("configuration file exceeds the size limit", path=path)
                    _parse_layer(data, target)
                    updated_global = data if scope == "global" else original_global
                    updated_workspace = data if scope == "workspace" else original_workspace
                    updated = self._snapshot(updated_global, updated_workspace)
                except (tomlkit.exceptions.TOMLKitError, UnicodeEncodeError) as exc:
                    raise ConfigError(f"invalid configuration: {exc}", path=path) from exc
                if (
                    self._read(self.global_path) != original_global
                    or (
                        self.workspace_path is not None
                        and self._read(self.workspace_path) != original_workspace
                    )
                ):
                    raise RuntimeError("configuration changed during save; reload before retrying")
                if data != (original_global if scope == "global" else original_workspace):
                    if scope == "workspace" and self._workspace_guard is not None:
                        self._workspace_guard()
                    self._write(target, data)
                return updated

    def _profile_lock_path(self) -> Path:
        state_home = os.environ.get("XDG_STATE_HOME")
        base = Path(state_home).expanduser() if state_home else Path.home() / ".local" / "state"
        name = hashlib.sha256(os.fsencode(self.global_path)).hexdigest()
        return base / "mypr" / "config-locks" / f"{name}.lock"

    @contextmanager
    def _profile_lock(self, *, exclusive: bool, create: bool):
        path = self._profile_lock_path()
        if create:
            path.parent.mkdir(parents=True, exist_ok=True)
            stream = open_regular(path, "ab")
        else:
            try:
                stream = open_regular(path)
            except FileNotFoundError:
                yield False
                return
        try:
            fcntl.flock(stream, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield True
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
            stream.close()

    @contextmanager
    def _locks(self, *, create: bool, target: Path | None = None):
        paths = sorted(
            (path for path in (self.global_path, self.workspace_path) if path is not None),
            key=str,
        )
        locks = []
        try:
            for path in paths:
                target_lock = create and path == target
                if target_lock:
                    path.parent.mkdir(parents=True, exist_ok=True)
                lock_path = path.with_suffix(".lock")
                if target_lock:
                    lock_path.parent.mkdir(parents=True, exist_ok=True)
                    stream = open_regular(lock_path, "ab")
                    mode = fcntl.LOCK_EX
                else:
                    try:
                        stream = open_regular(lock_path)
                    except FileNotFoundError:
                        continue
                    mode = fcntl.LOCK_SH
                fcntl.flock(stream, mode)
                locks.append(stream)
            yield
        finally:
            for stream in reversed(locks):
                fcntl.flock(stream, fcntl.LOCK_UN)
                stream.close()

    @staticmethod
    def _read(path: Path | None) -> bytes | None:
        if path is None:
            return None
        try:
            flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(path, flags)
        except FileNotFoundError:
            return None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_CONFIG_BYTES:
                raise ConfigError(
                    "configuration file is not a bounded regular file", path=str(path)
                )
            chunks = []
            remaining = _MAX_CONFIG_BYTES + 1
            while remaining:
                chunk = os.read(fd, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
            if len(data) > _MAX_CONFIG_BYTES:
                raise ConfigError("configuration file exceeds the size limit", path=str(path))
            return data
        finally:
            os.close(fd)

    @staticmethod
    def _write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent, prefix=".mypr-config-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            if path.exists():
                temporary.chmod(path.stat().st_mode & 0o777)
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


class MCPConfig:
    """Compatibility facade over the global/workspace configuration store."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        global_path: str | os.PathLike[str] | None = None,
        *,
        workspace_guard=None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.core = ConfigStore(
            self.workspace, global_path, workspace_guard=workspace_guard
        )
        self.path = self.core.workspace_path

    def _raw(self) -> bytes | None:
        return ConfigStore._read(self.path)

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
        snapshot = self.core.load()
        return copy.deepcopy(snapshot.values["mcp"]["servers"]), snapshot.revision

    def load_document(self):
        raw = self._raw()
        try:
            document = tomlkit.parse(raw.decode("utf-8")) if raw is not None else tomlkit.document()
        except (UnicodeDecodeError, tomlkit.exceptions.ParseError) as exc:
            raise ConfigError(f"invalid TOML: {exc}", path=str(self.path)) from exc
        return document, _revision(raw)

    def load_all(self) -> ConfigSnapshot:
        return self.core.load()

    @property
    def revision(self) -> str:
        return self.core.load().revision

    def save_section(self, section: str, values: dict, expected_revision: str | None) -> str:
        snapshot = self.core.set(section, values, expected_revision=expected_revision)
        return snapshot.revision

    def save_lsp(self, servers: dict, expected_revision: str | None) -> str:
        return self.core.save_servers("lsp", servers, expected_revision).revision

    def save(self, servers, expected_revision):
        return self.core.save_servers("mcp", servers, expected_revision).revision

    def save_server(
        self,
        section: str,
        name: str,
        definition: Mapping[str, object] | None,
        expected_revision: str | None = None,
    ) -> ConfigSnapshot:
        return self.core.save_server(section, name, definition, expected_revision)
