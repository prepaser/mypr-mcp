"""Command builders and output normalizers for optional search engines.

The workspace search implementation owns process execution.  This module only
turns validated search options into argv and converts ast-grep's JSON records
to the small, engine-independent match shape used by the search API.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_MODES = {"matches", "files", "counts", "exists"}
_AST_FORBIDDEN = {
    "fix",
    "fixes",
    "rewrite",
    "rewrites",
    "transform",
    "transforms",
    "ruleDirs",
    "testConfigs",
}
_AST_MATCHER_KEYS = {
    "all",
    "any",
    "field",
    "follows",
    "has",
    "inside",
    "kind",
    "matches",
    "not",
    "nthChild",
    "pattern",
    "precedes",
    "regex",
    "stopBy",
}
_AST_STRICTNESS = {"cst", "smart", "ast", "relaxed", "signature", "template"}


def build(workspace: Path, backend: str, options: Mapping[str, Any]) -> list[str]:
    """Build an argv list for an optional search backend.

    ``workspace`` is used only for manager-owned configuration paths.  The
    caller must execute the command with the workspace as its cwd.
    """

    if not isinstance(options, Mapping):
        raise TypeError("search options must be a mapping")
    if backend == "rga":
        return _build_rga(Path(workspace), options)
    if backend == "ast":
        return _build_ast(Path(workspace), options)
    raise ValueError("backend must be rga or ast")


def _build_rga(workspace: Path, options: Mapping[str, Any]) -> list[str]:
    pattern = options.get("pattern")
    if not isinstance(pattern, (str, list, tuple)) or (
        isinstance(pattern, (list, tuple)) and not pattern
    ):
        raise ValueError("rga search requires a non-empty pattern")
    patterns = [pattern] if isinstance(pattern, str) else list(pattern)
    if not all(isinstance(value, str) and value for value in patterns):
        raise ValueError("pattern must be a non-empty string or list of strings")
    mode = _mode(options)
    _ensure_rga_config(workspace)
    command = ["rga", "--rga-config-file=" + str(_rga_config(workspace))]
    cache_path = options.get("_cache_path", _rga_cache(workspace))
    if not isinstance(cache_path, (str, Path)) or not str(cache_path):
        raise ValueError("_cache_path must be a non-empty path")
    command.append("--rga-cache-path=" + str(cache_path))
    command.append("--rga-max-archive-recursion=" + str(_archive_depth(options)))
    command.extend(["--no-config", "--threads", "2"])
    if options.get("cache") is False and "_cache_path" not in options:
        command.append("--rga-no-cache")
    if options.get("accurate"):
        command.append("--rga-accurate")
    adapters = options.get("adapters")
    if adapters is not None:
        values = _strings(adapters, "adapters")
        if not values or any(not value.strip() or "\0" in value for value in values):
            raise ValueError("adapters must contain non-empty names without NUL")
        command.append("--rga-adapters=" + ",".join(values))

    # rga passes the regular rg options through to its embedded ripgrep.
    if mode == "matches" or (mode == "counts" and options.get("scan_limit")):
        command.extend(["--json", "--line-number"])
    elif mode == "files":
        command.extend(["--files-with-matches", "--null"])
    elif mode == "counts":
        command.extend(["--count-matches", "--with-filename", "--null"])
    else:
        command.append("--quiet")
    _append_rg_options(command, options)
    if len(patterns) > 1:
        for value in patterns:
            command.extend(["--regexp", value])
        command.append("--")
    else:
        command.extend(["--", patterns[0]])
    command.extend(_paths(options.get("paths")))
    return command


def _append_rg_options(command: list[str], options: Mapping[str, Any]) -> None:
    if options.get("fixed"):
        command.append("--fixed-strings")
    if options.get("ignore_case"):
        command.append("--ignore-case")
    if options.get("hidden"):
        command.append("--hidden")
    if options.get("no_ignore"):
        command.append("--no-ignore")
    if options.get("word"):
        command.append("--word-regexp")
    if options.get("line"):
        command.append("--line-regexp")
    if options.get("multiline"):
        command.append("--multiline")
    if options.get("dotall"):
        command.append("--multiline-dotall")
    engine = options.get("regex_engine", "default")
    if engine not in {"default", "pcre2"}:
        raise ValueError("regex_engine must be default or pcre2")
    if engine == "pcre2":
        command.append("--pcre2")
    context = _nonnegative_int(options.get("context", 0), "context")
    before = options.get("before")
    after = options.get("after")
    if context and (before is not None or after is not None):
        raise ValueError("context cannot be combined with before or after")
    if context:
        command.extend(["--context", str(context)])
    else:
        if before is not None:
            command.extend(["--before-context", str(_nonnegative_int(before, "before"))])
        if after is not None:
            command.extend(["--after-context", str(_nonnegative_int(after, "after"))])
    for value in _strings(options.get("glob"), "glob", allow_none=True):
        if not value:
            raise ValueError("glob must not be empty")
        command.extend(["--glob", value])


def _build_ast(workspace: Path, options: Mapping[str, Any]) -> list[str]:
    lang = options.get("lang")
    if not isinstance(lang, str) or not lang.strip():
        raise ValueError("lang is required for AST search")
    pattern = options.get("pattern")
    rule = options.get("rule")
    if (pattern is None) == (rule is None):
        raise ValueError("AST search requires exactly one of pattern or rule")
    executable = options.get("executable", "ast-grep")
    if not isinstance(executable, str) or not executable:
        raise ValueError("executable must be a non-empty path")
    if Path(executable).name == "sg":
        _verify_sg(executable)
    _ensure_ast_config(workspace)
    command = [executable]
    constraints = options.get("constraints")
    utils = options.get("utils")
    has_extras = constraints is not None or utils is not None
    run_query = rule is None and not has_extras
    if not run_query and options.get("strictness", "smart") != "smart":
        raise ValueError("set strictness in the rule pattern object for rule scans")
    if run_query:
        if not isinstance(pattern, str) or not pattern:
            raise ValueError("pattern must be a non-empty string")
        command.extend(
            [
                "run",
                "--config",
                str(_ast_config(workspace)),
                "--pattern",
                pattern,
                "--lang",
                lang,
            ]
        )
    else:
        if rule is None:
            if not isinstance(pattern, str) or not pattern:
                raise ValueError("pattern must be a non-empty string")
            rule = {"pattern": pattern}
        _validate_bare_matcher(rule)
        command.extend(["scan", "--config", str(_ast_config(workspace))])
        inline = _inline_rule(rule, lang, options)
        command.extend(["--inline-rules", inline, "--include-metadata"])
        command.append("--hint=mypr-search")
    if run_query:
        strictness = options.get("strictness", "smart")
        if strictness not in _AST_STRICTNESS:
            raise ValueError("strictness is not supported by ast-grep")
        command.extend(["--strictness", strictness])
    command.extend(["--json=stream", "--threads", "2", "--color", "never"])
    _append_ast_options(command, options)
    command.extend(["--", *_paths(options.get("paths"))])
    return command


def _append_ast_options(command: list[str], options: Mapping[str, Any]) -> None:
    if options.get("follow"):
        command.append("--follow")
    if options.get("hidden"):
        command.extend(["--no-ignore", "hidden"])
    no_ignore = options.get("no_ignore")
    if no_ignore:
        values = [no_ignore] if isinstance(no_ignore, str) else no_ignore
        if values is True:
            values = ["dot", "exclude", "global", "parent", "vcs"]
        for value in _strings(values, "no_ignore"):
            if value not in {"hidden", "dot", "exclude", "global", "parent", "vcs"}:
                raise ValueError("no_ignore contains an unsupported ignore class")
            command.extend(["--no-ignore", value])
    for value in _strings(options.get("glob"), "glob", allow_none=True):
        if not value:
            raise ValueError("glob must not be empty")
        command.extend(["--globs", value])


def _inline_rule(value: Any, lang: str, options: Mapping[str, Any]) -> str:
    if not isinstance(value, Mapping):
        raise TypeError("rule must be a bare matcher mapping")
    _validate_bare_matcher(value)
    _reject_ast_mutation(value)
    result = dict(value)
    result.setdefault("id", "mypr-search")
    result.setdefault("language", lang)
    result.setdefault("severity", "hint")
    if "rule" not in result:
        matcher = {key: result.pop(key) for key in list(result) if key in _AST_MATCHER_KEYS}
        if matcher:
            result["rule"] = matcher
    for key in ("constraints", "utils"):
        if key in options and options[key] is not None:
            result[key] = options[key]
    _reject_ast_mutation(result)
    # JSON is valid YAML and avoids adding a second serializer dependency.
    return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


def _reject_ast_mutation(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in _AST_FORBIDDEN:
                raise ValueError(f"AST search does not support rule field {key!r}")
            _reject_ast_mutation(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_ast_mutation(child)


def _validate_bare_matcher(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise TypeError("rule must be a bare matcher mapping")
    if not value:
        raise ValueError("rule must not be empty")
    unknown = [key for key in value if str(key) not in _AST_MATCHER_KEYS]
    if unknown:
        raise ValueError(f"AST search does not support full-config field {unknown[0]!r}")
    _reject_ast_mutation(value)


def parse_ast(record: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one ast-grep JSON match to the workspace search schema."""

    if not isinstance(record, Mapping):
        raise TypeError("AST output record must be an object")
    raw_range = record.get("range")
    normalized_range = _range(raw_range)
    start = normalized_range["start"]
    result: dict[str, Any] = {
        "kind": "match",
        "path": _string_value(record, "file", "path"),
        "line": start["line"],
        "column": start["column"],
        "text": str(record.get("text", "")),
        "range": normalized_range,
    }
    language = record.get("language", record.get("lang"))
    if isinstance(language, str):
        result["language"] = language
    captures = _captures(record.get("metaVariables", record.get("captures")))
    if captures:
        result["captures"] = captures
    metadata = (
        ("ruleId", "rule_id"),
        ("severity", "severity"),
        ("message", "message"),
        ("note", "note"),
    )
    for source, target in metadata:
        if source in record and record[source] is not None:
            result[target] = record[source]
    return result


def _captures(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for group in ("single", "multi"):
        source = value.get(group)
        if not isinstance(source, Mapping):
            continue
        if group == "single":
            target: dict[str, Any] = {}
            for name, item in source.items():
                candidate = _capture(item)
                if candidate is not None:
                    target[str(name)] = candidate
        else:
            target = {}
            for name, items in source.items():
                if not isinstance(items, list):
                    continue
                parsed = [candidate for item in items if (candidate := _capture(item)) is not None]
                target[str(name)] = parsed
        result[group] = target
    return result


def _capture(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    raw_range = value.get("range")
    try:
        normalized = _range(raw_range)
    except TypeError, ValueError:
        return None
    return {"text": str(value.get("text", "")), "range": normalized}


def _range(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("AST match has no range")
    start = _position(value.get("start"), value, "start")
    end = _position(value.get("end"), value, "end")
    return {"start": start, "end": end}


def _position(value: Any, parent: Mapping[str, Any], side: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ValueError("AST match has an invalid range position")
    line = _integer(value.get("line"), "line") + 1
    column = _integer(value.get("column"), "column") + 1
    byte = None
    byte_offsets = parent.get("byteOffset", parent.get("byte_offset"))
    if isinstance(byte_offsets, Mapping):
        byte = byte_offsets.get(side)
    if byte is None:
        byte = value.get("byte", value.get("byteOffset"))
        if isinstance(byte, Mapping):
            byte = byte.get(side)
    return {"line": line, "column": column, "byte": _integer(byte, "byte offset")}


async def inspect_backends(workspace: Path, runner: Any) -> dict[str, Any]:
    """Inspect optional search executables through the supplied async runner."""

    root = Path(workspace)
    await asyncio.to_thread(_ensure_rga_config, root)
    await asyncio.to_thread(_ensure_ast_config, root)
    result: dict[str, Any] = {"workspace": str(root), "backends": {}}
    rg = await _inspect_one(runner, root, ["rg", "--version"])
    if rg["available"]:
        pcre2 = await _inspect_one(runner, root, ["rg", "--pcre2-version"])
        rg["features"] = {"pcre2": pcre2["available"]}
    rg["binary"] = shutil.which("rg") or "rg"
    result["backends"]["rg"] = _public_backend("rg", rg)

    rga = await _inspect_one(runner, root, ["rga", "--version"])
    if rga["available"]:
        adapters = await _inspect_one(
            runner,
            root,
            [
                "rga",
                "--rga-config-file=" + str(_rga_config(root)),
                "--rga-no-cache",
                "--rga-list-adapters",
            ],
        )
        rga["adapters"] = _parse_adapters(adapters.get("stdout", ""))
        rga["features"] = {
            "accurate": True,
            "cache": True,
            "adapters": rga["adapters"],
        }
        rga["dependencies"] = _adapter_dependencies(rga["adapters"])
    rga["binary"] = shutil.which("rga") or "rga"
    result["backends"]["rga"] = _public_backend("rga", rga)

    ast: dict[str, Any] | None = None
    for executable in ("ast-grep", "sg"):
        candidate = await _inspect_one(runner, root, [executable, "--version"])
        if candidate["available"] and (executable == "ast-grep" or _is_ast_grep(candidate)):
            candidate["binary"] = executable
            candidate["binary_path"] = shutil.which(executable) or executable
            ast = candidate
            break
    if ast is None:
        ast = {"available": False, "binary": None, "stdout": "", "stderr": "", "error": None}
    run_help = scan_help = {}
    if ast.get("available"):
        run_help = await _inspect_one(runner, root, [ast["binary"], "run", "--help"])
        scan_help = await _inspect_one(runner, root, [ast["binary"], "scan", "--help"])
    run_flags = run_help.get("stdout", "")
    scan_flags = scan_help.get("stdout", "")
    ast["features"] = {
        "json_stream": bool(
            run_help.get("available") and "--json" in run_flags and "stream" in run_flags
        ),
        "inline_rules": bool(scan_help.get("available") and "--inline-rules" in scan_flags),
    }
    result["backends"]["ast"] = _public_backend("ast", ast)
    return result


async def _inspect_one(runner: Any, workspace: Path, command: list[str]) -> dict[str, Any]:
    try:
        if hasattr(runner, "run"):
            result = runner.run(
                command,
                cwd=workspace,
                timeout=2,
                check=False,
                max_bytes=65536,
            )
        else:
            result = runner(command, cwd=workspace, timeout=2, max_bytes=65536)
        if inspect.isawaitable(result):
            result = await result
    except Exception as exc:
        return {"available": False, "stdout": "", "stderr": "", "error": str(exc)[:256]}
    if isinstance(result, Mapping):
        stdout = str(result.get("stdout", ""))
        stderr = str(result.get("stderr", ""))
        code = result.get("returncode", result.get("code", 0))
        available = not result.get("error") and not result.get("timed_out") and code == 0
        return {
            "available": bool(available),
            "stdout": stdout,
            "stderr": stderr,
            "error": result.get("error"),
        }
    if isinstance(result, tuple) and len(result) >= 2:
        code, stdout = result[:2]
        stderr = result[2] if len(result) > 2 else ""
        return {"available": code == 0, "stdout": str(stdout), "stderr": str(stderr), "error": None}
    return {"available": False, "stdout": "", "stderr": "", "error": "invalid runner result"}


def _public_backend(name: str, value: Mapping[str, Any]) -> dict[str, Any]:
    result = {"name": name, "available": bool(value.get("available")), "version": _version(value)}
    if value.get("binary"):
        result["binary"] = value["binary"]
    if value.get("binary_path"):
        result["binary_path"] = value["binary_path"]
    if "features" in value:
        result["features"] = value["features"]
    if "adapters" in value:
        result["adapters"] = value["adapters"]
    if "dependencies" in value:
        result["dependencies"] = value["dependencies"]
    if not result["available"] and value.get("error"):
        result["error"] = str(value["error"])[:256]
    return result


def _version(value: Mapping[str, Any]) -> str | None:
    text = str(value.get("stdout", "")).strip()
    return text.splitlines()[0][:256] if text else None


def _is_ast_grep(value: Mapping[str, Any]) -> bool:
    text = (str(value.get("stdout", "")) + " " + str(value.get("stderr", ""))).lower()
    return "ast-grep" in text or "ast_grep" in text or "astgrep" in text


def _verify_sg(executable: str) -> None:
    path = shutil.which(executable) if Path(executable).name == executable else executable
    if not path:
        raise RuntimeError("sg is not installed")
    try:
        result = subprocess.run(
            [path, "--version"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("unable to verify sg as ast-grep") from exc
    text = (result.stdout + " " + result.stderr).lower()
    if result.returncode != 0 or not _is_ast_grep({"stdout": text}):
        raise ValueError("sg is not a verified ast-grep executable")


def _ensure_rga_config(workspace: Path) -> None:
    _write_once(_rga_config(workspace), "{}\n")


def _ensure_ast_config(workspace: Path) -> None:
    _write_once(_ast_config(workspace), "ruleDirs: []\n")


def _write_once(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            pass
    finally:
        temporary_path.unlink(missing_ok=True)


def _adapter_dependencies(adapters: list[str]) -> dict[str, dict[str, Any]]:
    requirements = {
        "pandoc": ["pandoc"],
        "poppler": ["pdftotext"],
        "ffmpeg": ["ffmpeg"],
    }
    return {
        adapter: {
            "required": requirements.get(adapter, []),
            "available": {
                executable: bool(shutil.which(executable))
                for executable in requirements.get(adapter, [])
            },
        }
        for adapter in adapters
    }


def _parse_adapters(value: Any) -> list[str]:
    adapters: list[str] = []
    for line in str(value).splitlines():
        line = line.strip()
        match = re.fullmatch(r"-\s+\*\*([^*]+)\*\*", line)
        if match:
            name = match.group(1).strip()
        elif re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", line):
            name = line
        else:
            continue
        if name not in adapters and name.lower() != "adapters":
            adapters.append(name)
    return adapters


def _rga_config(workspace: Path) -> Path:
    return workspace / ".mypr" / "searches" / "config" / "rga.json"


def _rga_cache(workspace: Path) -> Path:
    return workspace / ".mypr" / "searches" / "rga-cache"


def _ast_config(workspace: Path) -> Path:
    return workspace / ".mypr" / "searches" / "config" / "sgconfig.yml"


def _paths(value: Any) -> list[str]:
    if value is None:
        return ["."]
    values = _strings(value, "paths")
    if not values or any(not item for item in values):
        raise ValueError("paths must contain non-empty strings")
    return values


def _strings(value: Any, name: str, *, allow_none: bool = False) -> list[str]:
    if value is None and allow_none:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
        return list(value)
    raise ValueError(f"{name} must be a string or list of strings")


def _mode(options: Mapping[str, Any]) -> str:
    mode = options.get("mode", "matches")
    if mode not in _MODES:
        raise ValueError("mode must be matches, files, counts, or exists")
    return str(mode)


def _archive_depth(options: Mapping[str, Any]) -> int:
    value = options.get("archive_depth", 5)
    return _nonnegative_int(value, "archive_depth")


def _nonnegative_int(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _integer(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _string_value(record: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value:
            return value
    raise ValueError("AST match has no file path")


__all__ = ["build", "inspect_backends", "parse_ast"]
