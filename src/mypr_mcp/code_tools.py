"""Read-only code navigation through explicitly configured language servers."""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import os
import re
import secrets
import signal
import sys
import tempfile
from collections import OrderedDict
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .async_utils import finish_owned, wait_owned
from .lsp_config import LSPConfig, validate_servers
from .lsp_edits import (
    EditError,
    EditPlanStore,
    PlannedOperation,
    _safe_path,
    apply_text_edits,
    sha256,
)
from .snapshots import SnapshotStore

MAX_SERVERS = 4
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
MAX_OPEN_DOCUMENTS = 128
MAX_MESSAGE_BYTES = 8 * 1024 * 1024
MAX_RESULT_BYTES = 1024 * 1024
MAX_RESULTS = 500
MAX_DIAGNOSTICS = 256
MAX_HOVER_CHARS = 64 * 1024
MAX_STDERR_BYTES = 16 * 1024
MAX_SYMBOL_DEPTH = 32
MAX_SYMBOL_TEXT = 1024
MAX_HIERARCHY_ITEMS = 64
DEFAULT_RESULT_BYTES = 32 * 1024
CALL_CANCEL_GRACE = 0.1
COORDINATE_SYSTEM = "one_based_unicode_code_points"
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_LANGUAGE_IDS = {
    ".c": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".cxx": "cpp",
    ".h": "cpp",
    ".hh": "cpp",
    ".hpp": "cpp",
    ".hxx": "cpp",
    ".go": "go",
    ".js": "javascript",
    ".jsx": "javascriptreact",
    ".py": "python",
    ".pyi": "python",
    ".rs": "rust",
    ".ts": "typescript",
    ".tsx": "typescriptreact",
}


class CodeError(RuntimeError):
    """A language server or code navigation operation failed."""


@dataclass(slots=True)
class _Document:
    path: Path
    uri: str
    language: str
    text: str
    version: int


def _position(text: str, line: Any, character: Any, encoding: str) -> dict[str, int]:
    if type(line) is not int or line < 1:
        raise ValueError("line must be a positive one-based integer")
    if type(character) is not int or character < 1:
        raise ValueError("character must be a positive one-based integer")
    lines = _source_lines(text)
    if line > len(lines):
        raise ValueError("line is outside the document")
    else:
        value = lines[line - 1]
    column = character - 1
    if column > len(value):
        raise ValueError("character is outside the line")
    prefix = value[:column]
    if encoding == "utf-32":
        units = len(prefix)
    elif encoding == "utf-8":
        units = len(prefix.encode("utf-8"))
    else:
        units = len(prefix.encode("utf-16-le")) // 2
    return {"line": line - 1, "character": units}


def _user_character(line: str, offset: Any, encoding: str) -> int:
    if type(offset) is not int or offset < 0:
        return 1
    if encoding == "utf-32":
        return min(offset, len(line)) + 1
    if encoding == "utf-8":
        units = 0
        chars = 0
        for char in line:
            if units >= offset:
                break
            units += len(char.encode("utf-8"))
            chars += 1
        return chars + 1
    units = 0
    chars = 0
    for char in line:
        if units >= offset:
            break
        units += len(char.encode("utf-16-le")) // 2
        chars += 1
    return chars + 1


def _range(value: Any, text: str, encoding: str) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    lines = _source_lines(text)
    result: dict[str, Any] = {}
    for side in ("start", "end"):
        pos = value.get(side)
        if not isinstance(pos, dict) or type(pos.get("line")) is not int:
            return None
        index = max(0, pos["line"])
        source = lines[index] if index < len(lines) else ""
        result[side] = {
            "line": index + 1,
            "character": _user_character(source, pos.get("character"), encoding),
        }
    return result


def _coordinate_range(value: Any, text: str | None, encoding: str) -> tuple[Any, str]:
    if value is None:
        return None, "not_provided"
    if text is None:
        return None, "source_unavailable"
    converted = _range(value, text, encoding)
    return converted, "converted" if converted is not None else "invalid"


def _result_limit(value: Any) -> int:
    if type(value) is not int or not 512 <= value <= MAX_RESULT_BYTES:
        raise ValueError(f"max_bytes must be an integer between 512 and {MAX_RESULT_BYTES}")
    return value


def _uri_path(uri: Any) -> Path | None:
    if not isinstance(uri, str):
        return None
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        return None
    return Path(unquote(parsed.path))


def _source_lines(text: str) -> list[str]:
    return re.split(r"\r\n|\r|\n", text)


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _clean_diagnostic(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    raw_range = value.get("range")
    if not isinstance(raw_range, dict):
        return None
    bounds = {}
    for side in ("start", "end"):
        point = raw_range.get(side)
        if (
            not isinstance(point, dict)
            or type(point.get("line")) is not int
            or point["line"] < 0
            or type(point.get("character")) is not int
            or point["character"] < 0
        ):
            return None
        bounds[side] = {"line": point["line"], "character": point["character"]}
    result: dict[str, Any] = {
        "range": bounds,
        "message": str(value.get("message", ""))[:1024],
    }
    severity = value.get("severity")
    if type(severity) is int and 1 <= severity <= 4:
        result["severity"] = severity
    code = value.get("code")
    if isinstance(code, str):
        result["code"] = code[:128]
    elif type(code) is int:
        result["code"] = code
    source = value.get("source")
    if isinstance(source, str):
        result["source"] = source[:128]
    return result


def _supports(value: Any) -> bool:
    return value is True or isinstance(value, dict)


class _LanguageServer:
    def __init__(
        self,
        root: Path,
        name: str,
        command: tuple[str, ...],
        languages: frozenset[str],
        timeout: float,  # noqa: ASYNC109
    ) -> None:
        self.root = root
        self.name = name
        self.command = command
        self.languages = languages
        self.timeout = timeout
        self.process: asyncio.subprocess.Process | None = None
        self.capabilities: dict[str, Any] = {}
        self.position_encoding = "utf-16"
        self.documents: OrderedDict[str, _Document] = OrderedDict()
        self.diagnostics_cache: dict[str, dict[str, Any]] = {}
        self._diag_generation: dict[str, int] = {}
        self._diag_events: dict[str, asyncio.Event] = {}
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._next_id = 0
        self._write_lock = asyncio.Lock()
        self._operation_lock = asyncio.Lock()
        self._closed = False
        self._closing = False
        self._failure: str | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._stderr = bytearray()
        self._stderr_truncated = False
        self.initialized = False
        self.generation = secrets.token_hex(16)

    async def start(self) -> None:
        client_pid = os.getpid()
        guard = Path(__file__).with_name("process_guard.py")
        guarded_command = [
            sys.executable,
            "-I",
            str(guard),
            "--parent-pid",
            str(client_pid),
            "--tree",
            "--",
            *self.command,
        ]
        try:
            self.process = await asyncio.create_subprocess_exec(
                *guarded_command,
                cwd=self.root,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=MAX_MESSAGE_BYTES + 1,
                start_new_session=True,
            )
        except (OSError, ValueError) as exc:
            raise CodeError(f"unable to start language server {self.name!r}: {exc}") from exc
        self._reader_task = asyncio.create_task(
            self._read_loop(), name=f"mypr:lsp:{self.name}:read"
        )
        self._stderr_task = asyncio.create_task(
            self._drain_stderr(), name=f"mypr:lsp:{self.name}:stderr"
        )
        root_uri = self.root.as_uri()
        try:
            result = await self._request(
                "initialize",
                {
                    "processId": client_pid,
                    "clientInfo": {"name": "mypr-mcp", "version": "1"},
                    "rootUri": root_uri,
                    "workspaceFolders": [{"uri": root_uri, "name": self.root.name}],
                    "capabilities": {
                        "general": {"positionEncodings": ["utf-8", "utf-16"]},
                        "workspace": {
                            "configuration": True,
                            "workspaceFolders": True,
                            "applyEdit": False,
                        },
                        "textDocument": {
                            "synchronization": {"dynamicRegistration": False},
                            "publishDiagnostics": {"relatedInformation": True},
                        },
                    },
                    "trace": "off",
                },
                timeout=self.timeout,
            )
            if not isinstance(result, dict):
                raise CodeError("language server returned an invalid initialize result")
            caps = result.get("capabilities")
            if not isinstance(caps, dict):
                raise CodeError("language server returned no capabilities")
            encoding = caps.get("positionEncoding", "utf-16")
            if encoding not in {"utf-8", "utf-16"}:
                raise CodeError(
                    f"language server selected unsupported position encoding {encoding!r}"
                )
            self.capabilities = caps
            await self._notify("initialized", {})
            self.initialized = True
        except BaseException:
            await self.aclose()
            raise

    async def _drain_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        while chunk := await self.process.stderr.read(4096):
            self._stderr.extend(chunk)
            excess = len(self._stderr) - MAX_STDERR_BYTES
            if excess > 0:
                del self._stderr[:excess]
                self._stderr_truncated = True

    async def _read_message(self) -> dict[str, Any] | None:
        assert self.process is not None and self.process.stdout is not None
        headers: dict[str, str] = {}
        total = 0
        while True:
            line = await self.process.stdout.readline()
            if not line:
                if total == 0:
                    return None
                raise CodeError("language server closed during an LSP header")
            total += len(line)
            if total > 8192:
                raise CodeError("language server sent an oversized LSP header")
            if line in (b"\r\n", b"\n"):
                break
            try:
                key, value = line.decode("ascii").split(":", 1)
            except (UnicodeError, ValueError) as exc:
                raise CodeError("language server sent a malformed LSP header") from exc
            headers[key.strip().lower()] = value.strip()
        try:
            size = int(headers["content-length"])
        except (KeyError, ValueError) as exc:
            raise CodeError("language server omitted a valid Content-Length") from exc
        if not 0 <= size <= MAX_MESSAGE_BYTES:
            raise CodeError(f"language server message exceeds {MAX_MESSAGE_BYTES} bytes")
        body = await self.process.stdout.readexactly(size)
        try:
            message = json.loads(body)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CodeError("language server sent invalid JSON") from exc
        if not isinstance(message, dict):
            raise CodeError("language server sent a non-object JSON-RPC message")
        return message

    async def _read_loop(self) -> None:
        try:
            while True:
                message = await self._read_message()
                if message is None:
                    break
                if message.get("jsonrpc") != "2.0":
                    raise CodeError("language server sent an invalid JSON-RPC version")
                if "id" in message and type(message["id"]) not in (int, str):
                    raise CodeError("language server sent an invalid JSON-RPC ID")
                if "params" in message and not isinstance(message["params"], (dict, list)):
                    raise CodeError("language server sent invalid JSON-RPC parameters")
                if "method" in message:
                    if not isinstance(message["method"], str):
                        raise CodeError("language server sent an invalid JSON-RPC method")
                    await self._handle_method(message)
                elif "id" in message and ("result" in message) != ("error" in message):
                    if type(message["id"]) not in (int, str):
                        raise CodeError("language server sent an invalid JSON-RPC response ID")
                    future = self._pending.pop(message["id"], None)
                    if future is not None and not future.done():
                        if "error" in message:
                            error = message["error"]
                            detail = (
                                error.get("message", "request failed")
                                if isinstance(error, dict)
                                else str(error)
                            )
                            future.set_exception(CodeError(f"LSP {detail}"[:2048]))
                        else:
                            future.set_result(message.get("result"))
                else:
                    raise CodeError("language server sent a malformed JSON-RPC message")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closing:
                self._failure = str(exc).strip()[:1024] or type(exc).__name__
                self._fail_pending(CodeError(self._failure))
        finally:
            if not self._closing:
                code = self.process.returncode if self.process is not None else None
                self._failure = self._failure or f"language server output closed (status {code})"
                self._fail_pending(CodeError(self._failure))
                self._terminate_group(signal.SIGTERM)
                if self.process is not None and self.process.returncode is None:
                    with suppress(Exception):
                        async with asyncio.timeout(1):
                            await self.process.wait()
                for _ in range(10):
                    if not self._group_exists():
                        break
                    await asyncio.sleep(0.05)
                if self._group_exists():
                    self._terminate_group(signal.SIGKILL)
                if self.process is not None and self.process.returncode is None:
                    with suppress(Exception):
                        await self.process.wait()

    def _terminate_group(self, sig: signal.Signals) -> None:
        if self.process is None:
            return
        if os.name == "posix":
            with suppress(ProcessLookupError, PermissionError):
                os.killpg(self.process.pid, sig)
        elif self.process.returncode is None:
            with suppress(ProcessLookupError):
                if sig == signal.SIGKILL:
                    self.process.kill()
                else:
                    self.process.terminate()

    def _group_exists(self) -> bool:
        if self.process is None or os.name != "posix":
            return False
        try:
            os.killpg(self.process.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _fail_pending(self, error: Exception) -> None:
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(error)

    async def _handle_method(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params", {})
        if method == "textDocument/publishDiagnostics" and isinstance(params, dict):
            uri = params.get("uri")
            if isinstance(uri, str) and uri in self.documents:
                current = self.documents[uri]
                version = params.get("version")
                if version is not None and (type(version) is not int or version > current.version):
                    return
                previous = self.diagnostics_cache.get(uri)
                previous_version = previous.get("version") if previous is not None else None
                if (
                    version is not None
                    and type(previous_version) is int
                    and version < previous_version
                ):
                    return
                if (
                    version is None
                    and type(previous_version) is int
                    and previous_version == current.version
                ):
                    return
                values = params.get("diagnostics")
                if not isinstance(values, list):
                    return
                diagnostics = []
                dropped = False
                for item in values[:MAX_DIAGNOSTICS] if isinstance(values, list) else []:
                    clean = _clean_diagnostic(item)
                    if clean is not None:
                        diagnostics.append(clean)
                    else:
                        dropped = True
                snapshot = {
                    "uri": uri,
                    "version": version,
                    "diagnostics": diagnostics,
                    "truncated": len(values) > MAX_DIAGNOSTICS or dropped,
                }
                self.diagnostics_cache[uri] = snapshot
                self._diag_generation[uri] = self._diag_generation.get(uri, 0) + 1
                self._diag_events.setdefault(uri, asyncio.Event()).set()
            return
        if "id" not in message:
            return
        if method == "workspace/configuration":
            items = params.get("items", []) if isinstance(params, dict) else []
            result = [None] * len(items) if isinstance(items, list) else []
        elif method == "workspace/workspaceFolders":
            result = [{"uri": self.root.as_uri(), "name": self.root.name}]
        elif method in {
            "client/registerCapability",
            "client/unregisterCapability",
            "window/workDoneProgress/create",
            "window/showMessageRequest",
            "workspace/semanticTokens/refresh",
            "workspace/diagnostic/refresh",
        }:
            result = [] if method.endswith("/refresh") else None
        elif method == "workspace/applyEdit":
            result = {"applied": False, "failureReason": "mypr code navigation is read-only"}
        else:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {"code": -32601, "message": f"unsupported client request: {method}"},
                }
            )
            return
        await self._send({"jsonrpc": "2.0", "id": message["id"], "result": result})

    async def _send(self, message: dict[str, Any]) -> None:
        if (
            self.process is None
            or self.process.stdin is None
            or self.process.returncode is not None
        ):
            raise CodeError(self._failure or "language server is not running")
        try:
            body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise CodeError(f"LSP message is not JSON serializable: {exc}") from exc
        if len(body) > MAX_MESSAGE_BYTES:
            raise CodeError("LSP request exceeds the message size limit")
        frame = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body
        try:
            async with asyncio.timeout(self.timeout):
                async with self._write_lock:
                    self.process.stdin.write(frame)
                    await self.process.stdin.drain()
        except TimeoutError as exc:
            raise CodeError("language server stopped accepting LSP messages") from exc
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise CodeError(self._failure or "language server closed its input") from exc

    async def _notify(
        self,
        method: str,
        params: dict[str, Any],
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:  # noqa: ASYNC109
        if timeout is None:
            await self._send({"jsonrpc": "2.0", "method": method, "params": params})
        else:
            async with asyncio.timeout(timeout):
                await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        cancel_timeout: float | None = 1,
    ) -> Any:  # noqa: ASYNC109
        if self._failure:
            raise CodeError(self._failure)
        self._next_id += 1
        ident = self._next_id
        future = asyncio.get_running_loop().create_future()
        self._pending[ident] = future
        try:
            async with asyncio.timeout(self.timeout if timeout is None else timeout):
                await self._send(
                    {"jsonrpc": "2.0", "id": ident, "method": method, "params": params or {}}
                )
                return await future
        except TimeoutError as exc:
            if cancel_timeout is not None:
                with suppress(CodeError, TimeoutError):
                    await self._notify("$/cancelRequest", {"id": ident}, timeout=cancel_timeout)
            raise CodeError(f"LSP request {method} timed out") from exc
        except asyncio.CancelledError:
            if cancel_timeout is not None:
                with suppress(CodeError, TimeoutError):
                    await self._notify("$/cancelRequest", {"id": ident}, timeout=cancel_timeout)
            raise
        finally:
            if self._pending.get(ident) is future:
                self._pending.pop(ident, None)
            if not future.done():
                future.cancel()

    def _path(self, value: str | os.PathLike[str]) -> Path:
        if not isinstance(value, (str, os.PathLike)):
            raise TypeError("path must be a string or path-like object")
        supplied = Path(value).expanduser()
        path = (supplied if supplied.is_absolute() else self.root / supplied).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("code path must stay inside the workspace") from exc
        if not path.is_file():
            raise FileNotFoundError(path)
        return path

    @staticmethod
    def _read_file(path: Path) -> str:
        with path.open("rb") as stream:
            raw = stream.read(MAX_DOCUMENT_BYTES + 1)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValueError(f"document exceeds {MAX_DOCUMENT_BYTES} bytes")
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("code navigation requires UTF-8 source files") from exc

    async def _document(
        self, path_value: str | os.PathLike[str], language: str | None = None
    ) -> tuple[_Document, int]:
        path = self._path(path_value)
        text = await asyncio.to_thread(self._read_file, path)
        selected = language or _LANGUAGE_IDS.get(path.suffix.lower())
        if selected is None and len(self.languages) == 1:
            selected = next(iter(self.languages))
        if selected not in self.languages:
            raise ValueError(
                f"language {selected!r} is not configured for server {self.name!r}; "
                f"configured: {', '.join(sorted(self.languages))}"
            )
        uri = path.as_uri()
        current = self.documents.get(uri)
        generation = self._diag_generation.get(uri, 0)
        if current is None:
            current = _Document(path, uri, selected, text, 1)
            self.documents[uri] = current
            await self._evict_documents(exclude=uri)
            await self._notify(
                "textDocument/didOpen",
                {
                    "textDocument": {
                        "uri": uri,
                        "languageId": selected,
                        "version": current.version,
                        "text": text,
                    }
                },
            )
        elif current.text != text or current.language != selected:
            current.version += 1
            current.text = text
            current.language = selected
            await self._notify(
                "textDocument/didChange",
                {
                    "textDocument": {"uri": uri, "version": current.version},
                    "contentChanges": [{"text": text}],
                },
            )
        self.documents.move_to_end(uri)
        return current, generation

    async def _evict_documents(self, *, exclude: str) -> None:
        while len(self.documents) > MAX_OPEN_DOCUMENTS:
            uri, document = next(iter(self.documents.items()))
            if uri == exclude:
                self.documents.move_to_end(uri)
                uri, document = next(iter(self.documents.items()))
            self.documents.pop(uri)
            self.diagnostics_cache.pop(uri, None)
            self._diag_events.pop(uri, None)
            self._diag_generation.pop(uri, None)
            await self._notify("textDocument/didClose", {"textDocument": {"uri": document.uri}})

    def _require(self, capability: str) -> None:
        if not self.initialized:
            raise CodeError("language server is not initialized")
        if not _supports(self.capabilities.get(capability)):
            raise CodeError(f"language server {self.name!r} does not support {capability}")

    async def _location(self, value: Any, text_cache: dict[str, str]) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        uri = value.get("uri", value.get("targetUri"))
        raw_range = value.get("range", value.get("targetSelectionRange", value.get("targetRange")))
        path = _uri_path(uri)
        if path is None:
            return None
        doc = self.documents.get(str(uri))
        if doc is not None:
            text = doc.text
        elif str(uri) in text_cache:
            text = text_cache[str(uri)]
        elif len(text_cache) < 16:
            try:
                text = await asyncio.to_thread(self._read_file, path)
                text_cache[str(uri)] = text
            except OSError, ValueError:
                text = None
        else:
            text = None
        return {
            "path": str(path),
            "range": _range(raw_range, text, self.position_encoding) if text is not None else None,
        }

    def _workspace_uri_path(self, uri: Any) -> Path | None:
        path = _uri_path(uri)
        if path is None:
            return None
        try:
            path.resolve().relative_to(self.root)
        except OSError, ValueError:
            return None
        return path

    async def _text_for_uri(self, uri: str, cache: dict[str, str]) -> str | None:
        document = self.documents.get(uri)
        if document is not None:
            return document.text
        if uri in cache:
            return cache[uri]
        path = self._workspace_uri_path(uri)
        if path is None or len(cache) >= 16:
            return None
        try:
            text = await asyncio.to_thread(self._read_file, path)
        except OSError, ValueError:
            return None
        cache[uri] = text
        return text

    def _document_provenance(self, uri: str, text: str | None) -> dict[str, Any]:
        document = self.documents.get(uri)
        return {
            "document_version": document.version if document is not None else None,
            "coordinate_source": (
                "open_document"
                if document is not None
                else "disk"
                if text is not None
                else "unavailable"
            ),
        }

    async def _symbol_location(
        self, uri: Any, raw_range: Any, cache: dict[str, str]
    ) -> dict[str, Any] | None:
        path = self._workspace_uri_path(uri)
        if path is None:
            return None
        text = await self._text_for_uri(str(uri), cache)
        converted, status = _coordinate_range(raw_range, text, self.position_encoding)
        return {
            "path": str(path),
            "range": converted,
            "range_status": status,
            **self._document_provenance(str(uri), text),
        }

    async def _clean_symbol(
        self,
        value: Any,
        text: str,
        document_uri: str,
        cache: dict[str, str],
        budget: list[int],
        omitted: list[bool],
        depth: int = 0,
    ) -> dict[str, Any] | None:
        if not isinstance(value, dict) or not isinstance(value.get("name"), str):
            omitted[0] = True
            return None
        kind = value.get("kind")
        if (
            type(kind) is not int
            or not 1 <= kind <= 26
            or budget[0] <= 0
            or depth >= MAX_SYMBOL_DEPTH
        ):
            omitted[0] = True
            return None
        budget[0] -= 1
        raw_range = value.get("range")
        selection_range = value.get("selectionRange")
        location = value.get("location")
        if isinstance(location, dict):
            if raw_range is None:
                raw_range = location.get("range")
            uri = location.get("uri")
        else:
            uri = None
        symbol_uri = str(uri) if isinstance(uri, str) else document_uri
        symbol_text = await self._text_for_uri(symbol_uri, cache)
        converted_range, range_status = _coordinate_range(
            raw_range, symbol_text, self.position_encoding
        )
        converted_selection, selection_status = _coordinate_range(
            selection_range if selection_range is not None else raw_range,
            symbol_text,
            self.position_encoding,
        )
        result: dict[str, Any] = {
            "name": value["name"][:MAX_SYMBOL_TEXT],
            "kind": kind,
            "range": converted_range,
            "range_status": range_status,
            "selection_range": converted_selection,
            "selection_range_status": selection_status,
            **self._document_provenance(symbol_uri, symbol_text),
        }
        if isinstance(value.get("detail"), str):
            result["detail"] = value["detail"][:MAX_SYMBOL_TEXT]
        if isinstance(value.get("containerName"), str):
            result["container"] = value["containerName"][:MAX_SYMBOL_TEXT]
        if isinstance(uri, str):
            item_location = await self._symbol_location(uri, raw_range, cache)
            if item_location is not None:
                result["location"] = item_location
        children = value.get("children")
        if children is not None:
            if not isinstance(children, list):
                omitted[0] = True
                result["children"] = []
            else:
                clean_children = []
                for child in children:
                    clean = await self._clean_symbol(
                        child, text, document_uri, cache, budget, omitted, depth + 1
                    )
                    if clean is not None:
                        clean_children.append(clean)
                result["children"] = clean_children
        return result

    async def document_symbols(
        self,
        path: str | os.PathLike[str],
        *,
        language: str | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        """Return a bounded symbol tree for a workspace document."""
        max_bytes = _result_limit(max_bytes)
        self._require("documentSymbolProvider")
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            raw = await self._request(
                "textDocument/documentSymbol", {"textDocument": {"uri": doc.uri}}
            )
            if raw is None:
                raw = []
            if not isinstance(raw, list):
                raise CodeError("language server returned malformed document symbols")
            cache: dict[str, str] = {doc.uri: doc.text}
            budget = [MAX_RESULTS]
            omitted = [False]
            result = {
                "path": str(doc.path),
                "document_version": doc.version,
                "coordinate_system": COORDINATE_SYSTEM,
                "symbols": [],
                "truncated": False,
            }
            truncated = len(raw) > MAX_RESULTS
            for item in raw[:MAX_RESULTS]:
                clean = await self._clean_symbol(
                    item, doc.text, doc.uri, cache, budget, omitted
                )
                if clean is None:
                    truncated = True
                    continue
                result["symbols"].append(clean)
                if _json_size(result) > max_bytes:
                    result["symbols"].pop()
                    truncated = True
                    break
            if omitted[0]:
                truncated = True
            result["truncated"] = truncated
            return result

    async def workspace_symbols(
        self, query: str = "", *, max_bytes: int = DEFAULT_RESULT_BYTES
    ) -> dict[str, Any]:
        """Search workspace symbols and return bounded file locations."""
        max_bytes = _result_limit(max_bytes)
        if not isinstance(query, str) or len(query) > 4096 or "\x00" in query:
            raise ValueError("query must be a string of at most 4096 characters")
        self._require("workspaceSymbolProvider")
        async with self._operation_lock:
            raw = await self._request("workspace/symbol", {"query": query})
            if raw is None:
                raw = []
            if not isinstance(raw, list):
                raise CodeError("language server returned malformed workspace symbols")
            cache: dict[str, str] = {}
            result = {
                "query": query,
                "coordinate_system": COORDINATE_SYSTEM,
                "symbols": [],
                "truncated": False,
            }
            truncated = len(raw) > MAX_RESULTS
            for item in raw[:MAX_RESULTS]:
                if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                    truncated = True
                    continue
                kind = item.get("kind")
                location = item.get("location")
                if type(kind) is not int or not 1 <= kind <= 26 or not isinstance(location, dict):
                    truncated = True
                    continue
                clean_location = await self._symbol_location(
                    location.get("uri"), location.get("range"), cache
                )
                if clean_location is None:
                    truncated = True
                    continue
                symbol = {"name": item["name"][:MAX_SYMBOL_TEXT], "kind": kind, **clean_location}
                if isinstance(item.get("containerName"), str):
                    symbol["container"] = item["containerName"][:MAX_SYMBOL_TEXT]
                result["symbols"].append(symbol)
                if _json_size(result) > max_bytes:
                    result["symbols"].pop()
                    truncated = True
                    break
            result["truncated"] = truncated
            return result

    async def _clean_call_item(self, value: Any, cache: dict[str, str]) -> dict[str, Any] | None:
        if not isinstance(value, dict) or not isinstance(value.get("name"), str):
            return None
        kind = value.get("kind")
        uri = value.get("uri")
        path = self._workspace_uri_path(uri)
        if type(kind) is not int or not 1 <= kind <= 26 or path is None:
            return None
        text = await self._text_for_uri(str(uri), cache)
        converted_range, range_status = _coordinate_range(
            value.get("range"), text, self.position_encoding
        )
        converted_selection, selection_status = _coordinate_range(
            value.get("selectionRange"), text, self.position_encoding
        )
        result: dict[str, Any] = {
            "name": value["name"][:MAX_SYMBOL_TEXT],
            "kind": kind,
            "path": str(path),
            "range": converted_range,
            "range_status": range_status,
            "selection_range": converted_selection,
            "selection_range_status": selection_status,
            **self._document_provenance(str(uri), text),
        }
        if isinstance(value.get("detail"), str):
            result["detail"] = value["detail"][:MAX_SYMBOL_TEXT]
        return result

    async def calls(
        self,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        direction: str,
        language: str | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        """Return one-hop incoming or outgoing calls for symbols at a position."""
        if direction not in ("incoming", "outgoing"):
            raise ValueError("direction must be 'incoming' or 'outgoing'")
        max_bytes = _result_limit(max_bytes)
        self._require("callHierarchyProvider")
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            pos = _position(doc.text, line, character, self.position_encoding)
            deadline = asyncio.get_running_loop().time() + self.timeout
            try:
                prepared = await self._request(
                    "textDocument/prepareCallHierarchy",
                    {"textDocument": {"uri": doc.uri}, "position": pos},
                    timeout=self.timeout,
                    cancel_timeout=CALL_CANCEL_GRACE,
                )
            except CodeError as exc:
                if (
                    str(exc) == "LSP request textDocument/prepareCallHierarchy timed out"
                    and asyncio.get_running_loop().time() >= deadline
                ):
                    return {
                        "direction": direction,
                        "path": str(doc.path),
                        "document_version": doc.version,
                        "coordinate_system": COORDINATE_SYSTEM,
                        "groups": [],
                        "ambiguous": False,
                        "truncated": True,
                    }
                raise
            if prepared is None:
                prepared = []
            if not isinstance(prepared, list):
                raise CodeError("language server returned malformed call hierarchy items")
            candidates = []
            seen = set()
            valid_count = 0
            for item in prepared[:MAX_HIERARCHY_ITEMS]:
                clean = await self._clean_call_item(item, {doc.uri: doc.text})
                if clean is None:
                    continue
                valid_count += 1
                identity = (
                    clean["path"],
                    json.dumps(clean["selection_range"], sort_keys=True),
                    clean["name"],
                    clean["kind"],
                )
                if identity not in seen:
                    candidates.append((identity, item, clean))
                    seen.add(identity)
            truncated = len(prepared) > MAX_HIERARCHY_ITEMS or valid_count != len(prepared)
            result = {
                "direction": direction,
                "path": str(doc.path),
                "document_version": doc.version,
                "coordinate_system": COORDINATE_SYSTEM,
                "groups": [],
                "ambiguous": len(candidates) > 1,
                "truncated": truncated,
            }
            method = (
                "callHierarchy/incomingCalls"
                if direction == "incoming"
                else "callHierarchy/outgoingCalls"
            )
            call_budget = MAX_RESULTS
            for _, item, clean_item in candidates:
                if call_budget <= 0:
                    truncated = True
                    break
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    truncated = True
                    break
                try:
                    raw_calls = await self._request(
                        method,
                        {"item": item},
                        timeout=remaining,
                        cancel_timeout=CALL_CANCEL_GRACE,
                    )
                except CodeError as exc:
                    if (
                        str(exc) == f"LSP request {method} timed out"
                        and asyncio.get_running_loop().time() >= deadline
                    ):
                        truncated = True
                        break
                    raise
                if raw_calls is None:
                    raw_calls = []
                if not isinstance(raw_calls, list):
                    raise CodeError("language server returned malformed call hierarchy results")
                calls = []
                cache = {doc.uri: doc.text}
                available = call_budget
                for call in raw_calls[:available]:
                    call_budget -= 1
                    if not isinstance(call, dict):
                        truncated = True
                        continue
                    target = call.get("from" if direction == "incoming" else "to")
                    clean_target = await self._clean_call_item(target, cache)
                    ranges = call.get("fromRanges")
                    if clean_target is None or not isinstance(ranges, list):
                        truncated = True
                        continue
                    range_uri = (
                        target.get("uri")
                        if direction == "incoming"
                        else item.get("uri")
                        if isinstance(item, dict)
                        else None
                    )
                    range_text = (
                        await self._text_for_uri(range_uri, cache)
                        if isinstance(range_uri, str)
                        else None
                    )
                    clean_ranges = []
                    if len(ranges) > MAX_RESULTS:
                        truncated = True
                    for raw_range in ranges[:MAX_RESULTS]:
                        converted, range_status = _coordinate_range(
                            raw_range, range_text, self.position_encoding
                        )
                        clean_ranges.append({"range": converted, "coordinate_status": range_status})
                    calls.append({"item": clean_target, "ranges": clean_ranges})
                    if _json_size(result) + _json_size(calls) > max_bytes:
                        calls.pop()
                        truncated = True
                        break
                if len(raw_calls) > available:
                    truncated = True
                result["groups"].append({"item": clean_item, "calls": calls})
                if _json_size(result) > max_bytes:
                    result["groups"].pop()
                    truncated = True
                    break
            result["truncated"] = truncated
            return result

    async def _locations(self, value: Any) -> tuple[list[dict[str, Any]], bool]:
        if value is None:
            return [], False
        if not isinstance(value, (dict, list)):
            return [], True
        values = value if isinstance(value, list) else [value]
        result = []
        text_cache: dict[str, str] = {}
        truncated = len(values) > MAX_RESULTS
        for location in values[:MAX_RESULTS]:
            item = await self._location(location, text_cache)
            if item is None:
                truncated = True
                continue
            result.append(item)
            if _json_size(result) > MAX_RESULT_BYTES:
                result.pop()
                truncated = True
                break
        return result, truncated

    async def definition(
        self,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Find definitions at one-based source line and Unicode character."""
        self._require("definitionProvider")
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            pos = _position(doc.text, line, character, self.position_encoding)
            result = await self._request(
                "textDocument/definition", {"textDocument": {"uri": doc.uri}, "position": pos}
            )
            locations, truncated = await self._locations(result)
            return {"locations": locations, "truncated": truncated}

    async def references(
        self,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        include_declaration: bool = True,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Return up to 500 references using one-based source coordinates."""
        if type(include_declaration) is not bool:
            raise TypeError("include_declaration must be a boolean")
        self._require("referencesProvider")
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            pos = _position(doc.text, line, character, self.position_encoding)
            result = await self._request(
                "textDocument/references",
                {
                    "textDocument": {"uri": doc.uri},
                    "position": pos,
                    "context": {"includeDeclaration": include_declaration},
                },
            )
            locations, truncated = await self._locations(result)
            return {"locations": locations, "truncated": truncated}

    async def hover(
        self,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        language: str | None = None,
    ) -> dict[str, Any] | None:
        """Return hover text and an optional source range."""
        self._require("hoverProvider")
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            pos = _position(doc.text, line, character, self.position_encoding)
            result = await self._request(
                "textDocument/hover", {"textDocument": {"uri": doc.uri}, "position": pos}
            )
        if result is None:
            return None
        if not isinstance(result, dict):
            raise CodeError("language server returned malformed hover data")
        contents = result.get("contents")
        if isinstance(contents, dict):
            rendered = str(contents.get("value", ""))
        elif isinstance(contents, list):
            rendered = "\n\n".join(
                str(item.get("value", "")) if isinstance(item, dict) else str(item)
                for item in contents
            )
        else:
            rendered = str(contents or "")
        rendered = str(rendered)[:MAX_HOVER_CHARS]
        return {
            "contents": rendered,
            "range": _range(result.get("range"), doc.text, self.position_encoding),
            "truncated": len(str(contents or "")) > MAX_HOVER_CHARS,
        }

    async def diagnostics(
        self,
        path: str | os.PathLike[str],
        *,
        language: str | None = None,
        wait_ms: int = 1500,
    ) -> dict[str, Any]:
        """Return cached or fresh diagnostics; pending means no snapshot arrived yet."""
        if type(wait_ms) is not int or not 0 <= wait_ms <= 30000:
            raise ValueError("wait_ms must be an integer between 0 and 30000")
        async with self._operation_lock:
            doc, generation = await self._document(path, language)
            provider = self.capabilities.get("diagnosticProvider")
            if _supports(provider):
                previous = self.diagnostics_cache.get(doc.uri, {}).get("resultId")
                params: dict[str, Any] = {"textDocument": {"uri": doc.uri}}
                if previous is not None:
                    params["previousResultId"] = previous
                result = await self._request("textDocument/diagnostic", params)
                if isinstance(result, dict) and result.get("kind") == "unchanged":
                    cached = self.diagnostics_cache.get(doc.uri)
                    if cached is None or previous is None:
                        result = await self._request(
                            "textDocument/diagnostic", {"textDocument": {"uri": doc.uri}}
                        )
                    else:
                        result = {
                            "kind": "full",
                            "items": cached.get("diagnostics", []),
                            "resultId": previous,
                            "myprTruncated": cached.get("truncated", False),
                        }
                if not isinstance(result, dict) or result.get("kind") != "full":
                    raise CodeError("language server returned no full diagnostics snapshot")
                values = result.get("items", [])
                if not isinstance(values, list):
                    raise CodeError("language server returned malformed diagnostic items")
                diagnostics = []
                dropped = False
                for item in values[:MAX_DIAGNOSTICS] if isinstance(values, list) else []:
                    clean = _clean_diagnostic(item)
                    if clean is not None:
                        diagnostics.append(clean)
                    else:
                        dropped = True
                snapshot = {
                    "uri": doc.uri,
                    "version": doc.version,
                    "diagnostics": diagnostics,
                    "resultId": result.get("resultId"),
                    "truncated": (
                        len(values) > MAX_DIAGNOSTICS
                        or dropped
                        or bool(result.get("myprTruncated"))
                    ),
                }
                self.diagnostics_cache[doc.uri] = snapshot
            else:
                cached = self.diagnostics_cache.get(doc.uri)
                if cached is not None and cached.get("version") == doc.version:
                    snapshot = cached
                elif wait_ms:
                    event = self._diag_events.setdefault(doc.uri, asyncio.Event())
                    deadline = asyncio.get_running_loop().time() + wait_ms / 1000
                    observed_generation = generation
                    while True:
                        snapshot = self.diagnostics_cache.get(doc.uri)
                        if (
                            snapshot is not None
                            and type(snapshot.get("version")) is int
                            and snapshot.get("version") == doc.version
                        ):
                            break
                        current_generation = self._diag_generation.get(doc.uri, 0)
                        if current_generation != observed_generation:
                            observed_generation = current_generation
                            continue
                        remaining = deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            break
                        event.clear()
                        if self._diag_generation.get(doc.uri, 0) != observed_generation:
                            continue
                        try:
                            async with asyncio.timeout(remaining):
                                await event.wait()
                        except TimeoutError:
                            break
                    snapshot = self.diagnostics_cache.get(doc.uri)
                else:
                    snapshot = cached
        if (
            snapshot is None
            or type(snapshot.get("version")) is not int
            or snapshot.get("version") != doc.version
        ):
            version = snapshot.get("version") if snapshot is not None else None
            state = (
                "pending"
                if snapshot is None
                else "version_unknown"
                if type(version) is not int
                else "stale"
            )
            return {
                "path": str(doc.path),
                "version": doc.version,
                "ready": False,
                "diagnostics": None,
                "state": state,
            }
        values = snapshot.get("diagnostics", [])
        if not isinstance(values, list):
            raise CodeError("language server returned malformed diagnostics")
        output = []
        truncated = bool(snapshot.get("truncated")) or len(values) > MAX_DIAGNOSTICS
        for item in values[:MAX_DIAGNOSTICS]:
            if not isinstance(item, dict):
                continue
            candidate = {
                "range": _range(item.get("range"), doc.text, self.position_encoding),
                "severity": item.get("severity"),
                "code": item.get("code"),
                "source": item.get("source"),
                "message": str(item.get("message", ""))[:2048],
            }
            output.append(candidate)
            if _json_size(output) > MAX_RESULT_BYTES:
                output.pop()
                truncated = True
                break
        return {
            "path": str(doc.path),
            "version": snapshot.get("version", doc.version),
            "ready": True,
            "diagnostics": output,
            "truncated": truncated,
        }

    async def rename(
        self,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        new_name: str,
        *,
        language: str | None = None,
    ) -> tuple[_Document, Any]:
        provider = self.capabilities.get("renameProvider")
        if not _supports(provider):
            raise CodeError(f"language server {self.name!r} does not support renameProvider")
        if (
            not isinstance(new_name, str)
            or not new_name
            or len(new_name) > 1024
            or "\x00" in new_name
        ):
            raise ValueError("new_name must be a non-empty string of at most 1024 characters")
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            position = _position(doc.text, line, character, self.position_encoding)
            result = await self._request(
                "textDocument/rename",
                {
                    "textDocument": {"uri": doc.uri},
                    "position": position,
                    "newName": new_name,
                },
            )
            return doc, result

    async def code_actions(
        self,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        end_line: int | None = None,
        end_character: int | None = None,
        language: str | None = None,
        only: list[str] | None = None,
    ) -> tuple[_Document, list[Any]]:
        provider = self.capabilities.get("codeActionProvider")
        if not _supports(provider):
            raise CodeError(f"language server {self.name!r} does not support codeActionProvider")
        if end_line is None:
            end_line = line
        if end_character is None:
            end_character = character
        async with self._operation_lock:
            doc, _ = await self._document(path, language)
            start = _position(doc.text, line, character, self.position_encoding)
            end = _position(doc.text, end_line, end_character, self.position_encoding)
            context: dict[str, Any] = {"diagnostics": []}
            if only is not None:
                if not isinstance(only, list) or not all(isinstance(item, str) for item in only):
                    raise ValueError("only must be a list of strings")
                context["only"] = only
            result = await self._request(
                "textDocument/codeAction",
                {
                    "textDocument": {"uri": doc.uri},
                    "range": {"start": start, "end": end},
                    "context": context,
                },
            )
            if result is None:
                result = []
            if not isinstance(result, list):
                raise CodeError("language server returned malformed code actions")
            return doc, result

    async def resolve_code_action(self, action: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(action, dict):
            raise ValueError("action must be an object")
        if not _supports(self.capabilities.get("codeActionProvider")):
            raise CodeError(f"language server {self.name!r} does not support codeActionProvider")
        return await self._request("codeAction/resolve", action)

    async def workspace_diagnostics(self, previous_result_ids: list[dict[str, Any]] | None) -> Any:
        provider = self.capabilities.get("diagnosticProvider")
        if not isinstance(provider, dict) or provider.get("workspaceDiagnostics") is not True:
            raise CodeError(
                f"language server {self.name!r} does not support workspace diagnostics"
            )
        params: dict[str, Any] = {"identifier": self.name}
        if previous_result_ids:
            params["previousResultIds"] = previous_result_ids
        async with self._operation_lock:
            return await self._request("workspace/diagnostic", params)

    def status(self) -> dict[str, Any]:
        process = self.process
        return {
            "name": self.name,
            "languages": sorted(self.languages),
            "running": (
                process is not None
                and process.returncode is None
                and not self._closed
                and self._failure is None
            ),
            "initialized": self.initialized,
            "pid": process.pid if process is not None and process.returncode is None else None,
            "documents": len(self.documents),
            "capabilities": [
                feature
                for feature in (
                    "definitionProvider",
                    "referencesProvider",
                    "hoverProvider",
                    "documentSymbolProvider",
                    "workspaceSymbolProvider",
                    "callHierarchyProvider",
                    "diagnosticProvider",
                    "renameProvider",
                    "codeActionProvider",
                )
                if _supports(self.capabilities.get(feature))
            ],
            "error": self._failure
            or (
                f"language server exited (status {process.returncode})"
                if process is not None and process.returncode is not None
                else None
            ),
            "stderr_bytes": len(self._stderr),
            "stderr_truncated": self._stderr_truncated,
        }

    async def aclose(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await wait_owned(self._close_task)

    async def _close(self) -> None:
        if self._closed:
            return
        self._closing = True
        process = self.process
        if process is not None:
            if process.returncode is None and self.initialized:
                with suppress(Exception):
                    await self._request("shutdown", timeout=2)
                with suppress(Exception):
                    await self._notify("exit", {}, timeout=1)
            if process.returncode is None and process.stdin is not None:
                process.stdin.close()
            if process.returncode is None:
                try:
                    async with asyncio.timeout(1):
                        await process.wait()
                except TimeoutError:
                    self._terminate_group(signal.SIGTERM)
                    try:
                        async with asyncio.timeout(1):
                            await process.wait()
                    except TimeoutError:
                        self._terminate_group(signal.SIGKILL)
                        await process.wait()
            self._terminate_group(signal.SIGTERM)
            for _ in range(20):
                if not self._group_exists():
                    break
                await asyncio.sleep(0.05)
            if self._group_exists():
                self._terminate_group(signal.SIGKILL)
        self._closed = True
        self._fail_pending(CodeError("language server closed"))
        for task in (self._reader_task, self._stderr_task):
            if task is not None and task is not asyncio.current_task() and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task is not None),
            return_exceptions=True,
        )


class CodeTools:
    """Manage configured language servers and bounded LSP edit plans."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        fs: Any = None,
        config_rpc: Callable[..., Any] | None = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.fs = fs
        self._config_rpc = config_rpc
        self._config = LSPConfig(self.workspace)
        try:
            self._definitions, self._config_revision = self._config.load()
        except (OSError, UnicodeError, ValueError) as exc:
            self._definitions, self._config_revision = {}, None
            self._config_error = str(exc)
        else:
            self._config_error = None
        self._servers: dict[str, _LanguageServer] = {}
        self._lock = asyncio.Lock()
        self._plans = EditPlanStore(self.workspace)
        self._plan_servers: dict[str, str] = {}
        self._actions: dict[str, tuple[str, dict[str, Any], str, int, str]] = {}
        self._workspace_diag_results: dict[str, dict[str, list[dict[str, Any]]]] = {}
        self._diagnostic_snapshots = SnapshotStore(self.workspace / ".mypr", name="lsp-diagnostics")
        self._closed = False

    @staticmethod
    def _name(name: str) -> str:
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ValueError(
                "server name must be 1-64 letters, numbers, dots, underscores, or dashes"
            )
        return name

    @staticmethod
    def _configuration(
        command: list[str] | tuple[str, ...], languages: list[str] | tuple[str, ...]
    ) -> tuple[tuple[str, ...], frozenset[str]]:
        if not isinstance(command, (list, tuple)) or not command or len(command) > 64:
            raise ValueError("command must be a non-empty list of at most 64 strings")
        if any(
            not isinstance(arg, str) or not arg or "\x00" in arg or len(arg) > 4096
            for arg in command
        ):
            raise ValueError(
                "command arguments must be non-empty strings of at most 4096 characters"
            )
        if not isinstance(languages, (list, tuple)) or not languages or len(languages) > 64:
            raise ValueError("languages must be a non-empty list of language IDs")
        if any(
            not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.+-]{1,64}", value)
            for value in languages
        ):
            raise ValueError(
                "language IDs must be 1-64 letters, numbers, dots, underscores, pluses, or dashes"
            )
        return tuple(command), frozenset(languages)

    async def configure(
        self,
        name: str,
        command: list[str] | tuple[str, ...],
        languages: list[str] | tuple[str, ...],
        *,
        timeout: float = 10,  # noqa: ASYNC109
        persist: bool = True,
    ) -> dict[str, Any]:  # noqa: ASYNC109
        """Start an installed server with ``command=[..., '--stdio']`` and LSP language IDs.

        Repeating the same configuration reuses its process. Reconfiguring a name
        replaces that process after the new server initializes successfully.
        """
        name = self._name(name)
        if type(persist) is not bool:
            raise TypeError("persist must be a boolean")
        command_value, language_values = self._configuration(command, languages)
        if type(timeout) not in (int, float) or not 1 <= timeout <= 60:
            raise ValueError("timeout must be between 1 and 60 seconds")
        if self._closed:
            raise CodeError("code tools are closed")
        async with self._lock:
            existing = self._servers.get(name)
            if (
                existing is not None
                and existing.process is not None
                and existing.process.returncode is None
                and existing._failure is None
            ):
                if existing.command == command_value and existing.languages == language_values:
                    async with existing._operation_lock:
                        cancelled = False
                        if persist:
                            _, cancelled = await finish_owned(
                                self._persist_definition(  # noqa: ASYNC109
                                    name, command_value, language_values, timeout
                                )
                            )
                        existing.timeout = float(timeout)
                    if cancelled:
                        raise asyncio.CancelledError
                    result = existing.status()
                    return result
            if existing is None and len(self._servers) >= MAX_SERVERS:
                raise CodeError(f"at most {MAX_SERVERS} language servers may be configured")
            replacement = _LanguageServer(
                self.workspace, name, command_value, language_values, float(timeout)
            )
            await replacement.start()
            result = replacement.status()
            persist_cancelled = False
            if persist:
                try:
                    _, persist_cancelled = await finish_owned(
                        self._persist_definition(  # noqa: ASYNC109
                            name, command_value, language_values, timeout
                        )
                    )
                except BaseException:
                    with suppress(BaseException):
                        await replacement.aclose()
                    raise
            self._servers[name] = replacement
            close_cancelled = False
            if existing is not None:
                _, close_cancelled = await finish_owned(existing.aclose())
            if persist_cancelled or close_cancelled:
                raise asyncio.CancelledError
            return result

    async def _persist_definition(  # noqa: ASYNC109
        self,
        name: str,
        command: tuple[str, ...],
        languages: frozenset[str],
        timeout: float,  # noqa: ASYNC109
    ) -> None:
        definitions = dict(self._definitions)
        definitions[name] = {
            "command": list(command),
            "languages": sorted(languages),
            "timeout": float(timeout),
        }
        validate_servers(definitions)
        if self._config_rpc is not None:
            result = self._config_rpc(
                "set_lsp",
                definitions,
                expected_revision=self._config_revision,
                expected_servers=self._definitions,
            )
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, dict):
                result = result.get("revision")
            if not isinstance(result, str) or not result:
                raise CodeError("LSP configuration callback returned no revision")
            self._config_revision = result
        else:
            self._config_revision = self._config.save(definitions, self._config_revision)
        self._definitions = definitions

    async def start(
        self,
        name: str,
        command: list[str] | tuple[str, ...],
        languages: list[str] | tuple[str, ...],
        *,
        timeout: float = 10,  # noqa: ASYNC109
        persist: bool = True,
    ) -> dict[str, Any]:  # noqa: ASYNC109
        """Alias for configure; the command must name an already installed server."""
        return await self.configure(name, command, languages, timeout=timeout, persist=persist)

    def _server(self, name: str) -> _LanguageServer:
        name = self._name(name)
        try:
            server = self._servers[name]
        except KeyError as exc:
            raise CodeError(f"language server {name!r} is not configured") from exc
        if server._failure or server.process is None or server.process.returncode is not None:
            raise CodeError(server._failure or f"language server {name!r} exited")
        return server

    async def _get_server(self, name: str) -> _LanguageServer:
        key = self._name(name)
        server = self._servers.get(key)
        if server is None:
            definition = self._definitions.get(key)
            if definition is None:
                if self._config_error:
                    raise CodeError(f"unable to load saved LSP configuration: {self._config_error}")
                raise CodeError(f"language server {key!r} is not configured")
            await self.configure(
                key,
                definition["command"],
                definition["languages"],
                timeout=definition["timeout"],
                persist=False,
            )
        return self._server(key)

    async def definition(
        self,
        name: str,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Find definitions at one-based source line and Unicode character."""
        server = await self._get_server(name)
        return await server.definition(path, line, character, language=language)

    async def references(
        self,
        name: str,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        include_declaration: bool = True,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Find references at one-based source line and Unicode character."""
        server = await self._get_server(name)
        return await server.references(
            path, line, character, include_declaration=include_declaration, language=language
        )

    async def hover(
        self,
        name: str,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        language: str | None = None,
    ) -> dict[str, Any] | None:
        """Return hover documentation at one-based source coordinates."""
        server = await self._get_server(name)
        return await server.hover(path, line, character, language=language)

    async def document_symbols(
        self,
        name: str,
        path: str | os.PathLike[str],
        *,
        language: str | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        """Return the symbol tree for a workspace document."""
        server = await self._get_server(name)
        return await server.document_symbols(
            path, language=language, max_bytes=max_bytes
        )

    async def workspace_symbols(
        self, name: str, query: str = "", *, max_bytes: int = DEFAULT_RESULT_BYTES
    ) -> dict[str, Any]:
        """Search workspace symbols."""
        server = await self._get_server(name)
        return await server.workspace_symbols(query, max_bytes=max_bytes)

    async def calls(
        self,
        name: str,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        direction: str,
        language: str | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        """Return one-hop incoming or outgoing calls for symbols at a position."""
        server = await self._get_server(name)
        return await server.calls(
            path,
            line,
            character,
            direction=direction,
            language=language,
            max_bytes=max_bytes,
        )

    async def diagnostics(
        self,
        name: str,
        path: str | os.PathLike[str],
        *,
        language: str | None = None,
        wait_ms: int = 1500,
    ) -> dict[str, Any]:
        """Read diagnostics for a file; ready is false until a snapshot arrives."""
        server = await self._get_server(name)
        return await server.diagnostics(path, language=language, wait_ms=wait_ms)

    async def _read_edit_bytes(self, path: Path) -> bytes | None:
        def read() -> bytes | None:
            if not path.exists():
                return None
            if path.is_symlink() or not path.is_file():
                raise EditError(f"LSP edit target is not a regular file: {path}")
            return path.read_bytes()

        return await asyncio.to_thread(read)

    def _edit_uri_path(self, uri: Any) -> Path:
        if not isinstance(uri, str):
            raise EditError("LSP workspace edit URI is invalid")
        return _safe_path(self.workspace, uri)

    async def _workspace_edit_plan(
        self,
        server: _LanguageServer,
        edit: Any,
        *,
        title: str,
        unsupported_reason: str | None = None,
    ) -> Any:
        if edit is None:
            raise EditError("language server returned no workspace edit")
        if not isinstance(edit, dict):
            raise EditError("language server returned malformed workspace edit")
        operations: list[PlannedOperation] = []
        states: dict[Path, bytes | None] = {}
        versions: dict[Path, int | None] = {}

        async def state(path: Path) -> bytes | None:
            if path not in states:
                states[path] = await self._read_edit_bytes(path)
            return states[path]

        def expected_version(path: Path, version: Any) -> None:
            if version is None:
                return
            if type(version) is not int or version < 0:
                raise EditError("LSP workspace edit document version is invalid")
            for document in server.documents.values():
                if document.path == path and document.version != version:
                    raise EditError("LSP workspace edit document version is stale")
            versions[path] = version

        async def update(path: Path, edits: Any, version: Any = None) -> None:
            current = await state(path)
            if current is None:
                raise EditError(f"LSP text edit target does not exist: {path}")
            expected_version(path, version)
            try:
                text = current.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise EditError(f"LSP text edit target is not UTF-8: {path}") from exc
            new_text = apply_text_edits(text, edits, server.position_encoding)
            new = new_text.encode("utf-8")
            original = current
            states[path] = new
            for operation in operations:
                if operation.path == path and operation.operation in {"create", "update", "rename"}:
                    operation.new = new
                    return
            operations.append(
                PlannedOperation("update", path, original, new, sha256(original), version=version)
            )

        document_changes = edit.get("documentChanges")
        if document_changes is not None:
            if not isinstance(document_changes, list):
                raise EditError("LSP documentChanges must be a list")
            for change in document_changes:
                if not isinstance(change, dict):
                    raise EditError("LSP document change is malformed")
                kind = change.get("kind")
                if kind in ("create", "rename", "delete"):
                    options = change.get("options")
                    if options is not None and not isinstance(options, dict):
                        raise EditError("LSP document change options are malformed")
                    if options and any(options.get(key) for key in ("ignoreIfExists", "overwrite")):
                        raise EditError("LSP document change overwrite options are unsupported")
                    if kind == "create":
                        path = self._edit_uri_path(change.get("uri"))
                        if await state(path) is not None:
                            raise EditError(f"LSP create target already exists: {path}")
                        states[path] = b""
                        operations.append(PlannedOperation("create", path, None, b"", None))
                    elif kind == "delete":
                        path = self._edit_uri_path(change.get("uri"))
                        old = await state(path)
                        if old is None:
                            raise EditError(f"LSP delete target does not exist: {path}")
                        states[path] = None
                        operations.append(PlannedOperation("delete", path, old, None, sha256(old)))
                    else:
                        source = self._edit_uri_path(change.get("oldUri"))
                        destination = self._edit_uri_path(change.get("newUri"))
                        old = await state(source)
                        if old is None:
                            raise EditError(f"LSP rename source does not exist: {source}")
                        if await state(destination) is not None:
                            raise EditError(f"LSP rename destination already exists: {destination}")
                        states[source] = None
                        states[destination] = old
                        operations.append(
                            PlannedOperation(
                                "rename",
                                destination,
                                None,
                                old,
                                None,
                                source=source,
                                source_old=old,
                                destination_expected=None,
                            )
                        )
                    continue
                text_document = change.get("textDocument")
                if not isinstance(text_document, dict):
                    raise EditError("LSP text document edit is malformed")
                path = self._edit_uri_path(text_document.get("uri"))
                await update(path, change.get("edits"), text_document.get("version"))
        changes = edit.get("changes")
        if changes is not None:
            if not isinstance(changes, dict):
                raise EditError("LSP workspace changes must be an object")
            for uri, edits in changes.items():
                await update(self._edit_uri_path(uri), edits)
        if not operations and unsupported_reason is None:
            raise EditError("language server returned an empty workspace edit")
        plan = await self._plans.acreate(
            self.workspace,
            operations,
            server.generation,
            title,
            unsupported_reason,
            server.name,
        )
        self._plan_servers[plan.ident] = server.name
        return plan

    async def rename(
        self,
        name: str,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        new_name: str,
        *,
        language: str | None = None,
    ) -> dict[str, Any]:
        server = await self._get_server(name)
        document, edit = await server.rename(path, line, character, new_name, language=language)
        plan = await self._workspace_edit_plan(server, edit, title=f"Rename to {new_name}")
        result = plan.result()
        result.update(
            {
                "operation": "rename",
                "path": str(document.path),
                "document_version": document.version,
            }
        )
        return result

    async def actions(
        self,
        name: str,
        path: str | os.PathLike[str],
        line: int,
        character: int,
        *,
        end_line: int | None = None,
        end_character: int | None = None,
        language: str | None = None,
        only: list[str] | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        max_bytes = _result_limit(max_bytes)
        server = await self._get_server(name)
        document, raw_actions = await server.code_actions(
            path,
            line,
            character,
            end_line=end_line,
            end_character=end_character,
            language=language,
            only=only,
        )
        result: dict[str, Any] = {
            "path": str(document.path),
            "document_version": document.version,
            "actions": [],
            "truncated": len(raw_actions) > MAX_RESULTS,
        }
        for raw in raw_actions[:MAX_RESULTS]:
            if not isinstance(raw, dict):
                result["truncated"] = True
                continue
            title = raw.get("title")
            if not isinstance(title, str):
                title = str(raw.get("command", ""))
            action_id = secrets.token_urlsafe(18)
            self._actions[action_id] = (
                server.name,
                copy.deepcopy(raw),
                server.generation,
                document.version,
                document.uri,
            )
            command = raw.get("command")
            if isinstance(command, dict):
                has_command = isinstance(command.get("command"), str)
            else:
                has_command = isinstance(command, str)
            edit_value = raw.get("edit")
            item = {
                "action_id": action_id,
                "title": title[:1024],
                "kind": raw.get("kind") if isinstance(raw.get("kind"), str) else None,
                "has_edit": isinstance(edit_value, dict),
                "has_command": has_command,
                "supported": isinstance(edit_value, dict) and not has_command,
            }
            if isinstance(raw.get("disabled"), dict):
                item["disabled"] = str(raw["disabled"].get("reason", ""))[:1024]
            elif isinstance(raw.get("disabled"), str):
                item["disabled"] = raw["disabled"][:1024]
            result["actions"].append(item)
            if _json_size(result) > max_bytes:
                result["actions"].pop()
                result["truncated"] = True
                break
        return result

    async def prepare_action(self, action_id: str) -> dict[str, Any]:
        if not isinstance(action_id, str) or not action_id:
            raise ValueError("action_id must be a non-empty string")
        try:
            server_name, action, generation, version, _uri = self._actions[action_id]
        except KeyError as exc:
            raise EditError("unknown or expired code action") from exc
        server = await self._get_server(server_name)
        if server.generation != generation:
            raise EditError("code action belongs to an older language-server generation")
        command = action.get("command")
        has_command = isinstance(command, str) or (
            isinstance(command, dict) and isinstance(command.get("command"), str)
        )
        if "edit" not in action and not has_command:
            provider = server.capabilities.get("codeActionProvider")
            if not isinstance(provider, dict) or provider.get("resolveProvider") is not True:
                raise EditError("code action requires unsupported codeAction/resolve")
            action = await server.resolve_code_action(action)
        unsupported = None
        if has_command:
            unsupported = "code actions requiring command execution are unsupported"
        if not isinstance(action.get("edit"), dict):
            if unsupported is None:
                unsupported = "code action did not return a workspace edit"
            edit = {"changes": {}}
        else:
            edit = action["edit"]
        plan = await self._workspace_edit_plan(
            server,
            edit,
            title=str(action.get("title", "Code action"))[:1024],
            unsupported_reason=unsupported,
        )
        result = plan.result()
        result.update({"action_id": action_id, "document_version": version})
        return result

    async def apply_edit(self, plan_id: str) -> dict[str, Any]:
        plan = await self._plans.aget(plan_id)
        server_name = self._plan_servers.get(plan.ident) or plan.server
        if server_name is None:
            raise EditError("LSP edit plan has no language server")
        server = await self._get_server(server_name)
        if server.generation != plan.generation:
            raise EditError("LSP edit plan belongs to an older language-server generation")
        if plan.unsupported_reason is not None:
            raise EditError(plan.unsupported_reason)
        async with server._operation_lock:
            virtual: dict[Path, bytes | None] = {}
            for operation in plan.operations:
                if operation.operation == "rename":
                    assert operation.source is not None
                    if operation.source not in virtual:
                        virtual[operation.source] = await self._read_edit_bytes(operation.source)
                    if operation.path not in virtual:
                        virtual[operation.path] = await self._read_edit_bytes(operation.path)
                    if (
                        virtual[operation.source] != operation.source_old
                        or virtual[operation.path] is not None
                    ):
                        raise EditError("LSP edit plan is stale")
                    virtual[operation.source] = None
                    virtual[operation.path] = operation.new
                    continue
                if operation.path not in virtual:
                    virtual[operation.path] = await self._read_edit_bytes(operation.path)
                if sha256(virtual[operation.path]) != operation.expected:
                    raise EditError("LSP edit plan is stale")
                virtual[operation.path] = operation.new
            result = await self._apply_edit_plan(plan)
        await self._plans.aconsume(plan)
        self._plan_servers.pop(plan.ident, None)
        result.update({"plan_id": plan.ident, "applied": True})
        return result

    async def _apply_edit_plan(self, plan: Any) -> dict[str, Any]:
        for method_name in (
            "_apply_lsp_plan", "apply_lsp_plan", "apply_workspace_edit", "apply_edit_plan"
        ):
            method = getattr(self.fs, method_name, None) if self.fs is not None else None
            if method is not None:
                result = method(plan)
                if inspect.isawaitable(result):
                    result = await result
                if not isinstance(result, dict):
                    raise EditError("filesystem edit coordinator returned an invalid result")
                return result
        if self.fs is not None and all(
            hasattr(self.fs, method)
            for method in ("write_bytes", "delete", "move")
        ):
            changes = []
            for operation in plan.operations:
                try:
                    if operation.operation == "rename":
                        assert operation.source is not None
                        result = self.fs.move(
                            str(operation.source),
                            str(operation.path),
                            expected_hash=sha256(operation.source_old),
                            overwrite=False,
                            history=True,
                        )
                        if inspect.isawaitable(result):
                            result = await result
                        if operation.new != operation.source_old:
                            result = self.fs.write_bytes(
                                str(operation.path),
                                operation.new or b"",
                                expected_hash=sha256(operation.source_old),
                                overwrite=True,
                                create_parents=False,
                                history=True,
                            )
                            if inspect.isawaitable(result):
                                result = await result
                    elif operation.operation == "delete":
                        result = self.fs.delete(
                            str(operation.path), expected_hash=operation.expected, history=True
                        )
                        if inspect.isawaitable(result):
                            result = await result
                    else:
                        result = self.fs.write_bytes(
                            str(operation.path),
                            operation.new or b"",
                            expected_hash=operation.expected,
                            overwrite=operation.old is not None,
                            create_parents=True,
                            history=True,
                        )
                        if inspect.isawaitable(result):
                            result = await result
                    changes.append(result)
                except Exception as exc:
                    raise EditError(f"filesystem edit failed: {exc}") from exc
            return {"changes": changes}
        backups: dict[Path, bytes | None] = {}
        touched = {operation.path for operation in plan.operations}
        touched.update(
            operation.source
            for operation in plan.operations
            if operation.source is not None
        )
        for path in touched:
            backups[path] = await self._read_edit_bytes(path)
        try:
            for operation in plan.operations:
                if operation.operation == "rename":
                    assert operation.source is not None
                    operation.path.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(operation.source, operation.path)
                elif operation.operation == "delete":
                    operation.path.unlink()
                else:
                    operation.path.parent.mkdir(parents=True, exist_ok=True)
                    fd, temporary = tempfile.mkstemp(
                        prefix=f".{operation.path.name}.", dir=operation.path.parent
                    )
                    try:
                        with os.fdopen(fd, "wb") as stream:
                            stream.write(operation.new or b"")
                            stream.flush()
                            os.fsync(stream.fileno())
                        os.replace(temporary, operation.path)
                    finally:
                        Path(temporary).unlink(missing_ok=True)  # noqa: ASYNC240
        except BaseException:
            for path, data in backups.items():
                if data is None:
                    path.unlink(missing_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(data)
            raise
        return plan.result()

    async def workspace_diagnostics(
        self,
        name: str,
        *,
        cursor: str | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        """Read the server's workspace diagnostic report as a bounded snapshot."""
        max_bytes = _result_limit(max_bytes)
        server = await self._get_server(name)
        if cursor is not None:
            snapshot, offset = self._diagnostic_snapshots.decode(
                cursor, expected_kind="workspace-diagnostic"
            )
            query = snapshot.get("query")
            if not isinstance(query, dict) or query.get("server") != server.name:
                raise ValueError("cursor belongs to a different language server")
        else:
            previous = []
            for uri, report in self._workspace_diag_results.get(server.name, {}).items():
                result_id = report.get("result_id")
                if result_id is not None:
                    previous.append({"uri": uri, "value": result_id})
            raw = await server.workspace_diagnostics(previous or None)
            if not isinstance(raw, dict):
                raise CodeError("language server returned malformed workspace diagnostics")
            reports = raw.get("items", [])
            if not isinstance(reports, list):
                raise CodeError("language server returned malformed workspace diagnostic items")
            previous_reports = self._workspace_diag_results.setdefault(server.name, {})
            items: list[dict[str, Any]] = []
            for report in reports:
                if not isinstance(report, dict) or not isinstance(report.get("uri"), str):
                    continue
                uri = report["uri"]
                path = server._workspace_uri_path(uri)
                if path is None:
                    continue
                kind = report.get("kind", "full")
                if kind == "unchanged":
                    result_id = report.get("resultId")
                    cached = previous_reports.get(uri)
                    if cached is None:
                        continue
                    cleaned = copy.deepcopy(cached)
                    cleaned["kind"] = "unchanged"
                    cleaned["result_id"] = result_id
                    items.append(cleaned)
                    continue
                if kind != "full" or not isinstance(report.get("items", []), list):
                    continue
                text = await server._text_for_uri(uri, {})
                diagnostics = []
                dropped = False
                values = report.get("items", [])
                for value in values[:MAX_DIAGNOSTICS]:
                    clean = _clean_diagnostic(value)
                    if clean is None:
                        dropped = True
                        continue
                    clean["range"] = _range(clean["range"], text, server.position_encoding)
                    diagnostics.append(clean)
                item = {
                    "uri": uri,
                    "path": str(path),
                    "kind": "full",
                    "version": report.get("version"),
                    "result_id": report.get("resultId"),
                    "diagnostics": diagnostics,
                    "truncated": len(values) > MAX_DIAGNOSTICS or dropped,
                }
                previous_reports[uri] = copy.deepcopy(item)
                items.append(item)
            ident = self._diagnostic_snapshots.create(
                {"server": server.name}, items, kind="workspace-diagnostic"
            )
            snapshot = self._diagnostic_snapshots.load(ident)
            offset = 0
        items = snapshot.get("items")
        if not isinstance(items, list):
            raise CodeError("invalid workspace diagnostic snapshot")
        page_items: list[Any] = []
        truncated = False
        index = offset
        while index < len(items):
            item = items[index]
            candidate = page_items + [item]
            page = {
                "server": name,
                "snapshot_id": snapshot["id"],
                "reports": candidate,
                "complete": False,
                "truncated": truncated,
            }
            if _json_size(page) > max_bytes:
                if not page_items:
                    reduced = dict(item)
                    diagnostics = reduced.get("diagnostics")
                    if isinstance(diagnostics, list):
                        reduced["diagnostics"] = []
                    reduced["truncated"] = True
                    if _json_size({**page, "reports": [reduced]}) <= max_bytes:
                        page_items.append(reduced)
                        index += 1
                    else:
                        truncated = True
                break
            page_items.append(item)
            index += 1
        has_more = index < len(items)
        next_cursor = (
            self._diagnostic_snapshots.cursor(snapshot["id"], index, "workspace-diagnostic")
            if has_more
            else None
        )
        output = {
            "server": name,
            "snapshot_id": snapshot["id"],
            "reports": page_items,
            "complete": not has_more,
            "truncated": truncated,
            "next_cursor": next_cursor,
        }
        while page_items and _json_size(output) > max_bytes:
            page_items.pop()
            index -= 1
            has_more = True
            next_cursor = self._diagnostic_snapshots.cursor(
                snapshot["id"], index, "workspace-diagnostic"
            )
            output["reports"] = page_items
            output["complete"] = False
            output["next_cursor"] = next_cursor
            output["truncated"] = True
        if _json_size(output) > max_bytes:
            raise EditError("workspace diagnostic metadata exceeds max_bytes")
        return output

    def status(self, name: str | None = None) -> dict[str, Any]:
        """Show configured LSP processes and their supported features."""
        if name is not None:
            key = self._name(name)
            try:
                return self._servers[key].status()
            except KeyError as exc:
                definition = self._definitions.get(key)
                if definition is None:
                    raise CodeError(f"language server {key!r} is not configured") from exc
                return {
                    "name": key,
                    "languages": list(definition["languages"]),
                    "running": False,
                    "initialized": False,
                    "pid": None,
                    "documents": 0,
                    "capabilities": [],
                    "saved": True,
                    "error": self._config_error,
                }
        names = set(self._definitions) | set(self._servers)
        servers = []
        for key in sorted(names):
            if key in self._servers:
                servers.append(self._servers[key].status())
            else:
                servers.append(self.status(key))
        return {"servers": servers, "config_error": self._config_error}

    async def reload(self) -> dict[str, Any]:
        """Reload saved definitions without starting stopped language servers."""
        if self._config_rpc is not None:
            loaded = self._config_rpc("get_lsp")
            if inspect.isawaitable(loaded):
                loaded = await loaded
            if not isinstance(loaded, dict):
                raise CodeError("LSP configuration callback returned an invalid snapshot")
            definitions = validate_servers(loaded.get("servers", {}))
            revision = loaded.get("revision")
            if revision is not None and not isinstance(revision, str):
                raise CodeError("LSP configuration callback returned an invalid revision")
        else:
            definitions, revision = self._config.load()
        async with self._lock:
            changed = {
                name
                for name in set(self._definitions) | set(definitions)
                if self._definitions.get(name) != definitions.get(name)
            }
            targets = [
                (name, self._servers.pop(name))
                for name in changed
                if name in self._servers
            ]
            self._definitions = definitions
            self._config_revision = revision
            self._config_error = None
            await asyncio.gather(*(server.aclose() for _, server in targets))
        return {"changed": sorted(changed), "servers": self.status()["servers"]}

    async def remove(self, name: str, *, persist: bool = True) -> dict[str, Any]:
        """Stop and optionally remove a saved language-server definition."""
        key = self._name(name)
        if type(persist) is not bool:
            raise TypeError("persist must be a boolean")
        async with self._lock:
            server = self._servers.get(key)
            exists = key in self._definitions or server is not None
            if persist and key in self._definitions:
                definitions = dict(self._definitions)
                definitions.pop(key)
                if self._config_rpc is not None:
                    result = self._config_rpc(
                        "set_lsp",
                        definitions,
                        expected_revision=self._config_revision,
                        expected_servers=self._definitions,
                    )
                    if inspect.isawaitable(result):
                        result = await result
                    if isinstance(result, dict):
                        result = result.get("revision")
                    if not isinstance(result, str) or not result:
                        raise CodeError("LSP configuration callback returned no revision")
                    self._config_revision = result
                else:
                    self._config_revision = self._config.save(
                        definitions, self._config_revision
                    )
                self._definitions = definitions
            if server is not None:
                self._servers.pop(key, None)
                await server.aclose()
            removed = exists if persist else server is not None
        if not exists:
            raise CodeError(f"language server {key!r} is not configured")
        return {
            "name": key,
            "removed": removed,
            "running": False,
            "saved": key in self._definitions,
        }

    async def close(self, name: str | None = None) -> None:
        """Stop one running server or all servers while keeping saved definitions."""
        async with self._lock:
            if name is None:
                targets = list(self._servers.items())
                self._servers.clear()
            else:
                key = self._name(name)
                server = self._servers.pop(key, None)
                targets = [(key, server)] if server is not None else []
            await asyncio.gather(
                *(server.aclose() for _, server in targets if server is not None),
                return_exceptions=False,
            )

    async def aclose(self) -> None:
        """Stop all configured language servers during workspace reset or shutdown."""
        if self._closed:
            return
        self._closed = True
        await self.close()


__all__ = ["CodeError", "CodeTools"]
