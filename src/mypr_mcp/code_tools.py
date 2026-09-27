"""Read-only code navigation through explicitly configured language servers."""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

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
    units = len(prefix) if encoding == "utf-32" else len(prefix.encode("utf-16-le")) // 2
    return {"line": line - 1, "character": units}


def _user_character(line: str, offset: Any, encoding: str) -> int:
    if type(offset) is not int or offset < 0:
        return 1
    if encoding == "utf-32":
        return min(offset, len(line)) + 1
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
        timeout: float,
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

    async def start(self) -> None:
        try:
            self.process = await asyncio.create_subprocess_exec(
                *self.command,
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
                    "processId": os.getpid(),
                    "clientInfo": {"name": "mypr-mcp", "version": "1"},
                    "rootUri": root_uri,
                    "workspaceFolders": [{"uri": root_uri, "name": self.root.name}],
                    "capabilities": {
                        "general": {"positionEncodings": ["utf-16"]},
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
            if encoding != "utf-16":
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
        task = self._close_task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.shield(task)
            raise

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
    """Manage a bounded set of explicitly configured read-only LSP clients."""

    def __init__(self, workspace: str | os.PathLike[str]) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self._servers: dict[str, _LanguageServer] = {}
        self._lock = asyncio.Lock()
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
    ) -> dict[str, Any]:  # noqa: ASYNC109
        """Start an installed server with ``command=[..., '--stdio']`` and LSP language IDs.

        Repeating the same configuration reuses its process. Reconfiguring a name
        replaces that process after the new server initializes successfully.
        """
        name = self._name(name)
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
                        existing.timeout = float(timeout)
                    return existing.status()
            if existing is None and len(self._servers) >= MAX_SERVERS:
                raise CodeError(f"at most {MAX_SERVERS} language servers may be configured")
            replacement = _LanguageServer(
                self.workspace, name, command_value, language_values, float(timeout)
            )
            await replacement.start()
            self._servers[name] = replacement
            if existing is not None:
                await existing.aclose()
            return replacement.status()

    async def start(
        self,
        name: str,
        command: list[str] | tuple[str, ...],
        languages: list[str] | tuple[str, ...],
        *,
        timeout: float = 10,  # noqa: ASYNC109
    ) -> dict[str, Any]:  # noqa: ASYNC109
        """Alias for configure; the command must name an already installed server."""
        return await self.configure(name, command, languages, timeout=timeout)

    def _server(self, name: str) -> _LanguageServer:
        name = self._name(name)
        try:
            server = self._servers[name]
        except KeyError as exc:
            raise CodeError(f"language server {name!r} is not configured") from exc
        if server._failure or server.process is None or server.process.returncode is not None:
            raise CodeError(server._failure or f"language server {name!r} exited")
        return server

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
        return await self._server(name).definition(path, line, character, language=language)

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
        return await self._server(name).references(
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
        return await self._server(name).hover(path, line, character, language=language)

    async def document_symbols(
        self,
        name: str,
        path: str | os.PathLike[str],
        *,
        language: str | None = None,
        max_bytes: int = DEFAULT_RESULT_BYTES,
    ) -> dict[str, Any]:
        """Return the symbol tree for a workspace document."""
        return await self._server(name).document_symbols(
            path, language=language, max_bytes=max_bytes
        )

    async def workspace_symbols(
        self, name: str, query: str = "", *, max_bytes: int = DEFAULT_RESULT_BYTES
    ) -> dict[str, Any]:
        """Search workspace symbols."""
        return await self._server(name).workspace_symbols(query, max_bytes=max_bytes)

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
        return await self._server(name).calls(
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
        return await self._server(name).diagnostics(path, language=language, wait_ms=wait_ms)

    def status(self, name: str | None = None) -> dict[str, Any]:
        """Show configured LSP processes and their supported features."""
        if name is not None:
            key = self._name(name)
            try:
                return self._servers[key].status()
            except KeyError as exc:
                raise CodeError(f"language server {key!r} is not configured") from exc
        return {"servers": [server.status() for server in self._servers.values()]}

    async def close(self, name: str | None = None) -> None:
        """Stop one configured server or all servers."""
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
