"""Bounded image and PDF operations isolated from the workspace kernel."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import math
import os
import signal
import sys
from pathlib import Path
from typing import Any

from .async_utils import finish_owned, wait_owned
from .document_tools import DocumentExtractor

_WORKER = Path(__file__).with_name("media_worker.py")
_GUARD = Path(__file__).with_name("process_guard.py")
_WORKERS = asyncio.Semaphore(2)
_MAX_INPUT_BYTES = 64 * 1024 * 1024
_MAX_OUTPUT_BYTES = 2 * 1024 * 1024
_MAX_WORKER_OUTPUT = 8 * 1024 * 1024
_TIMEOUT = 15
_CLEANUP_TIMEOUT = 2


class MediaToolError(RuntimeError):
    """A bounded media operation failed in its worker process."""


def _kill_worker(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        try:
            process.terminate()
        except (AttributeError, ProcessLookupError):
            with contextlib.suppress(ProcessLookupError):
                process.kill()


def _kill_worker_force(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with contextlib.suppress(AttributeError, ProcessLookupError, PermissionError):
        os.killpg(process.pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        process.kill()


async def _cleanup_worker(
    process: asyncio.subprocess.Process, communication: asyncio.Task | None
) -> None:
    if process.stdin is not None and not process.stdin.is_closing():
        process.stdin.close()
    _kill_worker(process)
    try:
        await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT)
    except TimeoutError:
        _kill_worker_force(process)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT)
    if communication is None:
        return
    if communication.done():
        await asyncio.gather(communication, return_exceptions=True)
    else:
        transport = getattr(process, "_transport", None)
        stdout_transport = transport.get_pipe_transport(1) if transport is not None else None
        if stdout_transport is not None:
            stdout_transport.close()
        communication.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(communication, return_exceptions=True), _CLEANUP_TIMEOUT
            )
        except TimeoutError:
            pass
    if process.returncode is None:
        _kill_worker_force(process)
        try:
            await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT)
        except TimeoutError:
            pass


async def _cleanup_worker_uncancellable(
    process: asyncio.subprocess.Process, communication: asyncio.Task | None
) -> None:
    await wait_owned(_cleanup_worker(process, communication), propagate=False)


async def inspect_image(path: Path, display: str, *, max_input_bytes: int) -> dict[str, Any]:
    _validate_limit("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
    return await _call("image_info", path, display, {"max_input_bytes": max_input_bytes})


async def transform_image(
    path: Path,
    display: str,
    *,
    max_output_bytes: int,
    max_input_bytes: int,
    resize: tuple[int, int] | None,
    crop: tuple[int, int, int, int] | None,
) -> tuple[bytes, dict[str, Any]]:
    _validate_limit("max_output_bytes", max_output_bytes, _MAX_OUTPUT_BYTES)
    _validate_limit("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
    resize_value = _validate_box("resize", resize, 2, max_pixels=16_000_000)
    crop_value = _validate_box("crop", crop, 4)
    result = await _call(
        "image_transform",
        path,
        display,
        {
            "max_input_bytes": max_input_bytes,
            "max_output_bytes": max_output_bytes,
            "resize": resize_value,
            "crop": crop_value,
        },
    )
    try:
        return base64.b64decode(result.pop("data"), validate=True), result
    except (KeyError, ValueError) as exc:
        raise MediaToolError("The media worker returned invalid image data") from exc


def display_image(data: bytes, image_format: str, metadata: dict[str, Any], *, alt: str):
    """Build an IPython image whose text representation carries compact provenance."""
    from IPython.display import Image

    class _MediaImage(Image):
        def _repr_mimebundle_(self, include=None, exclude=None):
            bundle, mime_metadata = super()._repr_mimebundle_(include, exclude)
            if (include is None or "text/plain" in include) and (
                exclude is None or "text/plain" not in exclude
            ):
                bundle["text/plain"] = _image_summary(metadata)
            return bundle, mime_metadata

    return _MediaImage(
        data=data,
        format=image_format.lower(),
        embed=True,
        alt=alt,
        metadata={"mypr": metadata},
    )


def _image_summary(metadata: dict[str, Any]) -> str:
    parts = [str(metadata.get("path", "image"))]
    page = metadata.get("page")
    if page is not None:
        parts.append(f"page {page}")
    width, height = metadata.get("width"), metadata.get("height")
    if width and height:
        parts.append(f"{width}×{height}px")
    original_width = metadata.get("original_width")
    original_height = metadata.get("original_height")
    if original_width and original_height:
        parts.append(f"source {original_width}×{original_height}px")
    if metadata.get("bounds") is not None:
        parts.append(f"bounds {metadata['bounds']}pt")
    if metadata.get("clip") is not None:
        parts.append(f"clip {metadata['clip']}pt")
    if metadata.get("crop") is not None:
        parts.append(f"crop {metadata['crop']}px")
    if metadata.get("resize") is not None:
        parts.append(f"fit {metadata['resize']}px")
    if metadata.get("dpi") is not None:
        parts.append(f"{metadata['dpi']}dpi")
    if metadata.get("revision"):
        parts.append(f"sha256 {metadata['revision'][:12]}")
    return "Image: " + "; ".join(parts)


class Documents:
    """Workspace PDF operations backed by a short-lived constrained worker."""

    def __init__(self, filesystem: Any) -> None:
        self._filesystem = filesystem
        self._extractor = DocumentExtractor(filesystem)

    async def ocr(
        self,
        path: str | os.PathLike[str],
        *,
        language: str = "eng",
        start_page: int = 1,
        max_pages: int = 5,
        dpi: int = 200,
        cursor: str | None = None,
        resume_cursor: str | None = None,
        max_bytes: int = 32_768,
        max_input_bytes: int = _MAX_INPUT_BYTES,
    ) -> dict[str, Any]:
        """OCR a PDF, PNG, or JPEG and return paged text with word coordinates."""
        return await self._extractor.ocr(
            path,
            language=language,
            start_page=start_page,
            max_pages=max_pages,
            dpi=dpi,
            cursor=cursor,
            resume_cursor=resume_cursor,
            max_bytes=max_bytes,
            max_input_bytes=max_input_bytes,
        )

    async def extract(
        self,
        path: str | os.PathLike[str],
        *,
        cursor: str | None = None,
        max_bytes: int = 32_768,
        max_input_bytes: int = _MAX_INPUT_BYTES,
        cached_values: bool = False,
    ) -> dict[str, Any]:
        """Extract structured text from DOCX, PPTX, or XLSX."""
        return await self._extractor.extract(
            path,
            cursor=cursor,
            max_bytes=max_bytes,
            max_input_bytes=max_input_bytes,
            cached_values=cached_values,
        )

    async def backends(self) -> dict[str, Any]:
        """Report OCR and Office extraction dependencies and Tesseract languages."""
        return await self._extractor.backends()

    async def info(
        self,
        path: str | os.PathLike[str],
        *,
        page: int | None = None,
        max_input_bytes: int = _MAX_INPUT_BYTES,
    ) -> dict[str, Any]:
        """Inspect a PDF. Returns source path, SHA-256 revision, size, page count,
        metadata, and optional page bounds and rotation (coordinates are PDF points).
        """
        _validate_limit("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
        if page is not None:
            _validate_positive_int("page", page)
        resolved, display = self._filesystem._path(path)
        return await _call(
            "pdf_info",
            resolved,
            display,
            {"page": page, "max_input_bytes": max_input_bytes},
        )

    async def read(
        self,
        path: str | os.PathLike[str],
        *,
        start_page: int = 1,
        max_pages: int = 5,
        max_chars: int = 20_000,
        cursor: str | None = None,
        max_input_bytes: int = _MAX_INPUT_BYTES,
    ) -> dict[str, Any]:
        """Extract bounded text from consecutive pages. Returns source path and SHA-256
        revision plus pages with 1-based page numbers, text, truncation, rotation, and
        page bounds in PDF points. Pass ``next_cursor`` back as ``cursor`` to continue;
        the cursor is bound to the source revision and resumes at the exact text offset.
        """
        _validate_positive_int("start_page", start_page)
        _validate_limit("max_pages", max_pages, 10)
        _validate_limit("max_chars", max_chars, 64_000)
        _validate_limit("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
        if cursor is not None and (
            not isinstance(cursor, str) or len(cursor) > 2048 or start_page != 1
        ):
            raise ValueError(
                "cursor must be at most 2048 characters and cannot be combined with start_page"
            )
        resolved, display = self._filesystem._path(path)
        return await _call(
            "pdf_read",
            resolved,
            display,
            {
                "start_page": start_page,
                "max_pages": max_pages,
                "max_chars": max_chars,
                "cursor": cursor,
                "max_input_bytes": max_input_bytes,
            },
        )

    async def render_page(
        self,
        path: str | os.PathLike[str],
        page: int,
        *,
        dpi: int = 120,
        clip: tuple[float, float, float, float] | None = None,
        max_bytes: int = _MAX_OUTPUT_BYTES,
        max_input_bytes: int = _MAX_INPUT_BYTES,
    ):
        """Render one PDF page as an inline PNG. Metadata includes source path, SHA-256
        revision, 1-based page, page bounds, requested clip (PDF points), actual DPI,
        and rendered pixel dimensions. Large pages are scaled to the worker pixel limit.
        """
        _validate_positive_int("page", page)
        _validate_limit("dpi", dpi, 300)
        if dpi < 36:
            raise ValueError("dpi must be between 36 and 300")
        _validate_limit("max_bytes", max_bytes, _MAX_OUTPUT_BYTES)
        _validate_limit("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
        clip_value = _validate_clip(clip)
        resolved, display = self._filesystem._path(path)
        result = await _call(
            "pdf_render",
            resolved,
            display,
            {
                "page": page,
                "dpi": dpi,
                "clip": clip_value,
                "max_bytes": max_bytes,
                "max_input_bytes": max_input_bytes,
            },
        )
        try:
            data = base64.b64decode(result.pop("data"), validate=True)
        except (KeyError, ValueError) as exc:
            raise MediaToolError("The media worker returned invalid page image data") from exc
        return display_image(
            data=data,
            image_format="PNG",
            metadata=result,
            alt=f"{display}, page {page}",
        )


async def _call(
    operation: str, path: Path, display: str, options: dict[str, Any]
) -> dict[str, Any]:
    request = json.dumps(
        {"operation": operation, "path": str(path), "display": display, **options},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(request) > 64 * 1024:
        raise ValueError("Media request exceeds its size limit")
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        in {
            "PATH",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
            "SYSTEMROOT",
            "WINDIR",
            "TEMP",
            "TMP",
            "TMPDIR",
        }
    }
    async with _WORKERS:
        command = [sys.executable, "-I", str(_WORKER)]
        if sys.platform == "linux":
            command = [
                sys.executable,
                str(_GUARD),
                "--parent-pid",
                str(os.getpid()),
                "--tree",
                "--",
                *command,
            ]
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
                start_new_session=sys.platform != "win32",
            )
        )
        process, cancelled = await finish_owned(launch)
        if cancelled:
            await _cleanup_worker_uncancellable(process, None)
            raise asyncio.CancelledError

        async def communicate_bounded() -> tuple[bytes, bool]:
            async def read_stdout() -> tuple[bytes, bool]:
                chunks = []
                size = 0
                exceeded = False
                while chunk := await process.stdout.read(64 * 1024):
                    if exceeded:
                        continue
                    remaining = _MAX_WORKER_OUTPUT - size
                    if len(chunk) > remaining:
                        if remaining > 0:
                            chunks.append(chunk[:remaining])
                        exceeded = True
                        _kill_worker(process)
                        continue
                    chunks.append(chunk)
                    size += len(chunk)
                return b"".join(chunks), exceeded

            reader = asyncio.create_task(read_stdout())
            try:
                process.stdin.write(request)
                await process.stdin.drain()
                process.stdin.close()
                stdout, exceeded = await reader
                await process.wait()
                return stdout, exceeded
            except BaseException:
                if not reader.done():
                    reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                raise

        communication = asyncio.create_task(communicate_bounded())
        try:
            stdout, exceeded = await asyncio.wait_for(asyncio.shield(communication), _TIMEOUT)
        except TimeoutError:
            await _cleanup_worker_uncancellable(process, communication)
            raise MediaToolError("Media operation exceeded its 15-second time limit") from None
        except BaseException:
            await _cleanup_worker_uncancellable(process, communication)
            raise
    if exceeded:
        raise MediaToolError("Media worker response exceeded its size limit")
    if process.returncode != 0:
        raise MediaToolError(
            "Media worker exited with status "
            f"{process.returncode}; the file may be malformed or exceed resource limits"
        )
    try:
        result = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MediaToolError("Media worker returned an invalid response") from exc
    if not isinstance(result, dict) or result.get("ok") is not True:
        if not isinstance(result, dict):
            raise MediaToolError("Media worker returned an invalid response")
        kind = result.get("kind")
        message = result.get("error", "Media operation failed")
        if kind == "MissingDependency":
            raise ImportError(message)
        if kind == "FileNotFoundError":
            raise FileNotFoundError(message)
        if kind == "PermissionError":
            raise PermissionError(message)
        if kind == "IsADirectoryError":
            raise IsADirectoryError(message)
        if kind == "ValueError":
            raise ValueError(message)
        raise MediaToolError(message)
    return result["result"]


def _validate_limit(name: str, value: int, maximum: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")


def _validate_positive_int(name: str, value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _validate_box(
    name: str,
    value: tuple[int, ...] | None,
    length: int,
    *,
    max_pixels: int = 0,
) -> list[int] | None:
    if value is None:
        return None
    if (
        not isinstance(value, (tuple, list))
        or len(value) != length
        or any(not isinstance(part, int) or isinstance(part, bool) for part in value)
    ):
        raise ValueError(f"{name} must contain {length} integers")
    result = list(value)
    if length == 2:
        if any(part < 1 or part > 8192 for part in result):
            raise ValueError(f"{name} dimensions must be between 1 and 8192")
        if max_pixels and result[0] * result[1] > max_pixels:
            raise ValueError(f"{name} exceeds the {max_pixels}-pixel output limit")
    else:
        left, top, right, bottom = result
        if min(left, top) < 0 or right <= left or bottom <= top:
            raise ValueError(f"{name} must be a non-empty pixel rectangle")
    return result


def _validate_clip(
    clip: tuple[float, float, float, float] | None,
) -> list[float] | None:
    if clip is None:
        return None
    if not isinstance(clip, (tuple, list)) or len(clip) != 4:
        raise ValueError("clip must contain four coordinates in PDF points")
    if any(
        not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value)
        for value in clip
    ):
        raise ValueError("clip coordinates must be finite numbers")
    left, top, right, bottom = map(float, clip)
    if right <= left or bottom <= top:
        raise ValueError("clip must be a non-empty rectangle")
    return [left, top, right, bottom]
