"""Short-lived media worker. Its JSON protocol is private to media_tools."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import stat
import sys
import warnings

_MAX_INPUT_BYTES = 64 * 1024 * 1024
_MAX_IMAGE_PIXELS = 40_000_000
_MAX_RENDER_PIXELS = 4_000_000
_MAX_OUTPUT_BYTES = 2 * 1024 * 1024


class _Failure(Exception):
    def __init__(self, kind: str, message: str) -> None:
        self.kind = kind
        super().__init__(message)


class _OutputLimit(Exception):
    pass


class _BoundedBytesIO(io.BytesIO):
    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit

    def write(self, value: bytes) -> int:
        if self.tell() + len(value) > self.limit:
            raise _OutputLimit
        return super().write(value)


def _limits() -> None:
    if sys.platform != "linux":
        return
    try:
        import resource

        for name, desired in (
            ("RLIMIT_CORE", 0),
            ("RLIMIT_CPU", 12),
            ("RLIMIT_FSIZE", 0),
            ("RLIMIT_NOFILE", 32),
            ("RLIMIT_AS", 1024 * 1024 * 1024),
        ):
            limit = getattr(resource, name, None)
            if limit is None:
                continue
            _, hard = resource.getrlimit(limit)
            value = min(desired, hard) if hard != resource.RLIM_INFINITY else desired
            resource.setrlimit(limit, (value, value))
    except (ImportError, OSError, ValueError):
        pass


def _read(path: str, display: str, limit: int) -> tuple[bytes, str]:
    if not isinstance(path, str) or not path or len(path) > 4096:
        raise _Failure("ValueError", "Invalid source path")
    if not isinstance(limit, int) or not 1 <= limit <= _MAX_INPUT_BYTES:
        raise _Failure("ValueError", "max_input_bytes exceeds the worker limit")
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        raise _Failure("FileNotFoundError", f"File not found: {display}") from None
    except PermissionError:
        raise _Failure("PermissionError", f"Permission denied: {display}") from None
    try:
        before = os.fstat(fd)
        if stat.S_ISDIR(before.st_mode):
            raise _Failure("IsADirectoryError", display)
        if not stat.S_ISREG(before.st_mode):
            raise _Failure("ValueError", f"Path must be a regular file: {display}")
        if before.st_size > limit:
            raise _Failure("ValueError", f"File exceeds max_input_bytes: {display}")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise _Failure("ValueError", f"File exceeds max_input_bytes: {display}")
        if _signature(before) != _signature(after):
            raise _Failure("ValueError", f"File changed while reading: {display}")
    finally:
        if fd >= 0:
            os.close(fd)
    return data, hashlib.sha256(data).hexdigest()


def _signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _image_open(data: bytes):
    try:
        from PIL import Image
    except ImportError:
        raise _Failure(
            "MissingDependency",
            "Pillow is missing. Run `await ws.packages.add('pillow')` and await its task.",
        ) from None
    Image.MAX_IMAGE_PIXELS = _MAX_IMAGE_PIXELS
    warnings.simplefilter("error", Image.DecompressionBombWarning)
    try:
        image = Image.open(io.BytesIO(data), formats=("PNG", "JPEG"))
    except (
        Image.UnidentifiedImageError,
        OSError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ) as exc:
        raise _Failure("ValueError", f"Expected a valid PNG or JPEG: {exc}") from None
    if image.format not in {"PNG", "JPEG"}:
        image.close()
        raise _Failure("ValueError", "Expected a PNG or JPEG image")
    width, height = image.size
    if width < 1 or height < 1 or width * height > _MAX_IMAGE_PIXELS:
        image.close()
        raise _Failure("ValueError", f"Image exceeds the {_MAX_IMAGE_PIXELS}-pixel limit")
    return image


def _image_info(request: dict) -> dict:
    data, revision = _read(
        request["path"], request["display"], request["max_input_bytes"]
    )
    with _image_open(data) as image:
        return {
            "path": request["display"],
            "revision": revision,
            "size_bytes": len(data),
            "format": image.format,
            "mode": image.mode,
            "width": image.width,
            "height": image.height,
        }


def _image_transform(request: dict) -> dict:
    from PIL import Image

    data, revision = _read(
        request["path"], request["display"], request["max_input_bytes"]
    )
    limit = request["max_output_bytes"]
    if not isinstance(limit, int) or not 1 <= limit <= _MAX_OUTPUT_BYTES:
        raise _Failure("ValueError", "max_bytes exceeds the inline image limit")
    with _image_open(data) as original:
        width, height = original.size
        source_mode = original.mode
        crop = request.get("crop")
        if crop is not None:
            if (
                not isinstance(crop, list)
                or len(crop) != 4
                or any(not isinstance(value, int) or isinstance(value, bool) for value in crop)
            ):
                raise _Failure("ValueError", "crop must contain four pixel coordinates")
            left, top, right, bottom = crop
            if left < 0 or top < 0 or right <= left or bottom <= top:
                raise _Failure("ValueError", "crop must be a non-empty pixel rectangle")
            if right > width or bottom > height:
                raise _Failure("ValueError", f"crop must fit within {width}x{height} pixels")
        resize = request.get("resize")
        if resize is not None and (
            not isinstance(resize, list)
            or len(resize) != 2
            or any(not isinstance(value, int) or isinstance(value, bool) for value in resize)
            or any(value < 1 or value > 8192 for value in resize)
            or resize[0] * resize[1] > 16_000_000
        ):
            raise _Failure("ValueError", "resize exceeds the supported dimensions")
        image = original.crop(crop) if crop is not None else original.copy()
        try:
            if resize is not None:
                image.thumbnail(tuple(resize), Image.Resampling.LANCZOS)
            result = _encode_image(image, original.format, limit, resize is not None)
            return {
                "data": base64.b64encode(result).decode("ascii"),
                "path": request["display"],
                "revision": revision,
                "size_bytes": len(data),
                "format": original.format,
                "mode": (
                    "RGB"
                    if original.format == "JPEG" and image.mode not in {"RGB", "L"}
                    else image.mode
                ),
                "source_mode": source_mode,
                "original_width": width,
                "original_height": height,
                "width": image.width,
                "height": image.height,
                "crop": crop,
                "resize": resize,
            }
        finally:
            image.close()


def _encode_image(image, image_format: str, limit: int, can_shrink: bool) -> bytes:
    from PIL import Image

    attempts = 5 if can_shrink else 1
    for attempt in range(attempts):
        target = image
        if image_format == "JPEG" and image.mode not in {"RGB", "L"}:
            target = image.convert("RGB")
        output = _BoundedBytesIO(limit)
        try:
            if image_format == "JPEG":
                target.save(output, format="JPEG", quality=88, optimize=False)
            else:
                target.save(output, format="PNG", compress_level=6)
            return output.getvalue()
        except _OutputLimit:
            if attempt + 1 == attempts:
                raise _Failure(
                    "ValueError",
                    "Encoded image exceeds max_bytes; use a smaller resize box or crop",
                ) from None
            image.thumbnail(
                (max(1, int(image.width * 0.75)), max(1, int(image.height * 0.75))),
                Image.Resampling.LANCZOS,
            )
        finally:
            output.close()
            if target is not image:
                target.close()
    raise AssertionError("unreachable")


def _open_pdf(data: bytes):
    try:
        import pymupdf
    except ImportError:
        raise _Failure(
            "MissingDependency",
            "PyMuPDF is missing. Run `await ws.packages.add('pymupdf')` and await its task.",
        ) from None
    try:
        document = pymupdf.open(stream=data, filetype="pdf")
    except Exception as exc:
        raise _Failure("ValueError", f"Could not open PDF: {exc}") from None
    if document.is_encrypted:
        document.close()
        raise _Failure("ValueError", "Password-protected PDFs are not supported")
    if document.page_count < 1:
        document.close()
        raise _Failure("ValueError", "PDF contains no pages")
    return pymupdf, document


def _page_bounds(page) -> list[float]:
    return [round(float(value), 4) for value in page.rect]


def _pdf_info(request: dict) -> dict:
    data, revision = _read(
        request["path"], request["display"], request["max_input_bytes"]
    )
    _, document = _open_pdf(data)
    try:
        result = {
            "path": request["display"],
            "revision": revision,
            "size_bytes": len(data),
            "page_count": document.page_count,
            "metadata": {
                str(key): _metadata_value(value)
                for key, value in document.metadata.items()
                if value is not None
            },
        }
        page_number = request.get("page")
        if page_number is not None:
            page = _get_page(document, page_number)
            result["page"] = page_number
            result["bounds"] = _page_bounds(page)
            result["rotation"] = page.rotation
        return result
    finally:
        document.close()


def _metadata_value(value: object) -> str:
    return str(value).replace("\x00", "")[:1024]


def _get_page(document, page_number: object):
    if not isinstance(page_number, int) or isinstance(page_number, bool):
        raise _Failure("ValueError", "page must be a positive integer")
    if not 1 <= page_number <= document.page_count:
        raise _Failure("ValueError", f"page must be between 1 and {document.page_count}")
    return document.load_page(page_number - 1)


def _pdf_read(request: dict) -> dict:
    data, revision = _read(
        request["path"], request["display"], request["max_input_bytes"]
    )
    _, document = _open_pdf(data)
    try:
        start_page = request["start_page"]
        max_pages = request["max_pages"]
        max_chars = request["max_chars"]
        if not isinstance(start_page, int) or isinstance(start_page, bool) or start_page < 1:
            raise _Failure("ValueError", "start_page must be a positive integer")
        if (
            not isinstance(max_pages, int)
            or isinstance(max_pages, bool)
            or not 1 <= max_pages <= 10
        ):
            raise _Failure("ValueError", "max_pages must be between 1 and 10")
        if (
            not isinstance(max_chars, int)
            or isinstance(max_chars, bool)
            or not 1 <= max_chars <= 64_000
        ):
            raise _Failure("ValueError", "max_chars must be between 1 and 64000")
        cursor = request.get("cursor")
        offset = 0
        if cursor is not None:
            if not isinstance(cursor, str) or len(cursor) > 2048:
                raise _Failure("ValueError", "cursor must be at most 2048 characters")
            try:
                raw_cursor = base64.b64decode(
                    cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True
                )
                parsed = json.loads(raw_cursor)
            except (ValueError, json.JSONDecodeError):
                raise _Failure("ValueError", "Invalid PDF continuation cursor") from None
            if (
                not isinstance(parsed, dict)
                or set(parsed) != {"v", "revision", "page", "offset"}
                or parsed.get("v") != 1
                or parsed.get("revision") != revision
                or not isinstance(parsed.get("page"), int)
                or isinstance(parsed.get("page"), bool)
                or not isinstance(parsed.get("offset"), int)
                or isinstance(parsed.get("offset"), bool)
                or parsed["page"] < 1
                or parsed["offset"] < 0
            ):
                if isinstance(parsed, dict) and parsed.get("revision") != revision:
                    raise _Failure(
                        "ValueError", "PDF changed since this continuation cursor was created"
                    )
                raise _Failure("ValueError", "Invalid PDF continuation cursor")
            start_page = parsed["page"]
            offset = parsed["offset"]
        if start_page > document.page_count:
            raise _Failure(
                "ValueError", f"start_page must be between 1 and {document.page_count}"
            )
        pages = []
        remaining = max_chars
        next_cursor = None
        page_number = start_page
        while page_number <= document.page_count and len(pages) < max_pages:
            page = _get_page(document, page_number)
            full_text = page.get_text("text", sort=True)
            if offset > len(full_text):
                raise _Failure("ValueError", "PDF continuation cursor offset is out of range")
            text = full_text[offset : offset + remaining]
            following_offset = offset + len(text)
            page_truncated = following_offset < len(full_text)
            remaining -= len(text)
            pages.append(
                {
                    "page": page_number,
                    "text": text,
                    "truncated": page_truncated,
                    "bounds": _page_bounds(page),
                    "rotation": page.rotation,
                }
            )
            if page_truncated:
                next_cursor = _make_cursor(revision, page_number, following_offset)
                break
            page_number += 1
            offset = 0
            if remaining == 0 and page_number <= document.page_count:
                next_cursor = _make_cursor(revision, page_number, 0)
                break
        if next_cursor is None and page_number <= document.page_count:
            next_cursor = _make_cursor(revision, page_number, offset)
        return {
            "path": request["display"],
            "revision": revision,
            "size_bytes": len(data),
            "page_count": document.page_count,
            "start_page": start_page,
            "pages": pages,
            "truncated": next_cursor is not None,
            "has_more": next_cursor is not None,
            "next_cursor": next_cursor,
        }
    finally:
        document.close()


def _make_cursor(revision: str, page: int, offset: int) -> str:
    raw = json.dumps(
        {"v": 1, "revision": revision, "page": page, "offset": offset},
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _pdf_render(request: dict) -> dict:
    data, revision = _read(
        request["path"], request["display"], request["max_input_bytes"]
    )
    pymupdf, document = _open_pdf(data)
    try:
        page_number = request["page"]
        page = _get_page(document, page_number)
        page_rect = page.rect
        bounds = _page_bounds(page)
        clip_values = request.get("clip")
        clip = pymupdf.Rect(clip_values) if clip_values is not None else page_rect
        if clip.is_empty or not page_rect.contains(clip):
            raise _Failure("ValueError", "clip must fit within the page bounds")
        width = float(clip.width)
        height = float(clip.height)
        requested_dpi = request["dpi"]
        if (
            not isinstance(requested_dpi, int)
            or isinstance(requested_dpi, bool)
            or not 36 <= requested_dpi <= 300
        ):
            raise _Failure("ValueError", "dpi must be between 36 and 300")
        max_bytes = request["max_bytes"]
        if not isinstance(max_bytes, int) or not 1 <= max_bytes <= _MAX_OUTPUT_BYTES:
            raise _Failure("ValueError", "max_bytes exceeds the inline image limit")
        scale = requested_dpi / 72
        scale = min(scale, (_MAX_RENDER_PIXELS / max(1, width * height)) ** 0.5)
        for _ in range(6):
            pixmap = page.get_pixmap(
                matrix=pymupdf.Matrix(scale, scale),
                clip=clip,
                colorspace=pymupdf.csRGB,
                alpha=False,
            )
            if pixmap.width * pixmap.height > _MAX_RENDER_PIXELS:
                scale *= (_MAX_RENDER_PIXELS / (pixmap.width * pixmap.height)) ** 0.5
                continue
            png = pixmap.tobytes("png")
            if len(png) <= max_bytes:
                return {
                    "data": base64.b64encode(png).decode("ascii"),
                    "path": request["display"],
                    "revision": revision,
                    "size_bytes": len(data),
                    "page": page_number,
                    "bounds": bounds,
                    "clip": clip_values,
                    "rotation": page.rotation,
                    "dpi": round(scale * 72, 2),
                    "width": pixmap.width,
                    "height": pixmap.height,
                    "format": "PNG",
                }
            scale *= 0.75
        raise _Failure(
            "ValueError",
            "Rendered page exceeds max_bytes after reducing resolution; use a smaller dpi or clip",
        )
    finally:
        document.close()


def _dispatch(request: dict) -> dict:
    operation = request.get("operation")
    handlers = {
        "image_info": _image_info,
        "image_transform": _image_transform,
        "pdf_info": _pdf_info,
        "pdf_read": _pdf_read,
        "pdf_render": _pdf_render,
    }
    try:
        handler = handlers[operation]
    except (KeyError, TypeError):
        raise _Failure("ValueError", "Unknown media operation") from None
    return handler(request)


def main() -> None:
    _limits()
    try:
        raw = sys.stdin.buffer.read(64 * 1024 + 1)
        if len(raw) > 64 * 1024:
            raise _Failure("ValueError", "Media request exceeds its size limit")
        request = json.loads(raw)
        if not isinstance(request, dict):
            raise _Failure("ValueError", "Invalid media request")
        result = _dispatch(request)
        response = {"ok": True, "result": result}
    except _Failure as exc:
        response = {"ok": False, "kind": exc.kind, "error": str(exc)[:2048]}
    except Exception as exc:
        response = {
            "ok": False,
            "kind": type(exc).__name__,
            "error": f"Media operation failed: {str(exc)[:1024]}",
        }
    sys.stdout.write(json.dumps(response, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
