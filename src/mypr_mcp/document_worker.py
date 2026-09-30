"""Bounded OCR and Office extraction worker."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import warnings
import zipfile
import zlib
from pathlib import Path
from typing import Any

_MAX_INPUT = 64 * 1024 * 1024
_MAX_OUTPUT = 8 * 1024 * 1024
_MAX_ITEMS = 50_000
_MAX_TEXT = 1024
_MAX_SCANNED_CELLS = 1_000_000
_MAX_IMAGE_PIXELS = 40_000_000
_MAX_RENDER_PIXELS = 16_000_000
_MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
_MAX_ARCHIVE_MEMBER = 128 * 1024 * 1024
_MAX_ARCHIVE_ENTRIES = 10_000
_MAX_COMPRESSION_RATIO = 200
_MAX_RESUME_TSV = 8 * 1024 * 1024


class _Failure(Exception):
    def __init__(self, kind: str, message: str) -> None:
        self.kind = kind
        super().__init__(message)


class _Collector:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []
        self.bytes = 0
        self.truncated = False
        self.reason: str | None = None

    def stop(self, reason: str) -> None:
        self.truncated = True
        self.reason = reason

    def add(self, item: dict[str, Any]) -> bool:
        if len(self.items) >= _MAX_ITEMS:
            self.truncated = True
            self.reason = "item_limit"
            return False
        text = item.get("text")
        if isinstance(text, str) and len(text) > _MAX_TEXT:
            item = {**item, "text": text[:_MAX_TEXT], "text_truncated": True}
            self.truncated = True
            self.reason = "text_item_limit"
        size = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
        if self.bytes + size > _MAX_OUTPUT:
            self.truncated = True
            self.reason = "result_size_limit"
            return False
        self.items.append(item)
        self.bytes += size
        return True


def _limits() -> None:
    if sys.platform != "linux":
        return
    try:
        import resource

        for name, desired in (
            ("RLIMIT_CORE", 0),
            ("RLIMIT_CPU", 65),
            ("RLIMIT_FSIZE", 16 * 1024 * 1024),
            ("RLIMIT_NOFILE", 48),
            ("RLIMIT_AS", 1024 * 1024 * 1024),
        ):
            limit = getattr(resource, name, None)
            if limit is None:
                continue
            _, hard = resource.getrlimit(limit)
            value = min(desired, hard) if hard != resource.RLIM_INFINITY else desired
            resource.setrlimit(limit, (value, value))
    except ImportError, OSError, ValueError:
        pass


def _read(request: dict[str, Any]) -> tuple[bytes, str]:
    path, display, limit = (
        request.get("path"),
        request.get("display"),
        request.get("max_input_bytes"),
    )
    if not isinstance(path, str) or not path or len(path) > 4096:
        raise _Failure("ValueError", "Invalid source path")
    if not isinstance(display, str) or len(display) > 4096:
        raise _Failure("ValueError", "Invalid display path")
    if type(limit) is not int or not 1 <= limit <= _MAX_INPUT:
        raise _Failure("ValueError", "max_input_bytes exceeds the 64 MiB worker limit")
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        raise _Failure("FileNotFoundError", f"File not found: {display}") from None
    except PermissionError:
        raise _Failure("PermissionError", f"Permission denied: {display}") from None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise _Failure("ValueError", "Source must be a regular file")
        if before.st_size > limit:
            raise _Failure("ValueError", "File exceeds max_input_bytes")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise _Failure("ValueError", "File exceeds max_input_bytes")
        if _signature(before) != _signature(after):
            raise _Failure("ValueError", "File changed while reading")
    finally:
        if fd >= 0:
            os.close(fd)
    return data, hashlib.sha256(data).hexdigest()


def _signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _clean_text(text: str) -> str:
    return "".join(char if char in "\t\n\r" or char.isprintable() else " " for char in text)


def _add_text(collector: _Collector, kind: str, location: dict[str, Any], text: str) -> bool:
    text = _clean_text(text)
    if not text.strip():
        return True
    if len(text) <= _MAX_TEXT:
        return collector.add({"type": kind, "location": location, "text": text})
    for part, offset in enumerate(range(0, len(text), _MAX_TEXT), 1):
        if not collector.add(
            {
                "type": kind,
                "location": location,
                "part": part,
                "offset": offset,
                "text": text[offset : offset + _MAX_TEXT],
            }
        ):
            return False
    return True


def _dependency(name: str, package: str) -> None:
    try:
        __import__(name)
    except ImportError:
        raise _Failure(
            "MissingDependency",
            f"{package} is missing. Install it with `job = await ws.packages.add('{package}')`, "
            "then await the task.",
        ) from None


def _archive(data: bytes) -> zipfile.ZipFile:
    import io

    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
        entries = archive.infolist()
    except (OSError, zipfile.BadZipFile) as exc:
        raise _Failure(
            "ValueError", f"Expected an unencrypted Office Open XML file: {exc}"
        ) from None
    if len(entries) > _MAX_ARCHIVE_ENTRIES:
        archive.close()
        raise _Failure("ValueError", "Office archive has too many entries")
    total = 0
    for entry in entries:
        if entry.flag_bits & 1:
            archive.close()
            raise _Failure("ValueError", "Encrypted Office documents are not supported")
        if entry.file_size > _MAX_ARCHIVE_MEMBER:
            archive.close()
            raise _Failure("ValueError", "Office archive member exceeds the expansion limit")
        total += entry.file_size
        if total > _MAX_ARCHIVE_BYTES:
            archive.close()
            raise _Failure("ValueError", "Office archive exceeds the expansion limit")
        if entry.file_size and entry.compress_size == 0:
            archive.close()
            raise _Failure("ValueError", "Office archive has an invalid compression ratio")
        if entry.compress_size and entry.file_size / entry.compress_size > _MAX_COMPRESSION_RATIO:
            archive.close()
            raise _Failure("ValueError", "Office archive exceeds the compression ratio limit")
    return archive


def _backend_report() -> dict[str, Any]:
    modules = {
        "pillow": "Pillow",
        "pymupdf": "PyMuPDF",
        "docx": "python-docx",
        "pptx": "python-pptx",
        "openpyxl": "openpyxl",
    }
    packages = {}
    for module, package in modules.items():
        try:
            available = importlib.util.find_spec(module) is not None
        except ImportError, ValueError:
            available = False
        packages[package] = {"available": available}
    executable = shutil.which("tesseract")
    tessdata: list[str] = []
    version = None
    error = None
    if executable:
        try:
            completed = subprocess.run(
                [executable, "--list-langs"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=3,
                check=False,
                env=_worker_env(),
            )
            lines = completed.stdout[: 64 * 1024].decode("utf-8", "replace").splitlines()
            tessdata = [line.strip() for line in lines[1:] if line.strip()][:256]
            version_run = subprocess.run(
                [executable, "--version"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=2,
                check=False,
                env=_worker_env(),
            )
            version = version_run.stdout[:1024].decode("utf-8", "replace").splitlines()[0:1]
            version = version[0] if version else None
            if completed.returncode:
                error = f"tesseract --list-langs exited with status {completed.returncode}"
        except (OSError, subprocess.TimeoutExpired) as exc:
            error = f"Could not query Tesseract: {exc}"
    return {
        "packages": packages,
        "tesseract": {
            "available": executable is not None,
            "path": executable,
            "version": version,
            "languages": tessdata,
            "error": error,
        },
    }


def _worker_env() -> dict[str, str]:
    names = {
        "PATH",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LD_LIBRARY_PATH",
        "TESSDATA_PREFIX",
        "OMP_THREAD_LIMIT",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "TMPDIR",
    }
    environment = {key: value for key, value in os.environ.items() if key in names}
    environment.setdefault("OMP_THREAD_LIMIT", "1")
    return environment


def _ocr(request: dict[str, Any]) -> dict[str, Any]:
    data, revision = _read(request)
    display = request["display"]
    suffix = Path(display).suffix.lower()
    language = request["language"]
    executable = shutil.which("tesseract")
    if executable is None:
        raise _Failure(
            "MissingDependency", "Tesseract is missing; install the system Tesseract package."
        )
    if suffix == ".pdf":
        _dependency("pymupdf", "pymupdf")
        import pymupdf

        try:
            document = pymupdf.open(stream=data, filetype="pdf")
        except Exception as exc:
            raise _Failure("ValueError", f"Invalid PDF: {exc}") from None
        if document.needs_pass:
            document.close()
            raise _Failure("ValueError", "Encrypted PDFs are not supported")
        first, max_pages, dpi = request["start_page"], request["max_pages"], request["dpi"]
        if first > document.page_count:
            document.close()
            raise _Failure(
                "ValueError", f"start_page exceeds PDF page count ({document.page_count})"
            )
        pdf_page_count = document.page_count
        last_page = min(pdf_page_count, first + max_pages - 1)
        coordinate_space = "rendered_page_pixels"
        more_pages = last_page < pdf_page_count
    elif suffix in {".png", ".jpg", ".jpeg"}:
        if request["start_page"] != 1:
            raise _Failure("ValueError", "start_page must be 1 for image files")
        _dependency("PIL", "pillow")
        import io

        from PIL import Image, ImageOps

        Image.MAX_IMAGE_PIXELS = _MAX_IMAGE_PIXELS
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        try:
            source = Image.open(io.BytesIO(data), formats=("PNG", "JPEG"))
            oriented = ImageOps.exif_transpose(source)
            if oriented.width * oriented.height > _MAX_IMAGE_PIXELS:
                raise ValueError("image exceeds the pixel limit")
            png = io.BytesIO()
            oriented.save(png, format="PNG")
            image_page = (1, png.getvalue(), oriented.width, oriented.height, request["dpi"])
            source.close()
            if oriented is not source:
                oriented.close()
        except Exception as exc:
            raise _Failure("ValueError", f"Expected a valid PNG or JPEG: {exc}") from None
        coordinate_space = "exif_normalized_image_pixels"
        more_pages = False
        last_page = 1
    else:
        raise _Failure("ValueError", "OCR supports PDF, PNG, and JPEG files")

    collector = _Collector()
    page_info = []
    resume: dict[str, Any] | None = None

    def extract_page(
        number: int, image_data: bytes, width: int, height: int, effective_dpi: int
    ) -> None:
        nonlocal resume
        tsv = _tesseract(executable, image_data, language)
        if len(tsv) > _MAX_RESUME_TSV:
            raise _Failure("ValueError", "Tesseract output exceeds the 8 MiB page limit")
        page_info.append({"page": number, "width": width, "height": height, "dpi": effective_dpi})
        rows = tsv.splitlines(keepends=True)
        word_offset = 0
        tsv_offset = len(rows[0]) if rows else 0
        for raw_row in rows[1:]:
            row_offset = tsv_offset
            tsv_offset += len(raw_row)
            row = raw_row.decode("utf-8", "replace").rstrip("\r\n")
            fields = row.split("\t", 11)
            if len(fields) != 12 or fields[0] != "5":
                continue
            word = _clean_text(fields[11])
            if not word:
                continue
            try:
                confidence = float(fields[10])
                left, top, box_width, box_height = map(int, fields[6:10])
                block, paragraph, line = map(int, fields[2:5])
            except ValueError:
                continue
            item = {
                "type": "word",
                "page": number,
                "text": word,
                "confidence": confidence,
                "bbox": [left, top, box_width, box_height],
                "block": block,
                "paragraph": paragraph,
                "line": line,
            }
            if not collector.add(item):
                if collector.reason in {"result_size_limit", "item_limit"}:
                    resume = {
                        "page": number,
                        "width": width,
                        "height": height,
                        "dpi": effective_dpi,
                        "word_offset": word_offset,
                        "tsv_offset": row_offset,
                        "tsv": base64.b64encode(zlib.compress(tsv, 6)).decode("ascii"),
                    }
                break
            word_offset += 1

    if suffix == ".pdf":
        try:
            for number in range(first, last_page + 1):
                page = document[number - 1]
                scale = dpi / 72
                area = page.rect.width * page.rect.height * scale * scale
                if area > _MAX_RENDER_PIXELS:
                    scale *= (_MAX_RENDER_PIXELS / area) ** 0.5
                pixmap = page.get_pixmap(matrix=pymupdf.Matrix(scale, scale), alpha=False)
                extract_page(
                    number,
                    pixmap.tobytes("png"),
                    pixmap.width,
                    pixmap.height,
                    round(scale * 72),
                )
                if collector.truncated:
                    break
        finally:
            document.close()
    else:
        extract_page(*image_page)
    truncated = collector.truncated or more_pages
    if collector.truncated:
        next_page = None
        truncation_reason = collector.reason
    elif more_pages:
        next_page = last_page + 1
        truncation_reason = "page_limit"
    else:
        next_page = None
        truncation_reason = None
    result = {
        "source": _source(display, data, revision, suffix[1:]),
        "coordinate_space": coordinate_space,
        "pages": page_info,
        "items": collector.items,
        "complete": not truncated,
        "truncated": truncated,
        "next_page": next_page,
        "truncation_reason": truncation_reason,
        "warnings": [],
    }
    if resume is not None:
        resume["next_page"] = (
            resume["page"] + 1 if resume["page"] < last_page else None
        )
        resume["remaining_pages"] = max(0, last_page - resume["page"])
        result["resume"] = resume
    return result


def _tesseract(executable: str, image_data: bytes, language: str) -> bytes:
    with tempfile.TemporaryDirectory(prefix="mypr-ocr-") as directory:
        output = os.path.join(directory, "result")
        try:
            completed = subprocess.run(
                [executable, "stdin", output, "-l", language, "tsv"],
                input=image_data,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=55,
                check=False,
                env=_worker_env(),
            )
        except subprocess.TimeoutExpired:
            raise _Failure("TimeoutError", "Tesseract exceeded its 55-second page limit") from None
        except OSError as exc:
            raise _Failure("MissingDependency", f"Could not run Tesseract: {exc}") from None
        tsv_path = output + ".tsv"
        if completed.returncode:
            raise _Failure(
                "ValueError",
                f"Tesseract failed with status {completed.returncode}; check the language data",
            )
        try:
            with open(tsv_path, "rb") as stream:
                tsv = stream.read(8 * 1024 * 1024 + 1)
        except FileNotFoundError:
            raise _Failure("ValueError", "Tesseract did not produce TSV output") from None
        if len(tsv) > 8 * 1024 * 1024:
            raise _Failure("ValueError", "Tesseract output exceeds the 8 MiB page limit")
        return tsv


def _source(display: str, data: bytes, revision: str, format_name: str) -> dict[str, Any]:
    return {"path": display, "revision": revision, "size_bytes": len(data), "format": format_name}


def _extract(request: dict[str, Any]) -> dict[str, Any]:
    data, revision = _read(request)
    display = request["display"]
    suffix = Path(display).suffix.lower()
    collector = _Collector()
    warnings: list[str] = []
    if suffix == ".docx":
        _dependency("docx", "python-docx")
        from docx import Document

        archive = _archive(data)
        archive.close()
        import io

        try:
            document = Document(io.BytesIO(data))
        except Exception as exc:
            raise _Failure("ValueError", f"Could not read DOCX: {exc}") from None
        paragraph_number = 0
        table_number = 0

        def walk_table(table, table_location: dict[str, Any], depth: int) -> None:
            for row_number, row in enumerate(table.rows, 1):
                for column_number, cell in enumerate(row.cells, 1):
                    cell_location = {
                        **table_location,
                        "row": row_number,
                        "column": column_number,
                    }
                    paragraph_index = 0
                    nested_index = 0
                    for child in cell.iter_inner_content():
                        if child.__class__.__name__ == "Paragraph":
                            paragraph_index += 1
                            if not _add_text(
                                collector,
                                "table_cell",
                                {**cell_location, "paragraph": paragraph_index},
                                child.text,
                            ):
                                return
                        else:
                            nested_index += 1
                            if depth >= 8:
                                collector.stop("table_depth_limit")
                                warnings.append(
                                    "A nested DOCX table exceeded the 8-level nesting limit"
                                )
                                return
                            walk_table(
                                child,
                                {**cell_location, "nested_table": nested_index},
                                depth + 1,
                            )
                        if collector.truncated:
                            return

        for element in document.iter_inner_content():
            if element.__class__.__name__ == "Paragraph":
                paragraph_number += 1
                if not _add_text(
                    collector, "paragraph", {"paragraph": paragraph_number}, element.text
                ):
                    break
            else:
                table_number += 1
                walk_table(element, {"table": table_number}, 0)
            if collector.truncated:
                break
    elif suffix == ".pptx":
        _dependency("pptx", "python-pptx")
        from pptx import Presentation
        from pptx.enum.shapes import MSO_SHAPE_TYPE

        archive = _archive(data)
        archive.close()
        import io

        try:
            presentation = Presentation(io.BytesIO(data))
        except Exception as exc:
            raise _Failure("ValueError", f"Could not read PPTX: {exc}") from None

        def walk_shapes(shapes, slide_number: int, parents: tuple[int, ...] = ()) -> None:
            for shape_number, shape in enumerate(shapes, 1):
                location = {"slide": slide_number, "shape": [*parents, shape_number]}
                if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
                    if len(parents) >= 8:
                        collector.stop("group_depth_limit")
                        warnings.append("A grouped shape exceeded the 8-level nesting limit")
                        return
                    walk_shapes(shape.shapes, slide_number, (*parents, shape_number))
                else:
                    if shape.has_text_frame and not _add_text(
                        collector, "text", location, shape.text
                    ):
                        return
                    if shape.has_table:
                        for row_number, row in enumerate(shape.table.rows, 1):
                            for column_number, cell in enumerate(row.cells, 1):
                                cell_location = {
                                    **location,
                                    "row": row_number,
                                    "column": column_number,
                                }
                                if not _add_text(collector, "table_cell", cell_location, cell.text):
                                    return
                if collector.truncated:
                    return

        for slide_number, slide in enumerate(presentation.slides, 1):
            walk_shapes(slide.shapes, slide_number)
            if collector.truncated:
                break
    elif suffix == ".xlsx":
        _dependency("openpyxl", "openpyxl")
        import io

        from openpyxl import load_workbook

        archive = _archive(data)
        archive.close()
        try:
            workbook = load_workbook(
                io.BytesIO(data), read_only=True, data_only=request["cached_values"]
            )
        except Exception as exc:
            raise _Failure("ValueError", f"Could not read XLSX: {exc}") from None
        try:
            scanned_cells = 0
            for sheet in workbook.worksheets:
                for row in sheet.iter_rows():
                    for cell in row:
                        scanned_cells += 1
                        if scanned_cells > _MAX_SCANNED_CELLS:
                            collector.stop("cell_scan_limit")
                            break
                        if cell.value is None:
                            continue
                        if not _add_text(
                            collector,
                            "cell",
                            {
                                "sheet": sheet.title,
                                "cell": cell.coordinate,
                                "row": cell.row,
                                "column": cell.column,
                            },
                            str(cell.value),
                        ):
                            break
                    if collector.truncated:
                        break
                if collector.truncated:
                    break
        finally:
            workbook.close()
    else:
        raise _Failure(
            "ValueError",
            "Office extraction supports .docx, .pptx, and .xlsx only. "
            "Macro-enabled, encrypted, and binary formats are unsupported.",
        )
    return {
        "source": _source(display, data, revision, suffix[1:]),
        "items": collector.items,
        "complete": not collector.truncated,
        "truncated": collector.truncated,
        "truncation_reason": collector.reason,
        "warnings": warnings,
    }


def _dispatch(request: dict[str, Any]) -> Any:
    operation = request.get("operation")
    if operation == "backends":
        return _backend_report()
    if operation == "ocr":
        return _ocr(request)
    if operation == "extract":
        return _extract(request)
    raise _Failure("ValueError", "Unknown document operation")


def main() -> int:
    _limits()
    try:
        raw = sys.stdin.buffer.read(64 * 1024 + 1)
        if len(raw) > 64 * 1024:
            raise _Failure("ValueError", "Document request exceeds its size limit")
        request = json.loads(raw)
        if not isinstance(request, dict):
            raise _Failure("ValueError", "Invalid document request")
        result = _dispatch(request)
        response = {"ok": True, "result": result}
    except _Failure as exc:
        response = {"ok": False, "kind": exc.kind, "error": str(exc)}
    except BaseException as exc:
        response = {"ok": False, "kind": "DocumentError", "error": f"{type(exc).__name__}: {exc}"}
    encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_OUTPUT and response.get("ok") is True:
        result = response.get("result")
        if isinstance(result, dict) and result.get("resume") is not None:
            result["items"] = []
            result["resume"]["word_offset"] = 0
            result["resume"]["tsv_offset"] = 0
            encoded = json.dumps(
                response, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        if (
            len(encoded) > _MAX_OUTPUT
            and isinstance(result, dict)
            and result.pop("resume", None) is not None
        ):
            response["result"] = result
            encoded = json.dumps(
                response, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
    if len(encoded) > _MAX_OUTPUT:
        encoded = json.dumps(
            {
                "ok": False,
                "kind": "ValueError",
                "error": "Document result exceeds the 7 MiB worker limit",
            }
        ).encode()
    sys.stdout.buffer.write(encoded)
    return 0


if __name__ == "__main__":
    sys.exit(main())
