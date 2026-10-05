# Images and PDFs

Image transforms and PDF operations use Pillow and PyMuPDF inside short-lived worker processes. Registered packages are prepared automatically when `dependencies.auto_install` is enabled. Prepare them explicitly with:

```python
await ws.dependencies.ensure("pillow", "pymupdf")
```

The MCP server's own environment is separate from the workspace kernel, so installing packages with `uvx` does not make them available here. When automatic installation is disabled, an operation reports the missing registered dependency and the explicit `ws.dependencies.ensure(...)` action. The worker limits each input to 64 MiB, each image to 40 million source pixels, PDF text reads to 10 pages and 64,000 characters, and each rendered image to 4 million pixels and 2 MiB. At most two workers run at once. On Linux, each worker also has CPU, memory, file-size, and file-descriptor limits, and a process guard cleans up its descendants when the kernel exits. Cancelling a media call waits for an in-progress worker launch and finishes process cleanup before propagating cancellation. Operations never write over the source file.

## Images

`await ws.fs.image(path)` keeps its existing behavior: it embeds an unchanged PNG or JPEG up to 2 MiB and does not require Pillow. Add a resize box or crop rectangle to transform it. `resize=(width, height)` fits the result inside the box without enlarging it. `crop=(left, top, right, bottom)` uses half-open pixel coordinates on the original image and is applied before resizing. For transformed images, `max_bytes` (2 MiB by default) caps output and `max_input_bytes` caps the source. With no transform, the existing `max_bytes` argument caps the raw source file.

```python
await ws.fs.image("screenshots/page.png", crop=(80, 40, 1480, 980), resize=(1000, 700))
await ws.fs.image_info("screenshots/page.png")
```

`image_info()` returns `path`, `revision` (SHA-256), `size_bytes`, `format`, `mode`, `width`, and `height`. JPEG EXIF orientation is applied before reporting dimensions or transforming an image. Crop coordinates and the `original_width`/`original_height` fields in transformed-image metadata use this displayed orientation, so they match what `image_info()` reports. Transformed inline images include the same source revision plus original and output dimensions and the applied crop/resize in their display metadata and readable text label.

## PDFs

`ws.docs.info(path, page=None)` returns the source `path`, SHA-256 `revision`, `size_bytes`, `page_count`, and bounded PDF `metadata`. Passing a 1-based `page` adds its `bounds` and `rotation`.

`ws.docs.read(path, start_page=1, max_pages=5, max_chars=20000, cursor=None)` extracts bounded text from consecutive pages. The response includes the source revision, each 1-based page number, extracted `text`, page-level `truncated`, `rotation`, and page `bounds`, plus `has_more` and `next_cursor`. Reuse `next_cursor` as `cursor` to continue at the exact page and Unicode character offset; the cursor contains the PDF revision, and a changed source is rejected. A cursor cannot be combined with `start_page`. Page rectangles and `clip` coordinates are PyMuPDF page coordinates with a top-left origin, in points (1/72 inch), reflecting the page crop box and rotation. Page numbers are 1-based in this API.

```python
await ws.docs.info("reports/quarterly.pdf", page=3)
await ws.docs.read("reports/quarterly.pdf", start_page=2, max_pages=3)
await ws.docs.render_page("reports/quarterly.pdf", 3, dpi=144, clip=(72, 72, 520, 720))
```

For a text budget smaller than the document, continue until `next_cursor` is `None`:

```python
result = await ws.docs.read("reports/quarterly.pdf", max_chars=20000)
pages = result["pages"]
while result["next_cursor"]:
    result = await ws.docs.read(
        "reports/quarterly.pdf", max_chars=20000, cursor=result["next_cursor"]
    )
    pages.extend(result["pages"])
```

`ws.docs.render_page(path, page, dpi=120, clip=None, max_bytes=2MiB)` returns an inline PNG. Its image metadata and readable text label carry the source path and revision, page number, page bounds, clip, actual DPI, and pixel dimensions. A page that would exceed the pixel or byte budget is rendered at a lower resolution; invalid page ranges, encrypted PDFs, unsupported image formats, and files over the input limit fail with a bounded error.

## OCR and Office files

`ws.docs.ocr(path, language="eng", start_page=1, max_pages=5, dpi=200, cursor=None, resume_cursor=None, max_bytes=32768)` runs OCR for PDF, PNG, and JPEG files. Install Tesseract on the workstation. Python backends are prepared automatically when enabled; system OCR data is reused when it provides every requested language. Explicitly prepare backends with:

```python
await ws.dependencies.ensure("pillow", "pymupdf")
await ws.docs.backends()
result = await ws.docs.ocr("scans/report.pdf", language="eng", max_pages=5)
```

`backends()` reports Python package availability, the Tesseract executable and version, and installed language codes. PDF OCR also requires PyMuPDF; PNG and JPEG OCR require Pillow. The default page budget is five pages at 200 DPI, and pages above 16 million pixels are rendered at a lower effective DPI. Image EXIF orientation is normalized before OCR; `start_page` must be 1 for images. Each word item contains its text, Tesseract confidence, and `[left, top, width, height]` bounding box; `coordinate_space` identifies the coordinate system and `pages` reports each rendered size and effective DPI. Follow `next_cursor` to read the immutable result in bounded pages. If a PDF has additional pages, `next_page` identifies the next page to pass as `start_page`. `complete` and `truncation_reason` distinguish a full result from page, item, or output limits. When a page's bounded word stream is cut, use its `resume_cursor` on a new call to continue that cached page without rerunning OCR; the cached page words are returned first, and only after they are exhausted does mypr OCR the next unprocessed page. Cached words remain readable even if the source file changes. `resume_cursor` and the ordinary result `cursor` are mutually exclusive. Before OCR runs for the next unprocessed page, mypr rechecks the source revision and rejects the resume if the source changed. The worker also binds the newly generated page to that revision before it is stored. The remaining page budget is preserved from the original request and never exceeds its original `max_pages`; `next_page`, `complete`, and `truncation_reason` describe the actual remaining PDF pages.

`ws.docs.extract(path, cursor=None, max_bytes=32768, cached_values=False)` extracts `.docx`, `.pptx`, and `.xlsx` files. The parser package for the selected format is prepared automatically when enabled. To prepare all three explicitly:

```python
await ws.dependencies.ensure("python-docx", "python-pptx", "openpyxl")
result = await ws.docs.extract("reports/summary.docx")
```

Results contain ordered blocks with paragraph, slide, table-cell, or worksheet cell locations and source SHA-256 revision. Long text is split into ordered `part` chunks with character offsets; concatenate the chunks sharing a location to reconstruct the original cell or text block. Nested DOCX tables and PowerPoint groups are traversed up to eight levels; deeper content is reported as incomplete with a warning. XLSX extraction uses read-only mode and returns formulas by default. Set `cached_values=True` to read cached cell values; formulas are never evaluated, and cells without cached values may be omitted. Repeat `cached_values=True` when continuing a cached-value query. Continue with `next_cursor`; pages remain tied to their original immutable result even if the source file changes. Results are stored under `.mypr/document-results/`, with at most 16 snapshots and 32 MiB total; older results expire as new results are added.

OCR and Office parsing run in guarded workers with a 60-second operation limit, two-worker concurrency, 64 MiB input limit, bounded result storage, and an Office ZIP expansion cap. Encrypted files, macro-enabled Office formats, legacy binary Office formats, and unsupported extensions are rejected. Cached page reads do not install anything. Tesseract remains a manual system dependency; missing registered language models are prepared per call without overriding an explicit `TESSDATA_PREFIX`.
