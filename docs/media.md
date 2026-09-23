# Images and PDFs

Image transforms and PDF operations use Pillow and PyMuPDF inside short-lived worker processes. Install them into the workspace kernel before the first call:

```python
ws.local["media_install"] = await ws.packages.add("pillow", "pymupdf")
await ws.local["media_install"]
```

The MCP server's own environment is separate from the workspace kernel, so installing packages with `uvx` does not make them available here. Operations fail with an install hint when a package is missing. The worker limits each input to 64 MiB, each image to 40 million source pixels, PDF text reads to 10 pages and 64,000 characters, and each rendered image to 4 million pixels and 2 MiB. At most two workers run at once. On Linux, each worker also has CPU, memory, file-size, and file-descriptor limits. Operations never write over the source file.

## Images

`await ws.fs.image(path)` keeps its existing behavior: it embeds an unchanged PNG or JPEG up to 2 MiB and does not require Pillow. Add a resize box or crop rectangle to transform it. `resize=(width, height)` fits the result inside the box without enlarging it. `crop=(left, top, right, bottom)` uses half-open pixel coordinates on the original image and is applied before resizing. For transformed images, `max_bytes` (2 MiB by default) caps output and `max_input_bytes` caps the source. With no transform, the existing `max_bytes` argument caps the raw source file.

```python
await ws.fs.image("screenshots/page.png", crop=(80, 40, 1480, 980), resize=(1000, 700))
await ws.fs.image_info("screenshots/page.png")
```

`image_info()` returns `path`, `revision` (SHA-256), `size_bytes`, `format`, `mode`, `width`, and `height`. Transformed inline images include the same source revision plus original and output dimensions and the applied crop/resize in their display metadata and readable text label.

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
