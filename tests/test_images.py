import base64
import os

import pytest
from conftest import decode_result, mcp_session

from mypr_mcp.filesystem import Filesystem

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jVZkAAAAASUVORK5CYII="
)


async def test_image_returns_embedded_png_without_modifying_file(tmp_path):
    path = tmp_path / "pixel.png"
    path.write_bytes(PNG)
    image = await Filesystem(tmp_path).image("pixel.png")
    assert image.format == "png"
    assert image.data == PNG
    assert path.read_bytes() == PNG
    with pytest.raises(ValueError, match="max_bytes"):
        await Filesystem(tmp_path).image("pixel.png", max_bytes=len(PNG) - 1)


async def test_image_rejects_non_images_and_non_regular_files(tmp_path):
    (tmp_path / "text").write_text("not a picture")
    with pytest.raises(ValueError, match="PNG or JPEG"):
        await Filesystem(tmp_path).image("text")
    os.mkfifo(tmp_path / "pipe")
    with pytest.raises(ValueError, match="regular file"):
        await Filesystem(tmp_path).image("pipe")


async def test_image_is_returned_as_mcp_image_content(workspace):
    (workspace / "pixel.png").write_bytes(PNG)
    async with mcp_session(workspace) as session:
        response = await session.call_tool(
            "execute", {"code": 'await ws.fs.image("pixel.png")', "wait_ms": 10000}
        )
        payload = decode_result(response)
        while payload["state"] not in {"succeeded", "failed", "lost", "cancelled"}:
            response = await session.call_tool(
                "poll", {"exec_id": payload["exec_id"], "wait_ms": 1000}
            )
            payload = decode_result(response)
        assert payload["state"] == "succeeded", payload
        images = [block for block in response.content if getattr(block, "type", None) == "image"]
        assert images
        assert base64.b64decode(images[0].data) == PNG
