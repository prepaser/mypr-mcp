import base64
import os
import threading

import mypr_mcp.cli as cli


async def test_execution_response_keeps_page_and_renders_agent_facing_details():
    payload = {
        "exec_id": "exec-1",
        "state": "succeeded",
        "cursor": 4,
        "has_more": True,
        "truncated": True,
        "output": [
            {"type": "stream", "stream": "stdout", "text": "out"},
            {"type": "stream", "stream": "stderr", "text": "err"},
            {"type": "result", "text": "value"},
        ],
        "warnings": [{"code": "display_issue", "text": "kept"}],
        "warnings_truncated": True,
        "inbox": {
            "unacked": 1,
            "has_more": False,
            "messages": [{"id": 7, "from": "bright-fox", "text": "hello", "truncated": True}],
        },
    }

    result = await cli.tool_result(payload)

    assert result.structured_content == payload
    text = result.content[0].text
    assert "exec_id=exec-1" in text
    assert "state=succeeded" in text
    assert "cursor=4" in text
    assert "has_more=true" in text
    assert "truncated=true" in text
    assert text.index("[stdout]") < text.index("[stderr]") < text.index("[result]")
    assert "out" in text and "err" in text and "value" in text
    assert "warning: display_issue: kept" in text
    assert "warnings_truncated=true" in text
    assert "hello" in text and "from=bright-fox" in text and "truncated=true" in text
    assert not text.lstrip().startswith("{")


async def test_split_stream_chunks_are_rendered_without_inserting_newlines():
    result = await cli.tool_result(
        {
            "exec_id": "exec-stream",
            "state": "succeeded",
            "cursor": 2,
            "has_more": False,
            "output": [
                {"type": "stream", "stream": "stdout", "text": "unbro"},
                {"type": "stream", "stream": "stdout", "text": "ken-line"},
            ],
        }
    )

    text = result.content[0].text
    assert "[stdout]\nunbroken-line" in text
    assert "unbro\nken-line" not in text


async def test_init_runtime_instructions_remain_visible():
    payload = {
        "client_id": "bright-fox",
        "runtime": {
            "manager_version": "1.0.1",
            "bridge_version": "1.0.1",
            "protocol_version": 1,
            "generation": "gen-1",
            "update_pending": False,
            "capabilities": ["execute", "help"],
            "instructions": "Use ws.help() for API details.",
        },
    }

    result = await cli.tool_result(payload)

    assert result.structured_content == payload
    text = result.content[0].text
    assert "client_id=bright-fox" in text
    assert "manager_version=1.0.1" in text
    assert "capabilities=execute, help" in text
    assert "instructions:\nUse ws.help() for API details." in text


async def test_inline_images_obey_aggregate_budget_and_keep_omitted_path(tmp_path, monkeypatch):
    first = tmp_path / "first.png"
    second = tmp_path / "second.jpg"
    third = tmp_path / "third.png"
    html = tmp_path / "page.html"
    main_thread = threading.get_ident()
    encode_threads = []
    original_encode = cli._encode_image

    def checked_encode(data):
        encode_threads.append(threading.get_ident())
        return original_encode(data)

    monkeypatch.setattr(cli, "_encode_image", checked_encode)
    first.write_bytes(b"a" * (3 * 1024 * 1024 // 4))
    second.write_bytes(b"b" * (3 * 1024 * 1024 // 2))
    third.write_bytes(b"c" * (5 * 1024 * 1024 // 4))
    payload = {
        "exec_id": "exec-images",
        "state": "succeeded",
        "cursor": 1,
        "has_more": False,
        "output": [
            {
                "type": "result",
                "text": "images",
                "artifacts": [
                    {"mime": "image/png", "path": str(first)},
                    {"mime": "image/jpeg", "path": str(second)},
                    {"mime": "image/png", "path": str(third)},
                    {"mime": "text/html", "path": str(html)},
                ],
            }
        ],
    }

    result = await cli.tool_result(payload)

    attached = [base64.b64decode(block.data) for block in result.content if hasattr(block, "data")]
    assert [len(value) for value in attached] == [3 * 1024 * 1024 // 4, 5 * 1024 * 1024 // 4]
    assert sum(map(len, attached)) == cli._IMAGE_RESPONSE_LIMIT
    assert encode_threads and all(thread != main_thread for thread in encode_threads)
    assert result.structured_content["output"] == payload["output"]
    assert len(result.structured_content["warnings"]) == 1
    assert result.structured_content["warnings"][0]["code"] == "artifact_omitted"
    text = result.content[0].text
    assert str(second) in text
    assert "per-response inline image limit (2 MiB) exceeded" in text
    assert str(third) in text
    assert str(html) in text


async def test_inline_image_read_is_bounded_and_runs_off_event_loop(tmp_path, monkeypatch):
    huge = tmp_path / "huge.png"
    with huge.open("wb") as file:
        file.truncate(8 * 1024 * 1024)
    main_thread = threading.get_ident()
    worker_threads = []
    read_sizes = []
    original = cli._read_image

    def checked_read(path, limit):
        worker_threads.append(threading.get_ident())
        data = original(path, limit)
        read_sizes.append(len(data))
        return data

    monkeypatch.setattr(cli, "_read_image", checked_read)
    result = await cli.tool_result(
        {
            "exec_id": "exec-huge",
            "state": "succeeded",
            "cursor": 1,
            "has_more": False,
            "output": [
                {
                    "type": "result",
                    "text": "image",
                    "artifacts": [{"mime": "image/png", "path": str(huge)}],
                }
            ],
        }
    )

    assert worker_threads and worker_threads[0] != main_thread
    assert read_sizes == [cli._IMAGE_RESPONSE_LIMIT + 1]
    assert not any(hasattr(block, "data") for block in result.content)
    assert "artifact_omitted" in result.structured_content["warnings"][0]["code"]


async def test_inline_image_reader_does_not_block_on_fifo(tmp_path):
    fifo = tmp_path / "artifact.png"
    os.mkfifo(fifo)

    result = await cli.tool_result(
        {
            "exec_id": "exec-fifo",
            "state": "succeeded",
            "cursor": 1,
            "has_more": False,
            "output": [
                {
                    "type": "result",
                    "text": "image",
                    "artifacts": [{"mime": "image/png", "path": str(fifo)}],
                }
            ],
        }
    )

    assert result.structured_content["warnings"][0]["code"] == "artifact_unavailable"
    assert str(fifo) in result.content[0].text


async def test_all_omitted_images_keep_path_and_reason_beyond_warning_limit(tmp_path):
    full = tmp_path / "full.png"
    full.write_bytes(b"x" * cli._IMAGE_RESPONSE_LIMIT)
    omitted = [tmp_path / f"omitted-{index}.png" for index in range(5)]
    for path in omitted:
        path.write_bytes(b"x")
    artifacts = [{"mime": "image/png", "path": str(full)}] + [
        {"mime": "image/png", "path": str(path)} for path in omitted
    ]

    result = await cli.tool_result(
        {
            "exec_id": "exec-many-images",
            "state": "succeeded",
            "cursor": 1,
            "has_more": False,
            "output": [{"type": "result", "text": "images", "artifacts": artifacts}],
        }
    )

    assert len([block for block in result.content if hasattr(block, "data")]) == 1
    assert result.structured_content["warnings_truncated"]
    text = result.content[0].text
    for path in omitted:
        issue = f"inline image omitted: {path}: per-response inline image limit (2 MiB) reached"
        assert issue in text


def test_decode_result_prefers_structured_content_and_accepts_legacy_json():
    from conftest import decode_result

    assert decode_result(
        type("MCPResult", (), {"structured_content": {"state": "succeeded"}, "content": []})()
    ) == {"state": "succeeded"}
    legacy = type(
        "MCPResult",
        (),
        {"structured_content": None, "content": [type("Text", (), {"text": '{"state":"old"}'})()]},
    )()
    assert decode_result(legacy) == {"state": "old"}
