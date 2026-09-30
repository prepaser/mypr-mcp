"""Short-lived worker for searching and diffing saved browser snapshots."""

from __future__ import annotations

import difflib
import io
import json
import re
import resource
import sys

_MAX_OUTPUT = 30 * 1024
_MAX_SNIPPET = 4096
_MAX_RESPONSE = 60 * 1024


def _limit_resource(name: int, requested: int) -> None:
    _, hard = resource.getrlimit(name)
    limit = requested if hard < 0 else min(requested, hard)
    resource.setrlimit(name, (limit, limit))


def _snippet(line: str) -> tuple[str, bool]:
    encoded = line.encode("utf-8")
    if len(encoded) <= _MAX_SNIPPET:
        return line, False
    return encoded[:_MAX_SNIPPET].decode("utf-8", errors="ignore"), True


def _find(value: dict) -> dict:
    text = value["snapshot"]
    query = value["query"]
    regex = bool(value["regex"])
    limit = int(value["limit"])
    offset = int(value["offset"])
    try:
        pattern = re.compile(query) if regex else None
    except re.error as exc:
        raise ValueError(f"invalid regular expression: {exc}") from exc
    matches = []
    output_bytes = 0
    line_number = 0
    next_line = offset
    has_more = False
    stream = io.StringIO(text)
    for line_number, raw in enumerate(stream):
        if line_number < offset:
            continue
        line = raw.rstrip("\r\n")
        match = pattern.search(line) if pattern is not None else None
        column = (
            match.start() if match is not None else -1
        ) if regex else line.find(query)
        if column < 0:
            next_line = line_number + 1
            continue
        excerpt, truncated = _snippet(line)
        item = {"line": line_number + 1, "column": column + 1, "text": excerpt}
        if truncated:
            item["truncated"] = True
        item_size = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
        if matches and output_bytes + item_size > _MAX_OUTPUT:
            next_line = line_number
            has_more = True
            break
        matches.append(item)
        output_bytes += item_size
        next_line = line_number + 1
        if len(matches) >= limit:
            continuation = next_line
            for _look_line_number, look_raw in enumerate(stream, continuation):
                look_line = look_raw.rstrip("\r\n")
                look_match = pattern.search(look_line) if pattern is not None else None
                look_column = (
                    look_match.start() if look_match is not None else -1
                ) if regex else look_line.find(query)
                if look_column >= 0:
                    has_more = True
                    break
            next_line = continuation
            break
    return {"matches": matches, "next_line": next_line, "has_more": has_more}


def _diff(value: dict) -> dict:
    separators = (
        "\n",
        "\r",
        "\v",
        "\f",
        "\x1c",
        "\x1d",
        "\x1e",
        "\x85",
        "\u2028",
        "\u2029",
    )
    line_count = 0
    for text in (value["before"], value["after"]):
        line_count += sum(text.count(separator) for separator in separators)
        line_count -= text.count("\r\n")
    if line_count + 2 > 50_000:
        raise ValueError("snapshot diff exceeds the 50,000-line worker limit")
    before = value["before"].splitlines()
    after = value["after"].splitlines()
    lines = difflib.unified_diff(
        before,
        after,
        fromfile=value["before_id"],
        tofile=value["after_id"],
        lineterm="",
    )
    diff = "\n".join(lines)
    offset = int(value["offset"])
    limit = int(value["limit"])
    end = offset
    used = 0
    while end < len(diff):
        char_size = len(diff[end].encode("utf-8"))
        if used + char_size > limit:
            break
        used += char_size
        end += 1
    if end == offset and end < len(diff):
        end += 1
    return {"diff": diff[offset:end], "next_offset": end, "has_more": end < len(diff)}


def main() -> int:
    try:
        _limit_resource(resource.RLIMIT_CPU, 3)
        _limit_resource(resource.RLIMIT_AS, 768 * 1024 * 1024)
        value = json.load(sys.stdin)
        if value["operation"] == "find":
            result = _find(value)
        elif value["operation"] == "diff":
            result = _diff(value)
        else:
            raise ValueError("unknown operation")
        output = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(output) > _MAX_RESPONSE:
            if value["operation"] == "find":
                matches = result["matches"]
                while matches and len(output) > _MAX_RESPONSE:
                    removed = matches.pop()
                    result["next_line"] = removed["line"] - 1
                    result["has_more"] = True
                    output = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
            else:
                original = result["diff"]
                while original and len(output) > _MAX_RESPONSE:
                    original = original[: max(1, len(original) // 2)]
                    result["diff"] = original
                    result["next_offset"] = int(value["offset"]) + len(original)
                    result["has_more"] = True
                    output = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
        if len(output) > _MAX_RESPONSE:
            raise ValueError("snapshot worker output limit exceeded")
        sys.stdout.buffer.write(output)
        return 0
    except Exception as exc:
        sys.stderr.write(f"{type(exc).__name__}: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
