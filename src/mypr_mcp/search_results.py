"""Incremental decoding of bounded search output."""

from __future__ import annotations

import json


class Results:
    def __init__(self, backend, mode, *, parse_match, limit=None):
        self.backend = backend
        self.mode = mode
        self.parse_match = parse_match
        self.limit = limit
        self.buffer = ""
        self.items = []
        self.counts = {}
        self.files = set()
        self.matches = 0
        self.invalid = False
        self.stopped = None

    def feed(self, text):
        if self.stopped:
            return self.stopped
        self.buffer += text
        while self.buffer:
            if self.backend != "ast" and self.mode == "files":
                if "\0" not in self.buffer:
                    break
                path, self.buffer = self.buffer.split("\0", 1)
                if path:
                    self.add_file(path)
            elif self.backend != "ast" and self.mode == "counts" and self.limit is None:
                sep = self.buffer.find("\0")
                end = self.buffer.find("\n", sep + 1) if sep >= 0 else -1
                if end < 0:
                    break
                path, count = self.buffer[:sep], self.buffer[sep + 1 : end]
                self.buffer = self.buffer[end + 1 :]
                try:
                    count = int(count)
                    if count < 0:
                        raise ValueError("negative count")
                    self.counts[path] = self.counts.get(path, 0) + count
                    self.matches += count
                except ValueError:
                    self.invalid = True
            else:
                if "\n" not in self.buffer:
                    break
                line, self.buffer = self.buffer.split("\n", 1)
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                    if self.backend == "ast":
                        from .search_backends import parse_ast

                        item = parse_ast(value)
                    elif isinstance(value, dict) and value.get("type") in {"match", "context"}:
                        item = self.parse_match(value)
                    else:
                        continue
                    if item is None:
                        self.invalid = True
                        continue
                except ValueError, KeyError, TypeError:
                    self.invalid = True
                    continue
                if self.backend == "rga":
                    item["coordinate_space"] = "extracted"
                    item["source_label"] = None
                    item["page"] = None
                    item["member"] = None
                else:
                    item["coordinate_space"] = "source"
                match = item.get("kind") == "match"
                count = 1
                if self.mode == "counts" and self.backend != "ast":
                    count = max(1, len(item.get("submatches", [])))
                if self.limit is not None and self.mode == "counts":
                    count = min(count, self.limit - self.matches)
                if match:
                    self.matches += count
                if self.mode == "exists" and match:
                    self.stopped = "matched"
                elif self.mode == "files" and match:
                    self.add_file(item["path"])
                elif self.mode == "counts" and match:
                    path = item["path"]
                    self.counts[path] = self.counts.get(path, 0) + count
                elif self.mode == "matches":
                    self.items.append(item)
            size = len(self.files) if self.mode == "files" else self.matches
            if not self.stopped and self.limit is not None and size >= self.limit:
                self.stopped = "scan_limit"
            if self.stopped:
                self.buffer = ""
                return self.stopped
        return None

    def add_file(self, path):
        if path not in self.files:
            self.files.add(path)
            self.items.append(path)

    def finish(self):
        if self.buffer.strip():
            self.invalid = True
        if self.mode == "counts":
            return [{"path": path, "count": count} for path, count in self.counts.items()]
        return self.items
