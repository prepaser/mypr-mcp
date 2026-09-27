# Git history API

`ws.git.log()` returns commit metadata and `ws.git.blame()` attributes committed lines. Both resolve the requested ref to a commit when the query starts, then keep that result in a bounded workspace snapshot. Later pages use the same snapshot even if the branch moves.

```python
page = await ws.git.log(path="src/app.py", author="Ada", since="2025-01-01")
commits = list(page["commits"])
while page["next_cursor"]:
    page = await ws.git.log(cursor=page["next_cursor"])
    commits.extend(page["commits"])

page = await ws.git.blame("src/app.py", start_line=10, end_line=30)
```

`log(ref="HEAD", *, path=None, author=None, since=None, until=None, cursor=None, max_entries=50, max_bytes=32768)` supports Git's author regular expression and date filter syntax. A path is workspace-relative (or an absolute path inside the repository) and matched literally. Each commit contains its hash, author, email, authored timestamp, and subject. The resolved commit hash is returned as `ref`.

`blame(path=None, ref="HEAD", *, start_line=None, end_line=None, cursor=None, max_entries=100, max_bytes=32768)` requires a path for a new query and accepts either both line bounds or neither. For later pages, pass only `cursor`. Each line contains its final and original line numbers, commit hash, author, email, author timestamp, summary, and source text.

Both methods return `has_more`, `next_cursor`, `snapshot_id`, `scan_truncated`, and warnings. Pass only the cursor to continue a query. `max_bytes` limits each page's item data and defaults to 32 KiB; one record larger than the requested budget raises an error. Git history collection is capped at 2 MiB per query. History snapshots retain at most 32 entries and 16 MiB under `.mypr/git-history/`; an expired cursor reports an error. These helpers read committed history and do not include uncommitted working tree changes.
