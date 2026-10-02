# Git history API

`ws.git.log()` returns commit metadata and `ws.git.blame()` attributes committed lines. Both resolve the requested ref to a commit when the query starts, then keep that result in a bounded workspace snapshot. Later pages use the same snapshot even if the branch moves. Pass `follow=True` to `log()` when a path should follow Git renames.

```python
page = await ws.git.log(path="src/app.py", author="Ada", since="2025-01-01")
commits = list(page["commits"])
while page["next_cursor"]:
    page = await ws.git.log(cursor=page["next_cursor"])
    commits.extend(page["commits"])

page = await ws.git.blame("src/app.py", start_line=10, end_line=30)
```

`await ws.git.commit_info(ref, include_files=True, include_patch=False)` returns one commit's parents, author and committer, subject, body, changed files, and insert/delete statistics. Patch text is opt-in and remains bounded by `max_bytes`. Files, statistics, and patch text compare against the first parent, including merge commits. `commit.comparison_base` identifies that parent's hash; it is `None` for a root commit, whose files are compared against an empty tree. The method is read-only and does not change the repository.

`log(ref="HEAD", *, path=None, author=None, since=None, until=None, cursor=None, max_entries=50, max_bytes=32768)` supports Git's author regular expression and date filter syntax. A path is workspace-relative (or an absolute path inside the repository) and matched literally. Each commit contains its hash, author, email, authored timestamp, and subject. The resolved commit hash is returned as `ref`.

`blame(path=None, ref="HEAD", *, start_line=None, end_line=None, cursor=None, max_entries=100, max_bytes=32768)` requires a path for a new query and accepts either both line bounds or neither. For later pages, pass only `cursor`. Each line contains its final and original line numbers, commit hash, author, email, author timestamp, summary, and source text.

Both methods return `has_more`, `next_cursor`, `snapshot_id`, `scan_truncated`, and warnings. Pass only the cursor to continue a query. A continuation may also repeat values from the original query; matching values are accepted, conflicting values are rejected. `max_entries` on record pages and `max_bytes` on every page are adjustable continuation budgets. The snapshot retains the original query and resolved commit, so a ref moving after the first page does not change the result. `max_bytes` limits each page's item data and defaults to 32 KiB; one record larger than the requested budget raises an error. Git history collection is capped at 2 MiB per query. History snapshots retain at most 32 entries and 16 MiB under `.mypr/git-history/`; an expired cursor reports an error. These helpers read committed history and do not include uncommitted working tree changes.
