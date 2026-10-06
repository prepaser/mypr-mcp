"""Shared storage for tools resolved from official releases.

The store is intentionally independent from a workspace.  Construction and
inspection are read-only; directories are created only by an installation.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import zipfile
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from .async_utils import finish_owned, wait_owned
from .dependency_catalog import (
    BINARY_NAMES,
    CATALOG,
    MODEL_NAMES,
    Artifact,
    resolve_artifact,
)
from .file_io import read_bytes

_INSTALL_TIMEOUT = 300.0
_MAX_NETWORK_TASKS = 2
_CHUNK_SIZE = 1024 * 1024
_MAX_ARCHIVE_MEMBERS = 10_000
_MAX_EXTRACTED_BYTES = 512 * 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_VERSION_RE = re.compile(r"(?<!\d)(\d+)(?:\.(\d+))(?:\.(\d+))?(?!\d)")
_MODEL_MARKER_VERSION_RE = re.compile(r"\d+(?:\.\d+)+")
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_.:-]+$")
_SYSTEM_MINIMUMS = {
    "rg": "14.0.0",
    "ast-grep": "0.40.0",
    "rga": "0.10.0",
    "pandoc": "2.9.0",
}

Download = Callable[[str, Path, int], Awaitable[None]]


class DependencyError(RuntimeError):
    """Base error for dependency lookup and installation failures."""

    def __init__(self, message: str, *, code: str = "dependency_install_failed") -> None:
        super().__init__(message)
        self.code = code


class UnsupportedDependency(DependencyError):
    """The requested artifact is unknown or unsupported on this host."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="dependency_unsupported")


def _platform_key() -> str | None:
    if os.name != "posix" or not sys_platform_linux():
        return None
    libc, _version = platform.libc_ver()
    if libc.lower() != "glibc":
        return None
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        return "x86_64"
    if machine in {"aarch64", "arm64"}:
        return "aarch64"
    return None


def sys_platform_linux() -> bool:
    # Kept as a function so platform behavior can be replaced by focused tests.
    import sys

    return sys.platform.startswith("linux")


def _default_data_root() -> Path:
    value = os.environ.get("XDG_DATA_HOME")
    root = Path(value).expanduser() if value else Path.home() / ".local" / "share"
    return (root if root.is_absolute() else Path.home() / ".local" / "share") / "mypr"


def _default_cache_root() -> Path:
    value = os.environ.get("XDG_CACHE_HOME")
    root = Path(value).expanduser() if value else Path.home() / ".cache"
    return (root if root.is_absolute() else Path.home() / ".cache") / "mypr"


def _version_tuple(text: str) -> tuple[int, int, int] | None:
    match = _VERSION_RE.search(text)
    if match is None:
        return None
    return tuple(int(part or 0) for part in match.groups())  # type: ignore[return-value]


def _display_version(text: str) -> str:
    match = _VERSION_RE.search(text)
    return ".".join(part for part in match.groups() if part) if match else text[:64]


def _compatible(version: str | None, required: str) -> bool:
    if version is None:
        return False
    actual = _version_tuple(version)
    expected = _version_tuple(required)
    if actual is None or expected is None:
        return False
    return actual >= expected


def _safe_relative(path: str) -> Path:
    candidate = Path(path)
    if (
        not candidate.parts
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        raise DependencyError("archive contains an unsafe path")
    return candidate


def _safe_extract(
    archive: Path,
    destination: Path,
    kind: str,
    *,
    max_bytes: int = _MAX_EXTRACTED_BYTES,
    allowed_links: tuple[str, ...] = (),
) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    members = 0
    extracted_bytes = 0
    if kind == "tar.gz":
        with tarfile.open(archive, "r:gz") as stream:
            for member in stream.getmembers():
                members += 1
                if members > _MAX_ARCHIVE_MEMBERS:
                    raise DependencyError("archive contains too many members")
                relative = _safe_relative(member.name)
                if member.issym() or member.islnk():
                    if Path(member.name).name in allowed_links:
                        raise DependencyError("requested executable is a link")
                    _safe_link_target(member.linkname)
                    continue
                if not (member.isdir() or member.isreg()):
                    raise DependencyError("archive contains a link or special file")
                if member.isreg():
                    extracted_bytes += member.size
                    if extracted_bytes > max_bytes:
                        raise DependencyError("archive expands beyond its size limit")
                target = destination / relative
                if not target.resolve().is_relative_to(destination.resolve()):
                    raise DependencyError("archive path escapes its destination")
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    extracted = stream.extractfile(member)
                    if extracted is None:
                        raise DependencyError("archive member cannot be read")
                    with target.open("xb") as output:
                        _copy_limited(extracted, output, max_bytes, extracted_bytes - member.size)
    elif kind == "zip":
        with zipfile.ZipFile(archive) as stream:
            for member in stream.infolist():
                members += 1
                if members > _MAX_ARCHIVE_MEMBERS:
                    raise DependencyError("archive contains too many members")
                relative = _safe_relative(member.filename)
                mode = member.external_attr >> 16
                file_type = stat.S_IFMT(mode)
                if file_type == stat.S_IFLNK:
                    if Path(member.filename).name in allowed_links:
                        raise DependencyError("requested executable is a link")
                    continue
                if file_type not in {0, stat.S_IFREG, stat.S_IFDIR}:
                    raise DependencyError("archive contains a link or special file")
                target = destination / relative
                if not target.resolve().is_relative_to(destination.resolve()):
                    raise DependencyError("archive path escapes its destination")
                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    extracted_bytes += member.file_size
                    if extracted_bytes > max_bytes:
                        raise DependencyError("archive expands beyond its size limit")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open("xb") as output, stream.open(member) as source:
                        _copy_limited(source, output, max_bytes, extracted_bytes - member.file_size)
    else:
        raise DependencyError(f"unsupported archive type: {kind}")


def _safe_link_target(value: str) -> None:
    target = Path(value)
    if target.is_absolute() or any(part in {"", ".", ".."} for part in target.parts):
        raise DependencyError("archive link target is unsafe")


def _copy_limited(source, destination, maximum: int, prior: int) -> None:
    copied = prior
    while chunk := source.read(_CHUNK_SIZE):
        copied += len(chunk)
        if copied > maximum:
            raise DependencyError("archive expands beyond its size limit")
        destination.write(chunk)


def _find_executable(root: Path, names: tuple[str, ...]) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for path in root.rglob("*"):
        if not path.is_file() or path.is_symlink() or path.name not in names:
            continue
        mode = path.stat().st_mode
        if not mode & stat.S_IXUSR:
            path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        found[path.name] = path
    missing = [name for name in names if name not in found]
    if missing:
        raise DependencyError(f"archive did not contain: {', '.join(missing)}")
    return found


def _open_lock(path: Path, deadline: float):
    import fcntl

    descriptor = os.open(
        path,
        os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    stream = os.fdopen(descriptor, "a+")
    try:
        while True:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return stream
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "timed out waiting for dependency installation lock"
                    ) from None
                time.sleep(min(0.1, max(0.001, deadline - time.monotonic())))
    except BaseException:
        stream.close()
        raise


def _close_lock(stream) -> None:
    import fcntl

    with contextlib.suppress(OSError):
        fcntl.flock(stream, fcntl.LOCK_UN)
    stream.close()


class DependencyStore:
    """Resolve official releases and share installations across workspaces."""

    def __init__(
        self,
        data_root: str | os.PathLike[str] | None = None,
        cache_root: str | os.PathLike[str] | None = None,
        *,
        downloader: Download | None = None,
        platform_key: str | None = None,
    ) -> None:
        self.data_root = Path(data_root or _default_data_root()).expanduser().resolve()
        self.cache_root = Path(cache_root or _default_cache_root()).expanduser().resolve()
        self.bin_root = self.data_root / "bin"
        self.model_root = self.data_root / "models"
        self._tools_root = self.data_root / "tools"
        self._locks_root = self.data_root / "locks"
        self._downloader = downloader
        self._platform = _platform_key() if platform_key is None else platform_key
        self._network = asyncio.Semaphore(_MAX_NETWORK_TASKS)
        self._guard = asyncio.Lock()
        self._inflight: dict[str, asyncio.Task[dict[str, Any]]] = {}
        self._closed = False

    @property
    def catalog_names(self) -> tuple[str, ...]:
        return (*BINARY_NAMES, *MODEL_NAMES)

    def names(self, kind: str | None = None) -> tuple[str, ...]:
        if kind is None:
            return self.catalog_names
        if kind == "binary":
            return BINARY_NAMES
        if kind == "model":
            return MODEL_NAMES
        raise ValueError("kind must be binary, model, or None")

    async def list(self, kind: str | None = None) -> list[dict[str, Any]]:
        return [await self.inspect(name) for name in self.names(kind)]

    async def inspect(self, name: str) -> dict[str, Any]:
        artifact = CATALOG.get(name)
        if artifact is None:
            return {
                "name": name,
                "kind": "unknown",
                "scope": "global",
                "source": None,
                "status": "unsupported",
                "version": None,
                "path": None,
                "reason": "dependency is not in the catalog",
            }
        if artifact.kind == "binary" and self._platform is None:
            return self._state(
                artifact, "unsupported", reason="requires Linux glibc x86_64/aarch64"
            )
        if artifact.kind == "binary":
            system = await asyncio.to_thread(self._inspect_system, artifact)
            shared = await asyncio.to_thread(self._inspect_shared_binary, artifact)
            if system is not None and system["status"] == "installed":
                return system
            if shared["status"] == "installed":
                return shared
            return system or shared
        return await asyncio.to_thread(self._inspect_model, artifact)

    async def ensure(self, name: str) -> dict[str, Any]:
        state = await self.inspect(name)
        if self._closed:
            raise RuntimeError("Dependency store is closed")
        if state["status"] == "installed" and not self._needs_model_copy(state):
            return state
        if state["status"] == "unsupported":
            raise UnsupportedDependency(state.get("reason") or f"unsupported dependency: {name}")
        async with self._guard:
            if self._closed:
                raise RuntimeError("Dependency store is closed")
            task = self._inflight.get(name)
            if task is None:
                task = asyncio.create_task(self._install(name), name=f"mypr-install:{name}")
                self._inflight[name] = task
                task.add_done_callback(
                    lambda task, name=name: self._release_inflight(name, task)
                )
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self._guard:
                    if self._inflight.get(name) is task:
                        self._inflight.pop(name, None)

    async def install(self, name: str) -> dict[str, Any]:
        state = await self.inspect(name)
        if self._closed:
            raise RuntimeError("Dependency store is closed")
        if state["status"] == "unsupported":
            raise UnsupportedDependency(state.get("reason") or f"unsupported dependency: {name}")
        async with self._guard:
            if self._closed:
                raise RuntimeError("Dependency store is closed")
            task = self._inflight.get(name)
            if task is None:
                task = asyncio.create_task(self._install(name), name=f"mypr-install:{name}")
                self._inflight[name] = task
                task.add_done_callback(
                    lambda task, name=name: self._release_inflight(name, task)
                )
        try:
            result = await asyncio.shield(task)
            if result.get("source") == "system":
                artifact = CATALOG.get(name)
                if artifact is not None and artifact.kind == "binary":
                    result = await asyncio.to_thread(self._inspect_shared_binary, artifact)
            return result
        finally:
            if task.done():
                async with self._guard:
                    if self._inflight.get(name) is task:
                        self._inflight.pop(name, None)

    async def _install(self, name: str) -> dict[str, Any]:
        artifact = CATALOG.get(name)
        if artifact is None:
            raise UnsupportedDependency(f"dependency is not in the catalog: {name}")
        if artifact.kind == "binary" and self._platform is None:
            raise UnsupportedDependency("requires Linux glibc x86_64/aarch64")
        try:
            async with asyncio.timeout(_INSTALL_TIMEOUT):
                await wait_owned(
                    asyncio.to_thread(self._locks_root.mkdir, parents=True, exist_ok=True)
                )
                lock_path = self._locks_root / f"{_lock_name(name)}.lock"
                stream, lock_cancelled = await finish_owned(
                    asyncio.to_thread(_open_lock, lock_path, time.monotonic() + _INSTALL_TIMEOUT)
                )
                try:
                    if lock_cancelled:
                        raise asyncio.CancelledError
                    current = await wait_owned(
                        asyncio.to_thread(
                            self._inspect_shared_binary
                            if artifact.kind == "binary"
                            else self._inspect_model,
                            artifact,
                        )
                    )
                    if current["source"] == "shared" and current["status"] == "installed":
                        if self._needs_model_copy(current):
                            destination = (
                                self.model_root / "tessdata_fast" / Path(current["path"]).name
                            )
                            await wait_owned(
                                asyncio.to_thread(
                                    _publish_model,
                                    Path(current["path"]),
                                    destination,
                                    replace(artifact, version=current["version"]),
                                )
                            )
                            return await asyncio.to_thread(self._inspect_model, artifact)
                        return current
                    async with self._network:
                        try:
                            artifact = await resolve_artifact(name, system=self._platform)
                        except (ValueError, OSError) as exc:
                            raise DependencyError(
                                str(exc), code="dependency_download_failed"
                            ) from exc
                        if artifact.kind == "model":
                            await self._install_model(artifact)
                        else:
                            await self._install_binary(artifact)
                finally:
                    await wait_owned(asyncio.to_thread(_close_lock, stream), propagate=False)
            if artifact.kind == "binary":
                shared = await asyncio.to_thread(self._inspect_shared_binary, artifact)
                if shared["status"] == "installed":
                    return shared
            else:
                shared = await asyncio.to_thread(self._inspect_model, artifact)
                if shared["status"] == "installed":
                    return shared
            return await self.inspect(name)
        except asyncio.CancelledError:
            raise
        except PermissionError as exc:
            raise DependencyError(str(exc), code="dependency_permission_denied") from exc
        except TimeoutError as exc:
            code = "dependency_install_failed"
            if "lock" not in str(exc).lower():
                code = "dependency_download_failed"
            raise DependencyError(str(exc) or f"timed out installing {name}", code=code) from exc

    async def _install_binary(self, artifact: Artifact) -> None:
        assert self._platform is not None
        asset = artifact
        await wait_owned(asyncio.to_thread(self._tools_root.mkdir, parents=True, exist_ok=True))
        temporary_download = await self._download_to_cache(asset)
        temporary_root_raw, root_cancelled = await finish_owned(
            asyncio.to_thread(tempfile.mkdtemp, prefix=f".{artifact.name}-", dir=self._tools_root)
        )
        temporary_root = Path(temporary_root_raw)
        try:
            if root_cancelled:
                raise asyncio.CancelledError
            payload = temporary_root / "payload"
            await wait_owned(
                asyncio.to_thread(
                    _safe_extract,
                    temporary_download,
                    payload,
                    asset.archive,
                    allowed_links=asset.executables,
                )
            )
            found = await wait_owned(
                asyncio.to_thread(_find_executable, payload, asset.executables)
            )
            try:
                staged_version = await wait_owned(
                    asyncio.to_thread(_run_version, found[asset.executables[0]])
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise DependencyError(
                    f"downloaded executable failed its version probe: {exc}"
                ) from exc
            if not _compatible(staged_version, asset.version):
                raise DependencyError("downloaded executable has an incompatible version")
            files = {
                executable: str(found[executable].relative_to(payload))
                for executable in asset.executables
            }
            metadata = {
                "name": artifact.name,
                "version": asset.version,
                "platform": self._platform,
                "sha256": asset.sha256,
                "version_output": staged_version[:512],
                "files": {
                    name: {"path": relative, "sha256": _sha256(payload / relative)}
                    for name, relative in files.items()
                },
            }
            (payload / ".mypr-complete.json").write_text(
                json.dumps(metadata, sort_keys=True) + "\n"
            )
            target = self._tools_root / artifact.name / asset.version / self._platform
            target.parent.mkdir(parents=True, exist_ok=True)
            backup = target.parent / f".{target.name}.{os.getpid()}.old"
            if target.exists():
                with contextlib.suppress(FileNotFoundError):
                    shutil.rmtree(backup)
                os.replace(target, backup)
            try:
                os.replace(payload, target)
            except BaseException:
                with contextlib.suppress(FileNotFoundError):
                    shutil.rmtree(target)
                if backup.exists():
                    os.replace(backup, target)
                raise
            else:
                with contextlib.suppress(FileNotFoundError):
                    shutil.rmtree(backup)
            for executable in asset.executables:
                alias = self.bin_root / executable
                source = target / found[executable].relative_to(payload)
                self._publish_alias(alias, source)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                shutil.rmtree(temporary_root)
            raise
        finally:
            with contextlib.suppress(FileNotFoundError):
                await wait_owned(
                    asyncio.to_thread(temporary_download.unlink, missing_ok=True), propagate=False
                )

    async def _install_model(self, artifact: Artifact) -> None:
        model_dir = self._model_dir(artifact)
        if model_dir.is_symlink():
            raise DependencyError("model directory is unsafe")
        temporary = await self._download_to_cache(artifact)
        try:
            await wait_owned(asyncio.to_thread(model_dir.mkdir, parents=True, exist_ok=True))
            destination = model_dir / f"{artifact.name.removeprefix('tessdata:')}.traineddata"
            await wait_owned(asyncio.to_thread(_publish_model, temporary, destination, artifact))
        finally:
            with contextlib.suppress(FileNotFoundError):
                await wait_owned(
                    asyncio.to_thread(temporary.unlink, missing_ok=True), propagate=False
                )

    async def _download_to_cache(self, artifact: Artifact) -> Path:
        await wait_owned(asyncio.to_thread(self._cache_root_prepare))
        (descriptor, raw_path), temporary_cancelled = await finish_owned(
            asyncio.to_thread(
                tempfile.mkstemp,
                prefix=f".{_lock_name(artifact.name)}-",
                suffix=".tmp",
                dir=self.cache_root / "downloads",
            )
        )
        # Close the descriptor before a downloader opens the path on platforms
        # that do not permit shared handles.
        os.close(descriptor)
        temporary = Path(raw_path)
        keep = False
        try:
            if temporary_cancelled:
                raise asyncio.CancelledError
            if self._downloader is not None:
                await self._downloader(artifact.url, temporary, artifact.max_bytes)
            else:
                await self._download_http(artifact.url, temporary, artifact.max_bytes)
            digest = await wait_owned(asyncio.to_thread(_sha256, temporary))
            if artifact.sha256 is not None and digest != artifact.sha256:
                raise DependencyError(
                    f"SHA-256 mismatch for {artifact.name}", code="dependency_integrity_error"
                )
            if artifact.git_sha1 is not None:
                blob_digest = await wait_owned(asyncio.to_thread(_git_sha1, temporary))
                if blob_digest != artifact.git_sha1:
                    raise DependencyError(
                        f"Git blob digest mismatch for {artifact.name}",
                        code="dependency_integrity_error",
                    )
            keep = True
            return temporary
        except DependencyError:
            raise
        except PermissionError as exc:
            raise DependencyError(str(exc), code="dependency_permission_denied") from exc
        except OSError as exc:
            raise DependencyError(str(exc), code="dependency_download_failed") from exc
        except BaseException:
            raise
        finally:
            if not keep:
                with contextlib.suppress(FileNotFoundError):
                    await wait_owned(
                        asyncio.to_thread(temporary.unlink, missing_ok=True), propagate=False
                    )

    def _cache_root_prepare(self) -> None:
        (self.cache_root / "downloads").mkdir(parents=True, exist_ok=True)

    async def _download_http(self, url: str, destination: Path, maximum: int) -> None:
        from urllib.parse import urlparse

        if urlparse(url).scheme != "https":
            raise DependencyError(
                "dependency downloads require HTTPS", code="dependency_download_failed"
            )
        import httpx2

        received = 0
        try:
            async with httpx2.AsyncClient(follow_redirects=True, timeout=30.0) as client:
                async with client.stream("GET", url) as response:
                    if response.url.scheme != "https":
                        raise DependencyError(
                            "dependency redirects must remain HTTPS",
                            code="dependency_download_failed",
                        )
                    response.raise_for_status()
                    length = response.headers.get("content-length")
                    if length is not None and int(length) > maximum:
                        raise DependencyError(f"download exceeds the {maximum} byte limit")
                    with destination.open("wb") as output:
                        async for chunk in response.aiter_bytes(_CHUNK_SIZE):
                            received += len(chunk)
                            if received > maximum:
                                raise DependencyError(f"download exceeds the {maximum} byte limit")
                            output.write(chunk)
                        output.flush()
                        os.fsync(output.fileno())
        except DependencyError:
            raise
        except Exception as exc:
            raise DependencyError(str(exc)[:512], code="dependency_download_failed") from exc

    def _inspect_system(self, artifact: Artifact) -> dict[str, Any] | None:
        names = artifact.executables
        candidates = (artifact.name, *names)
        if artifact.name == "ast-grep":
            candidates += ("sg",)
        for executable in candidates:
            path = shutil.which(executable)
            if path is None:
                continue
            raw_path = Path(path).absolute()
            candidate = raw_path.resolve()
            if _inside(raw_path, self.data_root) or _inside(candidate, self.data_root):
                continue
            try:
                result = _run_version(candidate)
            except (OSError, subprocess.TimeoutExpired) as exc:
                return self._state(
                    artifact,
                    "unusable",
                    source="system",
                    path=candidate,
                    reason=f"cannot run system tool: {exc}",
                )
            if not _compatible(
                result,
                _SYSTEM_MINIMUMS.get(artifact.name, artifact.version),
            ):
                return self._state(
                    artifact,
                    "unusable",
                    source="system",
                    reason=f"system {candidate} reports an incompatible version",
                    path=candidate,
                )
            return self._state(
                artifact, "installed", source="system", path=candidate, version=result
            )
        return None

    def _inspect_shared_binary(self, artifact: Artifact) -> dict[str, Any]:
        assert self._platform is not None
        if artifact.version:
            targets = [self._tools_root / artifact.name / artifact.version / self._platform]
        else:
            root = self._tools_root / artifact.name
            if not root.is_dir() or root.is_symlink():
                return self._state(artifact, "missing")
            targets = [
                path / self._platform
                for path in sorted(
                    root.iterdir(),
                    key=lambda path: _version_tuple(path.name) or (0, 0, 0),
                    reverse=True,
                )
                if _version_tuple(path.name) is not None
            ]
        problem = None
        for target in targets:
            state = self._inspect_binary_target(artifact, target)
            if state["status"] == "installed":
                return state
            if state["status"] == "unusable" and problem is None:
                problem = state
        return problem or self._state(artifact, "missing")

    def _inspect_binary_target(self, artifact: Artifact, target: Path) -> dict[str, Any]:
        if not target.exists():
            return self._state(artifact, "missing")
        if (
            target.is_symlink()
            or target.parent.is_symlink()
            or not target.is_dir()
            or not _inside(target.resolve(), self._tools_root)
        ):
            return self._state(
                artifact, "unusable", source="shared", reason="managed artifact path is unsafe"
            )
        marker = target / ".mypr-complete.json"
        if not marker.is_file() or marker.is_symlink():
            return self._state(
                artifact, "unusable", source="shared", reason="managed artifact is unverified"
            )
        try:
            metadata = json.loads(
                read_bytes(marker, max_bytes=_MAX_METADATA_BYTES, follow_symlinks=False)
            )
            if (
                not isinstance(metadata, dict)
                or metadata.get("name") != artifact.name
                or metadata.get("version") != target.parent.name
                or metadata.get("platform") != self._platform
                or not isinstance(metadata.get("files"), dict)
            ):
                raise ValueError
            paths = {}
            for name in artifact.executables:
                item = metadata["files"][name]
                relative = _safe_relative(item["path"])
                path = target / relative
                if (
                    not path.is_file()
                    or path.is_symlink()
                    or not path.resolve().is_relative_to(target.resolve())
                    or not os.access(path, os.X_OK)
                    or _sha256(path) != item["sha256"]
                ):
                    raise ValueError
                paths[name] = path
        except OSError, KeyError, TypeError, ValueError, DependencyError:
            return self._state(
                artifact,
                "unusable",
                source="shared",
                reason="managed artifact digest verification failed",
            )
        main = paths[artifact.executables[0]]
        try:
            version = _run_version(main)
        except OSError, subprocess.TimeoutExpired:
            return self._state(
                artifact,
                "unusable",
                source="shared",
                reason="managed tool failed its version probe",
            )
        if not _compatible(version, metadata["version"]) or not _compatible(
            version, _SYSTEM_MINIMUMS[artifact.name]
        ):
            return self._state(
                artifact,
                "unusable",
                source="shared",
                reason="managed tool has an incompatible version",
            )
        return self._state(artifact, "installed", source="shared", path=main, version=version)

    def _inspect_model(self, artifact: Artifact) -> dict[str, Any]:
        model_dir = self._model_dir(artifact)
        language = artifact.name.removeprefix("tessdata:")
        path = model_dir / f"{language}.traineddata"
        if model_dir.is_symlink():
            return self._state(
                artifact,
                "unusable",
                source="shared",
                model_dir=model_dir,
                reason="model directory is unsafe",
            )
        if not path.is_file() or path.is_symlink():
            return self._state(artifact, "missing", model_dir=model_dir)
        marker = model_dir / f".{language}.mypr-complete.json"
        version = artifact.version or _display_version(model_dir.name)
        if marker.exists() or marker.is_symlink():
            try:
                if marker.is_symlink():
                    raise ValueError
                metadata = json.loads(
                    read_bytes(marker, max_bytes=_MAX_METADATA_BYTES, follow_symlinks=False)
                )
                if (
                    not isinstance(metadata, dict)
                    or metadata.get("name") != artifact.name
                    or not isinstance(metadata.get("version"), str)
                    or _MODEL_MARKER_VERSION_RE.fullmatch(metadata.get("version", "")) is None
                    or _sha256(path) != metadata.get("sha256")
                ):
                    raise ValueError
                version = metadata["version"]
            except OSError, TypeError, ValueError:
                return self._state(
                    artifact,
                    "unusable",
                    source="shared",
                    model_dir=model_dir,
                    reason="model digest verification failed",
                )
        elif model_dir.name == "tessdata_fast":
            return self._state(
                artifact,
                "unusable",
                source="shared",
                model_dir=model_dir,
                reason="managed model is unverified",
            )
        if artifact.sha256 is not None and _sha256(path) != artifact.sha256:
            return self._state(
                artifact,
                "unusable",
                source="shared",
                model_dir=model_dir,
                reason="model hash mismatch",
            )
        return self._state(
            replace(artifact, version=version),
            "installed",
            source="shared",
            path=path,
            model_dir=model_dir,
        )

    def _model_dir(self, artifact: Artifact) -> Path:
        current = self.model_root / "tessdata_fast"
        if current.is_symlink():
            return current
        legacy = sorted(
            (
                path
                for path in self.model_root.glob("tessdata_fast-*")
                if path.is_dir() and not path.is_symlink() and _version_tuple(path.name) is not None
            ),
            key=lambda path: _version_tuple(path.name) or (0, 0, 0),
            reverse=True,
        )
        filename = f"{artifact.name.removeprefix('tessdata:')}.traineddata"
        for directory in (current, *legacy):
            if (directory / filename).is_file() and not (directory / filename).is_symlink():
                return directory
        return current if current.exists() or not legacy else legacy[0]

    def _needs_model_copy(self, state: dict[str, Any]) -> bool:
        current = self.model_root / "tessdata_fast"
        return (
            state["kind"] == "model"
            and current.is_dir()
            and not current.is_symlink()
            and state.get("model_dir") != str(current.resolve())
        )

    @staticmethod
    def _state(
        artifact: Artifact,
        status: str,
        *,
        source: str | None = None,
        path: Path | None = None,
        model_dir: Path | None = None,
        version: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": artifact.name,
            "kind": artifact.kind,
            "scope": "global",
            "source": source,
            "status": status,
            "version": _display_version(version)
            if version is not None
            else (artifact.version if status == "installed" else None),
            "path": str(path.resolve()) if path is not None else None,
        }
        if model_dir is not None:
            result["model_dir"] = str(model_dir.resolve())
        if reason:
            result["reason"] = reason
        return result

    def _publish_alias(self, alias: Path, source: Path) -> None:
        self.bin_root.mkdir(parents=True, exist_ok=True)
        if alias.exists() or alias.is_symlink():
            if alias.is_symlink() and _inside(alias.resolve(strict=False), self._tools_root):
                temporary = self.bin_root / f".{alias.name}.{os.getpid()}.tmp"
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()
                temporary.symlink_to(source)
                os.replace(temporary, alias)
            return
        temporary = self.bin_root / f".{alias.name}.{os.getpid()}.tmp"
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        temporary.symlink_to(source)
        os.replace(temporary, alias)

    async def close(self) -> None:
        self._closed = True
        async with self._guard:
            tasks = list(self._inflight.values())
            self._inflight.clear()
        if tasks:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _release_inflight(self, name: str, task: asyncio.Task[Any]) -> None:
        if self._inflight.get(name) is task:
            self._inflight.pop(name, None)
        if not task.cancelled():
            task.exception()


def _lock_name(name: str) -> str:
    if not isinstance(name, str) or not _SAFE_NAME_RE.fullmatch(name):
        raise ValueError("invalid dependency name")
    return name.replace(":", "-")


def _publish_model(source: Path, destination: Path, artifact: Artifact) -> None:
    temporary = None
    try:
        with (
            tempfile.NamedTemporaryFile(
                dir=destination.parent, prefix=f".{destination.name}.", delete=False
            ) as output,
            source.open("rb") as input,
        ):
            temporary = Path(output.name)
            shutil.copyfileobj(input, output, _CHUNK_SIZE)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        metadata = {
            "name": artifact.name,
            "version": artifact.version,
            "url": artifact.url,
            "sha256": _sha256(destination),
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.stem}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            json.dump(metadata, output, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination.parent / f".{destination.stem}.mypr-complete.json")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _run_version(path: Path) -> str:
    import subprocess

    result = subprocess.run(
        [str(path), "--version"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=5,
        text=True,
    )
    if result.returncode:
        raise OSError(f"version command exited with {result.returncode}")
    return result.stdout[:512]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _git_sha1(path: Path) -> str:
    digest = hashlib.sha1(f"blob {path.stat().st_size}\0".encode(), usedforsecurity=False)
    with path.open("rb") as stream:
        while chunk := stream.read(_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["DependencyError", "DependencyStore", "UnsupportedDependency"]
