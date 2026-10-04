"""Apply configuration snapshots without replacing the workspace kernel."""

from __future__ import annotations

import asyncio
import copy

from .async_utils import wait_owned
from .config import (
    ConfigStore,
    _validate_dependencies,
    parse_path,
    validate_mail_config,
    validate_web_config,
)
from .diagnostics import safe_error

HOT_LIMITS = (
    "response_bytes", "execute_wait_ms", "poll_wait_ms", "completed_records", "cache_bytes",
)
STARTUP_LIMITS = ("output_bytes", "completed_tasks")


class RuntimeConfig:
    def __init__(self, runtime, store, snapshot):
        self.runtime = runtime
        self.store = store
        self.applied = store.get(snapshot=snapshot)
        self.applying = False
        self._lock = asyncio.Lock()
        self.lsp_generation: str | None = None
        self.lsp_sequence = 0

    async def dispatch(self, request):
        method = request.get("method")
        if method == "reload":
            return await wait_owned(self.reload(request.get("force", False)))
        scope = request.get("scope", "effective" if method == "get" else "workspace")
        store = (
            ConfigStore(None, global_path=self.store.global_path)
            if scope == "global" and method in {"get", "set", "unset"} else self.store
        )
        snapshot = await wait_owned(asyncio.to_thread(store.load))
        path = request.get("path")
        if method == "get":
            return store.get(path, scope, snapshot)
        if method == "explain":
            result = self.store.explain(path, snapshot)
            desired = self.store.get(path, snapshot=snapshot)
            applied = self._applied_value(path)
            parts = parse_path(path)
            result.update(
                desired=desired, applied=applied, pending=desired != applied,
                restart_required=any(
                    tuple(field.split("."))[:len(parts)] == parts
                    for field in self.restart_required(snapshot)
                ),
            )
            return result
        if method not in {"set", "unset"}:
            raise ValueError("Unknown configuration operation")
        async with self._lock:
            self._check_mutation()
            arguments = [path]
            if method == "set":
                arguments.append(request["value"])
            saved = await wait_owned(asyncio.to_thread(
                getattr(store, method), *arguments, scope=scope,
                expected_revision=snapshot.revision,
            ))
            await self._record(method, scope=scope, path=path, revision=saved.revision)
        return {"saved": True, "revision": saved.revision}

    def _check_mutation(self):
        if self.applying:
            raise RuntimeError("Configuration reload is in progress; retry after it completes")
        runtime = self.runtime
        if runtime.stopping.is_set() or runtime.resetting or runtime.restarting:
            raise RuntimeError("Workspace lifecycle change is in progress")
        if not runtime.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")

    def _applied_value(self, path):
        values = copy.deepcopy(self.applied)
        if self.runtime.mcp is not None:
            values["mcp"]["servers"] = copy.deepcopy(self.runtime.mcp.config)
        for key in HOT_LIMITS:
            attribute = "response_limit" if key == "response_bytes" else key
            values["limits"][key] = getattr(self.runtime, attribute)
        values["storage"] = copy.deepcopy(self.runtime.storage_policy)
        mail = getattr(self.runtime, "mail", None)
        if mail is not None:
            configured = getattr(mail, "applied_config", None)
            if configured is None:
                configured = self.applied.get("mail")
            if isinstance(configured, dict):
                values["mail"] = copy.deepcopy(configured)
        web = getattr(self.runtime, "web", None)
        if web is not None:
            configured = getattr(web, "applied_config", None)
            if configured is None:
                configured = self.applied.get("web")
            if isinstance(configured, dict):
                values["web"] = copy.deepcopy(configured)
        dependencies = getattr(self.runtime, "dependencies", None)
        if dependencies is not None:
            configured = getattr(dependencies, "config", None)
            if isinstance(configured, dict):
                values["dependencies"] = copy.deepcopy(configured)
        value = values
        for part in parse_path(path):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return copy.deepcopy(value)

    def restart_required(self, snapshot):
        return [
            f"limits.{key}" for key in STARTUP_LIMITS
            if snapshot.values["limits"][key] != self.applied["limits"][key]
        ]

    def reset_lsp_generation(self, generation):
        if not isinstance(generation, str) or not generation:
            raise ValueError("kernel generation must be a non-empty string")
        self.lsp_generation = generation
        self.lsp_sequence = 0

    def record_lsp(self, definitions, *, sequence=None, generation=None):
        if generation is None:
            generation = self.lsp_generation or self.runtime.generation
        if generation != self.runtime.generation:
            return False
        if sequence is not None:
            if type(sequence) is not int or sequence < 0:
                return False
            if self.lsp_generation == generation and sequence <= self.lsp_sequence:
                if (
                    sequence == self.lsp_sequence
                    and self.applied["lsp"].get("servers") == definitions
                ):
                    return True
                return False
            self.lsp_generation = generation
            self.lsp_sequence = sequence
        elif self.lsp_generation is None:
            self.lsp_generation = generation
        self.applied["lsp"]["servers"] = copy.deepcopy(definitions)
        return True

    def reconcile_late_lsp(self, snapshot):
        if not isinstance(snapshot, dict):
            return False
        generation = snapshot.get("generation")
        if generation != self.runtime.generation:
            return False
        sequence = snapshot.get("sequence")
        if type(sequence) is not int or sequence < 0:
            return False
        definitions = snapshot.get("definitions")
        if not isinstance(definitions, dict):
            return False
        if not self.record_lsp(definitions, sequence=sequence, generation=generation):
            return False
        self.runtime.config = copy.deepcopy(self.applied)
        return True

    async def _record(self, method, **fields):
        if self.runtime.history is not None:
            await self.runtime.io(
                self.runtime.history.append, "config", method, fields, critical=True,
            )

    async def reload(self, force=False):
        if type(force) is not bool:
            raise TypeError("force must be a boolean")
        runtime = self.runtime
        async with self._lock:
            self._check_mutation()
            async with runtime._admission_lock:
                self._check_mutation()
                self.applying = True
                generation = runtime.generation
                bridge = runtime.mcp
                if bridge is not None:
                    bridge._config_applying = True
        try:
            if bridge is None:
                raise RuntimeError("Workspace configuration is not ready")
            async with bridge._mutation_lock:
                snapshot = await wait_owned(asyncio.to_thread(self.store.load))
            values = self.store.get(snapshot=snapshot)
            result = {
                "revision": snapshot.revision, "applied": {}, "deferred": {},
                "errors": {}, "restart_required": self.restart_required(snapshot),
            }
            async with runtime._admission_lock:
                if (
                    generation != runtime.generation or runtime.resetting
                    or runtime.stopping.is_set()
                ):
                    raise RuntimeError("Workspace changed during configuration reload")
                limits = values["limits"]
                changed = []
                for key in HOT_LIMITS:
                    attribute = "response_limit" if key == "response_bytes" else key
                    if getattr(runtime, attribute) != limits[key]:
                        changed.append(f"limits.{key}")
                    setattr(runtime, attribute, limits[key])
                    self.applied["limits"][key] = limits[key]
                runtime._trim_completed()
                runtime.shells.set_retention(runtime.completed_records, runtime.cache_bytes // 2)
                result["applied"]["manager"] = changed
                changed_storage = values["storage"] != runtime.storage_policy
                runtime.storage_policy = copy.deepcopy(values["storage"])
                self.applied["storage"] = copy.deepcopy(runtime.storage_policy)
                if changed_storage:
                    runtime._storage_wake.set()
                result["applied"]["storage"] = changed_storage
            try:
                dependencies_result = self._apply_dependencies(values["dependencies"])
                if dependencies_result.get("applied") is not None:
                    result["applied"]["dependencies"] = dependencies_result.get("applied")
                if dependencies_result.get("deferred"):
                    result["deferred"]["dependencies"] = dependencies_result["deferred"]
                if dependencies_result.get("errors"):
                    result["errors"]["dependencies"] = dependencies_result["errors"]
            except Exception as exc:
                result["errors"]["dependencies"] = safe_error(exc)
            try:
                result["applied"]["mcp"] = await bridge.apply_snapshot(snapshot, force=force)
                self.applied["mcp"] = copy.deepcopy(values["mcp"])
            except RuntimeError as exc:
                category = "deferred" if "active requests" in str(exc) else "errors"
                result[category]["mcp"] = safe_error(exc)
            except Exception as exc:
                result["errors"]["mcp"] = safe_error(exc)
            try:
                mail_result = await self._apply_mail(values["mail"], force=force)
                self._record_mail_application(mail_result)
                if mail_result.get("applied") is not None:
                    result["applied"]["mail"] = mail_result.get("applied")
                if mail_result.get("deferred"):
                    result["deferred"]["mail"] = mail_result["deferred"]
                if mail_result.get("errors"):
                    result["errors"]["mail"] = mail_result["errors"]
            except Exception as exc:
                result["errors"]["mail"] = safe_error(exc)
            try:
                web_result = await self._apply_web(values["web"], force=force)
                self._record_web_application(web_result)
                if web_result.get("applied") is not None:
                    result["applied"]["web"] = web_result.get("applied")
                if web_result.get("deferred"):
                    result["deferred"]["web"] = web_result["deferred"]
                if web_result.get("errors"):
                    result["errors"]["web"] = web_result["errors"]
            except Exception as exc:
                result["errors"]["web"] = safe_error(exc)
            if not runtime.healthy:
                result["deferred"]["lsp"] = "Kernel is unavailable"
            else:
                try:
                    applied = await self._apply_lsp(snapshot, generation, force)
                    if not applied["applied"]:
                        result["deferred"]["lsp"] = (
                            applied.get("deferred") or "LSP configuration could not be applied"
                        )
                    else:
                        recorded = self.record_lsp(
                            values["lsp"]["servers"],
                            sequence=applied.get("sequence"),
                            generation=applied.get("generation", generation),
                        )
                        if recorded:
                            self.applied["lsp"] = copy.deepcopy(values["lsp"])
                        result["applied"]["lsp"] = applied
                except Exception as exc:
                    result["errors"]["lsp"] = safe_error(exc)
            runtime.config = copy.deepcopy(self.applied)
            await self._record("reload", **result)
            return result
        finally:
            if bridge is not None:
                bridge._config_applying = False
            self.applying = False

    async def _apply_mail(self, desired, *, force: bool) -> dict:
        """Ask the manager-owned mail service to apply a validated snapshot."""

        service = getattr(self.runtime, "mail", None)
        if service is None:
            if desired == self.applied.get("mail", {}):
                return {"applied": {}}
            return {"deferred": "Mail service is unavailable"}
        apply_config = getattr(service, "apply_config", None)
        if not callable(apply_config):
            if desired == self.applied.get("mail", {}):
                return {"applied": {}}
            return {"deferred": "Mail service cannot reload configuration"}
        response = await apply_config(copy.deepcopy(desired), force=force)
        if not isinstance(response, dict):
            raise RuntimeError("Mail service returned an invalid configuration result")
        return copy.deepcopy(response)

    async def _apply_web(self, desired, *, force: bool) -> dict:
        """Ask the manager-owned web service to apply a validated snapshot."""

        service = getattr(self.runtime, "web", None)
        if service is None:
            if desired == self.applied.get("web", {}):
                return {"applied": {}}
            return {"deferred": "Web service is unavailable"}
        apply_config = getattr(service, "apply_config", None)
        if not callable(apply_config):
            if desired == self.applied.get("web", {}):
                return {"applied": {}}
            return {"deferred": "Web service cannot reload configuration"}
        response = await apply_config(copy.deepcopy(desired), force=force)
        if not isinstance(response, dict):
            raise RuntimeError("Web service returned an invalid configuration result")
        return copy.deepcopy(response)

    def _apply_dependencies(self, desired) -> dict:
        """Apply dependency policy to the manager-owned service synchronously."""

        desired = _validate_dependencies(desired)
        service = getattr(self.runtime, "dependencies", None)
        current = self.applied.get("dependencies", {})
        if desired == current:
            return {"applied": {}}
        if service is None:
            return {"deferred": "Dependency service is unavailable"}
        apply_config = getattr(service, "apply_config", None)
        if not callable(apply_config):
            return {"deferred": "Dependency service cannot reload configuration"}
        response = apply_config(copy.deepcopy(desired))
        if response is None:
            response = {"applied": True, "applied_config": desired}
        if not isinstance(response, dict):
            raise RuntimeError("Dependency service returned an invalid configuration result")
        if response.get("applied", True) is False:
            return copy.deepcopy(response)
        applied_config = response.get("applied_config", desired)
        normalized = _validate_dependencies(applied_config)
        self.applied["dependencies"] = copy.deepcopy(normalized)
        return copy.deepcopy(response)

    def _record_mail_application(self, result: dict) -> None:
        """Record only the normalized configuration confirmed by the service."""

        applied_config = result.get("applied_config")
        if not isinstance(applied_config, dict):
            return
        try:
            normalized = validate_mail_config(applied_config)
        except Exception:
            return
        if normalized == applied_config:
            self.applied["mail"] = copy.deepcopy(normalized)

    def _record_web_application(self, result: dict) -> None:
        """Record only the normalized configuration confirmed by the service."""

        applied_config = result.get("applied_config")
        if not isinstance(applied_config, dict):
            return
        try:
            normalized = validate_web_config(applied_config)
        except Exception:
            return
        if normalized == applied_config:
            self.applied["web"] = copy.deepcopy(normalized)

    async def _apply_lsp(self, snapshot, generation, force, *, starting=False):
        runtime = self.runtime
        if (
            generation != runtime.generation or runtime.stopping.is_set()
            or (runtime.resetting and not starting)
        ):
            raise RuntimeError("Workspace changed during configuration reload")
        message = runtime.kc.session.msg(
            "execute_request",
            {
                "code": "", "silent": True, "store_history": False,
                "user_expressions": {}, "allow_stdin": False, "stop_on_error": False,
            },
            metadata={
                "mypr_control": "config_reload", "generation": generation,
                "config": {"lsp": {
                    "servers": snapshot.values["lsp"]["servers"],
                    "revision": snapshot.revision,
                }},
                "force": force,
            },
        )
        ident = message["header"]["msg_id"]
        waiter = asyncio.get_running_loop().create_future()
        runtime.control_waiters[ident] = waiter
        try:
            runtime.kc.shell_channel.send(message)
            async with asyncio.timeout(10):
                reply = await waiter
            if reply.get("status") != "ok":
                raise RuntimeError(reply.get("evalue", "Kernel configuration reload failed"))
            if (
                generation != runtime.generation or runtime.stopping.is_set()
                or (runtime.resetting and not starting)
            ):
                raise RuntimeError("Workspace changed during configuration reload")
            result = reply.get("config_result")
            if not isinstance(result, dict) or type(result.get("applied")) is not bool:
                raise RuntimeError("Kernel returned an invalid configuration result")
            return result
        finally:
            runtime.control_waiters.pop(ident, None)
