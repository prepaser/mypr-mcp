from __future__ import annotations

import math
import threading
from collections import deque
from collections.abc import Iterable


class Timings:
    max_labels = 64

    def __init__(self, limit: int = 256, *, labels: Iterable[str] | None = None):
        if limit < 1:
            raise ValueError("limit must be positive")
        self.limit = limit
        self._labels = None if labels is None else frozenset(labels) | {"other"}
        self._samples: dict[str, deque[float]] = {}
        self._totals: dict[str, int] = {}
        self._lock = threading.Lock()

    def observe(self, name: str, seconds: float) -> None:
        if not isinstance(name, str) or not name or isinstance(seconds, bool):
            return
        name = name[:64]
        try:
            milliseconds = float(seconds) * 1000
        except OverflowError, TypeError, ValueError:
            return
        if not math.isfinite(milliseconds) or milliseconds < 0:
            return
        with self._lock:
            if self._labels is not None and name not in self._labels:
                name = "other"
            elif (
                self._labels is None
                and name not in self._samples
                and len(self._samples) >= self.max_labels - 1
            ):
                name = "other"
            samples = self._samples.get(name)
            if samples is None:
                samples = self._samples[name] = deque(maxlen=self.limit)
            samples.append(milliseconds)
            self._totals[name] = self._totals.get(name, 0) + 1

    def snapshot(self) -> dict[str, dict[str, int | float | str]]:
        with self._lock:
            snapshot = {
                name: (list(samples), self._totals[name]) for name, samples in self._samples.items()
            }
        result = {}
        for name, (samples, total) in snapshot.items():
            ordered = sorted(samples)
            result[name] = {
                "sample_count": len(ordered),
                "total_count": total,
                "p50": self._percentile(ordered, 0.50),
                "p95": self._percentile(ordered, 0.95),
                "max": round(ordered[-1], 3),
                "unit": "ms",
            }
        return result

    @staticmethod
    def _percentile(samples: list[float], percentile: float) -> float:
        index = max(0, math.ceil(percentile * len(samples)) - 1)
        return round(samples[index], 3)
