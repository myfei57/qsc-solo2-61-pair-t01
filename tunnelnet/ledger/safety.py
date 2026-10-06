"""Immediate safety isolation (lock-out / tag-out) for physical removals.

Structural ledger edits still take effect on running linkage only at shift
cutover. But when a device is physically pulled out mid-shift it must stop
being acted on *now*. Isolation is therefore a separate, immediate safety
interlock:

* it does not edit the topology, section or adjacency (the ledger and its
  history stay the single source of truth for structure);
* it flips an isolation flag the running linkage consults before acting, so
  an isolated device is never routed to or linked even while the draft version
  is pending activation;
* isolating an already isolated device is a harmless, idempotent no-op;
* lifting isolation later (e.g. the device returns) is explicitly recorded.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable

from .ledger import utc_now


@dataclass(frozen=True)
class Isolation:
    """One lock-out tag on a canonical device id."""

    device_id: str
    at: str
    author: str
    reason: str
    active: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "device_id": self.device_id,
            "at": self.at,
            "author": self.author,
            "reason": self.reason,
            "active": self.active,
        }


class IsolationBoard:
    """Append-only lock-out / tag-out board consulted by running linkage."""

    def __init__(self, clock: Callable[[], str] = utc_now) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._active: dict[str, Isolation] = {}
        self._log: list[Isolation] = []

    def isolate(self, device_id: str, *, author: str, reason: str) -> tuple[Isolation, bool]:
        """Immediately isolate a device; returns ``(tag, newly_tagged)``."""

        with self._lock:
            existing = self._active.get(device_id)
            if existing is not None:
                return existing, False
            tag = Isolation(device_id, self._clock(), author, reason, active=True)
            self._active[device_id] = tag
            self._log.append(tag)
            return tag, True

    def release(self, device_id: str, *, author: str) -> tuple[Isolation | None, bool]:
        """Lift an isolation tag; idempotent if the device is not isolated."""

        with self._lock:
            existing = self._active.pop(device_id, None)
            if existing is None:
                return None, False
            lifted = Isolation(device_id, self._clock(), author, existing.reason, active=False)
            self._log.append(lifted)
            return lifted, True

    def is_isolated(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self._active

    def active_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._active)

    def history(self) -> list[Isolation]:
        with self._lock:
            return list(self._log)
