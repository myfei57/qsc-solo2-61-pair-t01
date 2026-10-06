"""Application facade: register a change once, activate it everywhere.

``LedgerService`` is the single entry point the console uses. A request:

1. optimistically commits the work order to the append-only ledger (or returns
   the original result when the same command id is retried);
2. immediately dispatches the new version to every subsystem projection;
3. opens the order on the shift board so a failed activation cannot vanish at
   handover.

This is where the two locked policies live: **immediate activation at order
acceptance** (not end-of-shift) and **delta transport with snapshot repair**
(the manager sends only the touched devices, and rebuilds wholesale when a
consumer starts cold or fails its digest check).
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from .activation import ActivationManager
from . import delta as delta_module
from .model import Change, CommitResult
from .registry import Ledger, utc_now
from .shift import ShiftBoard


def new_command_id() -> str:
    """Client-side idempotency key, generated when the caller omits one."""

    return uuid.uuid4().hex


class LedgerService:
    """Owns the ledger, its activation manager and the shift board."""

    def __init__(self, ledger: Ledger | None = None, clock: Callable[[], str] = utc_now) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self.ledger = ledger or Ledger(clock=clock)
        self.activation = ActivationManager(self.ledger)
        self.shift = ShiftBoard()
        self.shift.bind_activation(self.activation)

    # ------------------------------------------------------------- commands

    def commit(
        self,
        changes: Sequence[Change | dict[str, Any]],
        *,
        work_order: str,
        expected_version: int | None = None,
        command_id: str = "",
        actor: str = "operator",
        at: str | None = None,
    ) -> dict[str, Any]:
        """Commit one work order and immediately activate it on all readers."""

        command_id = command_id or new_command_id()
        base = self.ledger.version() if expected_version is None else expected_version
        when = at or self._clock()

        with self._lock:
            result = self.ledger.commit(
                changes,
                work_order=work_order,
                expected_version=base,
                command_id=command_id,
                actor=actor,
                at=when,
            )
            if not result.duplicate:
                dispatch = self.activation.dispatch()
                segments = self._segments_of(result)
                self.shift.open(result, segments, actor, when)
                self.shift.close_ready()
            else:
                dispatch = {state.name: "duplicate" for state in self.activation.states()}
            return self._envelope(result, dispatch, command_id, duplicated=result.duplicate)

    def recover(self, consumer: str) -> dict[str, Any]:
        """Retry one failed downstream projection; safe to trigger repeatedly."""

        with self._lock:
            state = self.activation.recover(consumer)
            closed = self.shift.close_ready()
            return {"consumer": consumer, "outcome": state.status, "state": state.as_dict(),
                    "closed_orders": closed}

    def resync(self, consumer: str) -> dict[str, Any]:
        """Rebuild one projection wholesale from the full snapshot."""

        with self._lock:
            state = self.activation.resync(consumer)
            closed = self.shift.close_ready()
            return {"consumer": consumer, "outcome": state.status, "state": state.as_dict(),
                    "closed_orders": closed}

    # ---------------------------------------------------------------- reads

    def delta(self, base_version: int, version: int | None = None) -> dict[str, Any]:
        return delta_module.diff(self.ledger, base_version, version).as_dict()

    def snapshot(self, version: int | None = None) -> dict[str, Any]:
        return delta_module.snapshot(self.ledger, version).as_dict()

    def status(self) -> dict[str, Any]:
        """Single status document every subsystem and the console page read."""

        return {
            "version": self.ledger.version(),
            "head_hash": self.ledger.head_hash(),
            "active_version": self.activation.active_version(),
            "consumers": [state.as_dict() for state in self.activation.states()],
            "lagging": self.activation.lagging(),
            "degraded": self.activation.degraded(),
            "open_orders": [order.as_dict() for order in self.shift.open_orders()],
            "manual_watch_segments": self.shift.manual_watch_segments(),
            "shift": self.shift.shift,
        }

    # --------------------------------------------------------------- helpers

    def _segments_of(self, result: CommitResult) -> list[str]:
        at_version = self.ledger.state_at(result.version)
        segments = set()
        for event in self.ledger.events()[result.base_version:result.version]:
            device = at_version.get(event.device_id)
            if device is not None and device.segment:
                segments.add(device.segment)
        return sorted(segments)

    def _envelope(self, result: CommitResult, dispatch: dict[str, str],
                  command_id: str, *, duplicated: bool) -> dict[str, Any]:
        failed = [name for name, outcome in dispatch.items() if outcome == "failed"]
        return {
            "commit": result.as_dict(),
            "activation": dispatch,
            "failed_consumers": failed,
            "active_version": self.activation.active_version(),
            "command_id": command_id,
            "duplicate": duplicated,
        }
