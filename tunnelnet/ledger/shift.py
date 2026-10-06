"""Cross-shift handover rule for changes that have not finished activating.

The activation policy is immediate, but a downstream write can still fail and
leave a committed work order lagging into the next shift. The handover rule
is deliberately strict:

* A work order may close only once **every** consumer holds its version.
* A shift cannot hand over while a work order is still open. The blocked
  handover names the affected segments, which must run under manual watch
  until the projection catches up - fire linkage never silently uses a
  topology newer than the slowest consumer.
* If a modification genuinely straddles two shifts, the accepting operator
  must explicitly confirm the carried-over order (double-control). That marks
  its segments for manual watch and records who accepted what, so a cross班
  拆改 can never be handed over unacknowledged.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from .activation import ActivationManager
    from .model import CommitResult


class ShiftBlocked(RuntimeError):
    """Handover refused because a ledger change is not fully active."""

    def __init__(self, open_orders: list[str], segments: list[str]) -> None:
        super().__init__(
            f"handover blocked: {len(open_orders)} open work order(s) "
            f"{open_orders} on segments {segments}"
        )
        self.open_orders = open_orders
        self.segments = segments


@dataclass
class OpenOrder:
    """A committed work order that has not yet been confirmed active."""

    work_order: str
    version: int
    segments: list[str]
    actor: str
    opened_at: str
    confirmed_by: str = ""
    manual_watch: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_order": self.work_order,
            "version": self.version,
            "segments": list(self.segments),
            "actor": self.actor,
            "opened_at": self.opened_at,
            "confirmed_by": self.confirmed_by,
            "manual_watch": self.manual_watch,
        }


class ShiftBoard:
    """Tracks open ledger work orders across the current shift."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._orders: dict[str, OpenOrder] = {}
        self._shift = ""
        self._operator = ""

    def begin_shift(self, shift: str, operator: str) -> None:
        with self._lock:
            self._shift = shift
            self._operator = operator

    @property
    def shift(self) -> str:
        return self._shift

    @property
    def operator(self) -> str:
        return self._operator

    def open(self, result: "CommitResult", segments: list[str], actor: str, at: str) -> OpenOrder:
        """Record a freshly committed work order awaiting full activation."""

        with self._lock:
            order = self._orders.get(result.work_order)
            if order is not None:
                return order
            order = OpenOrder(
                work_order=result.work_order,
                version=result.version,
                segments=sorted(set(segments)),
                actor=actor,
                opened_at=at,
            )
            self._orders[result.work_order] = order
            return order

    def orders(self) -> list[OpenOrder]:
        with self._lock:
            return [self._orders[key] for key in sorted(self._orders)]

    def open_orders(self) -> list[OpenOrder]:
        with self._lock:
            return [order for order in (self._orders[key] for key in sorted(self._orders))
                    if not self._settled(order)]

    def _settled(self, order: OpenOrder) -> bool:
        activation = getattr(self, "_activation", None)
        if activation is None:
            return False
        return activation.active_version() >= order.version and not activation.degraded()

    def bind_activation(self, activation: "ActivationManager") -> None:
        self._activation = activation

    def close_ready(self) -> list[str]:
        """Close every order whose version every consumer now holds."""

        activation = getattr(self, "_activation", None)
        closed: list[str] = []
        with self._lock:
            if activation is None:
                return closed
            for key in sorted(self._orders):
                order = self._orders[key]
                if activation.active_version() >= order.version and not activation.degraded():
                    order.manual_watch = False
                    order.confirmed_by = order.confirmed_by or "auto:fully-active"
                    del self._orders[key]
                    closed.append(key)
        return closed

    def accept_cross_shift(self, work_order: str, accepting_operator: str) -> OpenOrder:
        """Next-shift operator acknowledges a carried-over change (two-person).

        Puts the order's segments under manual watch; acknowledgement does not
        close the order - it only permits it to travel across the shift.
        """

        with self._lock:
            order = self._orders.get(work_order)
            if order is None:
                raise ValueError(f"work order {work_order!r} is not open")
            if not accepting_operator or accepting_operator == order.actor:
                raise ValueError("cross-shift acceptance requires a second operator")
            order.confirmed_by = accepting_operator
            order.manual_watch = True
            return order

    def manual_watch_segments(self) -> list[str]:
        with self._lock:
            segments = {segment for order in self._orders.values() if order.manual_watch
                        for segment in order.segments}
            return sorted(segments)

    def handover(self, next_shift: str, next_operator: str) -> dict[str, Any]:
        """Attempt to hand the shift over; refuse while a change is in flight.

        Every open order must be cross-shift accepted by the *next* operator.
        An accepted order carries over under manual watch; an unaccepted one
        blocks the handover outright.
        """

        with self._lock:
            pending = self.open_orders()
            unaccepted = [order.work_order for order in pending if not order.confirmed_by
                          or order.confirmed_by != next_operator]
            if unaccepted:
                segments = sorted({segment for order in pending
                                   if order.work_order in unaccepted
                                   for segment in order.segments})
                raise ShiftBlocked(unaccepted, segments)
            for order in pending:
                order.manual_watch = True
            payload = {
                "from_shift": self._shift,
                "to_shift": next_shift,
                "carried_over": [order.work_order for order in pending],
                "manual_watch": self.manual_watch_segments(),
            }
            self._shift = next_shift
            self._operator = next_operator
            return payload
