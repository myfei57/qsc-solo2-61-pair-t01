"""Fan a committed ledger version out to the running subsystems.

The ledger commit is the source of truth; a subsystem's in-memory projection
is only a cache of it. Four consumers read that cache:

* ``env-routing``  - environment alarm routing
* ``fire-linkage`` - fire damper / zone linkage
* ``audit``        - the audit flow
* ``console``      - the console page

Activation policy (locked decision): **immediate at work-order acceptance,
never deferred to end of shift.** Commit and activation are one operation -
the new version is dispatched to every consumer before the commit returns.
A consumer that cannot write it is marked *degraded* and keeps running on its
last good version; the other consumers still activate, and the safety layer
(``ledger.shift``) covers the gap. Repeating the recovery call is safe: the
projection is rebuilt from the ledger and application is de-duplicated by
event id, so a retried delta never applies twice.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import delta as delta_module
from .model import Device, digest_of
from .registry import Ledger

ENV_ROUTING = "env-routing"
FIRE_LINKAGE = "fire-linkage"
AUDIT = "audit"
CONSOLE = "console"

# Everything that must read one and the same ledger version.
CONSUMERS: tuple[str, ...] = (ENV_ROUTING, FIRE_LINKAGE, AUDIT, CONSOLE)

ACTIVE = "active"
DEGRADED = "degraded"
LAGGING = "lagging"

# A downstream writer receives either an incremental Delta or a full Snapshot
# (a replace-everything repair); both carry version, head_hash and digest.
Applier = Callable[[str, Any], None]


class IdempotentApplier:
    """Wrap an applier so a redelivered event id cannot apply twice.

    Downstream delivery is at-least-once (a consumer that timed out may still
    have written). On delta delivery this wrapper remembers every event id it
    has handed to the inner applier and skips it on recovery, keeping the
    repeated trigger a no-op. A full snapshot carries no event list - it is a
    whole-projection replace, which is idempotent on its own.
    """

    def __init__(self, inner: Applier) -> None:
        self._inner = inner
        self._seen: set[str] = set()
        self._lock = threading.RLock()

    def __call__(self, consumer: str, block: Any) -> None:
        rows = getattr(block, "events", None)
        if rows:
            with self._lock:
                fresh = [row for row in rows if str(row["event_id"]) not in self._seen]
            if fresh:
                self._inner(consumer, delta_module.Delta(
                    base_version=block.base_version,
                    version=block.version,
                    head_hash=block.head_hash,
                    events=fresh,
                    devices=block.devices,
                    digest=block.digest,
                ))
            # Remember an event id only after the inner write returned - a
            # raised write stays unseen so the recovery retry redelivers it.
            with self._lock:
                for row in rows:
                    self._seen.add(str(row["event_id"]))
        else:
            # Full-snapshot resync: an idempotent whole-projection replace.
            self._inner(consumer, block)


def _succeed(consumer: str, block: "delta_module.Delta") -> None:
    """Default applier: a healthy subsystem accepts the block."""


@dataclass
class ConsumerState:
    """Where one subsystem's projection stands relative to the ledger."""

    name: str
    version: int = 0
    head_hash: str = "0" * 64
    digest: str = ""
    status: str = LAGGING
    attempts: int = 0
    last_error: str = ""
    applied_events: int = 0

    @property
    def caught_up(self) -> bool:
        return self.status == ACTIVE

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "head_hash": self.head_hash,
            "digest": self.digest,
            "status": self.status,
            "attempts": self.attempts,
            "last_error": self.last_error,
            "applied_events": self.applied_events,
        }


class ActivationManager:
    """Materialises one ledger per consumer and tracks every activation."""

    def __init__(self, ledger: Ledger, consumers: tuple[str, ...] = CONSUMERS) -> None:
        self._ledger = ledger
        self._lock = threading.RLock()
        self._states = {name: ConsumerState(name=name) for name in consumers}
        self._views: dict[str, dict[str, Device]] = {name: {} for name in consumers}
        self._appliers: dict[str, Applier] = {name: IdempotentApplier(_succeed) for name in consumers}

    # ------------------------------------------------------------- register

    def consumers(self) -> list[str]:
        return list(self._states)

    def register_applier(self, consumer: str, applier: Applier) -> None:
        """Attach the real downstream writer for one subsystem."""

        self._require(consumer)
        self._appliers[consumer] = IdempotentApplier(applier)

    # ------------------------------------------------------------- activate

    def dispatch(self) -> dict[str, Any]:
        """Push every lagging consumer up to the current ledger version.

        Each consumer is independent: one consumer's failed write never rolls
        back the ledger or the other consumers.
        """

        outcome: dict[str, Any] = {}
        with self._lock:
            for consumer in self._states:
                outcome[consumer] = self._advance(consumer)
        return outcome

    def recover(self, consumer: str) -> ConsumerState:
        """Retry activation for one degraded/lagging consumer (idempotent)."""

        self._require(consumer)
        with self._lock:
            self._advance(consumer)
            return self._states[consumer]

    def resync(self, consumer: str) -> ConsumerState:
        """Replace a consumer's whole projection from the full snapshot.

        Used on startup and whenever a digest check fails: rebuilding from the
        ledger repairs any gap or corruption in one atomic swap.
        """

        self._require(consumer)
        with self._lock:
            block = delta_module.snapshot(self._ledger)
            state = self._states[consumer]
            state.attempts += 1
            candidate = self._rebuild(block.version)
            self._appliers[consumer](consumer, block)
            self._commit_view(consumer, candidate, block.version, block.head_hash, block.digest)
            state.last_error = ""
            state.status = ACTIVE if block.version == self._ledger.version() else LAGGING
            return state

    # ----------------------------------------------------------------- read

    def state(self, consumer: str) -> ConsumerState:
        return self._states[self._require(consumer)]

    def states(self) -> list[ConsumerState]:
        return [self._states[name] for name in sorted(self._states)]

    def view(self, consumer: str) -> list[Device]:
        """Devices one subsystem currently acts on (its last good version)."""

        self._require(consumer)
        with self._lock:
            return [Device(**self._views[consumer][key].as_dict())
                    for key in sorted(self._views[consumer])]

    def active_version(self) -> int:
        """Highest version safe everywhere: the slowest consumer's watermark.

        Fire linkage must never act on a topology newer than what every
        consumer actually holds, so the safety-relevant runtime pins here.
        """

        return min((state.version for state in self._states.values()), default=0)

    def lagging(self) -> list[str]:
        head = self._ledger.version()
        with self._lock:
            return sorted(name for name, state in self._states.items() if state.version < head)

    def degraded(self) -> list[str]:
        with self._lock:
            return sorted(name for name, state in self._states.items() if state.status == DEGRADED)

    def verify(self, consumer: str) -> bool:
        """Compare the consumer's held digest with the canonical ledger digest.

        A consumer caches a projection; this asks whether that cache still
        equals the ledger content at the version it claims to hold. Drift
        (lost/corrupted write) marks it degraded so a full ``resync`` follows
        rather than acting on a silently different register.
        """

        self._require(consumer)
        with self._lock:
            state = self._states[consumer]
            if state.version == 0:
                return True
            expected = delta_module.snapshot(self._ledger, state.version).digest
            if state.digest != expected:
                state.status = DEGRADED
                state.last_error = "projection digest mismatch; resync required"
                return False
            return True

    # --------------------------------------------------------------- helpers

    def _advance(self, consumer: str) -> str:
        state = self._states[consumer]
        head = self._ledger.version()
        if state.version == head:
            return "current"
        block = delta_module.diff(self._ledger, state.version, head)
        candidate = self._rebuild(head)
        state.attempts += 1
        try:
            self._appliers[consumer](consumer, block)
        except Exception as exc:  # noqa: BLE001 - downstream failure is a state, not a crash
            state.status = DEGRADED
            state.last_error = str(exc) or exc.__class__.__name__
            return "failed"
        target = delta_module.snapshot(self._ledger, head)
        self._commit_view(consumer, candidate, head, target.head_hash, target.digest)
        state.last_error = ""
        state.status = ACTIVE
        return "activated"

    def _commit_view(self, consumer: str, devices: dict[str, Device], version: int,
                     head_hash: str, digest: str) -> None:
        state = self._states[consumer]
        self._views[consumer] = devices
        state.version = version
        state.head_hash = head_hash
        state.digest = digest
        state.applied_events = version

    def _rebuild(self, version: int) -> dict[str, Device]:
        return self._ledger.state_at(version)

    def _require(self, consumer: str) -> str:
        if consumer not in self._states:
            raise ValueError(f"unknown ledger consumer {consumer!r}")
        return consumer
