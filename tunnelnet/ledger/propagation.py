"""Fan-out of ledger versions to the downstream consumers.

The running linkages (environment routing, fire linkage, audit feed, console)
read the *activated* ledger version. This module delivers that version to each
named consumer with two rules fixed by the design:

* **delta first**: a consumer that is up to date receives only the new change
  sets since the version it acknowledged, never a whole-book resend;
* **snapshot on drift**: a consumer that has a gap, a parent-hash mismatch or
  no baseline receives a full snapshot to resync.

Applying a version to a consumer can fail for one device but not another. That
is a *partial failure*: every per-target result is recorded, the failed
targets stay retryable, and the consumer is not marked caught up until the
version is fully applied. Recovery (:meth:`Fanout.recover`) is idempotent:
re-running it for an already applied version is a no-op and returns the same
result.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

from .ledger import GENESIS_DIGEST, Ledger, Snapshot


class TargetState(str, Enum):
    """Apply outcome for one device in one version delivery."""

    APPLIED = "applied"
    FAILED = "failed"
    SKIPPED = "skipped"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class TargetResult:
    """Outcome of pushing one device row to one consumer."""

    target: str
    state: TargetState
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"target": self.target, "state": self.state.value, "detail": self.detail}


@dataclass(frozen=True)
class Delivery:
    """Result of one ``push`` to one consumer for one version."""

    consumer: str
    sequence: int
    mode: str  # "delta" or "snapshot"
    results: tuple[TargetResult, ...]
    complete: bool

    @property
    def failed(self) -> tuple[TargetResult, ...]:
        return tuple(result for result in self.results if result.state is TargetState.FAILED)

    def as_dict(self) -> dict[str, object]:
        return {
            "consumer": self.consumer,
            "sequence": self.sequence,
            "mode": self.mode,
            "complete": self.complete,
            "results": [result.as_dict() for result in self.results],
            "failed": [result.target for result in self.failed],
        }


class ConsumerState:
    """Bookkeeping for one downstream consumer."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.acked_sequence = 0
        self.acked_digest = GENESIS_DIGEST
        # sequence -> set of device targets still failing for that version.
        self._failed: dict[int, dict[str, str]] = {}

    def is_current(self, ledger: Ledger) -> bool:
        if self.acked_sequence != ledger.active_sequence:
            return False
        if self.acked_sequence == 0:
            return True
        change_set = ledger.change_set(self.acked_sequence)
        return change_set is not None and change_set.digest == self.acked_digest

    def record_failure(self, sequence: int, target: str, detail: str) -> None:
        self._failed.setdefault(sequence, {})[target] = detail

    def clear_failures(self, sequence: int) -> None:
        self._failed.pop(sequence, None)

    def failed_targets(self, sequence: int) -> dict[str, str]:
        return dict(self._failed.get(sequence, {}))

    def pending_failure_sequences(self) -> list[int]:
        return sorted(seq for seq, targets in self._failed.items() if targets)

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "acked_sequence": self.acked_sequence,
            "acked_digest": self.acked_digest,
            "failed": {
                seq: dict(targets) for seq, targets in self._failed.items() if targets
            },
        }


# A consumer apply-callable takes (snapshot_or_change_sets, mode) and returns a
# mapping of device target -> (ok, detail). The fanout turns that into states.
ApplyFn = "callable"


class Fanout:
    """Tracks consumer versions and performs delta/snapshot delivery."""

    def __init__(self, ledger: Ledger) -> None:
        self._ledger = ledger
        self._lock = threading.RLock()
        self._consumers: dict[str, ConsumerState] = {}

    def register(self, name: str) -> ConsumerState:
        with self._lock:
            state = self._consumers.get(name)
            if state is None:
                state = ConsumerState(name)
                self._consumers[name] = state
            return state

    def consumers(self) -> list[str]:
        with self._lock:
            return sorted(self._consumers)

    def state(self, name: str) -> ConsumerState:
        with self._lock:
            if name not in self._consumers:
                raise KeyError(f"unknown consumer {name}")
            return self._consumers[name]

    def states(self) -> list[dict[str, object]]:
        with self._lock:
            return [self._consumers[name].as_dict() for name in sorted(self._consumers)]

    # -- delivery ----------------------------------------------------------

    def push(self, name: str, apply_delta, apply_snapshot) -> Delivery:
        """Bring one registered consumer to the active version.

        ``apply_delta(change_sets)`` and ``apply_snapshot(snapshot)`` each
        return ``{device_id: (ok: bool, detail: str)}``. The fanout chooses the
        mode, records every target outcome and only advances the consumer's
        ack when the whole version applied without failure.
        """

        with self._lock:
            consumer = self.register(name)
            active_seq = self._ledger.active_sequence
            active_change = self._ledger.change_set(active_seq)
            active_digest = active_change.digest if active_change else GENESIS_DIGEST

            # Idempotent: a fully caught-up consumer is a no-op.
            if consumer.acked_sequence == active_seq and not consumer.pending_failure_sequences():
                if active_seq == 0 or consumer.acked_digest == active_digest:
                    return Delivery(name, active_seq, "delta", (), complete=True)

            use_snapshot, start = self._plan(consumer, active_seq)
            results: list[TargetResult] = []

            if use_snapshot:
                snapshot = self._ledger.snapshot(active=True)
                outcomes = apply_snapshot(snapshot)
                mode = "snapshot"
                self._absorb(consumer, active_seq, snapshot_targets(snapshot), outcomes, results)
                if not any(r.state is TargetState.FAILED for r in results):
                    consumer.clear_failures(active_seq)
                    consumer.acked_sequence = active_seq
                    consumer.acked_digest = snapshot.digest
                return Delivery(name, active_seq, mode, tuple(results), not self._has_failure(results))

            change_sets = [
                self._ledger.change_set(seq) for seq in range(start, active_seq + 1)
            ]
            change_sets = [cs for cs in change_sets if cs is not None]
            targets = sorted({target for cs in change_sets for target in cs.touching()})
            outcomes = apply_delta(change_sets)
            mode = "delta"
            self._absorb(consumer, active_seq, targets, outcomes, results)
            if not any(r.state is TargetState.FAILED for r in results):
                consumer.clear_failures(active_seq)
                consumer.acked_sequence = active_seq
                consumer.acked_digest = active_digest
            return Delivery(name, active_seq, mode, tuple(results), not self._has_failure(results))

    def recover(self, name: str, apply_delta, apply_snapshot) -> Delivery:
        """Idempotent retry of an unfinished delivery.

        Safe to call repeatedly: applied targets are reported as ``applied``
        and only still-failing targets keep the consumer behind.
        """

        with self._lock:
            consumer = self.register(name)
            if not consumer.pending_failure_sequences() and consumer.is_current(self._ledger):
                active = self._ledger.active_sequence
                return Delivery(name, active, "delta", (), complete=True)
            return self.push(name, apply_delta, apply_snapshot)

    # -- internals ---------------------------------------------------------

    def _plan(self, consumer: ConsumerState, active_seq: int) -> tuple[bool, int]:
        """Choose snapshot vs delta and the first version to send."""

        # Never baselined: seed with a snapshot.
        if consumer.acked_sequence == 0:
            return True, 1
        # Consumer ahead only if active moved backwards, which never happens;
        # a gap (missing committed range cannot occur locally) or a digest fork
        # at the acked boundary forces a snapshot.
        anchor = self._ledger.change_set(consumer.acked_sequence)
        if anchor is None or anchor.digest != consumer.acked_digest:
            return True, 1
        # Re-send the whole unacked block as a delta on (possibly retried)
        # delivery. It is still only the changed block, not the whole book.
        return False, consumer.acked_sequence + 1

    @staticmethod
    def _absorb(consumer, sequence, targets, outcomes, results) -> None:
        """Turn apply outcomes into per-target states and failure bookkeeping."""

        outcomes = outcomes or {}
        for target in targets:
            ok, detail = outcomes.get(target, (False, "consumer reported no result"))
            state = TargetState.APPLIED if ok else TargetState.FAILED
            if not ok:
                consumer.record_failure(sequence, target, str(detail))
            results.append(TargetResult(target=target, state=state, detail=str(detail)))

    @staticmethod
    def _has_failure(results: list[TargetResult]) -> bool:
        return any(result.state is TargetState.FAILED for result in results)


def snapshot_targets(snapshot: Snapshot) -> list[str]:
    return sorted(device.id for device in snapshot.devices)
