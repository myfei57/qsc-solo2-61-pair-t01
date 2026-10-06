"""Application facade over the unified equipment ledger.

Bundles the append-only :class:`Ledger`, the downstream :class:`Fanout` and the
immediate :class:`IsolationBoard`, and wires the four consumers that must all
read the same active version: environment routing, fire linkage, the audit
feed and the console.

Consumers are given apply callables by the host (the console/runtime injects
real ones); until then the built-in recording apply functions stand in so the
service is usable and testable on its own.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from .ledger import ChangeSet, Ledger, Snapshot
from .model import Change
from .propagation import Fanout
from .safety import IsolationBoard

# Consumers that must share one active ledger version.
ENV_ROUTING = "environment_routing"
FIRE_LINKAGE = "fire_linkage"
AUDIT_FEED = "audit_feed"
CONSOLE = "console"
CANONICAL_CONSUMERS = (ENV_ROUTING, FIRE_LINKAGE, AUDIT_FEED, CONSOLE)

# Host supplied apply callables: receive a delta/snapshot, return per-device
# ``{id: (ok, detail)}``.
DeltaApply = Callable[[list[ChangeSet]], dict[str, tuple[bool, str]]]
SnapshotApply = Callable[[Snapshot], dict[str, tuple[bool, str]]]


class LedgerService:
    def __init__(self, ledger: Ledger | None = None) -> None:
        self.ledger = ledger or Ledger()
        self.board = IsolationBoard()
        self.fanout = Fanout(self.ledger)
        for name in CANONICAL_CONSUMERS:
            self.fanout.register(name)
        # consumer -> pending per-target failures for the next delivery.
        self._failures: dict[str, dict[str, str]] = {}
        self._delta_appliers: dict[str, DeltaApply] = {}
        self._snapshot_appliers: dict[str, SnapshotApply] = {}
        for name in CANONICAL_CONSUMERS:
            self._delta_appliers[name] = self._make_delta(name)
            self._snapshot_appliers[name] = self._make_snapshot(name)

    # -- editing -----------------------------------------------------------

    def commit(
        self,
        changes: Iterable[Change],
        *,
        author: str,
        base_sequence: int | None = None,
        idempotency_key: str = "",
    ) -> ChangeSet:
        return self.ledger.commit(
            changes,
            author=author,
            base_sequence=base_sequence,
            idempotency_key=idempotency_key,
        )

    # -- shift boundary ----------------------------------------------------

    def pending(self) -> list[ChangeSet]:
        return self.ledger.pending()

    def cutover(self, *, target_sequence: int | None = None) -> dict[str, object]:
        """Activate a version at shift close and push it to every consumer.

        Activation itself is atomic in the ledger; fan-out afterwards may be a
        partial failure, which is reported per consumer without rolling the
        activated version back. Recovery is driven separately and idempotently.
        """

        activated = self.ledger.activate(target_sequence=target_sequence)
        deliveries = [
            self.fanout.push(name, self._delta_appliers[name], self._snapshot_appliers[name])
            for name in CANONICAL_CONSUMERS
        ]
        return {
            "activated_sequence": activated.sequence,
            "activated_digest": activated.digest,
            "deliveries": [delivery.as_dict() for delivery in deliveries],
            "complete": all(delivery.complete for delivery in deliveries),
        }

    def recover(self) -> dict[str, object]:
        """Re-drive every consumer that is behind or has failed targets.

        Idempotent: a consumer that is already current with no outstanding
        failures is a no-op, and re-running recovery does not double-apply.
        """

        deliveries = [
            self.fanout.recover(name, self._delta_appliers[name], self._snapshot_appliers[name])
            for name in CANONICAL_CONSUMERS
        ]
        return {
            "active_sequence": self.ledger.active_sequence,
            "deliveries": [delivery.as_dict() for delivery in deliveries],
            "complete": all(delivery.complete for delivery in deliveries),
        }

    # -- safety isolation --------------------------------------------------

    def isolate(self, device_id: str, *, author: str, reason: str) -> dict[str, object]:
        tag, newly = self.board.isolate(device_id, author=author, reason=reason)
        return {"isolation": tag.as_dict(), "newly_tagged": newly}

    def release(self, device_id: str, *, author: str) -> dict[str, object]:
        tag, lifted = self.board.release(device_id, author=author)
        return {"isolation": None if tag is None else tag.as_dict(), "lifted": lifted}

    # -- wiring hooks for the host to inject real consumer behaviour -------

    def set_applier(self, consumer: str, delta: DeltaApply, snapshot: SnapshotApply) -> None:
        if consumer not in CANONICAL_CONSUMERS:
            raise KeyError(f"unknown consumer {consumer}")
        self._delta_appliers[consumer] = delta
        self._snapshot_appliers[consumer] = snapshot

    def fail_target(self, consumer: str, target: str, detail: str = "write failed") -> None:
        """Arm the next delivery to ``consumer`` to fail one device target."""

        self._failures.setdefault(consumer, {})[target] = detail

    def clear_fail_targets(self, consumer: str | None = None) -> None:
        if consumer is None:
            self._failures.clear()
        else:
            self._failures.pop(consumer, None)

    # -- default recording appliers (one closure per consumer) -------------

    def _make_delta(self, consumer: str) -> DeltaApply:
        def apply_delta(change_sets: list[ChangeSet]) -> dict[str, tuple[bool, str]]:
            targets = sorted({target for cs in change_sets for target in cs.touching()})
            return self._apply_targets(consumer, targets)

        return apply_delta

    def _make_snapshot(self, consumer: str) -> SnapshotApply:
        def apply_snapshot(snapshot: Snapshot) -> dict[str, tuple[bool, str]]:
            targets = sorted(device.id for device in snapshot.devices)
            return self._apply_targets(consumer, targets)

        return apply_snapshot

    def _apply_targets(self, consumer: str, targets: list[str]) -> dict[str, tuple[bool, str]]:
        failures = self._failures.setdefault(consumer, {})
        out: dict[str, tuple[bool, str]] = {}
        for target in targets:
            if target in failures:
                # One transient device-write miss: consumed on this attempt, so
                # a retry succeeds unless the host arms the failure again.
                out[target] = (False, failures.pop(target))
            else:
                out[target] = (True, "")
        return out
