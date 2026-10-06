"""Append-only canonical ledger with versioned change sets and cutover.

The ledger is the single source of truth:

* every change is committed as an immutable, hash-chained change set with a
  monotonically increasing ``sequence`` (the *committed/draft* version);
* running linkages do not read the draft head, they read the *activated*
  version fixed by :meth:`Ledger.activate`. Activation is the one point where
  a new topology reaches the running environment routing and fire linkage;
* optimistic concurrency: a submitter names the base sequence it edited from
  and loses the commit if another writer committed first;
* a committed change set can never be rewritten; retiring a device keeps its
  historical section and adjacency forever.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace

from .graph import ConflictError, Projection, validate
from .model import Change, ChangeSet, Device, Op

def utc_now() -> str:
    import time

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


GENESIS_DIGEST = "0"


def digest_change_set(change_set: ChangeSet) -> str:
    """Stable SHA-256 over the parent pointer and the change set payload."""

    body = json.dumps(
        {
            "sequence": change_set.sequence,
            "parent": change_set.parent,
            "author": change_set.author,
            "at": change_set.at,
            "changes": [change.as_dict() for change in change_set.changes],
            "idempotency_key": change_set.idempotency_key,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Snapshot:
    """A materialised view tagged with the version it represents."""

    sequence: int
    digest: str
    devices: list[Device]
    aliases: list[tuple[str, str, str]]

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "digest": self.digest,
            "devices": [device.as_dict() for device in self.devices],
            "aliases": [
                {"alias": alias, "system": system, "device_id": device_id}
                for alias, system, device_id in self.aliases
            ],
        }


def _apply(projection: Projection, change_set: ChangeSet) -> None:
    for change in change_set.changes:
        if change.op is Op.UPSERT and change.device is not None:
            from .graph import symmetric

            projection._put(symmetric(change.device))
        elif change.op is Op.RETIRE:
            projection._retire(change.device_id)
        elif change.op is Op.ALIAS:
            projection._alias(change.alias, change.system, change.device_id)


class Ledger:
    """Owns the change log, both projections and the activated pointer."""

    def __init__(self, clock: Callable[[], str] = utc_now) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._log: list[ChangeSet] = []
        self._head = Projection()
        self._active_sequence = 0
        self._active_digest = GENESIS_DIGEST
        self._active_projection = Projection()
        self._idempotency: dict[str, int] = {}

    # -- introspection -----------------------------------------------------

    @property
    def committed_sequence(self) -> int:
        with self._lock:
            return len(self._log)

    @property
    def active_sequence(self) -> int:
        with self._lock:
            return self._active_sequence

    def head(self) -> Projection:
        """Projection at the draft head (latest committed change set)."""

        with self._lock:
            return self._head.copy()

    def active(self) -> Projection:
        """Projection running linkages are pinned to.

        This is the view environment routing, fire linkage, the audit feed and
        the console must read so they all share one version.
        """

        with self._lock:
            return self._active_projection.copy()

    def history(self) -> list[ChangeSet]:
        with self._lock:
            return list(self._log)

    def change_set(self, sequence: int) -> ChangeSet | None:
        with self._lock:
            if 1 <= sequence <= len(self._log):
                return self._log[sequence - 1]
            return None

    def device_ownership(self, device_id: str) -> list[dict[str, object]]:
        """Immutable section/adjacency provenance for one device id.

        Includes retired devices. Each committed change set that touched the
        id is returned in order, so after a removal its past ownership and
        neighbours are still legible and cannot be edited away.
        """

        with self._lock:
            out: list[dict[str, object]] = []
            for change_set in self._log:
                touched = [
                    change.as_dict()
                    for change in change_set.changes
                    if change.device_id == device_id
                ]
                if touched:
                    out.append(
                        {
                            "sequence": change_set.sequence,
                            "at": change_set.at,
                            "author": change_set.author,
                            "changes": touched,
                        }
                    )
            return out

    # -- commit / optimistic concurrency ----------------------------------

    def commit(
        self,
        changes: Iterable[Change],
        *,
        author: str,
        base_sequence: int | None = None,
        idempotency_key: str = "",
    ) -> ChangeSet:
        """Validate and append one change set.

        ``base_sequence`` is the version the editor read. When another writer
        has committed since then the commit is rejected with
        :class:`ConflictError` so concurrent edits to the same ledger cannot
        silently clobber one another. ``None`` optimistically edits from the
        current head.

        A repeated call with the same ``idempotency_key`` returns the change
        set already committed for that key instead of committing again.
        """

        change_tuple = tuple(changes)
        if not change_tuple:
            raise ValueError("change set is empty")

        with self._lock:
            if idempotency_key and idempotency_key in self._idempotency:
                sequence = self._idempotency[idempotency_key]
                return self._log[sequence - 1]

            current = len(self._log)
            if base_sequence is not None and base_sequence != current:
                raise ConflictError(
                    f"base version {base_sequence} is stale (head is {current})"
                )

            validate(self._head, change_tuple)

            # Persist the normalised upsert bodies (self-adjacency dropped),
            # so the stored change set matches the projected topology.
            from .graph import symmetric

            normalised = tuple(
                replace(change, device=symmetric(change.device))
                if change.op is Op.UPSERT and change.device is not None
                else change
                for change in change_tuple
            )

            parent = self._log[-1].digest if self._log else GENESIS_DIGEST
            change_set = ChangeSet(
                sequence=current + 1,
                parent=parent,
                author=author,
                at=self._clock(),
                changes=normalised,
                idempotency_key=idempotency_key,
            )
            change_set = replace(change_set, digest=digest_change_set(change_set))

            _apply(self._head, change_set)
            self._log.append(change_set)
            if idempotency_key:
                self._idempotency[idempotency_key] = change_set.sequence
            return change_set

    # -- activation (the single effective-version switch) ------------------

    def pending(self) -> list[ChangeSet]:
        """Committed change sets not yet activated into running linkages."""

        with self._lock:
            return list(self._log[self._active_sequence :])

    def can_activate(self) -> bool:
        with self._lock:
            return self._active_sequence < len(self._log)

    def activate(self, *, target_sequence: int | None = None) -> ChangeSet:
        """Atomically promote committed change sets to the running version.

        Normal callers promote the draft head at shift close. A specific
        ``target_sequence`` activates up to that version only, letting a shift
        hand over a draft that the next shift deliberately does not activate.
        Raises if the target is outside the committed log.
        """

        with self._lock:
            head = len(self._log)
            target = head if target_sequence is None else target_sequence
            if target < self._active_sequence:
                raise ValueError("cannot move the active version backwards")
            if target > head:
                raise ValueError(f"target version {target} is not committed (head is {head})")
            if target == self._active_sequence:
                return self._log[target - 1] if target else self._genesis_marker()

            projection = self._rebuild(target)
            change_set = self._log[target - 1]
            self._active_projection = projection
            self._active_sequence = target
            self._active_digest = change_set.digest
            return change_set

    # -- snapshots / materialisation ---------------------------------------

    def snapshot(self, *, active: bool = True) -> Snapshot:
        with self._lock:
            if active:
                return Snapshot(
                    sequence=self._active_sequence,
                    digest=self._active_digest,
                    devices=self._active_projection.devices(include_retired=True),
                    aliases=sorted(
                        (alias, system, device_id)
                        for (alias, system), device_id in self._active_projection.alias_map().items()
                    ),
                )
            digest = self._log[-1].digest if self._log else GENESIS_DIGEST
            return Snapshot(
                sequence=len(self._log),
                digest=digest,
                devices=self._head.devices(include_retired=True),
                aliases=sorted(
                    (alias, system, device_id)
                    for (alias, system), device_id in self._head.alias_map().items()
                ),
            )

    def projection_at(self, sequence: int) -> Projection:
        """Rebuild the topology exactly as it was at ``sequence``."""

        with self._lock:
            if sequence < 0 or sequence > len(self._log):
                raise ValueError(f"version {sequence} is out of range (head is {len(self._log)})")
            return self._rebuild(sequence)

    def _rebuild(self, sequence: int) -> Projection:
        projection = Projection()
        for change_set in self._log[:sequence]:
            _apply(projection, change_set)
        return projection

    def verify_chain(self) -> bool:
        """Recompute every digest and parent link; used by anti-entropy."""

        with self._lock:
            parent = GENESIS_DIGEST
            for change_set in self._log:
                if change_set.parent != parent:
                    return False
                if digest_change_set(change_set) != change_set.digest:
                    return False
                parent = change_set.digest
            return True

    def _genesis_marker(self) -> ChangeSet:
        return ChangeSet(
            sequence=0, parent="", author="", at="", changes=(), digest=GENESIS_DIGEST
        )
