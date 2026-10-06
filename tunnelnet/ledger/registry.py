"""The append-only ledger: commit, optimistic concurrency and replay.

Durability
----------
A validated work order is persisted as *one* commit document (the ordered
event list plus the command id) with the store's atomic temp-file/rename
write. A crash therefore leaves either the whole batch visible on replay or
none of it; the register never tears across devices.

Concurrency
-----------
Every commit carries the ``expected_version`` the editor read. Two editors
changing the same register serialise: the loser of the race sees
``LedgerConflict`` (HTTP 409) and must re-read, merge its batch and retry.
Repeated submissions carrying the same ``command_id`` return the original
result instead of applying twice (recovery/retries are idempotent).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

from tunnelnet.store import Store

from . import changeset
from .model import (
    GENESIS_HASH,
    Change,
    CommitResult,
    Device,
    Event,
    apply_event,
    seal,
)

COMMIT_KIND = "ledger-commits"
COMMAND_KIND = "ledger-commands"
_ID_WIDTH = 12


def utc_now() -> str:
    """Current UTC time in the compact form used on every event."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class LedgerError(RuntimeError):
    """The persisted log cannot be read or fails chain verification."""


class LedgerConflict(LedgerError):
    """The register moved between read and commit (optimistic-lock failure)."""

    def __init__(self, expected: int, actual: int) -> None:
        super().__init__(f"ledger version {actual} is newer than expected {expected}")
        self.expected = expected
        self.actual = actual


def _commit_id(base_version: int) -> str:
    return f"{base_version:0{_ID_WIDTH}d}"


class Ledger:
    """Thread safe, append-only source of truth for every equipment item."""

    def __init__(self, store: Store | None = None, clock: Callable[[], str] = utc_now) -> None:
        self._store = store
        self._clock = clock
        self._lock = threading.RLock()
        self._events: list[Event] = []
        self._devices: dict[str, Device] = {}
        self._commands: dict[str, CommitResult] = {}
        if store is not None:
            self._replay()

    # ------------------------------------------------------------------ read

    def version(self) -> int:
        with self._lock:
            return len(self._events)

    def head_hash(self) -> str:
        with self._lock:
            return self._events[-1].hash if self._events else GENESIS_HASH

    def count(self) -> int:
        with self._lock:
            return len(self._devices)

    def describe(self) -> str:
        return f"ledger version={self.version()} devices={self.count()}"

    def device(self, device_id: str) -> Device | None:
        with self._lock:
            device = self._devices.get(device_id)
            return None if device is None else self._clone(device)

    def resolve(self, code_or_id: str) -> Device | None:
        """Resolve a lifelong id or a current/retired position code."""

        with self._lock:
            device = self._devices.get(code_or_id)
            if device is not None:
                return self._clone(device)
            for candidate in self._devices.values():
                if code_or_id == candidate.code or code_or_id in candidate.aliases:
                    return self._clone(candidate)
        return None

    def list(self, include_retired: bool = False) -> list[Device]:
        with self._lock:
            return [
                self._clone(device)
                for device in (self._devices[key] for key in sorted(self._devices))
                if include_retired or not device.retired
            ]

    def segments(self) -> list[str]:
        with self._lock:
            return sorted({device.segment for device in self._devices.values() if device.segment})

    def events(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def history(self, device_id: str) -> list[Event]:
        """Every fact ever recorded for one device; nothing here is rewritten."""

        with self._lock:
            return [event for event in self._events if event.device_id == device_id]

    def neighbors(self, device_id: str) -> list[Device]:
        device = self.device(device_id)
        if device is None:
            return []
        return [found for found in (self.device(neighbour) for neighbour in device.adjacent)
                if found is not None]

    def state_at(self, version: int) -> dict[str, Device]:
        """Register as it stood at a past version (historical attribution)."""

        with self._lock:
            if version < 0 or version > len(self._events):
                raise LedgerError(f"version {version} is outside 0..{len(self._events)}")
            devices: dict[str, Device] = {}
            for event in self._events[:version]:
                devices = apply_event(devices, event)
        return devices

    def command_result(self, command_id: str) -> CommitResult | None:
        with self._lock:
            return self._commands.get(command_id)

    # ---------------------------------------------------------------- commit

    def commit(
        self,
        changes: Sequence[Change | dict[str, Any]],
        *,
        work_order: str,
        expected_version: int,
        command_id: str,
        actor: str = "operator",
        at: str | None = None,
    ) -> CommitResult:
        """Append one validated work order and advance the version.

        Raises ``ValueError``/``changeset.ValidationError`` for a bad batch and
        ``LedgerConflict`` when ``expected_version`` is stale.
        """

        if not work_order:
            raise ValueError("work_order is required")
        if not command_id:
            raise ValueError("command_id is required")
        if expected_version < 0:
            raise ValueError("expected_version must be >= 0")

        normalized = [
            change if isinstance(change, Change) else Change.from_payload(dict(change))
            for change in changes
        ]
        when = at or self._clock()

        with self._lock:
            duplicate = self._commands.get(command_id)
            if duplicate is not None:
                return CommitResult(**{**duplicate.as_dict(), "duplicate": True})

            base_version = len(self._events)
            if expected_version != base_version:
                raise LedgerConflict(expected_version, base_version)

            events = changeset.validate(
                normalized,
                self._devices,
                base_version=base_version,
                at=when,
                work_order=work_order,
                actor=actor,
                command_id=command_id,
                prev_hash=self.head_hash(),
            )

            result = CommitResult(
                work_order=work_order,
                version=base_version + len(events),
                base_version=base_version,
                event_ids=tuple(event.event_id for event in events),
                command_id=command_id,
            )
            self._persist(events, result, actor)

            for event in events:
                self._devices = apply_event(self._devices, event)
                self._events.append(event)
            self._commands[command_id] = result
            return result

    # ------------------------------------------------------------- integrity

    def verify_chain(self) -> bool:
        """Recompute every chain hash; raise ``LedgerError`` on tampering."""

        with self._lock:
            previous = GENESIS_HASH
            for index, event in enumerate(self._events, start=1):
                if event.seq != index:
                    raise LedgerError(f"event {index}: out-of-order seq {event.seq}")
                if event.prev_hash != previous:
                    raise LedgerError(f"event {event.event_id}: broken chain link")
                if seal(event).hash != event.hash:
                    raise LedgerError(f"event {event.event_id}: content hash mismatch")
                previous = event.hash
        return True

    # --------------------------------------------------------------- helpers

    def _persist(self, events: list[Event], result: CommitResult, actor: str) -> None:
        if self._store is None:
            return
        document = {
            "work_order": result.work_order,
            "base_version": result.base_version,
            "version": result.version,
            "command_id": result.command_id,
            "actor": actor,
            "events": [event.as_dict() for event in events],
        }
        self._store.save(COMMIT_KIND, _commit_id(result.base_version), document)
        self._store.save(
            COMMAND_KIND,
            result.command_id,
            {"work_order": result.work_order, "version": result.version,
             "base_version": result.base_version, "event_ids": list(result.event_ids)},
        )

    def _replay(self) -> None:
        assert self._store is not None
        for record_id in self._store.list(COMMIT_KIND):
            document = self._store.load(COMMIT_KIND, record_id)
            if document is None:
                raise LedgerError(f"commit {record_id} is unreadable")
            base_version = int(document["base_version"])
            if base_version != len(self._events):
                raise LedgerError(
                    f"commit {record_id} expected base {len(self._events)}, found {base_version}"
                )
            rows = document.get("events") or []
            for row in rows:  # type: ignore[union-attr]
                event = Event.from_dict(dict(row))  # type: ignore[arg-type]
                if seal(event).hash != event.hash or event.prev_hash != self.head_hash():
                    raise LedgerError(f"event {event.event_id} fails chain verification")
                self._devices = apply_event(self._devices, event)
                self._events.append(event)
            command_id = str(document["command_id"])
            self._commands[command_id] = CommitResult(
                work_order=str(document["work_order"]),
                version=int(document["version"]),
                base_version=base_version,
                event_ids=tuple(str(item) for item in document.get("event_ids", ())),  # type: ignore[arg-type]
                command_id=command_id,
            )

    @staticmethod
    def _clone(device: Device) -> Device:
        return Device(**device.as_dict())
