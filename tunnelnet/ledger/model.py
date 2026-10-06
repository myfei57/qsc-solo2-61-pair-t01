"""Device, change and event records of the unified ledger.

Devices carry two identifiers:

* ``id``  - the lifelong equipment serial, issued once and never reissued.
* ``code`` - the in-place position code (cabin / pit / detector / cable tag)
  that moves with the slot and may change on relocation. Retired position
  codes are kept as aliases so old subsystem numbers keep resolving.

Only facts are stored. The current register is a *projection*: it is rebuilt
by replaying every event in sequence. Events are chained with SHA-256, so the
history of a removed device cannot be silently rewritten.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

GENESIS_HASH = "0" * 64


class DeviceKind(str, Enum):
    """Every equipment class that shares the one register."""

    CABIN = "cabin"
    PIT = "pit"
    DETECTOR = "detector"
    CABLE = "cable"


DEVICE_KINDS = tuple(kind.value for kind in DeviceKind)

# Events that change where a device sits or what it sits next to.
POSITION_EVENTS = ("registered", "relocated", "retired")
TOPOLOGY_EVENTS = ("linked", "unlinked")


@dataclass(frozen=True)
class Device:
    """One equipment item as projected at a ledger version."""

    id: str
    kind: str
    code: str
    segment: str
    adjacent: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    retired: bool = False

    def __post_init__(self) -> None:
        # When rebuilt from JSON (as_dict) the tuple fields arrive as lists.
        object.__setattr__(self, "adjacent", tuple(self.adjacent))
        object.__setattr__(self, "aliases", tuple(self.aliases))

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "code": self.code,
            "segment": self.segment,
            "adjacent": list(self.adjacent),
            "aliases": list(self.aliases),
            "retired": self.retired,
        }


@dataclass(frozen=True)
class Change:
    """One requested fact inside a work order.

    ``device_id`` is the lifelong serial. ``neighbours`` (link/unlink) lists
    the lifelong serials on the other end of each adjacency edge.
    """

    op: str
    device_id: str
    kind: str = ""
    code: str = ""
    segment: str = ""
    neighbours: tuple[str, ...] = ()
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "device_id": self.device_id,
            "kind": self.kind,
            "code": self.code,
            "segment": self.segment,
            "neighbours": list(self.neighbours),
            "note": self.note,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "Change":
        neighbours = payload.get("neighbours") or ()
        if isinstance(neighbours, str):
            neighbours = (neighbours,)
        return cls(
            op=str(payload.get("op", "")),
            device_id=str(payload.get("device_id", "")),
            kind=str(payload.get("kind", "")),
            code=str(payload.get("code", "")),
            segment=str(payload.get("segment", "")),
            neighbours=tuple(str(item) for item in neighbours),
            note=str(payload.get("note", "")),
        )


@dataclass(frozen=True)
class Event:
    """An append-only, hash-chained fact."""

    seq: int
    event_id: str
    device_id: str
    op: str
    at: str
    work_order: str
    actor: str
    base_version: int
    command_id: str
    prev_hash: str
    payload: dict[str, Any] = field(default_factory=dict)
    hash: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "device_id": self.device_id,
            "op": self.op,
            "at": self.at,
            "work_order": self.work_order,
            "actor": self.actor,
            "base_version": self.base_version,
            "command_id": self.command_id,
            "prev_hash": self.prev_hash,
            "payload": self.payload,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "Event":
        return cls(
            seq=int(row["seq"]),
            event_id=str(row["event_id"]),
            device_id=str(row["device_id"]),
            op=str(row["op"]),
            at=str(row["at"]),
            work_order=str(row["work_order"]),
            actor=str(row.get("actor", "")),
            base_version=int(row.get("base_version", 0)),
            command_id=str(row.get("command_id", "")),
            prev_hash=str(row["prev_hash"]),
            payload=dict(row.get("payload") or {}),
            hash=str(row.get("hash", "")),
        )


@dataclass(frozen=True)
class CommitResult:
    """Outcome of a validated work order commit."""

    work_order: str
    version: int
    base_version: int
    event_ids: tuple[str, ...]
    command_id: str
    duplicate: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_order": self.work_order,
            "version": self.version,
            "base_version": self.base_version,
            "event_ids": list(self.event_ids),
            "command_id": self.command_id,
            "duplicate": self.duplicate,
        }


def canonical(value: Any) -> str:
    """Stable JSON text used for hashes and digest comparison."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest_of(value: Any) -> str:
    """Content digest of a projection (snapshot or delta)."""

    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _chained_hash(event: Event) -> str:
    body = canonical(
        {
            "seq": event.seq,
            "event_id": event.event_id,
            "device_id": event.device_id,
            "op": event.op,
            "at": event.at,
            "work_order": event.work_order,
            "actor": event.actor,
            "base_version": event.base_version,
            "command_id": event.command_id,
            "prev_hash": event.prev_hash,
            "payload": event.payload,
        }
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def seal(event: Event) -> Event:
    """Return the event with its chain hash filled in."""

    return Event(**{**event.as_dict(), "hash": _chained_hash(event)})


def _set_adjacency(devices: dict[str, Device], left: str, right: str, linked: bool) -> None:
    left_device = devices[left]
    right_device = devices[right]
    left_neighbours = set(left_device.adjacent)
    right_neighbours = set(right_device.adjacent)
    if linked:
        left_neighbours.add(right)
        right_neighbours.add(left)
    else:
        left_neighbours.discard(right)
        right_neighbours.discard(left)
    devices[left] = Device(**{**left_device.as_dict(), "adjacent": tuple(sorted(left_neighbours))})
    devices[right] = Device(**{**right_device.as_dict(), "adjacent": tuple(sorted(right_neighbours))})


def apply_event(devices: dict[str, Device], event: Event) -> dict[str, Device]:
    """Project one event onto a register, returning a new register state."""

    next_devices = copy.deepcopy(devices)
    payload = event.payload

    if event.op == "registered":
        next_devices[event.device_id] = Device(
            id=event.device_id,
            kind=str(payload.get("kind", "")),
            code=str(payload.get("code", "")),
            segment=str(payload.get("segment", "")),
            adjacent=(),
            aliases=(),
            retired=False,
        )
    elif event.op == "relocated":
        device = next_devices[event.device_id]
        code = str(payload.get("code", device.code))
        segment = str(payload.get("segment", device.segment))
        aliases = set(device.aliases)
        if code != device.code and device.code:
            aliases.add(device.code)
        next_devices[event.device_id] = Device(
            **{
                **device.as_dict(),
                "code": code,
                "segment": segment,
                "aliases": tuple(sorted(aliases)),
            }
        )
    elif event.op == "retired":
        device = next_devices[event.device_id]
        next_devices[event.device_id] = Device(**{**device.as_dict(), "retired": True})
    elif event.op == "linked":
        for neighbour in payload.get("neighbours", ()):  # type: ignore[union-attr]
            _set_adjacency(next_devices, event.device_id, str(neighbour), True)
    elif event.op == "unlinked":
        for neighbour in payload.get("neighbours", ()):  # type: ignore[union-attr]
            _set_adjacency(next_devices, event.device_id, str(neighbour), False)
    else:
        raise ValueError(f"unknown ledger op {event.op!r}")
    return next_devices
