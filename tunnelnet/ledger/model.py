"""Canonical equipment records held by the unified ledger.

Every device that used to live in its own subsystem book (cabin, sump pit,
smoke detector, cable section ...) is registered here exactly once under a
canonical identifier, a section and the set of devices adjacent to it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

# A canonical id is short, stable and human readable, e.g. "cabin-1".
ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{0,31}$")


class DeviceKind(str, Enum):
    """The equipment families unified by the ledger."""

    CABIN = "cabin"
    PIT = "pit"
    SMOKE = "smoke"
    CABLE = "cable"

    def __str__(self) -> str:
        return self.value


def is_canonical_id(value: str) -> bool:
    """Whether ``value`` is a well formed canonical identifier."""

    return bool(ID_PATTERN.match(value or ""))


@dataclass(frozen=True)
class Device:
    """One physical device as recorded in the canonical ledger.

    ``retired`` marks a device that has been physically removed. Retired
    devices stay in the immutable history forever and may never be edited or
    re-registered under the same id.
    """

    id: str
    kind: DeviceKind
    section: str
    name: str = ""
    adjacent: tuple[str, ...] = ()
    retired: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "kind": self.kind.value,
            "section": self.section,
            "name": self.name,
            "adjacent": list(self.adjacent),
            "retired": self.retired,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Device":
        raw_adjacent = payload.get("adjacent", ())
        return cls(
            id=str(payload.get("id", "")),
            kind=DeviceKind(str(payload.get("kind", ""))),
            section=str(payload.get("section", "")),
            name=str(payload.get("name", "")),
            adjacent=tuple(str(item) for item in raw_adjacent),
            retired=bool(payload.get("retired", False)),
        )


@dataclass(frozen=True)
class Alias:
    """A foreign subsystem id mapped onto one canonical device id.

    The alias table is what resolves the mismatched legacy numbering: each
    subsystem keeps calling a device its old name, but every name points at
    the same canonical record and therefore the same version.
    """

    alias: str
    system: str
    device_id: str

    def as_dict(self) -> dict[str, str]:
        return {"alias": self.alias, "system": self.system, "device_id": self.device_id}

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Alias":
        return cls(
            alias=str(payload.get("alias", "")),
            system=str(payload.get("system", "")),
            device_id=str(payload.get("device_id", "")),
        )


# ---------------------------------------------------------------------------
# Change set operations
# ---------------------------------------------------------------------------


class Op(str, Enum):
    """The single-device operations carried inside a change set."""

    UPSERT = "upsert"
    RETIRE = "retire"
    ALIAS = "alias"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Change:
    """One atomic device operation inside a committed change set."""

    op: Op
    device_id: str
    # Present for upsert: the desired canonical device body.
    device: Device | None = None
    # Present for alias: (foreign id, subsystem name).
    alias: str = ""
    system: str = ""

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"op": self.op.value, "device_id": self.device_id}
        if self.device is not None:
            payload["device"] = self.device.as_dict()
        if self.alias:
            payload["alias"] = self.alias
            payload["system"] = self.system
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Change":
        device_body = payload.get("device")
        return cls(
            op=Op(str(payload.get("op", ""))),
            device_id=str(payload.get("device_id", "")),
            device=None if device_body is None else Device.from_dict(dict(device_body)),
            alias=str(payload.get("alias", "")),
            system=str(payload.get("system", "")),
        )


def upsert(device: Device) -> Change:
    return Change(op=Op.UPSERT, device_id=device.id, device=device)


def retire(device_id: str) -> Change:
    return Change(op=Op.RETIRE, device_id=device_id)


def alias(foreign_id: str, system: str, device_id: str) -> Change:
    return Change(op=Op.ALIAS, device_id=device_id, alias=foreign_id, system=system)


@dataclass(frozen=True)
class ChangeSet:
    """An ordered, immutable batch of device changes.

    Change sets form a hash chain: ``parent`` is the hash of the previously
    committed change set, and ``digest`` covers the parent and the payload, so
    history cannot be silently rewritten and a consumer can detect a gap or a
    fork by checking the chain.
    """

    sequence: int
    parent: str
    author: str
    at: str
    changes: tuple[Change, ...]
    digest: str = ""
    # Provenance of the request that produced this change set, used to make a
    # retried submission a no-op rather than a second commit.
    idempotency_key: str = ""

    def touching(self) -> tuple[str, ...]:
        """Canonical device ids modified by this change set, sorted."""

        return tuple(sorted({change.device_id for change in self.changes}))

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "parent": self.parent,
            "author": self.author,
            "at": self.at,
            "changes": [change.as_dict() for change in self.changes],
            "digest": self.digest,
            "idempotency_key": self.idempotency_key,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ChangeSet":
        raw_changes = payload.get("changes", ())
        return cls(
            sequence=int(payload.get("sequence", 0)),
            parent=str(payload.get("parent", "")),
            author=str(payload.get("author", "")),
            at=str(payload.get("at", "")),
            changes=tuple(Change.from_dict(dict(item)) for item in raw_changes),
            digest=str(payload.get("digest", "")),
            idempotency_key=str(payload.get("idempotency_key", "")),
        )
