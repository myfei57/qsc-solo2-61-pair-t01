"""Topology projection and validation over the canonical devices.

The projection folds the append-only change log into the device map at a given
point in history. Adjacency is always recorded on both ends so that after a
removal "who was next to whom" can still be answered for the past.
"""

from __future__ import annotations

from .model import Device, DeviceKind


class LedgerError(ValueError):
    """A change set that would leave the ledger structurally invalid."""


class ConflictError(LedgerError):
    """Raised when an optimistic-concurrency base version is stale."""


class Projection:
    """The materialised device and alias view at one point in the log."""

    def __init__(self) -> None:
        self._devices: dict[str, Device] = {}
        self._aliases: dict[tuple[str, str], str] = {}

    def copy(self) -> "Projection":
        clone = Projection()
        clone._devices = dict(self._devices)
        clone._aliases = dict(self._aliases)
        return clone

    def get(self, device_id: str) -> Device | None:
        return self._devices.get(device_id)

    def require(self, device_id: str) -> Device:
        device = self._devices.get(device_id)
        if device is None:
            raise LedgerError(f"device {device_id} not found")
        return device

    def ids(self, *, include_retired: bool = False) -> list[str]:
        return sorted(
            device_id
            for device_id, device in self._devices.items()
            if include_retired or not device.retired
        )

    def devices(self, *, include_retired: bool = False) -> list[Device]:
        return [self._devices[device_id] for device_id in self.ids(include_retired=include_retired)]

    def by_kind(self, kind: DeviceKind, *, include_retired: bool = False) -> list[Device]:
        return [
            device
            for device in self.devices(include_retired=include_retired)
            if device.kind is kind or device.kind == kind
        ]

    def in_section(self, section: str) -> list[Device]:
        return [device for device in self.devices() if device.section == section]

    def neighbors(self, device_id: str) -> list[Device]:
        """Active devices adjacent to ``device_id`` at this projection."""

        device = self.require(device_id)
        out: list[Device] = []
        for neighbor_id in device.adjacent:
            neighbor = self._devices.get(neighbor_id)
            if neighbor is not None and not neighbor.retired:
                out.append(neighbor)
        return out

    def resolve_alias(self, foreign_id: str, system: str) -> str:
        return self._aliases.get((foreign_id, system), "")

    def alias_map(self) -> dict[tuple[str, str], str]:
        return dict(self._aliases)

    # -- mutation used only while folding a validated change set -----------

    def _put(self, device: Device) -> None:
        self._devices[device.id] = device

    def _retire(self, device_id: str) -> None:
        device = self.require(device_id)
        if device.retired:
            raise LedgerError(f"device {device_id} is already retired")
        # Sever adjacency on both ends while keeping the retired device in the
        # book, so historical ownership survives but the live topology drops it.
        for neighbor_id in device.adjacent:
            neighbor = self._devices.get(neighbor_id)
            if neighbor is None or neighbor.retired:
                continue
            self._devices[neighbor_id] = Device(
                id=neighbor.id,
                kind=neighbor.kind,
                section=neighbor.section,
                name=neighbor.name,
                adjacent=tuple(item for item in neighbor.adjacent if item != device_id),
                retired=neighbor.retired,
            )
        self._devices[device_id] = Device(
            id=device.id,
            kind=device.kind,
            section=device.section,
            name=device.name,
            adjacent=device.adjacent,
            retired=True,
        )

    def _alias(self, foreign_id: str, system: str, device_id: str) -> None:
        self.require(device_id)
        self._aliases[(foreign_id, system)] = device_id


def symmetric(device: Device) -> Device:
    """Normalise adjacency so the device's own id is never listed."""

    return Device(
        id=device.id,
        kind=device.kind,
        section=device.section,
        name=device.name,
        adjacent=tuple(sorted({item for item in device.adjacent if item and item != device.id})),
        retired=device.retired,
    )


def validate(projection: Projection, changes: tuple) -> None:
    """Validate a change set against the current projection.

    Two passes let a single change set introduce two brand new neighbours in
    any order:

      * pass one stages every upsert/retire and checks canonical ids plus the
        "never edit or re-use a retired device" rule;
      * pass two validates adjacency and alias targets against the fully
        staged projection;
      * a single foreign alias cannot point at two different devices.
    """

    from .model import Change, Op, is_canonical_id

    staged = projection.copy()
    upserts: list[Device] = []
    seen_aliases: set[tuple[str, str]] = set()

    for change in changes:
        if not isinstance(change, Change):
            raise LedgerError("change set entry is not a Change")
        if not is_canonical_id(change.device_id):
            raise LedgerError(f"device id {change.device_id!r} is not canonical")

        if change.op is Op.UPSERT:
            device = change.device
            if device is None:
                raise LedgerError("upsert change carries no device")
            if device.id != change.device_id:
                raise LedgerError("upsert device id does not match the change target")
            device = symmetric(device)
            current = staged.get(device.id)
            if current is not None and current.retired:
                raise LedgerError(f"retired device {device.id} cannot be edited or reused")
            staged._put(device)
            upserts.append(device)
        elif change.op is Op.RETIRE:
            staged._retire(change.device_id)
        elif change.op is Op.ALIAS:
            if not change.alias or not change.system:
                raise LedgerError("alias change requires both alias and system")
            key = (change.alias, change.system)
            existing = projection.resolve_alias(change.alias, change.system)
            if existing and existing != change.device_id:
                raise LedgerError(
                    f"alias {change.alias} in {change.system} already maps to {existing}"
                )
            if key in seen_aliases:
                raise LedgerError(f"alias {change.alias} in {change.system} declared twice")
            seen_aliases.add(key)
        else:  # pragma: no cover - exhaustive enum
            raise LedgerError(f"unknown change op {change.op}")

    # Adjacency is checked only after the whole set is staged.
    for device in upserts:
        for neighbor_id in device.adjacent:
            if not is_canonical_id(neighbor_id):
                raise LedgerError(f"adjacent id {neighbor_id!r} is not canonical")
            neighbor = staged.get(neighbor_id)
            if neighbor is None:
                raise LedgerError(f"device {device.id} is adjacent to unknown {neighbor_id}")
            if neighbor.retired:
                raise LedgerError(f"device {device.id} cannot be adjacent to retired {neighbor_id}")

    # Alias targets must exist in the current projection or this change set.
    for change in changes:
        if change.op is Op.ALIAS:
            staged.require(change.device_id)
