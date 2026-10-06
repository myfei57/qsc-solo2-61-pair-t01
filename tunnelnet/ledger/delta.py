"""Change propagation: delta events for transport, full snapshot for repair.

The rule is **send the delta, activate the whole version**:

* Downstream subsystems normally receive a small :class:`Delta` describing
  only the devices touched between two versions - cheap over any link.
* On startup, after a gap, or when a digest does not match, a consumer pulls
  the full :class:`Snapshot` for a version and replaces its projection in one
  step. Every message names its version and carries a content digest, so a
  silently dropped or corrupted delta is detected rather than healed blindly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .model import Device, Event, digest_of

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from .registry import Ledger


@dataclass(frozen=True)
class Snapshot:
    """The complete register at one version."""

    version: int
    head_hash: str
    devices: dict[str, dict[str, Any]]
    digest: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "head_hash": self.head_hash,
            "digest": self.digest,
            "devices": self.devices,
        }


@dataclass(frozen=True)
class Delta:
    """Only the devices and events touched between two versions."""

    base_version: int
    version: int
    head_hash: str
    events: list[dict[str, Any]] = field(default_factory=list)
    devices: dict[str, dict[str, Any]] = field(default_factory=dict)
    digest: str = ""

    @property
    def touched(self) -> list[str]:
        seen: list[str] = []
        for event in self.events:
            device_id = str(event["device_id"])
            if device_id not in seen:
                seen.append(device_id)
        return seen

    def as_dict(self) -> dict[str, Any]:
        return {
            "base_version": self.base_version,
            "version": self.version,
            "head_hash": self.head_hash,
            "digest": self.digest,
            "events": self.events,
            "devices": self.devices,
        }


def _device_map(devices: dict[str, Device]) -> dict[str, dict[str, Any]]:
    return {key: devices[key].as_dict() for key in sorted(devices)}


def snapshot(ledger: "Ledger", version: int | None = None) -> Snapshot:
    """Build the full projection at one version (default: head)."""

    target = ledger.version() if version is None else version
    devices = ledger.state_at(target)
    head_hash = ledger.events()[target - 1].hash if target else "0" * 64
    device_map = _device_map(devices)
    return Snapshot(
        version=target,
        head_hash=head_hash,
        devices=device_map,
        digest=digest_of({"version": target, "head_hash": head_hash, "devices": device_map}),
    )


def diff(ledger: "Ledger", base_version: int, version: int | None = None) -> Delta:
    """Build the small change block a consumer applies on top of ``base``."""

    target = ledger.version() if version is None else version
    if base_version < 0 or base_version > target:
        raise ValueError(f"invalid range {base_version}..{target}")
    events = ledger.events()[base_version:target]
    touched = {event.device_id for event in events}
    end_state = ledger.state_at(target)
    devices = {device_id: end_state[device_id].as_dict() for device_id in sorted(touched)}
    head_hash = snapshot(ledger, target).head_hash
    event_rows = [event.as_dict() for event in events]
    return Delta(
        base_version=base_version,
        version=target,
        head_hash=head_hash,
        events=event_rows,
        devices=devices,
        digest=digest_of(
            {
                "base_version": base_version,
                "version": target,
                "head_hash": head_hash,
                "events": event_rows,
                "devices": devices,
            }
        ),
    )


def event_range(ledger: "Ledger", base_version: int, version: int | None = None) -> list[Event]:
    """Raw events a consumer would replay to catch up from ``base_version``."""

    target = ledger.version() if version is None else version
    return ledger.events()[base_version:target]
