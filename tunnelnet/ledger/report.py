"""Read-only projections and summaries over the unified ledger."""

from __future__ import annotations

from dataclasses import dataclass

from .ledger import Ledger
from .model import DeviceKind
from .service import CANONICAL_CONSUMERS


@dataclass(frozen=True)
class LedgerSummary:
    """Counts and version pointers shown on the console."""

    committed_sequence: int
    active_sequence: int
    pending_changes: int
    total_devices: int
    active_devices: int
    retired_devices: int
    sections: int
    isolated: int
    consumers: int
    consumers_current: int

    def as_dict(self) -> dict[str, object]:
        return {
            "committed_sequence": self.committed_sequence,
            "active_sequence": self.active_sequence,
            "pending_changes": self.pending_changes,
            "total_devices": self.total_devices,
            "active_devices": self.active_devices,
            "retired_devices": self.retired_devices,
            "sections": self.sections,
            "isolated": self.isolated,
            "consumers": self.consumers,
            "consumers_current": self.consumers_current,
        }


def summarize(service) -> LedgerSummary:
    ledger: Ledger = service.ledger
    head = ledger.head()
    active = ledger.active()
    all_devices = head.devices(include_retired=True)
    # Devices the running linkages can act on come from the active version;
    # retired/new-draft counts come from the committed head.
    live = active.devices(include_retired=False)
    retired = sum(1 for device in all_devices if device.retired)
    sections = len({device.section for device in live if device.section})

    current = 0
    for name in CANONICAL_CONSUMERS:
        if service.fanout.state(name).is_current(ledger) and not service.fanout.state(
            name
        ).pending_failure_sequences():
            current += 1

    return LedgerSummary(
        committed_sequence=ledger.committed_sequence,
        active_sequence=ledger.active_sequence,
        pending_changes=len(ledger.pending()),
        total_devices=len(all_devices),
        active_devices=len(live),
        retired_devices=retired,
        sections=sections,
        isolated=len(service.board.active_ids()),
        consumers=len(CANONICAL_CONSUMERS),
        consumers_current=current,
    )


def describe(service) -> str:
    summary = summarize(service)
    return (
        f"ledger active=v{summary.active_sequence} committed=v{summary.committed_sequence} "
        f"pending={summary.pending_changes} devices={summary.active_devices} "
        f"retired={summary.retired_devices} isolated={summary.isolated} "
        f"consumers={summary.consumers_current}/{summary.consumers}"
    )


def kind_counts(projection) -> dict[str, int]:
    return {
        kind.value: len(projection.by_kind(kind))
        for kind in DeviceKind
        if projection.by_kind(kind)
    }
