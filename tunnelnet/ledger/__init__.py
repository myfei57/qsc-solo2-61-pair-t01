"""Unified equipment ledger: one register of devices, segments and adjacency.

The ledger is the single source of truth read by environment routing, fire
linkage, the audit flow and the console page. It is an append-only event log:
every fact (registration, relocation, re-linking, retirement) is one chained
event, so a removed device keeps an unalterable history of where it used to
belong.
"""

from .activation import (
    ACTIVE,
    AUDIT,
    CONSOLE,
    CONSUMERS,
    DEGRADED,
    ENV_ROUTING,
    FIRE_LINKAGE,
    LAGGING,
    ActivationManager,
    ConsumerState,
)
from .delta import Delta, Snapshot, diff, snapshot
from .model import (
    DEVICE_KINDS,
    Change,
    CommitResult,
    Device,
    DeviceKind,
    Event,
    apply_event,
    canonical,
    digest_of,
)
from .registry import Ledger, LedgerConflict, LedgerError
from .service import LedgerService, new_command_id
from .shift import ShiftBlocked, ShiftBoard

__all__ = [
    "ACTIVE",
    "AUDIT",
    "CONSUMERS",
    "CONSOLE",
    "DEVICE_KINDS",
    "DEGRADED",
    "ENV_ROUTING",
    "FIRE_LINKAGE",
    "LAGGING",
    "ActivationManager",
    "Change",
    "CommitResult",
    "ConsumerState",
    "Delta",
    "Device",
    "DeviceKind",
    "Event",
    "Ledger",
    "LedgerConflict",
    "LedgerError",
    "LedgerService",
    "ShiftBlocked",
    "ShiftBoard",
    "Snapshot",
    "apply_event",
    "canonical",
    "digest_of",
    "diff",
    "new_command_id",
    "snapshot",
]
