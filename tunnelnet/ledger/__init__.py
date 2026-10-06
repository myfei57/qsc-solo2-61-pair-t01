"""Unified, versioned equipment ledger.

One canonical book for cabins, sump pits, smoke detectors and cable sections.
Structural edits are immutable, hash-chained change sets that reach running
linkage only at a shift cutover; a physical removal is additionally covered by
an immediate safety isolation interlock.
"""

from .graph import ConflictError, LedgerError, Projection
from .ledger import GENESIS_DIGEST, Ledger, Snapshot, digest_change_set
from .model import (
    Alias,
    Change,
    ChangeSet,
    Device,
    DeviceKind,
    Op,
    alias,
    is_canonical_id,
    retire,
    upsert,
)
from .propagation import ConsumerState, Delivery, Fanout, TargetResult, TargetState
from .report import LedgerSummary, describe, kind_counts, summarize
from .safety import Isolation, IsolationBoard
from .service import (
    AUDIT_FEED,
    CANONICAL_CONSUMERS,
    CONSOLE,
    ENV_ROUTING,
    FIRE_LINKAGE,
    LedgerService,
)

__all__ = [
    "AUDIT_FEED",
    "CANONICAL_CONSUMERS",
    "CONSOLE",
    "ENV_ROUTING",
    "FIRE_LINKAGE",
    "Alias",
    "Change",
    "ChangeSet",
    "ConflictError",
    "ConsumerState",
    "Delivery",
    "Device",
    "DeviceKind",
    "Fanout",
    "GENESIS_DIGEST",
    "Isolation",
    "IsolationBoard",
    "Ledger",
    "LedgerError",
    "LedgerService",
    "LedgerSummary",
    "Op",
    "Projection",
    "Snapshot",
    "TargetResult",
    "TargetState",
    "alias",
    "describe",
    "digest_change_set",
    "is_canonical_id",
    "kind_counts",
    "retire",
    "summarize",
    "upsert",
]
