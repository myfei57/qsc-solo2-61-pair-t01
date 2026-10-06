"""Default values written on first boot."""

from __future__ import annotations

from tunnelnet.cabin import Cabin
from tunnelnet.cable import Cable
from tunnelnet.drain import Pit
from tunnelnet.fire import Detector
from tunnelnet.ledger import (
    Device,
    DeviceKind,
    alias,
    upsert,
)

from .runtime import Runtime

SEED_AT = "2026-01-01T00:00:00Z"


def seed_defaults(rt: Runtime) -> None:
    """Make sure every component has a usable starting value."""

    rt.namespaces.add("segment-a", "segment A")
    rt.namespaces.add("segment-b", "segment B")

    for index, (cabin_id, zone, partition) in enumerate(
        (("cabin-1", "zone-1", "part-1"), ("cabin-2", "zone-2", "part-2"), ("cabin-3", "zone-3", "part-3")),
        start=1,
    ):
        rt.cabins.upsert(
            Cabin(id=cabin_id, name=f"cabin {index}", zone=zone, partition=partition, revision=index)
        )
        rt.partitions.repartition(partition, cabin_id)
        rt.env.bind_sensor_at(f"sensor-{index}", partition)

    rt.drain.add_pit(Pit(id="pit-1", cabin="cabin-1", water_level=0.2))
    rt.drain.add_pit(Pit(id="pit-2", cabin="cabin-2", water_level=0.1))

    rt.fire.add_detector(Detector(id="detector-1", zone="zone-1"))
    rt.fire.add_detector(Detector(id="detector-2", zone="zone-2"))

    rt.power_book.set_baseline("cabin-1", 4200.0)
    rt.power_book.set_baseline("cabin-2", 3800.0)
    rt.power_book.set_baseline("cabin-3", 4600.0)

    rt.cables.add(Cable(id="cable-1", cabin="cabin-1", temp=42.0))
    rt.cables.add(Cable(id="cable-2", cabin="cabin-2", temp=46.0))

    rt.env.sample("sensor-1", 48.0, SEED_AT)
    rt.env.sample("sensor-1", 51.0, SEED_AT)
    rt.env.sample("sensor-2", 55.0, SEED_AT)
    rt.patrol.check_in("cabin-1", SEED_AT)
    rt.patrol.set_due("cabin-2", "2025-12-31T00:00:00Z")
    rt.quota.add_usage("cabin-1", 1200.0)
    rt.quota.add_usage("cabin-2", 900.0)
    rt.drain.record_cycle("cabin-1", SEED_AT)
    rt.drain.record_cycle("cabin-1", SEED_AT)
    rt.cables.sample("cable-1", 42.0)
    rt.cables.sample("cable-1", 47.0)

    rt.patrol.rebuild_route()

    seed_ledger(rt)


def seed_ledger(rt: Runtime) -> None:
    """Register the existing devices once in the canonical ledger and activate.

    Legacy subsystem ids are attached as aliases so the mismatched numbering
    resolves onto one canonical id and therefore one activated version.
    """

    ledger = rt.ledger
    changes = [
        upsert(Device(id="cabin-1", kind=DeviceKind.CABIN, section="zone-1", name="cabin 1",
                      adjacent=("pit-1", "cable-1", "detector-1"))),
        upsert(Device(id="cabin-2", kind=DeviceKind.CABIN, section="zone-2", name="cabin 2",
                      adjacent=("pit-2", "cable-2", "detector-2"))),
        upsert(Device(id="cabin-3", kind=DeviceKind.CABIN, section="zone-3", name="cabin 3")),
        upsert(Device(id="pit-1", kind=DeviceKind.PIT, section="zone-1",
                      adjacent=("cabin-1",))),
        upsert(Device(id="pit-2", kind=DeviceKind.PIT, section="zone-2",
                      adjacent=("cabin-2",))),
        upsert(Device(id="detector-1", kind=DeviceKind.SMOKE, section="zone-1",
                      adjacent=("cabin-1",))),
        upsert(Device(id="detector-2", kind=DeviceKind.SMOKE, section="zone-2",
                      adjacent=("cabin-2",))),
        upsert(Device(id="cable-1", kind=DeviceKind.CABLE, section="zone-1",
                      adjacent=("cabin-1",))),
        upsert(Device(id="cable-2", kind=DeviceKind.CABLE, section="zone-2",
                      adjacent=("cabin-2",))),
        # Legacy subsystem numbering mapped onto canonical ids.
        alias("FC-1001", "fire", "detector-1"),
        alias("PMP-PIT-01", "drain", "pit-1"),
        alias("CBL-A", "cable", "cable-1"),
    ]
    ledger.commit(changes, author="seed", idempotency_key="seed-v1")
    ledger.cutover()

