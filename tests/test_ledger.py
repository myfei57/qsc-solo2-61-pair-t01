"""Behavioural tests for the unified, versioned equipment ledger."""

from __future__ import annotations

import unittest

from tunnelnet.ledger import (
    AUDIT_FEED,
    CONSOLE,
    ENV_ROUTING,
    FIRE_LINKAGE,
    CANONICAL_CONSUMERS,
    ConflictError,
    Device,
    DeviceKind,
    Fanout,
    Ledger,
    LedgerError,
    LedgerService,
    TargetState,
    alias,
    describe,
    is_canonical_id,
    kind_counts,
    retire,
    summarize,
    upsert,
)
from tunnelnet.ledger.ledger import GENESIS_DIGEST, digest_change_set


def cabin(device_id: str, section: str = "seg-a", adjacent=()) -> Device:
    return Device(id=device_id, kind=DeviceKind.CABIN, section=section, adjacent=tuple(adjacent))


def smoke(device_id: str, section: str = "seg-a", adjacent=()) -> Device:
    return Device(id=device_id, kind=DeviceKind.SMOKE, section=section, adjacent=tuple(adjacent))


def pit(device_id: str, section: str = "seg-a", adjacent=()) -> Device:
    return Device(id=device_id, kind=DeviceKind.PIT, section=section, adjacent=tuple(adjacent))


class ModelCase(unittest.TestCase):
    def test_canonical_id_shape(self) -> None:
        for good in ("cabin-1", "pit", "smoke-9", "a", "x-1-2"):
            self.assertTrue(is_canonical_id(good), good)
        for bad in ("", "Cabin-1", "1cabin", "has space", "cabin_1", "-x", "x" * 40):
            self.assertFalse(is_canonical_id(bad), bad)

    def test_device_round_trip(self) -> None:
        device = cabin("cabin-1", adjacent=("pit-1",))
        again = Device.from_dict(device.as_dict())
        self.assertEqual(again, device)

    def test_change_round_trip(self) -> None:
        for change in (upsert(cabin("cabin-1")), retire("cabin-1"), alias("FC-1", "fire", "cabin-1")):
            self.assertEqual(change, change.__class__.from_dict(change.as_dict()))


class CommitAndVersionCase(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(clock=lambda: "t")

    def test_first_change_set_chains_from_genesis(self) -> None:
        change_set = self.ledger.commit([upsert(cabin("cabin-1"))], author="a")
        self.assertEqual(change_set.sequence, 1)
        self.assertEqual(change_set.parent, GENESIS_DIGEST)
        self.assertTrue(change_set.digest)
        self.assertEqual(digest_change_set(change_set), change_set.digest)
        second = self.ledger.commit([upsert(cabin("cabin-2"))], author="a")
        self.assertEqual(second.parent, change_set.digest)
        self.assertTrue(self.ledger.verify_chain())

    def test_draft_does_not_move_running_version_until_activation(self) -> None:
        self.ledger.commit([upsert(cabin("cabin-1"))], author="a")
        self.assertEqual(self.ledger.active_sequence, 0)
        self.assertEqual(self.ledger.active().ids(), [])
        self.assertEqual(self.ledger.head().ids(), ["cabin-1"])
        activated = self.ledger.activate()
        self.assertEqual(activated.sequence, 1)
        self.assertEqual(self.ledger.active_sequence, 1)
        self.assertEqual(self.ledger.active().ids(), ["cabin-1"])

    def test_empty_change_set_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.ledger.commit([], author="a")

    def test_activation_target_is_bounded(self) -> None:
        self.ledger.commit([upsert(cabin("cabin-1"))], author="a")
        with self.assertRaises(ValueError):
            self.ledger.activate(target_sequence=5)
        with self.assertRaises(ValueError):
            self.ledger.activate(target_sequence=-1)

    def test_cutover_is_one_atomic_version_step(self) -> None:
        # Three devices committed together; activation exposes all at once.
        self.ledger.commit(
            [upsert(cabin("cabin-1")), upsert(pit("pit-1")), upsert(smoke("detector-1"))],
            author="a",
        )
        self.ledger.activate()
        self.assertEqual(self.ledger.active().ids(), ["cabin-1", "detector-1", "pit-1"])


class OptimisticConcurrencyCase(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(clock=lambda: "t")
        self.ledger.commit([upsert(cabin("cabin-1"))], author="a")

    def test_stale_base_is_rejected(self) -> None:
        self.ledger.commit([upsert(cabin("cabin-2"))], author="a", base_sequence=1)
        with self.assertRaises(ConflictError):
            self.ledger.commit([upsert(cabin("cabin-3"))], author="b", base_sequence=1)

    def test_retry_on_fresh_base_succeeds(self) -> None:
        self.ledger.commit([upsert(cabin("cabin-2"))], author="a", base_sequence=1)
        change_set = self.ledger.commit([upsert(cabin("cabin-3"))], author="b", base_sequence=2)
        self.assertEqual(change_set.sequence, 3)

    def test_idempotency_key_makes_replay_a_noop(self) -> None:
        first = self.ledger.commit(
            [upsert(cabin("cabin-2"))], author="a", idempotency_key="req-1"
        )
        replay = self.ledger.commit(
            [upsert(cabin("cabin-different"))], author="a", idempotency_key="req-1"
        )
        self.assertEqual(replay, first)
        self.assertEqual(self.ledger.committed_sequence, 2)


class ValidationCase(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(clock=lambda: "t")

    def test_adjacency_to_unknown_device_rejected(self) -> None:
        with self.assertRaises(LedgerError):
            self.ledger.commit([upsert(cabin("cabin-1", adjacent=("ghost",)))], author="a")

    def test_brand_new_neighbours_may_be_introduced_together(self) -> None:
        self.ledger.commit(
            [
                upsert(cabin("cabin-1", adjacent=("pit-1",))),
                upsert(pit("pit-1", adjacent=("cabin-1",))),
            ],
            author="a",
        )
        self.assertEqual(self.ledger.head().neighbors("cabin-1")[0].id, "pit-1")

    def test_self_adjacency_is_dropped(self) -> None:
        change_set = self.ledger.commit([upsert(cabin("cabin-1", adjacent=("cabin-1",)))], author="a")
        self.assertEqual(change_set.changes[0].device.adjacent, ())

    def test_alias_must_target_existing_device(self) -> None:
        with self.assertRaises(LedgerError):
            self.ledger.commit([alias("FC-1", "fire", "ghost")], author="a")

    def test_one_alias_cannot_map_to_two_devices(self) -> None:
        self.ledger.commit([upsert(cabin("cabin-1")), alias("FC-1", "fire", "cabin-1")], author="a")
        with self.assertRaises(LedgerError):
            self.ledger.commit([alias("FC-1", "fire", "cabin-other")], author="a")

    def test_same_foreign_id_in_different_systems_is_fine(self) -> None:
        self.ledger.commit(
            [
                upsert(cabin("cabin-1")),
                upsert(pit("pit-1")),
                alias("x", "fire", "cabin-1"),
                alias("x", "drain", "pit-1"),
            ],
            author="a",
        )
        self.assertEqual(self.ledger.head().resolve_alias("x", "fire"), "cabin-1")
        self.assertEqual(self.ledger.head().resolve_alias("x", "drain"), "pit-1")


class RetirementCase(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(clock=lambda: "t")
        self.ledger.commit(
            [
                upsert(cabin("cabin-1", adjacent=("pit-1",))),
                upsert(pit("pit-1", adjacent=("cabin-1",))),
            ],
            author="a",
        )
        self.ledger.activate()

    def test_retirement_severs_live_adjacency_but_keeps_history(self) -> None:
        self.ledger.commit([retire("pit-1")], author="b")
        self.ledger.activate()
        active = self.ledger.active()
        self.assertEqual(active.ids(), ["cabin-1"])
        self.assertEqual(active.neighbors("cabin-1"), [])
        retired = active.get("pit-1")
        self.assertIsNotNone(retired)
        self.assertTrue(retired.retired)

    def test_retired_id_cannot_be_edited_or_reregistered(self) -> None:
        self.ledger.commit([retire("pit-1")], author="b")
        with self.assertRaises(LedgerError):
            self.ledger.commit([upsert(pit("pit-1"))], author="c")
        with self.assertRaises(LedgerError):
            self.ledger.commit([retire("pit-1")], author="c")

    def test_historical_ownership_survives_removal(self) -> None:
        self.ledger.commit([retire("pit-1")], author="b", idempotency_key="rm")
        ownership = self.ledger.device_ownership("pit-1")
        self.assertEqual([entry["sequence"] for entry in ownership], [1, 2])
        self.assertEqual(ownership[1]["changes"][0]["op"], "retire")

    def test_as_of_projection_reconstructs_past_topology(self) -> None:
        self.ledger.commit([retire("pit-1")], author="b")
        at_v1 = self.ledger.projection_at(1)
        self.assertEqual(at_v1.get("pit-1").retired, False)
        self.assertEqual(self.ledger.projection_at(2).get("pit-1").retired, True)


class SnapshotAndChainCase(unittest.TestCase):
    def test_snapshot_is_version_tagged(self) -> None:
        ledger = Ledger(clock=lambda: "t")
        ledger.commit([upsert(cabin("cabin-1"))], author="a")
        head = ledger.snapshot(active=False)
        self.assertEqual(head.sequence, 1)
        self.assertEqual([device.id for device in head.devices], ["cabin-1"])
        fresh = ledger.snapshot(active=True)
        self.assertEqual(fresh.sequence, 0)
        self.assertEqual(fresh.devices, [])

    def test_tampering_breaks_chain_verification(self) -> None:
        ledger = Ledger(clock=lambda: "t")
        ledger.commit([upsert(cabin("cabin-1"))], author="a")
        ledger.commit([upsert(cabin("cabin-2"))], author="a")
        self.assertTrue(ledger.verify_chain())
        # Reach into the immutable log and rewrite history: chain must detect it.
        with ledger._lock:
            ledger._log[0] = ledger._log[0].__class__(
                **{**ledger._log[0].__dict__, "author": "forged"}
            )
        self.assertFalse(ledger.verify_chain())


class FanoutCase(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService(Ledger(clock=lambda: "t"))
        self.service.commit([upsert(cabin("cabin-1"))], author="a")

    def test_first_delivery_is_a_snapshot_then_deltas(self) -> None:
        first = self.service.cutover()
        self.assertEqual({d["mode"] for d in first["deliveries"]}, {"snapshot"})
        self.service.commit([upsert(cabin("cabin-2"))], author="a")
        second = self.service.cutover()
        self.assertEqual({d["mode"] for d in second["deliveries"]}, {"delta"})
        for delivery in second["deliveries"]:
            self.assertEqual([r["target"] for r in delivery["results"]], ["cabin-2"])

    def test_all_four_consumers_share_the_active_version(self) -> None:
        self.service.cutover()
        for name in CANONICAL_CONSUMERS:
            self.assertEqual(self.service.fanout.state(name).acked_sequence, 1)

    def test_partial_failure_blocks_only_the_failing_consumer(self) -> None:
        self.service.cutover()
        self.service.commit(
            [upsert(cabin("cabin-2")), upsert(smoke("detector-9"))], author="a"
        )
        self.service.fail_target(FIRE_LINKAGE, "cabin-2", "timeout")
        result = self.service.cutover()
        self.assertFalse(result["complete"])
        # Other consumers advance; the fire consumer stays behind.
        self.assertEqual(self.service.fanout.state(ENV_ROUTING).acked_sequence, 2)
        self.assertEqual(self.service.fanout.state(CONSOLE).acked_sequence, 2)
        fire = self.service.fanout.state(FIRE_LINKAGE)
        self.assertEqual(fire.acked_sequence, 1)
        self.assertIn("cabin-2", fire.failed_targets(2))

    def test_recovery_is_idempotent(self) -> None:
        self.service.cutover()
        self.service.commit([upsert(cabin("cabin-2")), upsert(smoke("detector-9"))], author="a")
        self.service.fail_target(FIRE_LINKAGE, "cabin-2", "timeout")
        self.service.cutover()
        first = self.service.recover()
        self.assertTrue(first["complete"])
        self.assertEqual(self.service.fanout.state(FIRE_LINKAGE).acked_sequence, 2)
        # Repeated recovery is a no-op and reports complete without re-applying.
        second = self.service.recover()
        self.assertTrue(second["complete"])
        fire = [d for d in second["deliveries"] if d["consumer"] == FIRE_LINKAGE][0]
        self.assertEqual(fire["results"], [])
        self.assertEqual(fire["mode"], "delta")

    def test_snapshot_resync_when_consumer_digest_diverges(self) -> None:
        self.service.cutover()
        self.service.commit([upsert(cabin("cabin-2"))], author="a")
        state = self.service.fanout.state(FIRE_LINKAGE)
        state.acked_digest = "tampered"
        self.service.ledger.activate()
        result = self.service.recover()
        fire = [d for d in result["deliveries"] if d["consumer"] == FIRE_LINKAGE][0]
        self.assertEqual(fire["mode"], "snapshot")
        self.assertTrue(fire["complete"])


class CrossShiftCase(unittest.TestCase):
    def test_pending_change_set_carries_over_and_target_activation_holds_it(self) -> None:
        service = LedgerService(Ledger(clock=lambda: "t"))
        service.commit([upsert(cabin("cabin-1"))], author="a")
        service.cutover(target_sequence=1)
        service.commit([upsert(cabin("cabin-2"))], author="a")
        service.commit([upsert(cabin("cabin-3"))], author="a")
        # Outgoing shift hands over with v3 deliberately not active.
        service.cutover(target_sequence=2)
        self.assertEqual(service.ledger.active_sequence, 2)
        self.assertEqual(service.ledger.active().ids(), ["cabin-1", "cabin-2"])
        self.assertEqual([c.sequence for c in service.pending()], [3])
        # Incoming shift activates the carried-over draft.
        service.cutover()
        self.assertEqual(service.ledger.active_sequence, 3)


class IsolationCase(unittest.TestCase):
    def test_immediate_isolation_without_moving_topology(self) -> None:
        service = LedgerService(Ledger(clock=lambda: "t"))
        service.commit([upsert(pit("pit-9"))], author="a")
        service.cutover()
        self.assertFalse(service.board.is_isolated("pit-9"))
        tag, newly = service.board.isolate("pit-9", author="duty", reason="removed")
        self.assertTrue(newly)
        self.assertTrue(service.board.is_isolated("pit-9"))
        # Structure and active version are untouched by the safety action.
        self.assertEqual(service.ledger.active_sequence, 1)
        self.assertFalse(service.ledger.active().get("pit-9").retired)
        self.assertIn("pit-9", service.board.active_ids())

    def test_repeated_isolation_is_idempotent(self) -> None:
        service = LedgerService(Ledger(clock=lambda: "t"))
        service.commit([upsert(pit("pit-9"))], author="a")
        service.board.isolate("pit-9", author="duty", reason="removed")
        _, newly = service.board.isolate("pit-9", author="duty", reason="again")
        self.assertFalse(newly)
        tag, lifted = service.board.release("pit-9", author="duty")
        self.assertTrue(lifted)
        self.assertFalse(tag.active)
        self.assertFalse(service.board.is_isolated("pit-9"))
        self.assertIsNone(service.board.release("pit-9", author="duty")[0])


class SummaryCase(unittest.TestCase):
    def test_summary_and_describe(self) -> None:
        service = LedgerService(Ledger(clock=lambda: "t"))
        service.commit(
            [
                upsert(cabin("cabin-1", section="zone-1")),
                upsert(pit("pit-1", section="zone-1")),
                upsert(smoke("detector-1", section="zone-2")),
            ],
            author="a",
        )
        service.cutover()
        service.commit([retire("pit-1")], author="b")
        summary = summarize(service)
        self.assertEqual(summary.active_devices, 3)
        self.assertEqual(summary.sections, 2)
        self.assertEqual(summary.consumers_current, 4)
        self.assertIn("active=v1", describe(service))
        self.assertIn("pending=1", describe(service))
        head_projection = service.ledger.head()
        counts = kind_counts(head_projection)
        # Retired pit-1 is excluded from the live kind counts.
        self.assertEqual(set(counts), {"cabin", "smoke"})


if __name__ == "__main__":
    unittest.main()
