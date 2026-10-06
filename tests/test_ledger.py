"""Tests for the unified equipment ledger and its activation/shift rules."""

from __future__ import annotations

import os
import tempfile
import unittest

from tunnelnet.console.server import Server
from tunnelnet.ledger import (
    AUDIT,
    CONSOLE,
    CONSUMERS,
    ENV_ROUTING,
    FIRE_LINKAGE,
    Change,
    Ledger,
    LedgerConflict,
    LedgerError,
    LedgerService,
    ShiftBlocked,
)
from tunnelnet.ledger.activation import IdempotentApplier
from tunnelnet.ledger.changeset import ValidationError
from tunnelnet.store import Store


def register_pair(service: LedgerService, work_order: str = "WO-1",
                  command: str = "c1") -> dict:
    return service.commit(
        [
            {"op": "registered", "device_id": "EQ-A", "kind": "cabin",
             "code": "C-001", "segment": "K1+000"},
            {"op": "registered", "device_id": "EQ-B", "kind": "pit",
             "code": "P-001", "segment": "K1+050"},
        ],
        work_order=work_order, expected_version=0, command_id=command,
        actor="zhang", at="2026-10-06T08:00:00Z",
    )


class RegisterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService()

    def test_register_advances_version_by_event_count(self) -> None:
        result = register_pair(self.service)
        self.assertEqual(result["commit"]["version"], 2)
        self.assertEqual(result["commit"]["event_ids"], ["WO-1:1", "WO-1:2"])
        self.assertEqual(self.service.ledger.version(), 2)
        self.assertEqual(self.service.ledger.count(), 2)

    def test_one_register_holds_segment_code_kind_and_adjacency(self) -> None:
        register_pair(self.service)
        self.service.commit(
            [Change("linked", "EQ-A", neighbours=("EQ-B",))],
            work_order="WO-2", expected_version=2, command_id="c2",
        )
        device = self.service.ledger.device("EQ-A")
        self.assertEqual(device.segment, "K1+000")
        self.assertEqual(device.code, "C-001")
        self.assertEqual(device.kind, "cabin")
        self.assertEqual(device.adjacent, ("EQ-B",))
        self.assertEqual(self.service.ledger.device("EQ-B").adjacent, ("EQ-A",))

    def test_position_code_resolves_current_and_retired_alias(self) -> None:
        register_pair(self.service)
        self.service.commit(
            [Change("relocated", "EQ-A", code="C-009", segment="K2+000")],
            work_order="WO-2", expected_version=2, command_id="c2",
        )
        self.assertEqual(self.service.ledger.resolve("EQ-A").code, "C-009")
        self.assertEqual(self.service.ledger.resolve("C-009").id, "EQ-A")
        # The old position code still resolves to the same lifelong equipment.
        self.assertEqual(self.service.ledger.resolve("C-001").id, "EQ-A")
        self.assertIn("C-001", self.service.ledger.resolve("C-009").aliases)

    def test_duplicate_position_code_rejected(self) -> None:
        register_pair(self.service)
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("registered", "EQ-C", kind="cable", code="C-001",
                        segment="K1+000")],
                work_order="WO-2", expected_version=2, command_id="c2",
            )

    def test_unknown_kind_and_missing_fields_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("registered", "EQ-A", kind="pump", code="X", segment="K1")],
                work_order="WO-1", expected_version=0, command_id="c1",
            )
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("registered", "EQ-A", kind="cabin", segment="K1")],
                work_order="WO-2", expected_version=0, command_id="c2",
            )

    def test_self_adjacency_and_dangling_neighbour_rejected(self) -> None:
        register_pair(self.service)
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("linked", "EQ-A", neighbours=("EQ-A",))],
                work_order="WO-2", expected_version=2, command_id="c2",
            )
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("linked", "EQ-A", neighbours=("EQ-Z",))],
                work_order="WO-3", expected_version=2, command_id="c3",
            )

    def test_batch_is_atomic_when_a_later_change_is_invalid(self) -> None:
        with self.assertRaises(ValidationError):
            self.service.commit(
                [
                    Change("registered", "EQ-A", kind="cabin", code="C-1",
                           segment="K1"),
                    Change("registered", "EQ-B", kind="cable", code="",
                           segment="K1"),
                ],
                work_order="WO-1", expected_version=0, command_id="c1",
            )
        self.assertEqual(self.service.ledger.version(), 0)
        self.assertEqual(self.service.ledger.count(), 0)


class HistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService()
        register_pair(self.service)
        self.service.commit(
            [
                Change("linked", "EQ-A", neighbours=("EQ-B",)),
                Change("relocated", "EQ-A", code="C-009", segment="K2+000"),
            ],
            work_order="WO-2", expected_version=2, command_id="c2",
        )

    def test_history_is_append_only_and_complete(self) -> None:
        history = self.service.ledger.history("EQ-A")
        self.assertEqual([event.op for event in history], ["registered", "linked", "relocated"])
        self.assertTrue(all(event.hash for event in history))

    def test_retired_device_keeps_unalterable_attribution(self) -> None:
        # Must unlink before retiring.
        self.service.commit(
            [Change("unlinked", "EQ-A", neighbours=("EQ-B",))],
            work_order="WO-3", expected_version=4, command_id="c3",
        )
        self.service.commit(
            [Change("retired", "EQ-A")],
            work_order="WO-4", expected_version=5, command_id="c4",
        )
        retired = self.service.ledger.resolve("C-009")
        self.assertTrue(retired.retired)
        # Active listing hides it; full listing and history still attribute it.
        self.assertNotIn("EQ-A", [d.id for d in self.service.ledger.list()])
        self.assertIn("EQ-A", [d.id for d in self.service.ledger.list(include_retired=True)])
        ops = [event.op for event in self.service.ledger.history("EQ-A")]
        self.assertEqual(ops, ["registered", "linked", "relocated", "unlinked", "retired"])

    def test_retired_id_is_never_reissued(self) -> None:
        self.service.commit(
            [Change("unlinked", "EQ-A", neighbours=("EQ-B",))],
            work_order="WO-3", expected_version=4, command_id="c3",
        )
        self.service.commit(
            [Change("retired", "EQ-A")],
            work_order="WO-4", expected_version=5, command_id="c4",
        )
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("registered", "EQ-A", kind="cabin", code="C-2",
                        segment="K1")],
                work_order="WO-5", expected_version=6, command_id="c5",
            )

    def test_cannot_retire_while_still_linked(self) -> None:
        with self.assertRaises(ValidationError):
            self.service.commit(
                [Change("retired", "EQ-A")],
                work_order="WO-3", expected_version=4, command_id="c3",
            )

    def test_state_at_reconstructs_past_attribution(self) -> None:
        at_one = self.service.ledger.state_at(1)
        self.assertEqual(set(at_one), {"EQ-A"})
        at_two = self.service.ledger.state_at(2)
        self.assertEqual(set(at_two), {"EQ-A", "EQ-B"})
        self.assertEqual(at_two["EQ-A"].segment, "K1+000")

    def test_hash_chain_detects_tampering(self) -> None:
        ledger = self.service.ledger
        self.assertTrue(ledger.verify_chain())
        bad = ledger.events()[1]
        # The public API is append-only; forge a record inside the chain copy
        # to prove verification rejects a rewritten history.
        from tunnelnet.ledger.model import Event as E

        ledger._events[1] = E(**{**bad.as_dict(),
                                 "payload": {**bad.payload, "code": "FORGED"}})
        with self.assertRaises(LedgerError):
            ledger.verify_chain()


class ConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService()
        register_pair(self.service)

    def test_stale_expected_version_conflicts(self) -> None:
        # Both editors read version 2; the first commit wins.
        self.service.commit(
            [Change("relocated", "EQ-A", segment="K9+000")],
            work_order="WO-2", expected_version=2, command_id="c2",
        )
        with self.assertRaises(LedgerConflict):
            self.service.commit(
                [Change("relocated", "EQ-B", segment="K9+000")],
                work_order="WO-3", expected_version=2, command_id="c3",
            )

    def test_winner_and_reead_merge_both_survive(self) -> None:
        self.service.commit(
            [Change("relocated", "EQ-A", segment="K3+000")],
            work_order="WO-2", expected_version=2, command_id="c2",
        )
        # Loser re-reads current version (3) and retries; both changes persist.
        self.service.commit(
            [Change("relocated", "EQ-B", segment="K4+000")],
            work_order="WO-3", expected_version=3, command_id="c3",
        )
        self.assertEqual(self.service.ledger.device("EQ-A").segment, "K3+000")
        self.assertEqual(self.service.ledger.device("EQ-B").segment, "K4+000")

    def test_same_command_id_is_idempotent(self) -> None:
        first = self.service.commit(
            [Change("relocated", "EQ-A", segment="K3+000")],
            work_order="WO-X", expected_version=2, command_id="dup",
        )
        second = self.service.commit(
            [Change("relocated", "EQ-A", segment="K5+000")],
            work_order="WO-X", expected_version=2, command_id="dup",
        )
        self.assertFalse(first["commit"]["duplicate"])
        self.assertTrue(second["commit"]["duplicate"])
        # The different payload of the retry was ignored; no new event appended.
        self.assertEqual(self.service.ledger.version(), 3)
        self.assertEqual(self.service.ledger.device("EQ-A").segment, "K3+000")
        self.assertEqual(first["commit"]["event_ids"], second["commit"]["event_ids"])


class ActivationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService()

    def test_commit_immediately_activates_every_consumer(self) -> None:
        result = register_pair(self.service)
        self.assertEqual(result["activation"],
                         {name: "activated" for name in CONSUMERS})
        self.assertEqual(self.service.activation.active_version(), 2)
        self.assertEqual(self.service.activation.lagging(), [])
        # Fully active order auto-closes; nothing carries to shift end.
        self.assertEqual(self.service.shift.open_orders(), [])

    def test_fire_and_env_see_the_same_version_and_adjacency(self) -> None:
        register_pair(self.service)
        self.service.commit(
            [Change("linked", "EQ-A", neighbours=("EQ-B",))],
            work_order="WO-2", expected_version=2, command_id="c2",
        )
        for consumer in (ENV_ROUTING, FIRE_LINKAGE, AUDIT, CONSOLE):
            view = {device.id: device for device in self.service.activation.view(consumer)}
            self.assertEqual(self.service.activation.state(consumer).version, 3)
            self.assertEqual(view["EQ-A"].adjacent, ("EQ-B",))

    def test_downstream_write_failure_is_isolated_partial_failure(self) -> None:
        def fail_fire(consumer: str, block: object) -> None:
            raise RuntimeError("fire controller unreachable")

        self.service.activation.register_applier(FIRE_LINKAGE, fail_fire)
        register_pair(self.service)
        # Fire projection degraded on its last good version (0); others advance.
        self.assertEqual(self.service.activation.state(FIRE_LINKAGE).status, "degraded")
        self.assertEqual(self.service.activation.state(FIRE_LINKAGE).version, 0)
        self.assertEqual(self.service.activation.state(ENV_ROUTING).version, 2)
        self.assertEqual(self.service.activation.active_version(), 0)
        self.assertIn(FIRE_LINKAGE, self.service.activation.degraded())
        # The ledger itself fully committed; history is intact at version 2.
        self.assertEqual(self.service.ledger.version(), 2)

    def test_repeated_recovery_is_idempotent(self) -> None:
        attempts = {"count": 0}

        def fail_dispatch_then_once_more(consumer: str, block: object) -> None:
            attempts["count"] += 1
            # Fails on the initial dispatch and on the first recovery retry;
            # the second retry lands. Any later trigger must be a harmless no-op.
            if attempts["count"] <= 2:
                raise RuntimeError("still unreachable")

        applier = IdempotentApplier(fail_dispatch_then_once_more)
        self.service.activation.register_applier(FIRE_LINKAGE, applier)
        register_pair(self.service)

        first = self.service.recover(FIRE_LINKAGE)
        self.assertEqual(first["outcome"], "degraded")
        second = self.service.recover(FIRE_LINKAGE)
        self.assertEqual(second["outcome"], "active")
        # Triggering recovery again after success applies no new events.
        third = self.service.recover(FIRE_LINKAGE)
        self.assertEqual(third["outcome"], "active")
        self.assertEqual(third["state"]["version"], 2)

    def test_applier_redelivery_does_not_double_apply(self) -> None:
        seen_events: list[str] = []

        def recording(consumer: str, block: object) -> None:
            for row in block.events:  # type: ignore[attr-defined]
                seen_events.append(str(row["event_id"]))

        applier = IdempotentApplier(recording)
        register_pair(self.service)
        manager = self.service.activation
        manager.register_applier(FIRE_LINKAGE, applier)
        # Simulate an at-least-once redelivery of the same delta.
        from tunnelnet.ledger import diff

        block = diff(self.service.ledger, 0)
        applier(FIRE_LINKAGE, block)
        applier(FIRE_LINKAGE, block)
        self.assertEqual(sorted(seen_events), ["WO-1:1", "WO-1:2"])

    def test_resync_rebuilds_from_snapshot_and_closes_order(self) -> None:
        def fail_fire(consumer: str, block: object) -> None:
            raise RuntimeError("fire controller unreachable")

        self.service.activation.register_applier(FIRE_LINKAGE, fail_fire)
        register_pair(self.service)
        self.assertEqual(self.service.shift.open_orders()[0].work_order, "WO-1")

        # Repair the applier, then full-snapshot resync (idempotent).
        self.service.activation.register_applier(FIRE_LINKAGE,
                                                 lambda consumer, block: None)
        first = self.service.resync(FIRE_LINKAGE)
        second = self.service.resync(FIRE_LINKAGE)
        self.assertEqual(first["state"]["version"], 2)
        self.assertEqual(second["state"]["version"], 2)
        self.assertEqual(self.service.activation.active_version(), 2)
        self.assertEqual(self.service.shift.open_orders(), [])

    def test_digest_mismatch_is_detected_not_silently_healed(self) -> None:
        register_pair(self.service)
        state = self.service.activation.state(ENV_ROUTING)
        state.digest = "0" * 64
        self.assertFalse(self.service.activation.verify(ENV_ROUTING))
        self.assertEqual(self.service.activation.state(ENV_ROUTING).status, "degraded")


class DeltaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService()
        register_pair(self.service)
        self.service.commit(
            [Change("linked", "EQ-A", neighbours=("EQ-B",))],
            work_order="WO-2", expected_version=2, command_id="c2",
        )

    def test_delta_carries_only_touched_devices(self) -> None:
        block = self.service.delta(2)
        self.assertEqual(block["base_version"], 2)
        self.assertEqual(block["version"], 3)
        self.assertEqual(sorted(block["devices"]), ["EQ-A"])
        self.assertEqual(len(block["events"]), 1)
        self.assertTrue(block["digest"])

    def test_snapshot_rebuilds_full_register(self) -> None:
        snap = self.service.snapshot()
        self.assertEqual(snap["version"], 3)
        self.assertEqual(set(snap["devices"]), {"EQ-A", "EQ-B"})
        self.assertEqual(snap["devices"]["EQ-A"]["adjacent"], ["EQ-B"])

    def test_replaying_all_deltas_reproduces_snapshot(self) -> None:
        from tunnelnet.ledger import apply_event

        devices: dict = {}
        for event in self.service.ledger.events():
            devices = apply_event(devices, event)
        snap = self.service.snapshot(3)["devices"]
        self.assertEqual({key: devices[key].as_dict() for key in devices}, snap)


class ShiftTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LedgerService()

    def test_handover_blocked_while_change_not_fully_active(self) -> None:
        def fail_fire(consumer: str, block: object) -> None:
            raise RuntimeError("fire controller unreachable")

        self.service.activation.register_applier(FIRE_LINKAGE, fail_fire)
        register_pair(self.service)
        with self.assertRaises(ShiftBlocked) as caught:
            self.service.shift.handover("night", "wang")
        self.assertEqual(caught.exception.open_orders, ["WO-1"])
        self.assertEqual(caught.exception.segments, ["K1+000", "K1+050"])

    def test_cross_shift_acceptance_needs_a_second_operator(self) -> None:
        def fail_fire(consumer: str, block: object) -> None:
            raise RuntimeError("x")

        self.service.activation.register_applier(FIRE_LINKAGE, fail_fire)
        register_pair(self.service)
        with self.assertRaises(ValueError):
            self.service.shift.accept_cross_shift("WO-1", "zhang")
        order = self.service.shift.accept_cross_shift("WO-1", "wang")
        self.assertTrue(order.manual_watch)
        self.assertEqual(order.confirmed_by, "wang")
        payload = self.service.shift.handover("night", "wang")
        self.assertEqual(payload["carried_over"], ["WO-1"])
        self.assertEqual(payload["manual_watch"], ["K1+000", "K1+050"])

    def test_fully_active_order_allows_clean_handover(self) -> None:
        register_pair(self.service)
        payload = self.service.shift.handover("night", "wang")
        self.assertEqual(payload["carried_over"], [])
        self.assertEqual(payload["manual_watch"], [])


class PersistenceTests(unittest.TestCase):
    def test_log_replays_to_the_same_register_and_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store.open(os.path.join(directory, "store"))
            service = LedgerService(Ledger(store))
            register_pair(service)
            service.commit(
                [Change("linked", "EQ-A", neighbours=("EQ-B",))],
                work_order="WO-2", expected_version=2, command_id="c2",
            )
            # Restart: a brand new service replays the append-only commits.
            replayed = LedgerService(Ledger(Store.open(store.root)))
            self.assertEqual(replayed.ledger.version(), 3)
            self.assertTrue(replayed.ledger.verify_chain())
            self.assertEqual(replayed.ledger.device("EQ-A").adjacent, ("EQ-B",))
            # Duplicate command ids remain deduped after restart.
            again = replayed.commit(
                [Change("relocated", "EQ-B", segment="K9+000")],
                work_order="WO-9", expected_version=3, command_id="c2",
            )
            self.assertTrue(again["commit"]["duplicate"])
            self.assertEqual(replayed.ledger.version(), 3)

    def test_cold_consumer_resyncs_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store.open(os.path.join(directory, "store"))
            service = LedgerService(Ledger(store))
            register_pair(service)
            restarted = LedgerService(Ledger(Store.open(store.root)))
            # Consumers start cold (in-memory projections); snapshot resync.
            self.assertEqual(restarted.activation.active_version(), 0)
            for consumer in CONSUMERS:
                restarted.resync(consumer)
            self.assertEqual(restarted.activation.active_version(), 2)


class ConsoleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.server = Server(Store.open(os.path.join(self.directory.name, "store")),
                             web_dir="web")

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _post(self, path: str, payload: dict) -> tuple[int, dict]:
        response = self.server.dispatch("POST", path, payload)
        import json

        return response.status, json.loads(response.body)

    def _get(self, path: str, query: dict | None = None) -> tuple[int, dict]:
        response = self.server.dispatch("GET", path, query=query)
        import json

        return response.status, json.loads(response.body)

    def test_commit_endpoint_and_shared_reads(self) -> None:
        status, payload = self._post("/api/ledger/commit", {
            "work_order": "WO-1",
            "expected_version": 0,
            "command_id": "cmd-1",
            "actor": "zhang",
            "changes": [
                {"op": "registered", "device_id": "EQ-A", "kind": "detector",
                 "code": "D-001", "segment": "K1+000"},
            ],
        })
        self.assertEqual(status, 201)
        self.assertEqual(payload["active_version"], 1)

        status, body = self._get("/api/ledger")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 1)
        self.assertEqual({consumer["version"] for consumer in body["consumers"]}, {1})

        status, body = self._get("/api/ledger/devices")
        self.assertEqual(body["devices"][0]["id"], "EQ-A")

    def test_conflict_returns_409(self) -> None:
        self._post("/api/ledger/commit", {
            "work_order": "WO-1", "expected_version": 0, "command_id": "c1",
            "changes": [{"op": "registered", "device_id": "EQ-A", "kind": "cabin",
                         "code": "C-1", "segment": "K1"}],
        })
        status, body = self._post("/api/ledger/commit", {
            "work_order": "WO-2", "expected_version": 0, "command_id": "c2",
            "changes": [{"op": "registered", "device_id": "EQ-B", "kind": "cable",
                         "code": "L-1", "segment": "K1"}],
        })
        self.assertEqual(status, 409)
        self.assertIn("newer", body["error"])

    def test_recover_and_handover_endpoints(self) -> None:
        # Healthy commit; handover clean.
        self._post("/api/ledger/commit", {
            "work_order": "WO-1", "expected_version": 0, "command_id": "c1",
            "changes": [{"op": "registered", "device_id": "EQ-A", "kind": "pit",
                         "code": "P-1", "segment": "K1"}],
        })
        status, body = self._post("/api/ledger/handover",
                                  {"to_shift": "night", "next_operator": "wang"})
        self.assertEqual(status, 200)
        self.assertEqual(body["carried_over"], [])

    def test_delta_and_snapshot_endpoints(self) -> None:
        self._post("/api/ledger/commit", {
            "work_order": "WO-1", "expected_version": 0, "command_id": "c1",
            "changes": [{"op": "registered", "device_id": "EQ-A", "kind": "cabin",
                         "code": "C-1", "segment": "K1"}],
        })
        status, delta = self._get("/api/ledger/delta", {"base_version": ["0"]})
        self.assertEqual(delta["version"], 1)
        status, snap = self._get("/api/ledger/snapshot")
        self.assertEqual(set(snap["devices"]), {"EQ-A"})
        self.assertEqual(snap["devices"]["EQ-A"]["segment"], "K1")


if __name__ == "__main__":
    unittest.main()
