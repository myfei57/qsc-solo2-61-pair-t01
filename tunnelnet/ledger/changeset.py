"""Validate a whole work order against the current register.

A work order is atomic: every change must be acceptable together, or none of
it is written. This is what keeps "one device failed" from becoming a partial
ledger state. The checks run against the register *as the batch would look
after applying each earlier change in the same batch*, so a relocate and a
link in one order validate against the relocated position.
"""

from __future__ import annotations

from .model import DEVICE_KINDS, Change, apply_event, Event

BATCH_OPS = ("registered", "relocated", "linked", "unlinked", "retired")


class ValidationError(ValueError):
    """One change in the batch cannot be accepted."""


def _require(value: str, field_name: str, change: Change) -> str:
    if not value:
        raise ValidationError(f"{change.op}: {field_name} is required for {change.device_id!r}")
    return value


def _event_for(index: int, change: Change, base_version: int, at: str, work_order: str,
               actor: str, command_id: str, prev_hash: str) -> Event:
    payload: dict[str, object] = {"note": change.note}
    if change.op == "registered":
        payload.update(kind=change.kind, code=change.code, segment=change.segment)
    elif change.op == "relocated":
        payload.update(code=change.code, segment=change.segment)
    elif change.op in ("linked", "unlinked"):
        payload.update(neighbours=list(change.neighbours))
    elif change.op == "retired":
        pass
    return Event(
        seq=base_version + index,
        event_id=f"{work_order}:{index}",
        device_id=change.device_id,
        op=change.op,
        at=at,
        work_order=work_order,
        actor=actor,
        base_version=base_version,
        command_id=command_id,
        prev_hash=prev_hash,
        payload=payload,
    )


def validate(changes: list[Change], devices: dict, base_version: int, at: str,
             work_order: str, actor: str, command_id: str, prev_hash: str) -> list[Event]:
    """Return the sealed events for a batch or raise ``ValidationError``.

    ``devices`` is the live register at ``base_version`` and is not mutated;
    the batch is simulated on a throw-away copy.
    """

    if not changes:
        raise ValidationError("work order carries no changes")

    simulated = dict(devices)
    events: list[Event] = []
    staged_codes: set[str] = set()

    for index, change in enumerate(changes, start=1):
        _require(change.op, "op", change)
        _require(change.device_id, "device_id", change)
        if change.op not in BATCH_OPS:
            raise ValidationError(f"unknown op {change.op!r}")

        existing = simulated.get(change.device_id)

        if change.op == "registered":
            if existing is not None:
                raise ValidationError(f"{change.device_id} is already registered")
            kind = _require(change.kind, "kind", change)
            if kind not in DEVICE_KINDS:
                raise ValidationError(f"{change.device_id}: kind {kind!r} is not one of {DEVICE_KINDS}")
            code = _require(change.code, "code", change)
            segment = _require(change.segment, "segment", change)
            _check_code_free(simulated, staged_codes, code, change.device_id)
        else:
            if existing is None:
                raise ValidationError(f"{change.device_id} is not registered")
            if existing.retired and change.op != "retired":
                raise ValidationError(f"{change.device_id} is retired and cannot be changed")

        if change.op == "relocated":
            code = change.code or existing.code
            segment = change.segment or existing.segment
            _require(segment, "segment", change)
            _check_code_free(simulated, staged_codes, code, change.device_id)
        elif change.op in ("linked", "unlinked"):
            if not change.neighbours:
                raise ValidationError(f"{change.op}: neighbours are required for {change.device_id!r}")
            if change.device_id in change.neighbours:
                raise ValidationError(f"{change.device_id} cannot be adjacent to itself")
            for neighbour in change.neighbours:
                other = simulated.get(neighbour)
                if other is None:
                    raise ValidationError(f"neighbour {neighbour} is not registered")
                if other.retired:
                    raise ValidationError(f"neighbour {neighbour} is retired")
            if change.op == "unlinked":
                for neighbour in change.neighbours:
                    if neighbour not in existing.adjacent:
                        raise ValidationError(
                            f"{change.device_id} and {neighbour} are not currently adjacent"
                        )
        elif change.op == "retired":
            still_linked = [neighbour for neighbour in existing.adjacent
                            if not simulated.get(neighbour, existing).retired]
            if still_linked:
                raise ValidationError(
                    f"{change.device_id} cannot retire while linked to active devices "
                    f"{sorted(still_linked)}; re-link or unlink first"
                )

        event = _event_for(index, change, base_version, at, work_order, actor, command_id, prev_hash)
        from .model import seal

        event = seal(event)
        simulated = apply_event(simulated, event)
        # Track codes that newly became occupied inside this batch.
        if change.op in ("registered", "relocated"):
            staged_codes.add(event.payload.get("code"))
        events.append(event)
        prev_hash = event.hash

    return events


def _check_code_free(devices: dict, staged_codes: set[str], code: str, owner: str) -> None:
    """Position codes must be unique among active devices."""

    if code in staged_codes:
        raise ValidationError(f"position code {code!r} is already taken in this work order")
    for device in devices.values():
        if device.id != owner and not device.retired and device.code == code:
            raise ValidationError(f"position code {code!r} already belongs to {device.id}")
