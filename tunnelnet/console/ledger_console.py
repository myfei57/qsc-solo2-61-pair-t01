"""Console endpoints over the unified equipment ledger.

These handlers expose one register to the page: the current devices and their
adjacency, the version/activation status every subsystem shares, the small
delta and the full snapshot, plus commit / recover / resync / handover
commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tunnelnet.ledger import (
    FIRE_LINKAGE,
    LedgerConflict,
    LedgerError,
    ShiftBlocked,
)

from .http import Request, RequestError, Response, json_response

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from .server import Server


def _service(server: "Server"):
    return server.runtime.ledger_book


def ledger_status(server: "Server", request: Request) -> Response:
    return json_response(_service(server).status())


def ledger_devices(server: "Server", request: Request) -> Response:
    include_retired = request.str_field("include_retired", request.query_field("include_retired", ""))
    retired = include_retired in ("1", "true", "yes")
    ledger = _service(server).ledger
    return json_response(
        {
            "version": ledger.version(),
            "head_hash": ledger.head_hash(),
            "devices": [device.as_dict() for device in ledger.list(include_retired=retired)],
        }
    )


def ledger_device(server: "Server", request: Request) -> Response:
    device_id = request.str_field("id", request.query_field("id", ""))
    if not device_id:
        raise RequestError(400, "id is required")
    ledger = _service(server).ledger
    device = ledger.resolve(device_id)
    if device is None:
        raise RequestError(404, f"device {device_id} not found")
    return json_response(
        {
            "device": device.as_dict(),
            "events": [event.as_dict() for event in ledger.history(device.id)],
            "neighbors": [neighbour.as_dict() for neighbour in ledger.neighbors(device.id)],
        }
    )


def ledger_segments(server: "Server", request: Request) -> Response:
    return json_response({"segments": _service(server).ledger.segments()})


def ledger_commit(server: "Server", request: Request) -> Response:
    work_order = request.str_field("work_order")
    if not work_order:
        raise RequestError(400, "work_order is required")
    raw_changes = request.payload.get("changes", [])
    if not isinstance(raw_changes, list) or not raw_changes:
        raise RequestError(400, "changes must be a non-empty list")
    expected_version = request.int_field("expected_version", _service(server).ledger.version())
    try:
        payload = _service(server).commit(
            raw_changes,  # type: ignore[arg-type]
            work_order=work_order,
            expected_version=expected_version,
            command_id=request.str_field("command_id"),
            actor=request.str_field("actor", "operator"),
            at=request.str_field("at") or None,
        )
    except LedgerConflict as exc:
        raise RequestError(409, str(exc)) from exc
    status = 200 if payload.get("duplicate") else 201
    return json_response(payload, status=status)


def ledger_delta(server: "Server", request: Request) -> Response:
    raw_query = request.query_field("base_version", "")
    raw_body = request.payload.get("base_version")
    try:
        base = int(raw_query) if raw_query else (
            int(raw_body) if raw_body is not None else 0
        )
    except (TypeError, ValueError) as exc:
        raise RequestError(400, "base_version must be an integer") from exc
    try:
        return json_response(_service(server).delta(base))
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc


def ledger_snapshot(server: "Server", request: Request) -> Response:
    raw = request.query_field("version", "")
    try:
        version = int(raw) if raw else None
    except ValueError as exc:
        raise RequestError(400, "version must be an integer") from exc
    try:
        return json_response(_service(server).snapshot(version))
    except LedgerError as exc:
        raise RequestError(400, str(exc)) from exc


def ledger_activation(server: "Server", request: Request) -> Response:
    service = _service(server)
    return json_response(
        {
            "active_version": service.activation.active_version(),
            "consumers": [state.as_dict() for state in service.activation.states()],
        }
    )


def ledger_recover(server: "Server", request: Request) -> Response:
    consumer = request.str_field("consumer", request.query_field("consumer", ""))
    if not consumer:
        raise RequestError(400, "consumer is required")
    try:
        return json_response(_service(server).recover(consumer))
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc


def ledger_resync(server: "Server", request: Request) -> Response:
    consumer = request.str_field("consumer", request.query_field("consumer", FIRE_LINKAGE))
    try:
        return json_response(_service(server).resync(consumer))
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc


def ledger_handover(server: "Server", request: Request) -> Response:
    next_shift = request.str_field("to_shift", request.query_field("to_shift", ""))
    next_operator = request.str_field("next_operator", request.query_field("next_operator", ""))
    if not next_shift or not next_operator:
        raise RequestError(400, "to_shift and next_operator are required")
    try:
        return json_response(_service(server).shift.handover(next_shift, next_operator))
    except ShiftBlocked as exc:
        raise RequestError(409, str(exc)) from exc


def ledger_accept(server: "Server", request: Request) -> Response:
    work_order = request.str_field("work_order")
    accepting_operator = request.str_field("accepting_operator")
    if not work_order or not accepting_operator:
        raise RequestError(400, "work_order and accepting_operator are required")
    try:
        order = _service(server).shift.accept_cross_shift(work_order, accepting_operator)
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response(order.as_dict())


def ledger_verify(server: "Server", request: Request) -> Response:
    consumer = request.str_field("consumer", request.query_field("consumer", ""))
    if not consumer:
        raise RequestError(400, "consumer is required")
    try:
        ok = _service(server).activation.verify(consumer)
    except ValueError as exc:
        raise RequestError(400, str(exc)) from exc
    return json_response({"consumer": consumer, "verified": ok})
