"""Tests for the 2-stage SELF_CHECK validator.

Drives :meth:`UltraStateMachine._state_self_check` with a fake
``_evaluate_scene`` returning scripted ``(qr_ok, qr_payload,
tube_present)`` tuples and asserts that:

    * cloud-bound IoT events match the canonical contract
      (``cartridge_validation_started`` once, ``cartridge_
      validation_ended`` once on Stage A, ``cartridge_validation_
      failed`` only for hard failures), and
    * local ``self_check_substate`` EventBus payloads describe
      the operator UI banner correctly, and
    * the SM transitions to ``RUNNING_PROTOCOL`` only after Stage
      A has latched and a subsequent scene shows the tube.

The tests stub ``_loop_back_to_drawer_open`` so a single call to
``_state_self_check`` does not block on a real drawer-edge wait.

The async helpers are driven via :func:`asyncio.run` rather than
``pytest-asyncio`` so the suite does not need an extra plugin
loaded.
"""

from __future__ import annotations

import asyncio
from typing import Any, Iterable

from ultra.events import EventBus
from ultra.services.state_machine import (
    SystemState,
    UltraStateMachine,
    _SceneEvalError,
)


class _FakeIoT:
    '''Captures publish_event calls in arrival order.'''

    def __init__(self) -> None:
        self.events: list[dict] = []

    def publish_event(self, **kwargs: Any) -> bool:
        self.events.append(kwargs)
        return True

    def types(self) -> list[str]:
        return [e['event_type'] for e in self.events]


def _make_sm(
        *, skip_qr: bool = False, skip_tube: bool = False,
) -> tuple[UltraStateMachine, _FakeIoT, list[dict]]:
    cfg: dict = {
        'startup': {
            'skip_qr': skip_qr,
            'skip_tube_check': skip_tube,
        },
        'stm32': {},
    }
    bus = EventBus()
    iot = _FakeIoT()
    substates: list[dict] = []
    bus.on(
        'self_check_substate',
        lambda data: substates.append(data),
    )
    sm = UltraStateMachine(
        config=cfg, event_bus=bus, monitor=None,
        iot_client=iot,
    )

    async def _stub_loop_back() -> None:
        sm.state = SystemState.DRAWER_OPEN_LOAD_CARTRIDGE

    sm._loop_back_to_drawer_open = _stub_loop_back  # type: ignore[assignment]
    return sm, iot, substates


def _script(
        sm: UltraStateMachine,
        scenes: Iterable,
) -> None:
    '''Replace ``_evaluate_scene`` with a scripted iterator.

    Each entry in ``scenes`` may be either a ``(qr_ok, payload,
    tube_present)`` tuple or an :class:`Exception` instance to
    raise (used to model a hard hardware failure).
    '''
    it = iter(scenes)

    async def _fake(*, run_qr: bool = True, run_tube: bool = True):
        try:
            nxt = next(it)
        except StopIteration as exc:
            raise AssertionError(
                'scene script exhausted',
            ) from exc
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt

    sm._evaluate_scene = _fake  # type: ignore[assignment]


def _run_cycles(sm: UltraStateMachine, n: int) -> None:
    async def _runner() -> None:
        for _ in range(n):
            if sm.state == SystemState.RUNNING_PROTOCOL:
                break
            await sm._state_self_check()
            # _emit_substate fires through ``EventBus.emit_sync``
            # which schedules the synchronous handler via
            # ``call_soon_threadsafe`` -- yield control twice so
            # the bus task drains before the next cycle.
            for _ in range(2):
                await asyncio.sleep(0)

    asyncio.run(_runner())


def test_qr_invalid_keeps_awaiting() -> None:
    sm, iot, subs = _make_sm()
    _script(sm, [(False, None, False)])
    _run_cycles(sm, 1)

    assert sm._cartridge_loaded is False
    assert iot.types() == ['cartridge_validation_started']
    assert subs[-1]['substate'] == 'awaiting_cartridge'
    assert subs[-1]['reason'] == 'qr_invalid'


def test_qr_invalid_with_tube_keeps_awaiting() -> None:
    sm, iot, subs = _make_sm()
    _script(sm, [(False, None, True)])
    _run_cycles(sm, 1)

    assert sm._cartridge_loaded is False
    assert iot.types() == ['cartridge_validation_started']
    assert subs[-1]['substate'] == 'awaiting_cartridge'
    assert subs[-1]['reason'] == 'qr_invalid'


def test_tube_present_before_qr_validation() -> None:
    sm, iot, subs = _make_sm()
    _script(sm, [(True, 'CART-A', True)])
    _run_cycles(sm, 1)

    # Tube observed before Stage A latched -- soft retry only,
    # NO IoT event beyond the one-shot started.
    assert sm._cartridge_loaded is False
    assert iot.types() == ['cartridge_validation_started']
    assert subs[-1]['substate'] == 'awaiting_cartridge'
    assert subs[-1]['reason'] == (
        'tube_present_before_cartridge_validation'
    )


def test_stage_a_latches_validation_ended() -> None:
    sm, iot, subs = _make_sm()
    _script(sm, [(True, 'CART-A', False)])
    _run_cycles(sm, 1)

    assert sm._cartridge_loaded is True
    assert sm._last_qr_payload == 'CART-A'
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
    ]
    assert iot.events[-1].get('cartridge_id') == 'CART-A'
    assert subs[-1]['substate'] == (
        'cartridge_loaded_awaiting_tube'
    )


def test_cartridge_lost_after_validation_publishes_failed() -> None:
    sm, iot, _subs = _make_sm()
    _script(
        sm,
        [
            (True, 'CART-A', False),
            (False, None, False),
        ],
    )
    _run_cycles(sm, 2)

    assert sm._cartridge_loaded is False
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
        'cartridge_validation_failed',
    ]
    failed_extra = iot.events[-1].get('extra') or {}
    assert failed_extra.get('reason') == (
        'cartridge_lost_after_validation'
    )
    # validation_ended_emitted must be cleared so a future Stage
    # A pass within the same assay can re-emit it.
    assert sm._validation_ended_emitted is False


def test_stage_a_revalidation_does_not_double_emit() -> None:
    sm, iot, _ = _make_sm()
    _script(
        sm,
        [
            (True, 'CART-A', False),
            (True, 'CART-A', False),
        ],
    )
    _run_cycles(sm, 2)

    # Two scene cycles, both Stage A; cloud must only see one
    # started/ended pair.
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
    ]
    assert sm._cartridge_loaded is True


def test_stage_b_transitions_to_running_protocol() -> None:
    sm, iot, _ = _make_sm()
    _script(
        sm,
        [
            (True, 'CART-A', False),
            (True, 'CART-A', True),
        ],
    )
    _run_cycles(sm, 2)

    assert sm.state == SystemState.RUNNING_PROTOCOL
    # Cloud sees exactly one started + one ended; test_started
    # is published by _state_running_protocol, not here.
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
    ]


def test_full_happy_sequence_with_retries() -> None:
    sm, iot, _ = _make_sm()
    _script(
        sm,
        [
            (False, None, False),
            (False, None, True),
            (True, 'CART-A', True),
            (True, 'CART-A', False),
            (True, 'CART-A', True),
        ],
    )
    _run_cycles(sm, 5)

    assert sm.state == SystemState.RUNNING_PROTOCOL
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
    ]


def test_re_pass_after_clear_re_emits_ended() -> None:
    sm, iot, _ = _make_sm()
    _script(
        sm,
        [
            (True, 'CART-A', False),
            (False, None, False),
            (True, 'CART-A', False),
        ],
    )
    _run_cycles(sm, 3)

    # started=1 (never reset within an assay), ended=2 (the
    # cartridge was lost and re-validated), failed=1 in the
    # middle.
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
        'cartridge_validation_failed',
        'cartridge_validation_ended',
    ]
    assert sm._cartridge_loaded is True


def test_hard_failure_publishes_validation_failed() -> None:
    sm, iot, subs = _make_sm()
    _script(sm, [_SceneEvalError('stm32_connect_failed')])
    _run_cycles(sm, 1)

    types = iot.types()
    assert 'cartridge_validation_started' in types
    assert 'cartridge_validation_failed' in types
    assert sm._cartridge_loaded is False
    assert subs[-1]['substate'] == 'awaiting_cartridge'
    # Loop-back stub leaves SM in DRAWER_OPEN_LOAD_CARTRIDGE so
    # the operator can retry the cycle.
    assert sm.state == SystemState.DRAWER_OPEN_LOAD_CARTRIDGE


def test_bench_skip_qr_and_tube_auto_advances() -> None:
    '''skip_qr + skip_tube_check should advance through Stage A
    on cycle 1 (tube forced absent because not yet latched) and
    Stage B on cycle 2 (tube forced present because latched).'''
    sm, iot, _ = _make_sm(skip_qr=True, skip_tube=True)

    async def _fake(*, run_qr: bool = True, run_tube: bool = True):
        # _evaluate_scene early-returns when both skips disable
        # all checks. Mirror that here.
        return (False, None, False)

    sm._evaluate_scene = _fake  # type: ignore[assignment]

    _run_cycles(sm, 2)

    assert sm.state == SystemState.RUNNING_PROTOCOL
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
    ]


def test_bench_skip_tube_only_two_cycles() -> None:
    '''skip_tube_check alone (real QR) needs the operator to
    insert the cartridge twice -- cycle 1 latches Stage A, cycle
    2 with the tube-skip force-true triggers Stage B.'''
    sm, iot, _ = _make_sm(skip_qr=False, skip_tube=True)
    _script(
        sm,
        [
            (True, 'CART-A', False),
            (True, 'CART-A', False),
        ],
    )
    _run_cycles(sm, 2)

    assert sm.state == SystemState.RUNNING_PROTOCOL
    assert iot.types() == [
        'cartridge_validation_started',
        'cartridge_validation_ended',
    ]
