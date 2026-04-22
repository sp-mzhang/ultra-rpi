'''Synchronous orchestrator for the carousel alignment workflow.

Performs the full sequence end-to-end so both the GUI button
(``POST /api/camera/align-carousel``) and the recipe step
(``align_to_carousel``) execute identical logic:

  1. BLDC motor preflight (power-on, ERROR recovery, idle check)
     -- mirrors what the firmware-managed ``centrifuge_lock`` /
     ``goto_*`` sequences do internally for the raw
     ``centrifuge_move_angle`` opcode used here.
  2. Optional Z-axis homing.
  3. Move gantry to the configured probe pose.
  4. Toolhead camera LED on; wait the configured settle time.
  5. Grab a fresh BGR frame (one that was captured *after* the
     LED turned on).
  6. Detect markers, average every decoded marker's orientation,
     compute the CW correction against the blister reference.
  7. Anchor the centrifuge target to the blister station angle
     (``aligner.station_deg('blister') + delta``) so repeated
     runs are idempotent, and command ``centrifuge_move_angle``
     with a 3-attempt + ``bldc_reset`` retry.
  8. LED off (always, in a finally clause).

The function is sync because the protocol thread already blocks
on STM32 serial; the FastAPI route hands off to a thread executor
to call it. Returns a structured dict the GUI converts to JSON
and the step uses to log + decide success/failure.
'''
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np

LOG = logging.getLogger(__name__)


@dataclass
class AlignmentRunResult:
    '''Outcome of a single :func:`run_alignment` call.

    ``ok`` is True iff the centrifuge actually moved to the new
    target (or the alignment converged with no motion required
    -- e.g. delta below the deadband). On any hard failure
    (camera, BLDC, marker detection) ``ok`` is False and the
    caller can read ``reason`` / ``payload`` for diagnostics.
    '''
    ok: bool
    reason: str | None
    payload: dict[str, Any]


def _result_to_dict(r, extras: dict | None = None) -> dict:
    '''Mirror the GUI ``_result_to_dict`` shape so JSON payloads
    from the endpoint and the recipe step look identical.'''
    out: dict = {
        'side': r.side,
        'avg_deg': (
            round(r.avg_deg, 3)
            if r.avg_deg is not None else None
        ),
        'reference_deg': r.reference_deg,
        'c_cw_deg': (
            round(r.c_cw_deg, 3)
            if r.c_cw_deg is not None else None
        ),
        'delta_motor_deg': (
            round(r.delta_motor_deg, 3)
            if r.delta_motor_deg is not None else None
        ),
        'polarity': r.polarity,
        'markers': [
            {
                'payload': m.payload,
                'angle_deg': round(m.angle_deg, 3),
                'center_px': [
                    round(m.center_px[0], 1),
                    round(m.center_px[1], 1),
                ],
                'size_px': [
                    round(m.size_px[0], 1),
                    round(m.size_px[1], 1),
                ],
            }
            for m in r.markers
        ],
        'reason': r.reason,
    }
    if extras:
        out.update(extras)
    return out


def ensure_centrifuge_ready(stm32) -> dict:
    '''Bring the BLDC up to an idle/ready state.

    Mirrors the firmware-managed ``centrifuge_lock`` /
    ``centrifuge_unlock`` / ``centrifuge_goto_*`` sequences,
    which self-contain motor setup. The raw
    ``centrifuge_move_angle`` opcode (used here) does NOT, so
    we replicate the recovery ladder on the Python side:

      1. Poll ``centrifuge_status``.
      2. If the driver is offline, ``centrifuge_power`` on and
         wait for it to come up.
      3. If latched in ERROR, ``bldc_reset`` (which issues
         ``BLDC_STOP_MOTOR`` + ``BLDC_CLEAR_ERROR`` and verifies
         READY) and re-check.
      4. If state still isn't idle-like (IDLE / READY /
         FREE_STOP) with ``|rpm| <= _RPM_IDLE_MAX``, raise
         ``RuntimeError`` -- the motor is genuinely busy and we
         shouldn't interrupt it.

    Returns the final (ready) status dict.
    '''
    from ultra.protocol.steps import (
        bldc_reset, check_bldc_errors, log_bldc_health,
    )

    IDLE_STATES = (
        stm32.CFUGE_ST_IDLE,
        stm32.CFUGE_ST_READY,
        5,  # BLDC_STATE_FREE_STOP
    )

    def _status():
        r = stm32.send_command(
            cmd={'cmd': 'centrifuge_status'},
            timeout_s=5.0,
        )
        if not isinstance(r, dict):
            raise RuntimeError('centrifuge_status failed')
        return r

    log_bldc_health(stm32, 'pre-align')
    status = _status()

    if not status.get('driver_online', True):
        LOG.info(
            'align: BLDC driver offline; powering on',
        )
        stm32.send_command(
            cmd={'cmd': 'centrifuge_power', 'enable': True},
            timeout_s=5.0,
        )
        time.sleep(1.0)
        status = _status()

    if int(status.get('state', -1)) == stm32.CFUGE_ST_ERROR:
        LOG.warning(
            'align: BLDC in ERROR (flags=%s); running '
            'bldc_reset',
            status.get('error_flags', '0x0000'),
        )
        check_bldc_errors(stm32)
        bldc_reset(stm32)
        time.sleep(0.5)
        status = _status()

    st = int(status.get('state', -1))
    rpm_abs = abs(int(status.get('rpm', 0)))
    if st not in IDLE_STATES or rpm_abs > stm32._RPM_IDLE_MAX:
        raise RuntimeError(
            f'Centrifuge not ready after init '
            f'(state={st}, rpm={rpm_abs}, '
            f'flags={status.get("error_flags", "?")})',
        )
    return status


def move_angle_with_retry(
    stm32,
    target_001: int,
    move_rpm: int,
    max_retries: int = 3,
):
    '''``centrifuge_move_angle`` with BLDC reset between attempts.

    Mirrors ``CentrifugeRotateStep._MAX_RETRIES = 3``: on
    failure, log the error flags, reset the BLDC driver, wait
    briefly, retry. Returns the final RSP dict (success or last
    failure) so the caller can distinguish timeout vs.
    driver-reject and surface ``error_code``.
    '''
    from ultra.protocol.steps import bldc_reset, check_bldc_errors

    last = None
    for attempt in range(1, max_retries + 1):
        last = stm32.send_command(
            cmd={
                'cmd': 'centrifuge_move_angle',
                'angle_001deg': target_001,
                'move_rpm': move_rpm,
            },
            timeout_s=60.0,
        )
        if (
            isinstance(last, dict)
            and last.get('error_code', 0xFF) == 0
        ):
            return last
        LOG.warning(
            'align: centrifuge_move_angle failed '
            '(attempt %d/%d, rsp=%s)',
            attempt, max_retries, last,
        )
        check_bldc_errors(stm32)
        if attempt < max_retries:
            bldc_reset(stm32)
            time.sleep(0.5)
    return last


def run_alignment(
    *,
    stm32,
    aligner,
    align_cfg: dict,
    get_frame: Callable[[int], 'np.ndarray | None'],
    cache_frame: Callable[
        ['np.ndarray', Any], None,
    ] | None = None,
    home_z_first: bool | None = None,
) -> AlignmentRunResult:
    '''Execute one full carousel-alignment cycle.

    Args:
        stm32: STM32 hardware interface.
        aligner: A ``CarouselAligner`` (already built from
            config).
        align_cfg: The ``carousel_align`` block from the app
            config dict (probe_pose, led_settle_ms,
            centrifuge.move_rpm, etc.).
        get_frame: Callable ``(settle_ms: int) -> np.ndarray``
            returning a fresh BGR frame strictly captured after
            the LED turned on. Returns None on timeout / camera
            failure. Each caller wires this to its own camera
            handle (the GUI uses a long-lived
            :class:`CameraStream`; tests may inject a stub).
        cache_frame: Optional callable
            ``(frame, result) -> None`` invoked once a frame
            has been captured and a result computed. Used by
            the GUI to render the annotated preview.
        home_z_first: Override for ``align_cfg.home_z_first``.

    Returns:
        ``AlignmentRunResult`` with ``ok`` set per
        :class:`AlignmentRunResult`'s docstring.
    '''
    probe = align_cfg.get('probe_pose') or {}
    if home_z_first is None:
        home_z_first = bool(align_cfg.get('home_z_first', True))
    led_settle_ms = int(align_cfg.get('led_settle_ms', 250))
    cent_cfg = align_cfg.get('centrifuge') or {}
    move_rpm = int(cent_cfg.get('move_rpm', 100))

    t0 = time.time()
    led_on = False
    try:
        try:
            status = ensure_centrifuge_ready(stm32)
        except RuntimeError as exc:
            return AlignmentRunResult(
                ok=False,
                reason=str(exc),
                payload={'elapsed_s': round(time.time() - t0, 3)},
            )
        cur_001deg = int(status.get('angle_001deg', 0))

        if home_z_first:
            r = stm32.send_command_wait_done(
                cmd={'cmd': 'home_z_axis'},
                timeout_s=30.0,
            )
            if r is None:
                return AlignmentRunResult(
                    ok=False, reason='home_z_axis_failed',
                    payload={
                        'elapsed_s': round(time.time() - t0, 3),
                    },
                )

        r = stm32.send_command_wait_done(
            cmd={
                'cmd': 'move_gantry',
                'x_mm': float(probe.get('x_mm', 0.0)),
                'y_mm': float(probe.get('y_mm', 70.0)),
                'z_mm': float(probe.get('z_mm', 0.0)),
                'speed': float(probe.get('xy_speed_mms', 40.0)),
                'z_speed': float(probe.get('z_speed_mms', 10.0)),
            },
            timeout_s=30.0,
        )
        if r is None:
            return AlignmentRunResult(
                ok=False, reason='move_gantry_failed',
                payload={
                    'elapsed_s': round(time.time() - t0, 3),
                    'current_001deg': cur_001deg,
                },
            )

        if not stm32.cam_led_set(on=True):
            return AlignmentRunResult(
                ok=False, reason='cam_led_set_on_failed',
                payload={
                    'elapsed_s': round(time.time() - t0, 3),
                    'current_001deg': cur_001deg,
                },
            )
        led_on = True

        frame = get_frame(led_settle_ms)
        if frame is None:
            return AlignmentRunResult(
                ok=False, reason='no_camera_frame',
                payload={
                    'elapsed_s': round(time.time() - t0, 3),
                    'current_001deg': cur_001deg,
                },
            )

        result = aligner.compute(frame)

        if cache_frame is not None:
            try:
                cache_frame(frame, result)
            except Exception as exc:
                LOG.warning(
                    'align: cache_frame raised: %s', exc,
                )

        if result.delta_motor_deg is None:
            # Detection-side validation failed (too few markers,
            # missing frame, etc). Surface the reason but don't
            # try to move.
            return AlignmentRunResult(
                ok=False,
                reason=result.reason or 'no_alignment_delta',
                payload=_result_to_dict(result, {
                    'moved': False,
                    'target_001deg': None,
                    'current_001deg': cur_001deg,
                    'station_001deg': None,
                    'elapsed_s': round(time.time() - t0, 3),
                }),
            )

        # Anchor target to the blister station (single-reference
        # aligner; idempotent across re-runs).
        station_deg = aligner.station_deg('blister')
        station_001 = int(round(station_deg * 100)) % 36000
        delta_001 = int(round(result.delta_motor_deg * 100))
        target_001 = (station_001 + delta_001) % 36000

        rsp = move_angle_with_retry(
            stm32, target_001, move_rpm,
        )
        moved_ok = (
            isinstance(rsp, dict)
            and rsp.get('error_code', 0xFF) == 0
        )
        if not moved_ok:
            err_code = (
                rsp.get('error_code')
                if isinstance(rsp, dict) else None
            )
            return AlignmentRunResult(
                ok=False,
                reason=(
                    'centrifuge_move_angle_failed'
                    f' (err={err_code})'
                    if err_code is not None
                    else 'centrifuge_move_angle_timeout'
                ),
                payload=_result_to_dict(result, {
                    'moved': False,
                    'move_error': (
                        'centrifuge_move_angle_failed'
                        f' (err={err_code})'
                        if err_code is not None
                        else 'centrifuge_move_angle_timeout'
                    ),
                    'target_001deg': target_001,
                    'current_001deg': cur_001deg,
                    'station_001deg': station_001,
                    'elapsed_s': round(time.time() - t0, 3),
                }),
            )

        return AlignmentRunResult(
            ok=True, reason=None,
            payload=_result_to_dict(result, {
                'moved': True,
                'target_001deg': target_001,
                'current_001deg': cur_001deg,
                'station_001deg': station_001,
                'elapsed_s': round(time.time() - t0, 3),
            }),
        )
    finally:
        if led_on:
            try:
                stm32.cam_led_set(on=False)
            except Exception as exc:
                LOG.warning(
                    'align: cam_led_set(off) failed: %s', exc,
                )


def build_aligner_from_config(config: dict):
    '''Construct a ``CarouselAligner`` from the full app config.

    ``angle_open_initial_deg`` is sourced from the
    ``calibration`` block, matching what the firmware
    station-goto sequences use, so vision-side stations and
    firmware-side stations always agree.
    '''
    from ultra.vision.carousel_align import CarouselAligner
    cfg = (config.get('carousel_align') or {})
    cal = (config.get('calibration') or {})
    open_init = float(cal.get('angle_open_initial_deg', 290.0))
    return (
        CarouselAligner.from_config(
            cfg, angle_open_initial_deg=open_init,
        ),
        cfg,
    )
