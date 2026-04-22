'''Orchestrator for cartridge-QR and serum-tube presence checks.

Mirrors :mod:`ultra.vision.align_runner`: each public function
performs the full sequence end-to-end so both the GUI debug
endpoints and the state-machine callers execute identical
logic.

Per-check flow (no carousel rotation -- the cartridge is
drawer-locked with both the QR label and the serum slot already
in the toolhead-camera FOV):

  1. :func:`ensure_centrifuge_ready` as a safety gate. The
     camera LED shares PC12 with the centrifuge strobe, so we
     refuse to light it while the BLDC is spinning.
  2. ``move_gantry`` to the configured probe pose (QR:
     ``(0, 83, 0)``; tube: ``(20, 55, 0)``).
  3. ``cam_led_set(True)``, wait ``led_settle_ms``.
  4. Grab a fresh BGR frame captured strictly after the LED
     turned on.
  5. Run the relevant detector (QR or tube).
  6. ``cam_led_set(False)`` (always, in ``finally``).
  7. Cache an annotated preview JPEG via ``cache_frame`` for
     GUI readback.

Retries happen inside steps 3--5, bounded by
``checks.<name>.retries_per_close``.
'''
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

LOG = logging.getLogger(__name__)


@dataclass
class CheckResult:
    '''Outcome of one :func:`run_*_check` call.

    ``ok`` is True iff the corresponding detector produced a
    positive result (QR payload decoded, tube present). On any
    hard failure (camera, gantry, detector) ``ok`` is False and
    the caller reads ``reason`` for diagnostics.
    '''
    ok: bool
    reason: str | None
    payload: str | None = None           # QR only; None for tube
    extras: dict[str, Any] = field(default_factory=dict)


def _grab_frame(
    get_frame: Callable[[int], 'np.ndarray | None'],
    settle_ms: int,
) -> 'np.ndarray | None':
    try:
        return get_frame(settle_ms)
    except Exception as exc:
        LOG.warning('check: get_frame raised: %s', exc)
        return None


def _move_to_pose(stm32, probe: dict) -> bool:
    r = stm32.send_command_wait_done(
        cmd={
            'cmd': 'move_gantry',
            'x_mm': float(probe.get('x_mm', 0.0)),
            'y_mm': float(probe.get('y_mm', 0.0)),
            'z_mm': float(probe.get('z_mm', 0.0)),
            'speed': float(probe.get('xy_speed_mms', 40.0)),
            'z_speed': float(probe.get('z_speed_mms', 10.0)),
        },
        timeout_s=30.0,
    )
    return r is not None


def run_cartridge_qr_check(
    *,
    stm32,
    config: dict,
    get_frame: Callable[[int], 'np.ndarray | None'],
    cache_frame: Callable[
        ['np.ndarray', Any], None,
    ] | None = None,
) -> CheckResult:
    '''Decode the cartridge QR (or DataMatrix) via the toolhead camera.

    Args:
        stm32: STM32 hardware interface (same shape the recipe
            steps use).
        config: Full app config dict. Reads the ``checks.qr``
            block (probe_pose, min_payload_len,
            retries_per_close, led_settle_ms, format).
        get_frame: Callable ``(settle_ms: int) -> np.ndarray``
            returning a BGR frame captured strictly after the
            LED has been on for ``settle_ms``. Returns None on
            timeout / camera failure.
        cache_frame: Optional callable ``(frame, det) -> None``
            invoked once the detector has run, for GUI preview
            caching.

    Returns:
        :class:`CheckResult` with ``ok=True`` and the decoded
        payload on success.
    '''
    from ultra.vision import qr_detect
    from ultra.vision.align_runner import ensure_centrifuge_ready

    qr_cfg = ((config or {}).get('checks', {}) or {}).get('qr', {}) or {}
    probe = qr_cfg.get('probe_pose', {}) or {}
    retries = max(1, int(qr_cfg.get('retries_per_close', 2)))
    led_settle_ms = int(qr_cfg.get('led_settle_ms', 200))
    min_len = int(qr_cfg.get('min_payload_len', 1))
    fmt = str(qr_cfg.get('format', 'qr'))
    decode_timeout_ms = int(qr_cfg.get('decode_timeout_ms', 500))

    t0 = time.time()
    led_on = False
    try:
        try:
            ensure_centrifuge_ready(stm32)
        except RuntimeError as exc:
            return CheckResult(
                ok=False, reason=str(exc),
                extras={'elapsed_s': round(time.time() - t0, 3)},
            )

        if not _move_to_pose(stm32, probe):
            return CheckResult(
                ok=False, reason='move_gantry_failed',
                extras={'elapsed_s': round(time.time() - t0, 3)},
            )

        if not stm32.cam_led_set(on=True):
            return CheckResult(
                ok=False, reason='cam_led_set_on_failed',
                extras={'elapsed_s': round(time.time() - t0, 3)},
            )
        led_on = True

        last_reason: str | None = None
        for attempt in range(1, retries + 1):
            frame = _grab_frame(get_frame, led_settle_ms)
            if frame is None:
                last_reason = 'no_camera_frame'
                LOG.warning(
                    'qr: no fresh frame (attempt %d/%d)',
                    attempt, retries,
                )
                continue
            det = qr_detect.detect_qr(
                frame,
                min_payload_len=min_len,
                format=fmt,
                decode_timeout_ms=decode_timeout_ms,
            )
            if cache_frame is not None:
                try:
                    cache_frame(frame, det)
                except Exception as exc:
                    LOG.warning(
                        'qr: cache_frame raised: %s', exc,
                    )
            if det is not None:
                return CheckResult(
                    ok=True, reason=None,
                    payload=det.payload,
                    extras={
                        'source_pass': det.source_pass,
                        'bbox': list(det.bbox),
                        'attempt': attempt,
                        'elapsed_s': round(time.time() - t0, 3),
                    },
                )
            last_reason = 'no_qr_detected'
            LOG.info(
                'qr: no code (attempt %d/%d); will retry',
                attempt, retries,
            )

        return CheckResult(
            ok=False, reason=last_reason or 'no_qr_detected',
            extras={
                'attempts': retries,
                'elapsed_s': round(time.time() - t0, 3),
            },
        )
    finally:
        if led_on:
            try:
                stm32.cam_led_set(on=False)
            except Exception as exc:
                LOG.warning(
                    'qr: cam_led_set(off) failed: %s', exc,
                )


def run_serum_tube_check(
    *,
    stm32,
    config: dict,
    get_frame: Callable[[int], 'np.ndarray | None'],
    cache_frame: Callable[
        ['np.ndarray', Any], None,
    ] | None = None,
) -> CheckResult:
    '''Check whether a serum tube is seated in the carousel slot.

    No carousel rotation is issued: the cartridge is
    drawer-locked with the slot already in the camera's FOV.
    :func:`ensure_centrifuge_ready` still runs as a safety gate
    (the LED shares PC12 with the centrifuge strobe).

    Args:
        stm32: STM32 hardware interface.
        config: Full app config dict. Reads the
            ``checks.tube`` block (probe_pose, retries_per_close,
            led_settle_ms, roi, dark_threshold,
            mean_intensity_min, dark_ratio_max, use_hough,
            hough_radius_px).
        get_frame: Fresh-frame provider; see :func:`run_cartridge_qr_check`.
        cache_frame: Optional ``(frame, det) -> None`` hook for
            GUI preview caching.

    Returns:
        :class:`CheckResult` with ``ok=det.present``.
    '''
    from ultra.vision import tube_detect
    from ultra.vision.align_runner import ensure_centrifuge_ready

    tube_cfg = (
        ((config or {}).get('checks', {}) or {}).get('tube', {}) or {}
    )
    probe = tube_cfg.get('probe_pose', {}) or {}
    retries = max(1, int(tube_cfg.get('retries_per_close', 1)))
    led_settle_ms = int(tube_cfg.get('led_settle_ms', 200))

    roi = tube_cfg.get('roi', {}) or {}
    dark_threshold = int(tube_cfg.get('dark_threshold', 60))
    mean_intensity_min = float(
        tube_cfg.get('mean_intensity_min', 90.0),
    )
    dark_ratio_max = float(tube_cfg.get('dark_ratio_max', 0.35))
    use_hough = bool(tube_cfg.get('use_hough', True))
    hough_radius_px = tube_cfg.get(
        'hough_radius_px', {'min': 22, 'max': 34},
    )

    t0 = time.time()
    led_on = False
    try:
        try:
            ensure_centrifuge_ready(stm32)
        except RuntimeError as exc:
            return CheckResult(
                ok=False, reason=str(exc),
                extras={'elapsed_s': round(time.time() - t0, 3)},
            )

        if not _move_to_pose(stm32, probe):
            return CheckResult(
                ok=False, reason='move_gantry_failed',
                extras={'elapsed_s': round(time.time() - t0, 3)},
            )

        if not stm32.cam_led_set(on=True):
            return CheckResult(
                ok=False, reason='cam_led_set_on_failed',
                extras={'elapsed_s': round(time.time() - t0, 3)},
            )
        led_on = True

        last_extras: dict[str, Any] = {}
        last_reason: str | None = None
        for attempt in range(1, retries + 1):
            frame = _grab_frame(get_frame, led_settle_ms)
            if frame is None:
                last_reason = 'no_camera_frame'
                LOG.warning(
                    'tube: no fresh frame (attempt %d/%d)',
                    attempt, retries,
                )
                continue
            det = tube_detect.detect_tube(
                frame,
                roi=roi,
                dark_threshold=dark_threshold,
                mean_intensity_min=mean_intensity_min,
                dark_ratio_max=dark_ratio_max,
                use_hough=use_hough,
                hough_radius_px=hough_radius_px,
            )
            if cache_frame is not None:
                try:
                    cache_frame(frame, det)
                except Exception as exc:
                    LOG.warning(
                        'tube: cache_frame raised: %s', exc,
                    )

            last_extras = {
                'mean_intensity': round(det.mean_intensity, 2),
                'dark_ratio': round(det.dark_ratio, 4),
                'stage1_pass': det.stage1_pass,
                'stage2_pass': det.stage2_pass,
                'circle_count': det.circle_count,
                'roi': list(det.roi),
                'attempt': attempt,
                'elapsed_s': round(time.time() - t0, 3),
            }
            if det.present:
                return CheckResult(
                    ok=True, reason=None,
                    extras=last_extras,
                )
            last_reason = det.reason or 'tube_absent'
            LOG.info(
                'tube: absent (attempt %d/%d, reason=%s)',
                attempt, retries, last_reason,
            )

        return CheckResult(
            ok=False, reason=last_reason or 'tube_absent',
            extras={
                **last_extras,
                'attempts': retries,
            },
        )
    finally:
        if led_on:
            try:
                stm32.cam_led_set(on=False)
            except Exception as exc:
                LOG.warning(
                    'tube: cam_led_set(off) failed: %s', exc,
                )
