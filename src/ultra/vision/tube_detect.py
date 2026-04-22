'''Serum-tube presence detection (classical CV, no ML).

The cartridge is drawer-locked with the serum slot already in
the toolhead-camera FOV. :func:`detect_tube` looks straight
down at a configurable ROI with the ring LED on and combines
two independent signals:

1. **Stage 1 -- intensity statistics.** CLAHE + mean intensity +
   dark-pixel ratio. An empty slot is a dark hole (low mean,
   high dark ratio). A seated cap fills the ROI and reflects
   the LED (higher mean, lower dark ratio). Fast (~1 ms),
   always on.

2. **Stage 2 -- Hough circle.** ``cv2.HoughCircles`` with a
   calibrated radius band. Rejects Stage-1 false-passes such
   as a stray label covering the slot or a glare patch filling
   the ROI. Gated by the ``use_hough`` config flag.

The final verdict is ``stage1_pass AND stage2_pass`` (or just
Stage 1 when Hough is disabled). Thresholds are calibration
values, not learned weights -- update the YAML when optics
change.

When ``roi`` is the all-zero sentinel ``(0, 0, 0, 0)`` the
detector falls back to the full frame so an uncalibrated config
still runs (safe-fail-open during bring-up).
'''
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import cv2
import numpy as np

LOG = logging.getLogger(__name__)


@dataclass
class TubeDetection:
    '''Outcome of one :func:`detect_tube` call.

    ``present`` is the final verdict the state machine cares
    about. Everything else is surfaced so the GUI debug page can
    show operators which signal failed (and by how much) during
    calibration.
    '''
    present: bool
    mean_intensity: float
    dark_ratio: float
    stage1_pass: bool
    stage2_pass: bool
    circle_count: int
    circles: list[tuple[float, float, float]] = field(
        default_factory=list,
    )
    # Pass-through so the GUI can place the frame under the ROI
    # overlay without re-reading the config.
    roi: tuple[int, int, int, int] = (0, 0, 0, 0)
    reason: str | None = None
    annotated: np.ndarray | None = None


def _resolve_roi(
    frame_shape: tuple[int, int],
    roi: tuple[int, int, int, int] | dict | None,
) -> tuple[int, int, int, int]:
    '''Clamp ``roi`` to the frame; ``(0,0,0,0)`` = full frame.'''
    fh, fw = frame_shape[:2]
    if roi is None:
        return (0, 0, fw, fh)
    if isinstance(roi, dict):
        x = int(roi.get('x', 0) or 0)
        y = int(roi.get('y', 0) or 0)
        w = int(roi.get('w', 0) or 0)
        h = int(roi.get('h', 0) or 0)
    else:
        x, y, w, h = (int(v) for v in roi)
    if w <= 0 or h <= 0:
        return (0, 0, fw, fh)
    x = max(0, min(x, fw - 1))
    y = max(0, min(y, fh - 1))
    w = max(1, min(w, fw - x))
    h = max(1, min(h, fh - y))
    return (x, y, w, h)


def _clahe_gray(gray: np.ndarray) -> np.ndarray:
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    return clahe.apply(gray)


def detect_tube(
    frame_bgr: np.ndarray,
    *,
    roi: tuple[int, int, int, int] | dict | None,
    dark_threshold: int = 60,
    mean_intensity_min: float = 90.0,
    dark_ratio_max: float = 0.35,
    use_hough: bool = True,
    hough_radius_px: tuple[int, int] | dict = (22, 34),
) -> TubeDetection:
    '''Run both detection stages on one frame.

    Args:
        frame_bgr: Full BGR frame from the toolhead camera.
        roi: ``(x, y, w, h)`` tuple or dict in pixel space. Zero
            or negative dimensions request the full frame (safe
            fallback before calibration).
        dark_threshold: Pixel-value cutoff for the dark-ratio
            statistic.
        mean_intensity_min: Minimum ROI mean intensity for a
            pass.
        dark_ratio_max: Maximum dark-pixel fraction for a pass.
        use_hough: Enable Stage 2.
        hough_radius_px: ``(min, max)`` radius band or dict with
            ``min``/``max`` keys.

    Returns:
        A :class:`TubeDetection` with every sub-signal + an
        annotated preview image.
    '''
    rx, ry, rw, rh = _resolve_roi(frame_bgr.shape, roi)
    crop = frame_bgr[ry:ry + rh, rx:rx + rw]
    if crop.size == 0:
        LOG.warning(
            'tube: empty ROI %s for frame %s',
            (rx, ry, rw, rh), frame_bgr.shape,
        )
        return TubeDetection(
            present=False,
            mean_intensity=0.0,
            dark_ratio=1.0,
            stage1_pass=False,
            stage2_pass=False,
            circle_count=0,
            roi=(rx, ry, rw, rh),
            reason='empty_roi',
            annotated=frame_bgr.copy(),
        )

    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    gray = _clahe_gray(gray)

    mean_intensity = float(gray.mean())
    dark_ratio = float((gray < int(dark_threshold)).mean())

    stage1_pass = (
        mean_intensity >= float(mean_intensity_min)
        and dark_ratio <= float(dark_ratio_max)
    )

    circles_list: list[tuple[float, float, float]] = []
    stage2_pass = True
    if use_hough:
        if isinstance(hough_radius_px, dict):
            r_min = int(hough_radius_px.get('min', 22) or 22)
            r_max = int(hough_radius_px.get('max', 34) or 34)
        else:
            r_min = int(hough_radius_px[0])
            r_max = int(hough_radius_px[1])

        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        hough = cv2.HoughCircles(
            blurred,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(10, int(r_min * 0.8)),
            param1=100,
            param2=25,
            minRadius=r_min,
            maxRadius=r_max,
        )
        if hough is not None:
            for c in np.round(hough[0]).astype(int):
                # Offset back to full-frame coordinates so the
                # annotated preview uses a single coordinate
                # system.
                cx = float(c[0] + rx)
                cy = float(c[1] + ry)
                rr = float(c[2])
                circles_list.append((cx, cy, rr))
        stage2_pass = len(circles_list) > 0
    else:
        stage2_pass = True

    present = stage1_pass and stage2_pass
    reason: str | None = None
    if not present:
        if not stage1_pass:
            reason = (
                f'stage1_fail '
                f'(mean={mean_intensity:.1f}, '
                f'dark_ratio={dark_ratio:.3f})'
            )
        elif not stage2_pass:
            reason = 'stage2_fail (no_circle_in_band)'

    det = TubeDetection(
        present=present,
        mean_intensity=mean_intensity,
        dark_ratio=dark_ratio,
        stage1_pass=stage1_pass,
        stage2_pass=stage2_pass,
        circle_count=len(circles_list),
        circles=circles_list,
        roi=(rx, ry, rw, rh),
        reason=reason,
    )
    det.annotated = annotate(frame_bgr, det)
    return det


def annotate(
    frame_bgr: np.ndarray,
    det: TubeDetection,
) -> np.ndarray:
    '''Overlay ROI + circles + metrics on ``frame_bgr``.

    Returns a new BGR image; the input is not mutated. Colour
    follows the verdict: green for present, red for absent.
    '''
    out = frame_bgr.copy()
    ok = (0, 255, 0)
    bad = (0, 0, 255)
    colour = ok if det.present else bad

    rx, ry, rw, rh = det.roi
    if rw > 0 and rh > 0:
        cv2.rectangle(
            out, (rx, ry), (rx + rw, ry + rh), colour, 2,
        )
    for (cx, cy, rr) in det.circles:
        cv2.circle(
            out, (int(round(cx)), int(round(cy))), int(round(rr)),
            colour, 2,
        )
        cv2.circle(
            out, (int(round(cx)), int(round(cy))), 2, colour, 3,
        )

    hud = [
        f'tube: {"PRESENT" if det.present else "ABSENT"}',
        f'mean: {det.mean_intensity:.1f}',
        f'dark_ratio: {det.dark_ratio:.3f}',
        f'stage1: {"ok" if det.stage1_pass else "FAIL"}',
        f'stage2: {"ok" if det.stage2_pass else "FAIL"} '
        f'(circles={det.circle_count})',
    ]
    if det.reason:
        hud.append(f'reason: {det.reason}')

    y = 24
    for line in hud:
        cv2.putText(
            out, line, (12, y),
            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3,
        )
        cv2.putText(
            out, line, (12, y),
            cv2.FONT_HERSHEY_SIMPLEX, 0.55, colour, 1,
        )
        y += 22
    return out
