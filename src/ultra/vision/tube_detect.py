'''Serum-tube presence detection (classical CV, no ML).

The cartridge is drawer-locked with the serum slot already in
the toolhead-camera FOV. :func:`detect_tube` looks straight
down at a configurable ROI with the ring LED on and combines
two independent signals, both computed on the ROI only:

1. **Stage 1 -- intensity statistics.** CLAHE-equalised
   grayscale mean + dark-pixel ratio. An empty slot is
   dominated by the near-white plastic insert (high mean, low
   dark ratio). A seated tube changes the overall brightness
   profile (added shadows around the cap, coloured surface
   instead of white plastic).

2. **Stage 2 -- HSV saturation.** Mean saturation of the ROI.
   This is the robust colour-agnostic gate:

     * empty slot  -> white plastic, near-zero saturation
     * blue cap    -> high saturation
     * red cap     -> high saturation
     * any coloured cap -> high saturation

   Because a *tube cap of any colour* dumps chroma into the
   ROI but an empty slot does not, saturation separates
   populations far more reliably than grayscale shape
   detection (which is cap-colour-dependent and would miss red
   caps against a red tube body).

The final verdict is ``stage1_pass AND stage2_pass``.
Thresholds are calibration values, not learned weights --
update the YAML when optics change.

When ``roi`` is the all-zero sentinel ``(0, 0, 0, 0)`` the
detector falls back to the full frame so an uncalibrated config
still runs (safe-fail-open during bring-up). In that fallback
mode Stage 2 tends to under-report saturation because the
frame-average dilutes the slot's chroma; calibrate the ROI as
soon as possible (see ``docs/cartridge_tube_validation.md``).
'''
from __future__ import annotations

import logging
from dataclasses import dataclass

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

    ``method`` reports which detector path was taken:

    * ``'template'`` -- labelled reference crops were loaded and
      used as the primary signal. The ``stageN_pass`` fields
      still reflect the saturation / intensity gates run for
      reporting parity, but they do not drive ``present``.
    * ``'saturation'`` -- no (or insufficient) templates, the
      classic Stage 1 intensity + Stage 2 saturation pipeline
      produced the verdict.
    '''
    present: bool
    mean_intensity: float
    dark_ratio: float
    mean_saturation: float
    stage1_pass: bool
    stage2_pass: bool
    method: str = 'saturation'
    # Only populated when method == 'template'. NCC in [-1, 1],
    # -1 means no refs of that class were available.
    seated_score: float = 0.0
    empty_score: float = 0.0
    seated_count: int = 0
    empty_count: int = 0
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
    mean_saturation_min: float = 40.0,
    templates: dict | None = None,
    template_min_score: float = 0.5,
    template_margin: float = 0.0,
    template_search_px: int = 5,
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
            Stage-1 pass.
        dark_ratio_max: Maximum dark-pixel fraction for a
            Stage-1 pass.
        mean_saturation_min: Minimum HSV mean saturation for a
            Stage-2 pass (0..255 scale). An empty slot of white
            plastic usually reads 5-20; a seated cap of any
            colour reads 60+ under the ring LED.
        templates: Optional reference set from
            :func:`ultra.vision.tube_template.load_references`.
            When both classes contain at least one ref, the
            template-matching path becomes the primary verdict
            and saturation stages fall back to reporting-only.
        template_min_score: Floor NCC score the winning class
            must beat; below it the verdict is ABSENT with
            ``reason='template_low_confidence'`` (anomaly / ROI
            drifted off slot / camera covered).
        template_margin: Required ``seated - empty`` gap for a
            SEATED verdict. Useful when both classes score high
            simultaneously (e.g. partially seated).
        template_search_px: +/- pixel window searched around
            each ref's nominal position, absorbs slot jitter.

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
            mean_saturation=0.0,
            stage1_pass=False,
            stage2_pass=False,
            roi=(rx, ry, rw, rh),
            reason='empty_roi',
            annotated=frame_bgr.copy(),
        )

    # Stage 1: intensity on CLAHE-equalised grayscale.
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    gray = _clahe_gray(gray)
    mean_intensity = float(gray.mean())
    dark_ratio = float((gray < int(dark_threshold)).mean())
    stage1_pass = (
        mean_intensity >= float(mean_intensity_min)
        and dark_ratio <= float(dark_ratio_max)
    )

    # Stage 2: HSV saturation on the raw crop (no CLAHE --
    # CLAHE on luminance alone doesn't distort S, but running it
    # on all three channels would pump saturation artificially).
    hsv = cv2.cvtColor(crop, cv2.COLOR_BGR2HSV)
    mean_saturation = float(hsv[:, :, 1].mean())
    stage2_pass = mean_saturation >= float(mean_saturation_min)

    # Classifier selection: template matching takes over if
    # refs for *both* classes are available. Otherwise fall back
    # to saturation. Stage 1 / Stage 2 booleans are still
    # reported in both paths so the GUI can show the diagnostic.
    method = 'saturation'
    seated_score = 0.0
    empty_score = 0.0
    seated_count = 0
    empty_count = 0
    template_reason: str | None = None
    template_present: bool | None = None
    if templates:
        from ultra.vision import tube_template
        seated_count = len(templates.get(tube_template.LABEL_SEATED, []))
        empty_count = len(templates.get(tube_template.LABEL_EMPTY, []))
        if seated_count > 0 and empty_count > 0:
            match = tube_template.score_match(
                crop, templates,
                search_px=int(template_search_px),
            )
            seated_score = float(match.seated_score)
            empty_score = float(match.empty_score)
            template_present, template_reason = (
                tube_template.classify(
                    match,
                    min_score=float(template_min_score),
                    margin=float(template_margin),
                )
            )
            method = 'template'

    if method == 'template':
        present = bool(template_present)
        reason = template_reason
    else:
        present = stage1_pass and stage2_pass
        reason = None
        if not present:
            if not stage1_pass:
                reason = (
                    f'stage1_fail '
                    f'(mean={mean_intensity:.1f}, '
                    f'dark_ratio={dark_ratio:.3f})'
                )
            else:
                reason = (
                    f'stage2_fail '
                    f'(mean_saturation={mean_saturation:.1f})'
                )

    det = TubeDetection(
        present=present,
        mean_intensity=mean_intensity,
        dark_ratio=dark_ratio,
        mean_saturation=mean_saturation,
        stage1_pass=stage1_pass,
        stage2_pass=stage2_pass,
        method=method,
        seated_score=seated_score,
        empty_score=empty_score,
        seated_count=seated_count,
        empty_count=empty_count,
        roi=(rx, ry, rw, rh),
        reason=reason,
    )
    det.annotated = annotate(frame_bgr, det)
    return det


def annotate(
    frame_bgr: np.ndarray,
    det: TubeDetection,
) -> np.ndarray:
    '''Overlay ROI + per-stage metrics on ``frame_bgr``.

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

    hud = [
        f'tube: {"PRESENT" if det.present else "ABSENT"}',
        f'method: {det.method}',
    ]
    if det.method == 'template':
        hud.append(
            f'seated: {det.seated_score:.3f} '
            f'(n={det.seated_count})',
        )
        hud.append(
            f'empty:  {det.empty_score:.3f} '
            f'(n={det.empty_count})',
        )
    hud.extend([
        f'mean: {det.mean_intensity:.1f}',
        f'dark_ratio: {det.dark_ratio:.3f}',
        f'saturation: {det.mean_saturation:.1f}',
        f'stage1: {"ok" if det.stage1_pass else "FAIL"}',
        f'stage2: {"ok" if det.stage2_pass else "FAIL"}',
    ])
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
