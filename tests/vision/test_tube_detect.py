'''Unit tests for :mod:`ultra.vision.tube_detect`.

Exercises the two-stage detector with synthetic frames so the
suite does not need a live camera. Stage 1 is easy to hit with
uniform-fill crops; Stage 2 is validated by drawing a real
circle into the ROI.
'''
from __future__ import annotations

import pytest

cv2 = pytest.importorskip('cv2')
np = pytest.importorskip('numpy')

from ultra.vision import tube_detect as td  # noqa: E402


def _frame(value: int, shape: tuple[int, int] = (200, 200)) -> np.ndarray:
    '''Uniform grey BGR frame at ``value`` (0..255).'''
    h, w = shape
    return np.full((h, w, 3), value, dtype=np.uint8)


def _with_circle(
    frame: np.ndarray,
    centre: tuple[int, int],
    radius: int,
    value: int = 220,
) -> np.ndarray:
    '''Draw a filled disc on a copy of ``frame``.'''
    out = frame.copy()
    cv2.circle(out, centre, radius, (value, value, value), -1)
    return out


# ----------------------------------------------------------------
# Stage 1 (intensity) isolated
# ----------------------------------------------------------------

def test_empty_dark_slot_fails_stage1():
    frame = _frame(20)  # dark hole
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.35,
        use_hough=False,
    )
    assert det.stage1_pass is False
    assert det.present is False
    assert det.reason is not None
    assert 'stage1_fail' in det.reason


def test_bright_uniform_passes_stage1():
    frame = _frame(200)
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.35,
        use_hough=False,
    )
    assert det.stage1_pass is True
    # Hough disabled: Stage 2 auto-passes.
    assert det.present is True


def test_high_dark_ratio_fails_stage1():
    # Mostly dark with a tiny bright patch: mean may exceed the
    # threshold but dark-ratio stays high.
    frame = _frame(30)
    frame[10:20, 10:20] = 240
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=40.0,
        dark_ratio_max=0.1,
        use_hough=False,
    )
    assert det.stage1_pass is False
    assert 'dark_ratio' in (det.reason or '')


# ----------------------------------------------------------------
# Stage 2 (Hough)
# ----------------------------------------------------------------

def test_cap_like_circle_passes_stage2():
    frame = _with_circle(_frame(180), centre=(100, 100), radius=28)
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.5,
        use_hough=True,
        hough_radius_px=(22, 34),
    )
    assert det.stage1_pass is True
    assert det.circle_count >= 1
    assert det.stage2_pass is True
    assert det.present is True


def test_wrong_radius_band_fails_stage2():
    frame = _with_circle(_frame(180), centre=(100, 100), radius=28)
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.5,
        use_hough=True,
        hough_radius_px=(50, 80),  # deliberately off from the 28 px disc
    )
    assert det.stage2_pass is False
    assert det.present is False


# ----------------------------------------------------------------
# ROI handling
# ----------------------------------------------------------------

def test_zero_roi_falls_back_to_full_frame():
    frame = _frame(200, shape=(120, 160))
    det = td.detect_tube(
        frame,
        roi=(0, 0, 0, 0),
        use_hough=False,
    )
    assert det.roi == (0, 0, 160, 120)


def test_dict_roi_is_clamped():
    frame = _frame(200, shape=(120, 160))
    det = td.detect_tube(
        frame,
        roi={'x': 150, 'y': 100, 'w': 40, 'h': 80},
        use_hough=False,
    )
    # Clamped so x + w <= 160, y + h <= 120.
    x, y, w, h = det.roi
    assert x + w <= 160
    assert y + h <= 120
    assert w >= 1 and h >= 1


def test_annotated_image_is_returned():
    frame = _with_circle(_frame(180), centre=(100, 100), radius=28)
    det = td.detect_tube(frame, roi=None, use_hough=False)
    assert det.annotated is not None
    assert det.annotated.shape == frame.shape
