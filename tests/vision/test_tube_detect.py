'''Unit tests for :mod:`ultra.vision.tube_detect`.

Exercises the two-stage detector (intensity + HSV saturation)
with synthetic frames so the suite does not need a live camera.
Stage 1 is easy to hit with uniform-fill crops; Stage 2 is
validated by painting a saturated colour into the ROI (mimics a
tube cap; white plastic doesn't add saturation).
'''
from __future__ import annotations

import pytest

cv2 = pytest.importorskip('cv2')
np = pytest.importorskip('numpy')

from ultra.vision import tube_detect as td  # noqa: E402


def _gray_frame(
    value: int, shape: tuple[int, int] = (200, 200),
) -> np.ndarray:
    '''Uniform achromatic BGR frame at ``value`` (0..255).

    Achromatic = R == G == B, so HSV saturation is ~0 for every
    pixel. Simulates an empty slot of white plastic.
    '''
    h, w = shape
    return np.full((h, w, 3), value, dtype=np.uint8)


def _colour_frame(
    bgr: tuple[int, int, int],
    shape: tuple[int, int] = (200, 200),
) -> np.ndarray:
    '''Uniform strongly-saturated BGR frame; mimics a cap.'''
    h, w = shape
    out = np.zeros((h, w, 3), dtype=np.uint8)
    out[:] = bgr
    return out


# ----------------------------------------------------------------
# Stage 1 (intensity) isolated
# ----------------------------------------------------------------

def test_empty_dark_slot_fails_stage1():
    frame = _gray_frame(20)  # dark hole
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.35,
        mean_saturation_min=40.0,
    )
    assert det.stage1_pass is False
    assert det.present is False
    assert det.reason is not None
    assert 'stage1_fail' in det.reason


def test_bright_white_plastic_fails_stage2():
    # Empty slot: bright (Stage 1 passes) but achromatic (Stage
    # 2 fails, because white plastic has near-zero saturation).
    frame = _gray_frame(220)
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.35,
        mean_saturation_min=40.0,
    )
    assert det.stage1_pass is True
    assert det.stage2_pass is False
    assert det.present is False
    assert 'stage2_fail' in (det.reason or '')


def test_high_dark_ratio_fails_stage1():
    # Mostly dark with a tiny bright patch: mean may exceed the
    # threshold but dark-ratio stays high.
    frame = _gray_frame(30)
    frame[10:20, 10:20] = 240
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=40.0,
        dark_ratio_max=0.1,
        mean_saturation_min=0.0,
    )
    assert det.stage1_pass is False
    assert 'dark_ratio' in (det.reason or '')


# ----------------------------------------------------------------
# Stage 2 (HSV saturation) -- the colour-agnostic gate
# ----------------------------------------------------------------

def test_blue_cap_passes_both_stages():
    # BGR = (200, 50, 50) -> strongly saturated blue.
    frame = _colour_frame((200, 50, 50))
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=40.0,
        dark_ratio_max=0.6,
        mean_saturation_min=40.0,
    )
    assert det.stage1_pass is True
    assert det.stage2_pass is True
    assert det.present is True
    assert det.mean_saturation > 40.0


def test_red_cap_passes_both_stages():
    # BGR = (50, 50, 220) -> strongly saturated red. This is
    # the key case the old Hough-based detector kept missing.
    frame = _colour_frame((50, 50, 220))
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=40.0,
        dark_ratio_max=0.6,
        mean_saturation_min=40.0,
    )
    assert det.stage1_pass is True
    assert det.stage2_pass is True
    assert det.present is True


def test_low_saturation_fails_stage2_even_when_intensity_ok():
    # Near-white frame with a faint tint: bright enough for
    # Stage 1 but too desaturated for Stage 2.
    frame = _gray_frame(210)
    frame[:, :, 0] = 215  # slight blue wash, still near-grey
    det = td.detect_tube(
        frame,
        roi=None,
        dark_threshold=60,
        mean_intensity_min=90.0,
        dark_ratio_max=0.35,
        mean_saturation_min=40.0,
    )
    assert det.stage1_pass is True
    assert det.stage2_pass is False
    assert 'stage2_fail' in (det.reason or '')


# ----------------------------------------------------------------
# ROI handling
# ----------------------------------------------------------------

def test_zero_roi_falls_back_to_full_frame():
    frame = _gray_frame(200, shape=(120, 160))
    det = td.detect_tube(
        frame,
        roi=(0, 0, 0, 0),
        mean_saturation_min=0.0,
    )
    assert det.roi == (0, 0, 160, 120)


def test_dict_roi_is_clamped():
    frame = _gray_frame(200, shape=(120, 160))
    det = td.detect_tube(
        frame,
        roi={'x': 150, 'y': 100, 'w': 40, 'h': 80},
        mean_saturation_min=0.0,
    )
    # Clamped so x + w <= 160, y + h <= 120.
    x, y, w, h = det.roi
    assert x + w <= 160
    assert y + h <= 120
    assert w >= 1 and h >= 1


def test_roi_isolates_coloured_region_from_empty_surround():
    # Mostly achromatic frame with a saturated patch. With a
    # full-frame ROI, mean saturation is diluted; with a tight
    # ROI around the patch, saturation is strong. This is
    # exactly why calibrating the ROI matters.
    frame = _gray_frame(210, shape=(200, 200))
    frame[80:120, 80:120] = (50, 50, 220)  # red patch

    det_full = td.detect_tube(
        frame,
        roi=(0, 0, 0, 0),
        mean_saturation_min=40.0,
    )
    det_roi = td.detect_tube(
        frame,
        roi=(80, 80, 40, 40),
        mean_saturation_min=40.0,
    )
    assert det_full.mean_saturation < det_roi.mean_saturation
    assert det_roi.stage2_pass is True


def test_annotated_image_is_returned():
    frame = _colour_frame((200, 50, 50))
    det = td.detect_tube(
        frame, roi=None, mean_saturation_min=40.0,
    )
    assert det.annotated is not None
    assert det.annotated.shape == frame.shape


# ----------------------------------------------------------------
# Template-matching backend takes precedence over saturation
# ----------------------------------------------------------------

def test_template_path_takes_precedence_when_refs_present():
    '''If refs exist for both classes, verdict comes from NCC.'''
    from ultra.vision import tube_template as tt
    # Build a frame that would FAIL saturation (low-sat grey)
    # but matches an "empty" grey reference pattern.
    frame = _gray_frame(210, shape=(200, 200))
    # Matching the crop shape to the ref size; ROI is full frame.
    grey_ref = _gray_frame(210, shape=(200, 200))
    blue_ref = _colour_frame((200, 50, 50), shape=(200, 200))
    templates = {
        tt.LABEL_EMPTY: [('empty_1.png', grey_ref)],
        tt.LABEL_SEATED: [('seated_1.png', blue_ref)],
    }
    det = td.detect_tube(
        frame, roi=None,
        # Absurd saturation threshold so the saturation path
        # would say ABSENT. Template path must override.
        mean_saturation_min=200.0,
        templates=templates,
        template_min_score=0.5,
        template_search_px=0,
    )
    assert det.method == 'template'
    assert det.seated_count == 1
    assert det.empty_count == 1
    # Frame matches empty ref, so empty_score should dominate.
    assert det.empty_score > det.seated_score
    assert det.present is False
    assert 'template_empty_match' in (det.reason or '')


def test_saturation_fallback_when_one_class_missing():
    '''Refs for only one class -> template path disabled.'''
    from ultra.vision import tube_template as tt
    frame = _colour_frame((200, 50, 50))
    templates = {
        tt.LABEL_SEATED: [('s.png', frame.copy())],
        tt.LABEL_EMPTY: [],
    }
    det = td.detect_tube(
        frame, roi=None,
        mean_saturation_min=40.0,
        templates=templates,
    )
    # Only one class -> detector must fall back to saturation,
    # where the blue frame passes cleanly.
    assert det.method == 'saturation'
    assert det.present is True
