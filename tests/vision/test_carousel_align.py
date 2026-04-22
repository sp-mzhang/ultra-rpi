"""Unit tests for :mod:`ultra.vision.carousel_align`.

These exercise the math in ``CarouselAligner.compute()`` directly
by monkey-patching ``detect()`` to return pre-built
:class:`MarkerReading` instances, so the tests do not require a
camera or libdmtx. They do import the module, which pulls in
OpenCV -- skip the whole module if cv2 is missing so the suite
stays green on environments where only the motion code is
exercised.
"""
from __future__ import annotations

import math

import pytest

cv2 = pytest.importorskip('cv2')
np = pytest.importorskip('numpy')

from ultra.vision.carousel_align import (  # noqa: E402
    AlignmentResult,
    CarouselAligner,
    MarkerReading,
    SideConfig,
    _avg_angle,
    _wrap180,
)


# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------

def _build_aligner(polarity: int = -1, min_markers: int = 2):
    '''Build an aligner that matches the shipping config layout.

    ``angle_open_initial_deg = 290`` so blister_station = 20 and
    pipette_station = 200 -- mirroring ``config/ultra_default.yaml``.
    '''
    sides = {
        'blister': SideConfig(
            name='blister',
            markers={'L', 'T', 'R', 'U'},
            reference_deg=-90.0,
            station_offset_from_open_deg=-270.0,
        ),
        'pipette': SideConfig(
            name='pipette',
            markers={'S', 'I', 'O', 'X'},
            reference_deg=90.0,
            station_offset_from_open_deg=-90.0,
        ),
    }
    return CarouselAligner(
        sides=sides,
        polarity=polarity,
        min_markers=min_markers,
        decode_timeout_ms=100,
        use_clahe_fallback=False,
        angle_open_initial_deg=290.0,
    )


def _reading(payload: str, angle_deg: float) -> MarkerReading:
    return MarkerReading(
        payload=payload,
        angle_deg=angle_deg,
        center_px=(100.0, 100.0),
        size_px=(40.0, 40.0),
        corners=[(80, 80), (120, 80), (120, 120), (80, 120)],
    )


def _patched_compute(aligner: CarouselAligner, markers):
    '''Feed ``markers`` into ``compute()`` without touching the
    real detector. Uses a dummy non-None frame so the first
    ``compute()`` guard passes.'''
    aligner.detect = lambda frame: markers  # type: ignore[method-assign]
    dummy = np.zeros((4, 4, 3), dtype=np.uint8)
    return aligner.compute(dummy)


def _isclose(a: float, b: float, tol: float = 1e-6) -> bool:
    '''Signed-angle close, accounting for the wrap jump.'''
    return abs(_wrap180(a - b)) < tol


# -------------------------------------------------------------
# Sanity / building blocks
# -------------------------------------------------------------

def test_wrap180_edges():
    assert _wrap180(0.0) == 0.0
    assert _wrap180(180.0) == 180.0
    assert _wrap180(-180.0) == 180.0
    assert _wrap180(181.0) == pytest.approx(-179.0)
    assert _wrap180(-181.0) == pytest.approx(179.0)


def test_avg_angle_empty_and_single():
    assert _avg_angle([]) is None
    assert _avg_angle([17.5]) == pytest.approx(17.5)


def test_avg_angle_wrap_safe():
    # +179 and -179 average to 180 (or -180), not 0.
    avg = _avg_angle([179.0, -179.0])
    assert avg is not None
    assert _isclose(avg, 180.0)


def test_missing_blister_raises():
    with pytest.raises(ValueError):
        CarouselAligner(
            sides={'pipette': SideConfig('pipette', set(), 90.0)},
            polarity=-1,
            angle_open_initial_deg=290.0,
        )


# -------------------------------------------------------------
# compute() -- the headline cases from the plan
# -------------------------------------------------------------

def test_mixed_aligned_at_blister_delta_zero():
    '''2 blister + 2 pipette payloads, all at -90 deg.'''
    aligner = _build_aligner()
    markers = [
        _reading('L', -90.0),
        _reading('T', -90.0),
        _reading('S', -90.0),
        _reading('I', -90.0),
    ]
    r = _patched_compute(aligner, markers)

    assert isinstance(r, AlignmentResult)
    assert r.reason is None
    assert r.side == 'blister'
    assert len(r.markers) == 4
    assert r.avg_deg is not None and _isclose(r.avg_deg, -90.0)
    assert r.reference_deg == -90.0
    assert r.c_cw_deg is not None and _isclose(r.c_cw_deg, 0.0)
    assert r.delta_motor_deg is not None
    assert _isclose(r.delta_motor_deg, 0.0)


def test_mixed_off_by_five_signed_delta():
    '''Same mix at -85 deg: c_cw = +5; polarity = -1 -> delta = -5.'''
    aligner = _build_aligner(polarity=-1)
    markers = [
        _reading('L', -85.0),
        _reading('U31', -85.0),    # numeric-suffix payloads decode fine
        _reading('S', -85.0),
        _reading('X', -85.0),
    ]
    r = _patched_compute(aligner, markers)

    assert r.reason is None
    assert r.avg_deg is not None and _isclose(r.avg_deg, -85.0)
    assert r.c_cw_deg is not None and _isclose(r.c_cw_deg, 5.0)
    assert r.delta_motor_deg is not None
    assert _isclose(r.delta_motor_deg, -5.0)


def test_blister_only_off_by_three():
    '''All-blister readings at -87 deg: c_cw = +3; delta = -3.'''
    aligner = _build_aligner(polarity=-1)
    markers = [
        _reading('L', -87.0),
        _reading('T', -87.0),
        _reading('R', -87.0),
        _reading('U', -87.0),
    ]
    r = _patched_compute(aligner, markers)

    assert r.reason is None
    assert r.c_cw_deg is not None and _isclose(r.c_cw_deg, 3.0)
    assert r.delta_motor_deg is not None
    assert _isclose(r.delta_motor_deg, -3.0)


def test_stray_only_still_averaged():
    '''Two `'p'` stickers at -90 with min_markers=2: must still
    average in and produce delta 0.'''
    aligner = _build_aligner(min_markers=2)
    markers = [
        _reading('p', -90.0),
        _reading('p2', -90.0),
    ]
    r = _patched_compute(aligner, markers)

    assert r.reason is None
    assert r.side == 'blister'
    assert r.avg_deg is not None and _isclose(r.avg_deg, -90.0)
    assert r.delta_motor_deg is not None
    assert _isclose(r.delta_motor_deg, 0.0)


def test_too_few_markers_reports_reason():
    aligner = _build_aligner(min_markers=3)
    markers = [
        _reading('L', -90.0),
        _reading('T', -90.0),
    ]
    r = _patched_compute(aligner, markers)

    assert r.delta_motor_deg is None
    assert r.reason is not None
    assert r.reason.startswith('too_few_markers')


def test_no_frame_reports_reason():
    aligner = _build_aligner()
    r = aligner.compute(None)  # type: ignore[arg-type]

    assert r.delta_motor_deg is None
    assert r.reason == 'no_frame'


def test_station_deg_blister_matches_config():
    '''Sanity: station_deg('blister') == (290 + -270) % 360 == 20.'''
    aligner = _build_aligner()
    assert math.isclose(aligner.station_deg('blister'), 20.0)
