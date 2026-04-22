'''Unit tests for :mod:`ultra.vision.qr_detect`.

Skips when cv2 / numpy are unavailable (keeps the suite green
on the motion-only dev env). Uses ``qrcode`` when installed to
generate a real code; otherwise exercises only the fallback
paths + API contracts via a fake ``cv2.QRCodeDetector``.
'''
from __future__ import annotations

import pytest

cv2 = pytest.importorskip('cv2')
np = pytest.importorskip('numpy')

from ultra.vision import qr_detect as qr  # noqa: E402


def _render_qr(payload: str, size: int = 400) -> 'np.ndarray | None':
    '''Render ``payload`` into a white-bordered BGR frame.

    Returns ``None`` when the ``qrcode`` package is not
    installed -- the caller should skip the test in that case.
    '''
    try:
        import qrcode  # type: ignore[import-not-found]
    except Exception:
        return None
    img = qrcode.make(payload)
    arr = np.array(img.convert('L'))
    # Pad into a larger BGR frame so the code has a safe quiet
    # zone (``cv2.QRCodeDetector`` occasionally fails on
    # edge-hugging codes).
    pad = 100
    canvas = np.full(
        (arr.shape[0] + 2 * pad, arr.shape[1] + 2 * pad),
        255, dtype=np.uint8,
    )
    canvas[pad:pad + arr.shape[0], pad:pad + arr.shape[1]] = arr
    bgr = cv2.cvtColor(canvas, cv2.COLOR_GRAY2BGR)
    if size and size != bgr.shape[0]:
        bgr = cv2.resize(bgr, (size, size))
    return bgr


# ----------------------------------------------------------------
# Real-QR path
# ----------------------------------------------------------------

def test_detect_real_qr_raw_pass():
    frame = _render_qr('ULTRA-TEST-001')
    if frame is None:
        pytest.skip('qrcode package not installed')
    det = qr.detect_qr(frame, min_payload_len=4)
    assert det is not None
    assert det.payload == 'ULTRA-TEST-001'
    assert det.source_pass in ('raw', 'clahe', 'adaptive')
    assert det.corners.shape == (4, 2)
    x, y, w, h = det.bbox
    assert w > 0 and h > 0


def test_detect_rejects_too_short_payload():
    frame = _render_qr('HI')
    if frame is None:
        pytest.skip('qrcode package not installed')
    det = qr.detect_qr(frame, min_payload_len=5)
    assert det is None


def test_detect_no_code_returns_none():
    # Flat grey frame: no finder pattern anywhere.
    frame = np.full((480, 640, 3), 128, dtype=np.uint8)
    det = qr.detect_qr(frame, min_payload_len=1)
    assert det is None


# ----------------------------------------------------------------
# Format dispatch
# ----------------------------------------------------------------

def test_format_datamatrix_routes_through_dmtx(monkeypatch):
    from ultra.vision import qr_detect as qr_mod
    from ultra.vision import dmtx_detect as dmtx_mod

    class _Fake:
        data = b'CARTRIDGE-XYZ'
        bl = (10.0, 90.0)
        br = (90.0, 90.0)
        tr = (90.0, 10.0)
        tl = (10.0, 10.0)

        @property
        def payload(self) -> str:
            return self.data.decode('utf-8')

    def _fake_decode(*args, **kwargs):
        return [_Fake()]

    monkeypatch.setattr(
        dmtx_mod, 'decode_with_corners', _fake_decode,
    )

    frame = np.zeros((120, 120, 3), dtype=np.uint8)
    det = qr_mod.detect_qr(frame, format='datamatrix', min_payload_len=1)
    assert det is not None
    assert det.payload == 'CARTRIDGE-XYZ'
    assert det.source_pass == 'datamatrix'
    assert det.corners.shape == (4, 2)


def test_invalid_format_raises():
    frame = np.zeros((120, 120, 3), dtype=np.uint8)
    with pytest.raises(ValueError):
        qr.detect_qr(frame, format='barcode')


# ----------------------------------------------------------------
# Annotate never crashes
# ----------------------------------------------------------------

def test_annotate_without_detection():
    frame = np.full((120, 200, 3), 64, dtype=np.uint8)
    out = qr.annotate(frame, None)
    assert out.shape == frame.shape
    # Input is not mutated.
    assert np.array_equal(frame, np.full((120, 200, 3), 64, dtype=np.uint8))


def test_annotate_with_detection():
    corners = np.array(
        [[10.0, 10.0], [90.0, 10.0], [90.0, 90.0], [10.0, 90.0]],
        dtype=np.float32,
    )
    det = qr.QrDetection(
        payload='ABC123',
        corners=corners,
        bbox=(10, 10, 80, 80),
        source_pass='raw',
    )
    frame = np.full((120, 120, 3), 200, dtype=np.uint8)
    out = qr.annotate(frame, det)
    # Some pixels must have changed (polyline + text).
    assert not np.array_equal(out, frame)
