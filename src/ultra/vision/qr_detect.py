'''Cartridge QR / DataMatrix code detection.

Runs ``cv2.QRCodeDetector`` on a toolhead-camera frame using a
three-pass preprocessing ladder (raw -> CLAHE -> adaptive
threshold) and stops on the first hit. Returns a
:class:`QrDetection` with the payload, the four corners, and a
tight bounding box for GUI overlays.

For cartridges stamped with DataMatrix instead of QR, pass
``format='datamatrix'`` to route through the existing libdmtx
path in :mod:`ultra.vision.dmtx_detect`. The return shape is
identical so :mod:`ultra.vision.check_runner` does not branch.

No new runtime dependencies: ``cv2`` is already vendored via
``opencv-python-headless`` and ``pylibdmtx`` (imported lazily) is
already used by :mod:`ultra.vision.dmtx_detect`.
'''
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import cv2
import numpy as np

LOG = logging.getLogger(__name__)

_SUPPORTED_FORMATS = ('qr', 'datamatrix')


@dataclass
class QrDetection:
    '''One decoded cartridge code with pixel-space geometry.

    ``corners`` is an (N, 2) float32 array in ``[TL, TR, BR, BL]``
    order when the detector returns them that way (both OpenCV
    QR and our libdmtx wrapper are normalised to that layout).
    ``bbox`` is the axis-aligned bounding rectangle around
    ``corners`` as ``(x, y, w, h)`` for quick overlays.
    '''
    payload: str
    corners: np.ndarray
    bbox: tuple[int, int, int, int]
    source_pass: str = 'raw'  # 'raw' | 'clahe' | 'adaptive' | 'datamatrix'
    extras: dict = field(default_factory=dict)


def _bbox_from_corners(corners: np.ndarray) -> tuple[int, int, int, int]:
    xs = corners[:, 0]
    ys = corners[:, 1]
    x = int(round(float(xs.min())))
    y = int(round(float(ys.min())))
    w = int(round(float(xs.max() - xs.min())))
    h = int(round(float(ys.max() - ys.min())))
    return x, y, max(1, w), max(1, h)


def _clahe_gray(gray: np.ndarray) -> np.ndarray:
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    return clahe.apply(gray)


def _adaptive_threshold(gray: np.ndarray) -> np.ndarray:
    # Gaussian pre-blur tames the cross-hatched printing on the
    # cartridge label; ``blockSize=31`` and ``C=7`` are the
    # calibrated defaults carried over from the dmtx rescue pass.
    blurred = cv2.GaussianBlur(gray, (3, 3), 0)
    return cv2.adaptiveThreshold(
        blurred,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        7,
    )


def _try_qr(
    detector: cv2.QRCodeDetector,
    img: np.ndarray,
    *,
    source_pass: str,
    min_payload_len: int,
) -> QrDetection | None:
    '''Run the OpenCV QR detector on one preprocessing variant.'''
    try:
        payload, points, _ = detector.detectAndDecode(img)
    except cv2.error as exc:
        LOG.debug('qr: detectAndDecode(%s) raised: %s', source_pass, exc)
        return None
    if not payload or points is None:
        return None
    if len(payload) < min_payload_len:
        return None
    corners = np.asarray(points, dtype=np.float32).reshape(-1, 2)
    if corners.shape[0] < 4:
        return None
    return QrDetection(
        payload=str(payload),
        corners=corners,
        bbox=_bbox_from_corners(corners),
        source_pass=source_pass,
    )


def _detect_datamatrix(
    frame_bgr: np.ndarray,
    *,
    min_payload_len: int,
    decode_timeout_ms: int,
) -> QrDetection | None:
    '''DataMatrix escape hatch via the existing libdmtx wrapper.'''
    from ultra.vision.dmtx_detect import decode_with_corners

    rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    hits = decode_with_corners(
        rgb,
        timeout_ms=decode_timeout_ms,
        max_count=1,
    )
    for det in hits:
        payload = det.payload
        if len(payload) < min_payload_len:
            continue
        corners = np.asarray(
            # Reorder [BL, BR, TR, TL] -> [TL, TR, BR, BL] so the
            # bbox / annotate code below mirrors the QR path.
            [det.tl, det.tr, det.br, det.bl],
            dtype=np.float32,
        )
        return QrDetection(
            payload=payload,
            corners=corners,
            bbox=_bbox_from_corners(corners),
            source_pass='datamatrix',
        )
    return None


def detect_qr(
    frame_bgr: np.ndarray,
    *,
    min_payload_len: int = 1,
    format: str = 'qr',
    decode_timeout_ms: int = 500,
) -> QrDetection | None:
    '''Decode a cartridge QR (or DataMatrix) from a single frame.

    Args:
        frame_bgr: Full BGR frame from the toolhead camera
            (typically 1280x720).
        min_payload_len: Minimum decoded-string length accepted.
            Rejects single-character spurious decodes from
            specular glints etc.
        format: ``'qr'`` (default) routes through
            ``cv2.QRCodeDetector``; ``'datamatrix'`` routes
            through :func:`ultra.vision.dmtx_detect.decode_with_corners`.
        decode_timeout_ms: Budget for the DataMatrix path; ignored
            by the QR path.

    Returns:
        A :class:`QrDetection` on the first successful decode, or
        ``None`` if no pass produced a valid payload.
    '''
    fmt = (format or 'qr').lower()
    if fmt not in _SUPPORTED_FORMATS:
        raise ValueError(
            f'format must be one of {_SUPPORTED_FORMATS}, '
            f'got {format!r}',
        )

    if fmt == 'datamatrix':
        return _detect_datamatrix(
            frame_bgr,
            min_payload_len=min_payload_len,
            decode_timeout_ms=decode_timeout_ms,
        )

    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    detector = cv2.QRCodeDetector()

    # Pass 1: raw grayscale. Cheapest; wins on well-lit, flat
    # labels.
    det = _try_qr(
        detector, gray,
        source_pass='raw', min_payload_len=min_payload_len,
    )
    if det is not None:
        return det

    # Pass 2: CLAHE-equalised grayscale. Rescues low-contrast
    # stickers (faded print, dim LED).
    eq = _clahe_gray(gray)
    det = _try_qr(
        detector, eq,
        source_pass='clahe', min_payload_len=min_payload_len,
    )
    if det is not None:
        return det

    # Pass 3: adaptive-threshold binarisation. Rescues glare /
    # uneven illumination where Pass 1 and Pass 2 see washed-out
    # finder patterns.
    binary = _adaptive_threshold(eq)
    det = _try_qr(
        detector, binary,
        source_pass='adaptive', min_payload_len=min_payload_len,
    )
    return det


def annotate(
    frame_bgr: np.ndarray,
    det: QrDetection | None,
) -> np.ndarray:
    '''Draw the detected code outline + payload on ``frame_bgr``.

    Returns a new BGR image; the input is not mutated. When
    ``det`` is ``None``, writes a small "no code" banner so the
    cached preview is informative even on failures.
    '''
    out = frame_bgr.copy()
    ok = (0, 255, 0)
    bad = (0, 0, 255)

    if det is None:
        cv2.putText(
            out, 'QR: no code detected', (12, 28),
            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 0), 3,
        )
        cv2.putText(
            out, 'QR: no code detected', (12, 28),
            cv2.FONT_HERSHEY_SIMPLEX, 0.6, bad, 1,
        )
        return out

    pts = np.round(det.corners).astype(np.int32)
    cv2.polylines(
        out, [pts], isClosed=True, color=ok, thickness=2,
    )
    label = f'{det.payload}  [{det.source_pass}]'
    x, y, _, _ = det.bbox
    cv2.putText(
        out, label, (max(0, x), max(20, y - 8)),
        cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3,
    )
    cv2.putText(
        out, label, (max(0, x), max(20, y - 8)),
        cv2.FONT_HERSHEY_SIMPLEX, 0.55, ok, 1,
    )
    return out
