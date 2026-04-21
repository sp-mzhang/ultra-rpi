"""DataMatrix decoding with **rotated-corner** output.

Why this module exists
----------------------
``pylibdmtx.decode()`` (v0.1.10, current PyPI) only returns an
axis-aligned ``Rect(left, top, width, height)`` per decoded marker.
That forces downstream code to guess the marker's rotated corners
via adaptive thresholding + ``cv2.minAreaRect``, which is fragile
on small markers under variable illumination and ambiguous by 90
degrees (a square has no intrinsic orientation).

But ``libdmtx`` itself already computes a full 3x3 transform
(``DmtxRegion.fit2raw``) that maps the unit square in "fit"
coordinates onto the marker's physical pixel location in the
image. The L finder pattern makes this transform **unambiguous**:

    fit(0, 0) -> bottom-left  corner (origin of the solid L)
    fit(1, 0) -> bottom-right corner (end of the bottom arm)
    fit(1, 1) -> top-right    corner
    fit(0, 1) -> top-left     corner (end of the left arm)

Mapping those four points through ``fit2raw`` gives us the
marker's true rotated corners directly -- no thresholding, no
minAreaRect, no 90-degree ambiguity.

``pylibdmtx`` already exposes every C function we need via its
``pylibdmtx.wrapper`` module; this file just assembles them into a
decode loop that returns the four corner points.

libdmtx uses a **bottom-up Y** image convention (like OpenGL),
while numpy/OpenCV use top-down. We flip Y on output so callers
get image pixel coordinates without further thought.
"""
from __future__ import annotations

import math
from contextlib import contextmanager
from ctypes import byref, cast, string_at
from dataclasses import dataclass
from typing import Optional

import numpy as np

# Internal pylibdmtx bindings. These names are all in
# pylibdmtx.wrapper.__all__, so they are a stable surface to
# consume even though we're bypassing the pylibdmtx.decode()
# front door.
from pylibdmtx.pylibdmtx_error import PyLibDMTXError
from pylibdmtx.wrapper import (
    DmtxPackOrder,
    DmtxUndefined,
    DmtxVector2,
    c_ubyte_p,
    dmtxDecodeCreate,
    dmtxDecodeDestroy,
    dmtxDecodeMatrixRegion,
    dmtxImageCreate,
    dmtxImageDestroy,
    dmtxMatrix3VMultiplyBy,
    dmtxMessageDestroy,
    dmtxRegionDestroy,
    dmtxRegionFindNext,
    dmtxTimeAdd,
    dmtxTimeNow,
)

# Mirror pylibdmtx's pack-order map (copied verbatim so we don't
# depend on a private symbol).
_PACK_ORDER = {
    8: DmtxPackOrder.DmtxPack8bppK,
    16: DmtxPackOrder.DmtxPack16bppRGB,
    24: DmtxPackOrder.DmtxPack24bppRGB,
    32: DmtxPackOrder.DmtxPack32bppRGBX,
}


@dataclass
class DmtxDetection:
    """One decoded DataMatrix with its four rotated corners.

    Corners are in **image pixel coordinates** (Y grows down, to
    match OpenCV / numpy). Labels are relative to the marker's
    intrinsic frame as defined by its L finder pattern -- so
    ``bl`` is always the origin of the L regardless of how the
    marker happens to be rotated in the image.
    """

    data: bytes
    bl: tuple[float, float]
    br: tuple[float, float]
    tr: tuple[float, float]
    tl: tuple[float, float]

    @property
    def payload(self) -> str:
        """UTF-8-decoded payload, with replacement for any bytes
        that aren't valid UTF-8 (matches the probe script's
        historical behaviour)."""
        return self.data.decode('utf-8', errors='replace')

    @property
    def corners(self) -> list[tuple[float, float]]:
        """Corners in ``[BL, BR, TR, TL]`` order. Compatible with
        ``cv2.polylines`` (draws a closed quadrilateral)."""
        return [self.bl, self.br, self.tr, self.tl]

    @property
    def center(self) -> tuple[float, float]:
        xs = (self.bl[0] + self.br[0] + self.tr[0] + self.tl[0])
        ys = (self.bl[1] + self.br[1] + self.tr[1] + self.tl[1])
        return (xs / 4.0, ys / 4.0)

    @property
    def orientation_deg(self) -> float:
        """Angle of the bottom edge (BL -> BR) in **image**
        coordinates, wrapped to (-180, 180]. Axis-aligned = 0;
        positive = marker rotated clockwise in the image (Y grows
        down, so a CW tilt lifts BR below BL in image space...
        wait, just trust ``atan2``). Unambiguous because BL/BR
        are from the L finder, not a symmetric fit."""
        dx = self.br[0] - self.bl[0]
        dy = self.br[1] - self.bl[1]
        return math.degrees(math.atan2(dy, dx))

    @property
    def width_px(self) -> float:
        """Length of the bottom edge BL -> BR in pixels."""
        dx = self.br[0] - self.bl[0]
        dy = self.br[1] - self.bl[1]
        return math.hypot(dx, dy)

    @property
    def height_px(self) -> float:
        """Length of the left edge BL -> TL in pixels."""
        dx = self.tl[0] - self.bl[0]
        dy = self.tl[1] - self.bl[1]
        return math.hypot(dx, dy)

    @property
    def bbox(self) -> tuple[int, int, int, int]:
        """Axis-aligned bounding box ``(left, top, w, h)`` of the
        four corners. Useful for size filtering and debug
        overlays that want a rectangle."""
        xs = [self.bl[0], self.br[0], self.tr[0], self.tl[0]]
        ys = [self.bl[1], self.br[1], self.tr[1], self.tl[1]]
        left = int(math.floor(min(xs)))
        top = int(math.floor(min(ys)))
        right = int(math.ceil(max(xs)))
        bot = int(math.ceil(max(ys)))
        return (left, top, right - left, bot - top)


# ---------------------------------------------------------------
# ctypes resource managers (mirror pylibdmtx internals)
# ---------------------------------------------------------------

@contextmanager
def _image(pixels, width, height, pack):
    img = dmtxImageCreate(pixels, width, height, pack)
    if not img:
        raise PyLibDMTXError('dmtxImageCreate failed')
    try:
        yield img
    finally:
        dmtxImageDestroy(byref(img))


@contextmanager
def _decoder(img, shrink):
    dec = dmtxDecodeCreate(img, shrink)
    if not dec:
        raise PyLibDMTXError('dmtxDecodeCreate failed')
    try:
        yield dec
    finally:
        dmtxDecodeDestroy(byref(dec))


@contextmanager
def _region(dec, timeout):
    reg = dmtxRegionFindNext(dec, timeout)
    try:
        yield reg
    finally:
        if reg:
            dmtxRegionDestroy(byref(reg))


@contextmanager
def _message(dec, reg, corrections):
    msg = dmtxDecodeMatrixRegion(dec, reg, corrections)
    try:
        yield msg
    finally:
        if msg:
            dmtxMessageDestroy(byref(msg))


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def _pixel_data(image: np.ndarray) -> tuple[bytes, int, int, int]:
    """Convert a numpy image into (bytes, width, height, bpp),
    matching pylibdmtx's internal ``_pixel_data`` contract."""
    if image.dtype != np.uint8:
        image = image.astype(np.uint8)
    pixels = image.tobytes()
    height, width = image.shape[:2]
    if len(pixels) % (width * height) != 0:
        raise PyLibDMTXError(
            f'Inconsistent dimensions: {len(pixels)} bytes not '
            f'divisible by ({width}*{height})',
        )
    bpp = 8 * len(pixels) // (width * height)
    if bpp not in _PACK_ORDER:
        raise PyLibDMTXError(
            f'Unsupported bits-per-pixel {bpp}; expected one of '
            f'{sorted(_PACK_ORDER)}',
        )
    return pixels, width, height, bpp


def _vec_to_img(
    v: DmtxVector2, shrink: int, height: int,
) -> tuple[float, float]:
    """libdmtx fit2raw returns points in bottom-up Y (OpenGL-like)
    at the decoder's scanner resolution. Multiply by ``shrink``
    to get original-image pixels, then flip Y to match OpenCV's
    top-down convention."""
    return (shrink * v.X, height - shrink * v.Y)


# ---------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------

def decode_with_corners(
    image: np.ndarray,
    timeout_ms: Optional[int] = None,
    shrink: int = 1,
    max_count: Optional[int] = None,
    corrections: Optional[int] = None,
) -> list[DmtxDetection]:
    """Decode every DataMatrix barcode in ``image`` and return the
    four rotated corners for each.

    Args:
        image: H x W x {1,3,4} uint8 numpy array. For 3-channel
               input libdmtx expects RGB ordering (same as
               ``pylibdmtx.decode``).
        timeout_ms: per-call decode budget, or ``None`` = no cap.
        shrink: libdmtx scanner downscale factor (1 = no shrink).
        max_count: stop after this many successful decodes.
        corrections: libdmtx error-correction level, or ``None``
                     for library default.

    Returns:
        list of :class:`DmtxDetection`, in the order libdmtx
        discovered them (which is effectively arbitrary).

    Raises:
        PyLibDMTXError: on any libdmtx setup failure (out of
        memory, bad image shape, etc.).
    """
    dmtx_timeout = None
    if timeout_ms:
        now = dmtxTimeNow()
        dmtx_timeout = dmtxTimeAdd(now, int(timeout_ms))

    pixels, width, height, bpp = _pixel_data(image)
    results: list[DmtxDetection] = []

    with _image(
        cast(pixels, c_ubyte_p), width, height, _PACK_ORDER[bpp],
    ) as img:
        with _decoder(img, shrink) as dec:
            if corrections is None:
                corrections = DmtxUndefined
            while True:
                with _region(dec, dmtx_timeout) as reg:
                    if not reg:
                        break
                    with _message(dec, reg, corrections) as msg:
                        if not msg:
                            continue
                        # Map the four unit-square corners
                        # through fit2raw. These DmtxVector2
                        # instances are modified in place by
                        # dmtxMatrix3VMultiplyBy.
                        p00 = DmtxVector2(0.0, 0.0)
                        p10 = DmtxVector2(1.0, 0.0)
                        p11 = DmtxVector2(1.0, 1.0)
                        p01 = DmtxVector2(0.0, 1.0)
                        fit2raw = reg.contents.fit2raw
                        dmtxMatrix3VMultiplyBy(p00, fit2raw)
                        dmtxMatrix3VMultiplyBy(p10, fit2raw)
                        dmtxMatrix3VMultiplyBy(p11, fit2raw)
                        dmtxMatrix3VMultiplyBy(p01, fit2raw)
                        results.append(DmtxDetection(
                            data=string_at(msg.contents.output),
                            bl=_vec_to_img(p00, shrink, height),
                            br=_vec_to_img(p10, shrink, height),
                            tr=_vec_to_img(p11, shrink, height),
                            tl=_vec_to_img(p01, shrink, height),
                        ))
                if max_count and len(results) >= max_count:
                    break

    return results
