"""Carousel alignment via DataMatrix markers.

Runs libdmtx on a camera frame, classifies which side of the
carousel is visible by the set of decoded payload letters, averages
the per-marker orientation via complex-exponential (wrap-safe),
and computes the CW offset from a known per-side reference angle.

The motor delta is signed by a configurable ``polarity`` knob so
operators can flip direction without rebuilding.

See docs/plans/carousel_align_gui_button for the calibration
that sets ``blister.reference_deg = -90`` (markers read -90 in
the camera frame when the carousel is at the blister pose).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Iterable

import cv2
import numpy as np

from ultra.vision.dmtx_detect import (
    DmtxDetection, decode_with_corners,
)

LOG = logging.getLogger(__name__)


def _wrap180(deg: float) -> float:
    """Wrap ``deg`` into (-180, 180]."""
    x = (deg + 180.0) % 360.0 - 180.0
    # ``-180`` is equivalent to ``180``; prefer the positive rep.
    return 180.0 if x <= -180.0 else x


def _avg_angle(angles_deg: Iterable[float]) -> float | None:
    """Complex-exp mean of angles in degrees; None if empty."""
    xs = 0.0
    ys = 0.0
    n = 0
    for a in angles_deg:
        r = math.radians(a)
        xs += math.cos(r)
        ys += math.sin(r)
        n += 1
    if n == 0:
        return None
    return math.degrees(math.atan2(ys, xs))


def _enhance_for_decode(frame_bgr: np.ndarray) -> np.ndarray:
    """CLAHE on the L channel; mirrors the probe script fallback."""
    lab = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    l_eq = clahe.apply(l)
    return cv2.cvtColor(
        cv2.merge((l_eq, a, b)), cv2.COLOR_LAB2BGR,
    )


def _best_of(
    batches: list[list[DmtxDetection]],
) -> list[DmtxDetection]:
    """Deduplicate by center (25 px tolerance); prefer longer
    payload on collisions. Same heuristic as probe_markers_stream."""
    out: list[DmtxDetection] = []
    for batch in batches:
        for det in batch:
            cx, cy = det.center
            replaced = False
            for i, kept in enumerate(out):
                kcx, kcy = kept.center
                if abs(cx - kcx) < 25 and abs(cy - kcy) < 25:
                    if len(det.data) > len(kept.data):
                        out[i] = det
                    replaced = True
                    break
            if not replaced:
                out.append(det)
    return out


@dataclass
class SideConfig:
    name: str
    markers: set[str]
    reference_deg: float


@dataclass
class MarkerReading:
    payload: str
    angle_deg: float
    center_px: tuple[float, float]
    size_px: tuple[float, float]


@dataclass
class AlignmentResult:
    side: str | None
    markers: list[MarkerReading]
    avg_deg: float | None
    reference_deg: float | None
    c_cw_deg: float | None          # carousel CW offset from side reference
    delta_motor_deg: float | None   # what to add to the centrifuge angle
    polarity: int
    reason: str | None = None       # populated on validation failure


class CarouselAligner:
    """Stateless helper: detect markers, classify side, compute delta.

    All per-deployment tuning (marker sets, reference angles,
    polarity, min-markers, decode timeout) comes from the config
    dict so the GUI can hot-reload via ``app.config``.
    """

    def __init__(
        self,
        sides: dict[str, SideConfig],
        polarity: int = 1,
        min_markers: int = 2,
        decode_timeout_ms: int = 500,
        use_clahe_fallback: bool = True,
    ) -> None:
        self.sides = sides
        self.polarity = 1 if polarity >= 0 else -1
        self.min_markers = max(1, int(min_markers))
        self.decode_timeout_ms = int(decode_timeout_ms)
        self.use_clahe_fallback = bool(use_clahe_fallback)

    @classmethod
    def from_config(cls, cfg: dict) -> 'CarouselAligner':
        """Build from the ``carousel_align`` YAML section.

        Missing keys fall back to sane defaults so a partial config
        still boots (the endpoint raises cleanly if it's truly
        unusable).
        """
        sides_cfg = (cfg or {}).get('sides', {}) or {}
        sides: dict[str, SideConfig] = {}
        for name, raw in sides_cfg.items():
            markers = set((raw or {}).get('markers') or [])
            ref = float((raw or {}).get('reference_deg', 0.0))
            sides[name] = SideConfig(name, markers, ref)
        cent = (cfg or {}).get('centrifuge', {}) or {}
        return cls(
            sides=sides,
            polarity=int(cent.get('polarity', 1)),
            min_markers=int((cfg or {}).get('min_markers', 2)),
            decode_timeout_ms=int(
                (cfg or {}).get('decode_timeout_ms', 500),
            ),
            use_clahe_fallback=bool(
                (cfg or {}).get('use_clahe_fallback', True),
            ),
        )

    # --- detection ------------------------------------------------

    def detect(
        self, frame_bgr: np.ndarray,
    ) -> list[MarkerReading]:
        """Decode all DataMatrix markers in ``frame_bgr``.

        Runs the raw RGB frame; if ``use_clahe_fallback``, also
        runs a CLAHE-boosted variant and merges by location. Returns
        one entry per unique marker (by center) with angle,
        center, and size in pixels.
        """
        rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        batches = [
            decode_with_corners(
                rgb, timeout_ms=self.decode_timeout_ms, max_count=8,
            ),
        ]
        if self.use_clahe_fallback:
            enhanced = _enhance_for_decode(frame_bgr)
            rgb_eq = cv2.cvtColor(enhanced, cv2.COLOR_BGR2RGB)
            batches.append(
                decode_with_corners(
                    rgb_eq,
                    timeout_ms=self.decode_timeout_ms,
                    max_count=8,
                ),
            )
        merged = _best_of(batches)
        out: list[MarkerReading] = []
        for det in merged:
            out.append(MarkerReading(
                payload=det.payload,
                angle_deg=det.orientation_deg,
                center_px=det.center,
                size_px=(det.width_px, det.height_px),
            ))
        return out

    # --- classification / math ------------------------------------

    def classify_side(
        self, markers: list[MarkerReading],
    ) -> str | None:
        """Pick the side whose expected marker set best matches.

        Score = number of decoded payloads (stripped, upper-cased)
        that fall in the side's marker set. The side with the
        highest non-zero score wins. Returns None if no side has
        any match.
        """
        if not markers:
            return None
        payloads = {m.payload.strip().upper() for m in markers}
        best_name = None
        best_score = 0
        for name, side in self.sides.items():
            if not side.markers:
                continue
            score = len(payloads & side.markers)
            if score > best_score:
                best_score = score
                best_name = name
        return best_name if best_score > 0 else None

    def compute(
        self, frame_bgr: np.ndarray,
    ) -> AlignmentResult:
        """Full pipeline: detect -> classify -> average -> delta.

        Returns an :class:`AlignmentResult`. If any validation fails
        (no frame, too few markers, no matching side), ``reason``
        is populated and ``delta_motor_deg`` is None -- callers must
        NOT command any motion when that's the case.
        """
        if frame_bgr is None:
            return AlignmentResult(
                side=None, markers=[], avg_deg=None,
                reference_deg=None, c_cw_deg=None,
                delta_motor_deg=None, polarity=self.polarity,
                reason='no_frame',
            )
        markers = self.detect(frame_bgr)
        if len(markers) < self.min_markers:
            return AlignmentResult(
                side=None, markers=markers, avg_deg=None,
                reference_deg=None, c_cw_deg=None,
                delta_motor_deg=None, polarity=self.polarity,
                reason=(
                    f'too_few_markers: got {len(markers)}, '
                    f'need {self.min_markers}'
                ),
            )
        side_name = self.classify_side(markers)
        if side_name is None:
            return AlignmentResult(
                side=None, markers=markers, avg_deg=None,
                reference_deg=None, c_cw_deg=None,
                delta_motor_deg=None, polarity=self.polarity,
                reason='no_side_match',
            )
        side = self.sides[side_name]
        # Average only the markers that belong to this side so a
        # stray decode from the other side doesn't bias the result.
        angles = [
            m.angle_deg for m in markers
            if m.payload.strip().upper() in side.markers
        ]
        avg = _avg_angle(angles)
        if avg is None:
            return AlignmentResult(
                side=side_name, markers=markers, avg_deg=None,
                reference_deg=side.reference_deg, c_cw_deg=None,
                delta_motor_deg=None, polarity=self.polarity,
                reason='no_matching_markers_for_side',
            )
        c_cw = _wrap180(avg - side.reference_deg)
        delta = _wrap180(self.polarity * c_cw)
        return AlignmentResult(
            side=side_name, markers=markers, avg_deg=avg,
            reference_deg=side.reference_deg, c_cw_deg=c_cw,
            delta_motor_deg=delta, polarity=self.polarity,
        )
