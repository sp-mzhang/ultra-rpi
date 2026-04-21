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


def _payload_key(payload: str) -> str:
    """Normalise a decoded payload to its marker-identity key.

    Stickers are printed as a single identifying letter
    (``L`` / ``T`` / ``R`` / ``U`` on the blister side) and may
    carry an arbitrary numeric suffix for batch / revision
    tracking -- e.g. ``U31``, ``T02``. The carousel-side marker
    sets in ``config/ultra_default.yaml`` only store the letter,
    so matching is first-letter based: ``U31`` -> ``U``.

    Returns the upper-cased first character of the stripped
    payload, or the empty string for empty input. All payload
    comparisons (side classification, angle-average filtering,
    overlay colouring) route through here so they stay in sync.
    """
    s = (payload or '').strip().upper()
    return s[:1]


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


def annotate(
    frame_bgr: np.ndarray,
    result: 'AlignmentResult',
    side_markers: set[str] | None = None,
) -> np.ndarray:
    """Overlay marker outlines, orientation line, and a HUD.

    Green = marker was used in the average (payload in the chosen
    side's marker set). Magenta = decoded but not part of the
    average (wrong side or unrecognised payload). Caller passes
    ``side_markers`` = the set we want highlighted; if omitted and
    ``result.side`` is set, all decoded markers are drawn green.

    Returns a new BGR image; the input is not mutated. Colours are
    BGR to match OpenCV convention.
    """
    out = frame_bgr.copy()
    # Match on the first letter only (see _payload_key docstring
    # for why: stickers can carry a numeric suffix such as "U31").
    matched = {_payload_key(p) for p in (side_markers or set())}
    if not matched and result.side is not None:
        matched = {
            _payload_key(m.payload)
            for m in (result.markers or [])
        }

    ok = (0, 255, 0)
    ignored = (255, 0, 255)

    for m in (result.markers or []):
        if not m.corners or len(m.corners) < 4:
            continue
        pts = np.array(
            [[int(round(x)), int(round(y))] for (x, y) in m.corners],
            dtype=np.int32,
        )
        col = ok if _payload_key(m.payload) in matched else ignored
        cv2.polylines(
            out, [pts], isClosed=True, color=col, thickness=2,
        )
        # Emphasise the BL->BR (L-finder bottom) edge so the
        # orientation is visually unambiguous.
        cv2.line(
            out, tuple(pts[0]), tuple(pts[1]), col, 3,
        )
        cx = int(round(m.center_px[0]))
        cy = int(round(m.center_px[1]))
        label = f'{m.payload} {m.angle_deg:+.1f}deg'
        cv2.putText(
            out, label, (cx - 30, cy - 8),
            cv2.FONT_HERSHEY_SIMPLEX, 0.5, col, 2,
        )

    hud: list[str] = []
    if result.side is not None:
        hud.append(f'side: {result.side}')
    if result.avg_deg is not None:
        hud.append(f'avg:  {result.avg_deg:+.2f} deg')
    if result.reference_deg is not None:
        hud.append(f'ref:  {result.reference_deg:+.2f} deg')
    if result.c_cw_deg is not None:
        hud.append(f'c_cw: {result.c_cw_deg:+.2f} deg')
    if result.delta_motor_deg is not None:
        hud.append(f'move: {result.delta_motor_deg:+.2f} deg')
    if result.reason:
        hud.append(f'REASON: {result.reason}')

    y = 24
    for line in hud:
        cv2.putText(
            out, line, (12, y),
            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3,
        )
        cv2.putText(
            out, line, (12, y),
            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 255), 1,
        )
        y += 22
    return out


@dataclass
class SideConfig:
    name: str
    markers: set[str]
    reference_deg: float
    # Offset applied to ``angle_open_initial_deg`` to derive the
    # absolute motor angle where this side is mechanically at the
    # gantry. Mirrors the firmware station derivations:
    #   blister: -270, serum: -180, pipette: -90.
    # The orchestrator commands
    #   target_motor = station_deg + delta_motor_deg
    # where station_deg = (angle_open_initial_deg + offset) % 360.
    station_offset_from_open_deg: float = 0.0

    def station_deg(self, angle_open_initial_deg: float) -> float:
        '''Return the absolute station angle (0..360) for this side.'''
        return (
            angle_open_initial_deg
            + self.station_offset_from_open_deg
        ) % 360.0


@dataclass
class MarkerReading:
    payload: str
    angle_deg: float
    center_px: tuple[float, float]
    size_px: tuple[float, float]
    corners: list[tuple[float, float]] | None = None


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
        angle_open_initial_deg: float = 290.0,
    ) -> None:
        self.sides = sides
        self.polarity = 1 if polarity >= 0 else -1
        self.min_markers = max(1, int(min_markers))
        self.decode_timeout_ms = int(decode_timeout_ms)
        self.use_clahe_fallback = bool(use_clahe_fallback)
        self.angle_open_initial_deg = float(angle_open_initial_deg)

    def station_deg(self, side_name: str) -> float:
        '''Absolute station angle for ``side_name`` (0..360).

        Returns 0.0 if the side is unknown.
        '''
        side = self.sides.get(side_name)
        if side is None:
            return 0.0
        return side.station_deg(self.angle_open_initial_deg)

    @classmethod
    def from_config(
        cls,
        cfg: dict,
        angle_open_initial_deg: float | None = None,
    ) -> 'CarouselAligner':
        """Build from the ``carousel_align`` YAML section.

        ``angle_open_initial_deg`` should come from the recipe /
        ``calibration`` block; it's passed in explicitly so the
        aligner doesn't have to reach into the app config. If
        omitted, falls back to ``carousel_align.angle_open_initial_deg``
        and finally to 290.
        """
        sides_cfg = (cfg or {}).get('sides', {}) or {}
        sides: dict[str, SideConfig] = {}
        for name, raw in sides_cfg.items():
            markers = set((raw or {}).get('markers') or [])
            ref = float((raw or {}).get('reference_deg', 0.0))
            offset = float(
                (raw or {}).get(
                    'station_offset_from_open_deg', 0.0,
                ),
            )
            sides[name] = SideConfig(
                name, markers, ref,
                station_offset_from_open_deg=offset,
            )
        cent = (cfg or {}).get('centrifuge', {}) or {}
        if angle_open_initial_deg is None:
            angle_open_initial_deg = float(
                (cfg or {}).get('angle_open_initial_deg', 290.0),
            )
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
            angle_open_initial_deg=float(angle_open_initial_deg),
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
                corners=list(det.corners),
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
        # Compare on the first letter only so stickers with
        # numeric suffixes (e.g. "U31") still resolve to "U".
        payloads = {_payload_key(m.payload) for m in markers}
        best_name = None
        best_score = 0
        for name, side in self.sides.items():
            if not side.markers:
                continue
            side_keys = {_payload_key(s) for s in side.markers}
            score = len(payloads & side_keys)
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
        # First-letter matching (see _payload_key) means a sticker
        # decoded as e.g. "U31" still counts toward the "U" slot.
        side_keys = {_payload_key(s) for s in side.markers}
        angles = [
            m.angle_deg for m in markers
            if _payload_key(m.payload) in side_keys
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
