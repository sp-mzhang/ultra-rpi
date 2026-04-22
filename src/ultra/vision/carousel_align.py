"""Carousel alignment via DataMatrix markers.

Runs libdmtx on a camera frame, averages every decoded marker's
orientation via complex-exponential (wrap-safe), and computes the
CW offset from the blister-side reference angle. The carousel is
expected to be parked at the blister station before the aligner
runs (see ``align_to_carousel`` recipe step); the aligner then
applies a small camera-measured correction so the cartridge lands
precisely at the blister pose.

All visible DataMatrix markers on the carousel share the same
stamp orientation, so their decoded angles are statistically
equivalent samples of the same quantity. Averaging every decoded
marker -- regardless of which side of the carousel it belongs to
-- therefore only tightens the mean; no per-side classification
is needed.

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


def _adaptive_threshold_for_decode(
    frame_bgr: np.ndarray,
    block_size: int = 31,
    c: int = 5,
    blur_ksize: int = 3,
) -> np.ndarray:
    """Binarise the frame so libdmtx's L-finder sees clean edges.

    Returns a single-channel ``uint8`` image. Pipeline:

      1. BGR -> grayscale
      2. small Gaussian blur (``blur_ksize x blur_ksize``) to
         smooth speckle without rounding DataMatrix cell edges
      3. ``cv2.adaptiveThreshold`` (Gaussian-weighted, binary
         inverted) so printed "+" crosshairs and DataMatrix cells
         become crisp dark-on-light shapes with straight edges.

    Against the carousel's cross-hatched background this pass
    rescues markers the CLAHE variant cannot, because the
    crosses' smooth rounded edges get rejected by libdmtx's
    L-finder much faster than when they sit on a greyscale
    gradient.

    ``block_size`` must be odd and >= 3; it's the neighbourhood
    radius in pixels over which the local mean is computed. 31 px
    works well for the ~60 px carousel markers.
    """
    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    if blur_ksize and blur_ksize >= 3:
        gray = cv2.GaussianBlur(
            gray, (int(blur_ksize), int(blur_ksize)), 0,
        )
    block = int(block_size)
    if block < 3:
        block = 3
    if block % 2 == 0:
        block += 1
    return cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        block,
        int(c),
    )


def _enhance_for_decode(
    frame_bgr: np.ndarray,
    clip_limit: float = 2.0,
    tile_grid: int = 16,
) -> np.ndarray:
    """CLAHE on the L channel; mirrors the probe script fallback.

    Defaults are tuned for the carousel's small DataMatrix stamps
    sitting next to a field of printed "+" crosses: a finer tile
    grid (16x16) gives per-marker local adaptation, and a lower
    clip limit (2.0) avoids amplifying the crosses into L-finder
    candidates that confuse libdmtx.
    """
    lab = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(
        clipLimit=clip_limit,
        tileGridSize=(int(tile_grid), int(tile_grid)),
    )
    l_eq = clahe.apply(l)
    return cv2.cvtColor(
        cv2.merge((l_eq, a, b)), cv2.COLOR_LAB2BGR,
    )


def _tile_slices(
    h: int, w: int, rows: int, cols: int, overlap_px: int,
) -> list[tuple[int, int, int, int]]:
    """Yield (y0, y1, x0, x1) tile rects covering ``h x w``.

    Tiles overlap by ``overlap_px`` on each inner edge so a marker
    straddling a tile boundary still lands fully inside at least
    one tile. Tiles on the image border extend to the border.
    """
    rows = max(1, int(rows))
    cols = max(1, int(cols))
    overlap_px = max(0, int(overlap_px))
    if rows == 1 and cols == 1:
        return [(0, h, 0, w)]
    tile_h = h // rows
    tile_w = w // cols
    slices: list[tuple[int, int, int, int]] = []
    for r in range(rows):
        for c in range(cols):
            y0 = max(0, r * tile_h - overlap_px)
            y1 = h if r == rows - 1 else min(
                h, (r + 1) * tile_h + overlap_px,
            )
            x0 = max(0, c * tile_w - overlap_px)
            x1 = w if c == cols - 1 else min(
                w, (c + 1) * tile_w + overlap_px,
            )
            slices.append((y0, y1, x0, x1))
    return slices


def _offset_detection(
    det: DmtxDetection, dx: float, dy: float,
) -> DmtxDetection:
    """Return a copy of ``det`` with all corners shifted by
    ``(dx, dy)``. Used to remap tile-local detections back into
    full-frame coordinates."""
    def _sh(p: tuple[float, float]) -> tuple[float, float]:
        return (p[0] + dx, p[1] + dy)

    return DmtxDetection(
        data=det.data,
        bl=_sh(det.bl),
        br=_sh(det.br),
        tr=_sh(det.tr),
        tl=_sh(det.tl),
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
) -> np.ndarray:
    """Overlay marker outlines, orientation line, and a HUD.

    Every decoded marker contributes to the average, so every
    rectangle is drawn in green.

    Returns a new BGR image; the input is not mutated. Colours are
    BGR to match OpenCV convention.
    """
    out = frame_bgr.copy()
    ok = (0, 255, 0)

    for m in (result.markers or []):
        if not m.corners or len(m.corners) < 4:
            continue
        pts = np.array(
            [[int(round(x)), int(round(y))] for (x, y) in m.corners],
            dtype=np.int32,
        )
        cv2.polylines(
            out, [pts], isClosed=True, color=ok, thickness=2,
        )
        # Emphasise the BL->BR (L-finder bottom) edge so the
        # orientation is visually unambiguous.
        cv2.line(
            out, tuple(pts[0]), tuple(pts[1]), ok, 3,
        )
        cx = int(round(m.center_px[0]))
        cy = int(round(m.center_px[1]))
        label = f'{m.payload} {m.angle_deg:+.1f}deg'
        cv2.putText(
            out, label, (cx - 30, cy - 8),
            cv2.FONT_HERSHEY_SIMPLEX, 0.5, ok, 2,
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
    """Stateless helper: detect markers, average, compute delta.

    Always anchors to the blister-side reference and station angle
    -- every alignment snaps the carousel to the blister pose.
    Callers are expected to have parked the carousel near blister
    (e.g. via ``centrifuge_goto_blister``) before invoking the
    aligner; the correction is then small and safe.

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
        tile_decode: bool = True,
        tile_rows: int = 3,
        tile_cols: int = 3,
        tile_overlap_px: int = 60,
        clahe_clip_limit: float = 2.0,
        clahe_tile_grid: int = 16,
        use_adaptive_threshold: bool = True,
        adaptive_block_size: int = 31,
        adaptive_c: int = 5,
        adaptive_blur_ksize: int = 3,
        symbol_size: str | None = 'auto',
        edge_min_px: int | None = 20,
        edge_max_px: int | None = 200,
        edge_thresh: int | None = None,
        square_devn: int | None = None,
    ) -> None:
        self.sides = sides
        self.polarity = 1 if polarity >= 0 else -1
        self.min_markers = max(1, int(min_markers))
        self.decode_timeout_ms = int(decode_timeout_ms)
        self.use_clahe_fallback = bool(use_clahe_fallback)
        self.angle_open_initial_deg = float(angle_open_initial_deg)
        # Tiling: split the frame into rows x cols tiles with a
        # small overlap and decode each independently so libdmtx's
        # region scanner doesn't get stuck near the first marker.
        self.tile_decode = bool(tile_decode)
        self.tile_rows = max(1, int(tile_rows))
        self.tile_cols = max(1, int(tile_cols))
        self.tile_overlap_px = max(0, int(tile_overlap_px))
        # CLAHE params for the enhanced preprocessing pass.
        self.clahe_clip_limit = float(clahe_clip_limit)
        self.clahe_tile_grid = max(1, int(clahe_tile_grid))
        # Adaptive-threshold preprocessing pass: turns printed
        # crosshairs into smooth blobs that libdmtx's L-finder
        # rejects quickly, freeing budget for real markers.
        self.use_adaptive_threshold = bool(use_adaptive_threshold)
        self.adaptive_block_size = int(adaptive_block_size)
        self.adaptive_c = int(adaptive_c)
        self.adaptive_blur_ksize = int(adaptive_blur_ksize)
        # Region-scanner hints forwarded to every libdmtx call. See
        # ``ultra.vision.dmtx_detect.decode_with_corners`` for the
        # semantics. ``symbol_size='auto'`` (shape-auto) is the
        # safe default. Edge bounds cut away trivial tiny/huge
        # candidate edges the scanner would otherwise chase.
        self.symbol_size = symbol_size
        self.edge_min_px = edge_min_px
        self.edge_max_px = edge_max_px
        self.edge_thresh = edge_thresh
        self.square_devn = square_devn

        # Cache the one-and-only reference + station. Missing
        # blister config is a hard build error -- the aligner has
        # no fallback without it.
        blister = self.sides.get('blister')
        if blister is None:
            raise ValueError(
                "carousel_align.sides.blister is required "
                "(used as the single reference + target station)",
            )
        self._reference_deg: float = blister.reference_deg
        self._station_deg: float = blister.station_deg(
            self.angle_open_initial_deg,
        )

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
            tile_decode=bool(
                (cfg or {}).get('tile_decode', True),
            ),
            tile_rows=int(
                (cfg or {}).get('tile_rows', 3),
            ),
            tile_cols=int(
                (cfg or {}).get('tile_cols', 3),
            ),
            tile_overlap_px=int(
                (cfg or {}).get('tile_overlap_px', 60),
            ),
            clahe_clip_limit=float(
                (cfg or {}).get('clahe_clip_limit', 2.0),
            ),
            clahe_tile_grid=int(
                (cfg or {}).get('clahe_tile_grid', 16),
            ),
            use_adaptive_threshold=bool(
                (cfg or {}).get('use_adaptive_threshold', True),
            ),
            adaptive_block_size=int(
                (cfg or {}).get('adaptive_block_size', 31),
            ),
            adaptive_c=int(
                (cfg or {}).get('adaptive_c', 5),
            ),
            adaptive_blur_ksize=int(
                (cfg or {}).get('adaptive_blur_ksize', 3),
            ),
            symbol_size=(cfg or {}).get('symbol_size', 'auto'),
            edge_min_px=(cfg or {}).get('edge_min_px', 20),
            edge_max_px=(cfg or {}).get('edge_max_px', 200),
            edge_thresh=(cfg or {}).get('edge_thresh', None),
            square_devn=(cfg or {}).get('square_devn', None),
        )

    # --- detection ------------------------------------------------

    def detect(
        self, frame_bgr: np.ndarray,
    ) -> list[MarkerReading]:
        """Decode all DataMatrix markers in ``frame_bgr``.

        When ``tile_decode`` is on, splits the frame into
        ``tile_rows x tile_cols`` tiles with ``tile_overlap_px``
        overlap and runs libdmtx on each tile independently. Each
        tile's region scanner seeds from a fresh raster, which
        rescues markers the one-shot full-frame scanner misses
        after locking onto the first candidate.

        For every tile we run up to two preprocessing variants
        (raw + optional CLAHE), then merge detections across all
        tiles via ``_best_of`` (dedupe by center, keep longest
        payload).
        """
        h, w = frame_bgr.shape[:2]
        # Build the full-frame preprocessing variants once; we
        # slice them per-tile below so we don't repeat the
        # CLAHE / threshold work per tile.
        rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        rgb_eq: np.ndarray | None = None
        bin_img: np.ndarray | None = None
        if self.use_clahe_fallback:
            enhanced_bgr = _enhance_for_decode(
                frame_bgr,
                clip_limit=self.clahe_clip_limit,
                tile_grid=self.clahe_tile_grid,
            )
            rgb_eq = cv2.cvtColor(
                enhanced_bgr, cv2.COLOR_BGR2RGB,
            )
        if self.use_adaptive_threshold:
            bin_img = _adaptive_threshold_for_decode(
                frame_bgr,
                block_size=self.adaptive_block_size,
                c=self.adaptive_c,
                blur_ksize=self.adaptive_blur_ksize,
            )

        if self.tile_decode:
            tiles = _tile_slices(
                h, w,
                rows=self.tile_rows,
                cols=self.tile_cols,
                overlap_px=self.tile_overlap_px,
            )
        else:
            tiles = [(0, h, 0, w)]

        hint_kwargs = {
            'symbol_size': self.symbol_size,
            'edge_min': self.edge_min_px,
            'edge_max': self.edge_max_px,
            'edge_thresh': self.edge_thresh,
            'square_devn': self.square_devn,
        }

        batches: list[list[DmtxDetection]] = []
        for (y0, y1, x0, x1) in tiles:
            raw_tile = rgb[y0:y1, x0:x1]
            raw_hits = decode_with_corners(
                raw_tile,
                timeout_ms=self.decode_timeout_ms,
                max_count=8,
                **hint_kwargs,
            )
            batches.append([
                _offset_detection(d, x0, y0) for d in raw_hits
            ])
            if rgb_eq is not None:
                eq_tile = rgb_eq[y0:y1, x0:x1]
                eq_hits = decode_with_corners(
                    eq_tile,
                    timeout_ms=self.decode_timeout_ms,
                    max_count=8,
                    **hint_kwargs,
                )
                batches.append([
                    _offset_detection(d, x0, y0) for d in eq_hits
                ])
            if bin_img is not None:
                # Adaptive-threshold output is single-channel
                # uint8; libdmtx's 8bpp pack handles that natively
                # so we can pass the slice straight in without any
                # colour conversion.
                bin_tile = bin_img[y0:y1, x0:x1]
                bin_hits = decode_with_corners(
                    bin_tile,
                    timeout_ms=self.decode_timeout_ms,
                    max_count=8,
                    **hint_kwargs,
                )
                batches.append([
                    _offset_detection(d, x0, y0) for d in bin_hits
                ])
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
        # Always log what libdmtx actually returned so small-marker
        # regressions are visible even when we end up below
        # min_markers and compute() short-circuits with no HUD.
        if LOG.isEnabledFor(logging.INFO):
            if out:
                summary = ', '.join(
                    f"'{m.payload}'@{int(round(m.size_px[0]))}x"
                    f"{int(round(m.size_px[1]))}"
                    f"[ang={m.angle_deg:+.1f}]"
                    for m in out
                )
                LOG.info(
                    'carousel_align.detect: %d marker(s): %s',
                    len(out), summary,
                )
            else:
                LOG.info('carousel_align.detect: 0 markers')
        return out

    # --- math -----------------------------------------------------

    def compute(
        self, frame_bgr: np.ndarray,
    ) -> AlignmentResult:
        """Full pipeline: detect -> average -> delta.

        Averages every decoded marker's orientation (no payload
        filter) and returns the CW offset relative to the blister
        reference plus the signed motor delta to command.

        Returns an :class:`AlignmentResult`. If any validation
        fails (no frame, too few markers), ``reason`` is populated
        and ``delta_motor_deg`` is None -- callers must NOT
        command any motion when that's the case.
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
        # Every DataMatrix on the carousel shares the same stamp
        # orientation, so every decoded marker is an equally valid
        # angle sample. Average unconditionally.
        angles = [m.angle_deg for m in markers]
        avg = _avg_angle(angles)
        c_cw = _wrap180(avg - self._reference_deg)
        delta = _wrap180(self.polarity * c_cw)
        return AlignmentResult(
            side='blister', markers=markers, avg_deg=avg,
            reference_deg=self._reference_deg, c_cw_deg=c_cw,
            delta_motor_deg=delta, polarity=self.polarity,
        )
