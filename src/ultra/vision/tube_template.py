'''Per-machine template-matching backend for serum-tube presence.

When labelled reference ROI crops are available (captured via the
engineering GUI's "Capture as SEATED" / "Capture as EMPTY"
buttons), this module replaces the colour-based saturation gate
with a direct pixel-pattern comparison between the current ROI
and every saved reference of each class.

Why this beats saturation for some fleets:

* machine-specific -- captures the exact lighting, slot
  geometry, and sensor response of the unit where the refs were
  collected, so thresholds that work on one machine don't have
  to be re-derived on the next.
* colour-agnostic -- works for a white-capped tube that would
  look identical to empty white plastic under pure saturation.
* extensible without retraining -- new cap styles only need a
  fresh SEATED capture; nothing else has to change.

Algorithm (`score_match`):

1. Take the current ROI crop and every reference crop of the
   same class. References are expected to be the same pixel
   size as the ROI -- enforced at save time by capturing with
   the current ROI config.
2. For every reference, slide a small search window of
   ``search_px`` pixels in each direction via
   ``cv2.matchTemplate`` with ``TM_CCOEFF_NORMED``. This absorbs
   ~5 px of tube-position jitter inside the slot.
3. Keep the max NCC score for that reference. The class score
   is the max across all refs of that class.

Classification:

* ``seated_score`` is the max NCC against all SEATED refs.
* ``empty_score`` is the max NCC against all EMPTY refs.
* Predict seated iff ``seated_score > empty_score`` AND
  ``seated_score >= min_score``. The ``min_score`` floor rejects
  anomalous frames (camera covered, ROI drifted off the slot)
  where neither class matches well.

On-disk layout (``checks.tube.templates.dir``):

    tube_refs/
      seated_1737920001.png
      seated_1737920015.png
      empty_1737920130.png
      ...

Prefix is the label, suffix is capture time. Any file that
doesn't decode, has wrong dimensions, or uses an unknown prefix
is skipped at load time with a warning.
'''
from __future__ import annotations

import logging
import os
import os.path as op
import time
from dataclasses import dataclass

import cv2
import numpy as np

LOG = logging.getLogger(__name__)

LABEL_SEATED = 'seated'
LABEL_EMPTY = 'empty'
_VALID_LABELS = (LABEL_SEATED, LABEL_EMPTY)


@dataclass
class TemplateMatchResult:
    '''One frame's scores against the full reference set.

    Both scores are normalized cross-correlation, bounded in
    ``[-1.0, 1.0]``; higher = better. ``seated_count`` and
    ``empty_count`` record how many refs the score was computed
    over, so the GUI can warn ("only 1 seated ref, capture more").
    '''
    seated_score: float
    empty_score: float
    seated_count: int
    empty_count: int
    # Filename of the best-matching reference (the one that won
    # the class score), useful for debugging "which capture did
    # this match against".
    seated_best_ref: str | None = None
    empty_best_ref: str | None = None


def ensure_refs_dir(refs_dir: str) -> str:
    '''Create *refs_dir* if missing and return the absolute path.'''
    abs_dir = op.abspath(refs_dir)
    os.makedirs(abs_dir, exist_ok=True)
    return abs_dir


def save_reference(
    refs_dir: str,
    label: str,
    roi_crop_bgr: np.ndarray,
    *,
    ts: float | None = None,
) -> str:
    '''Write *roi_crop_bgr* to ``{refs_dir}/{label}_{ts}.png``.

    Args:
        refs_dir: Directory to write into (created if missing).
        label: ``'seated'`` or ``'empty'``. Anything else raises.
        roi_crop_bgr: Exactly the current-ROI crop. Callers are
            responsible for cropping before calling; mixing ROI
            sizes breaks template matching at load time.
        ts: Optional epoch seconds for the filename. Defaults to
            ``time.time()``.

    Returns:
        The absolute path of the saved PNG.
    '''
    if label not in _VALID_LABELS:
        raise ValueError(
            f'invalid label {label!r}; expected one of '
            f'{_VALID_LABELS}',
        )
    if roi_crop_bgr is None or roi_crop_bgr.size == 0:
        raise ValueError('empty ROI crop; nothing to save')
    abs_dir = ensure_refs_dir(refs_dir)
    t = ts if ts is not None else time.time()
    # Integer epoch avoids filesystem / URL quoting hazards.
    fname = f'{label}_{int(t * 1000)}.png'
    path = op.join(abs_dir, fname)
    ok = cv2.imwrite(path, roi_crop_bgr)
    if not ok:
        raise OSError(f'cv2.imwrite failed for {path}')
    LOG.info(
        'tube_template: saved %s reference %s (%dx%d)',
        label, fname, roi_crop_bgr.shape[1], roi_crop_bgr.shape[0],
    )
    return path


def delete_reference(refs_dir: str, filename: str) -> bool:
    '''Remove one saved reference; returns True on success.

    The filename is constrained to characters that prefix/suffix
    our naming scheme: no path separators, no parent-directory
    escapes. Invalid names raise :class:`ValueError`.
    '''
    if (
        not filename
        or '/' in filename
        or '\\' in filename
        or '..' in filename
    ):
        raise ValueError(f'invalid reference filename {filename!r}')
    abs_dir = ensure_refs_dir(refs_dir)
    path = op.join(abs_dir, filename)
    if not op.isfile(path):
        return False
    os.remove(path)
    LOG.info('tube_template: removed reference %s', filename)
    return True


def list_references(refs_dir: str) -> list[dict]:
    '''Return metadata for every reference on disk.

    Each entry: ``{filename, label, ts_ms, path, width, height}``.
    Unreadable files and unknown labels are skipped (logged).
    '''
    abs_dir = ensure_refs_dir(refs_dir)
    out: list[dict] = []
    for name in sorted(os.listdir(abs_dir)):
        path = op.join(abs_dir, name)
        if not op.isfile(path):
            continue
        if not name.lower().endswith('.png'):
            continue
        label, _, rest = name.partition('_')
        if label not in _VALID_LABELS:
            LOG.debug(
                'tube_template: skip %s (unknown label prefix)',
                name,
            )
            continue
        try:
            ts_ms = int(op.splitext(rest)[0])
        except ValueError:
            ts_ms = 0
        # Probe dims without holding the full image in memory.
        img = cv2.imread(path, cv2.IMREAD_COLOR)
        if img is None:
            LOG.warning('tube_template: cannot decode %s', name)
            continue
        h, w = img.shape[:2]
        out.append({
            'filename': name,
            'label': label,
            'ts_ms': ts_ms,
            'path': path,
            'width': int(w),
            'height': int(h),
        })
    return out


def load_references(
    refs_dir: str,
    required_size: tuple[int, int] | None = None,
) -> dict[str, list[tuple[str, np.ndarray]]]:
    '''Load every reference ROI crop from *refs_dir*.

    Args:
        refs_dir: Directory holding the PNG refs.
        required_size: Optional ``(w, h)``. Refs of a different
            size are skipped with a warning -- this is how the
            system recovers from a changed ROI without crashing
            (old refs no longer match the new crop geometry).

    Returns:
        ``{'seated': [(filename, img_bgr), ...], 'empty': [...]}``.
    '''
    out: dict[str, list[tuple[str, np.ndarray]]] = {
        LABEL_SEATED: [],
        LABEL_EMPTY: [],
    }
    for meta in list_references(refs_dir):
        img = cv2.imread(meta['path'], cv2.IMREAD_COLOR)
        if img is None:
            continue
        if required_size is not None:
            w, h = required_size
            if img.shape[1] != w or img.shape[0] != h:
                LOG.warning(
                    'tube_template: skip %s (size %dx%d != %dx%d)',
                    meta['filename'], img.shape[1], img.shape[0],
                    w, h,
                )
                continue
        out[meta['label']].append((meta['filename'], img))
    return out


def _score_single(
    roi_crop: np.ndarray,
    ref: np.ndarray,
    search_px: int,
) -> float:
    '''Best NCC between *roi_crop* and *ref* inside a search window.

    Implementation note: ``cv2.matchTemplate`` expects the
    template to be smaller than (or equal to) the image. We pad
    *roi_crop* by ``search_px`` on each side with reflection so
    the ref can be positioned ± ``search_px`` relative to its
    nominal location. Returns the max NCC observed.
    '''
    if ref.shape != roi_crop.shape:
        # Size mismatch: refuse to score rather than silently
        # resize (would mask a real ROI-drift bug).
        return float('-inf')
    if search_px <= 0:
        result = cv2.matchTemplate(
            roi_crop, ref, cv2.TM_CCOEFF_NORMED,
        )
        return float(result.max())
    padded = cv2.copyMakeBorder(
        roi_crop,
        search_px, search_px, search_px, search_px,
        cv2.BORDER_REFLECT_101,
    )
    result = cv2.matchTemplate(
        padded, ref, cv2.TM_CCOEFF_NORMED,
    )
    return float(result.max())


def score_match(
    roi_crop_bgr: np.ndarray,
    refs: dict[str, list[tuple[str, np.ndarray]]],
    *,
    search_px: int = 5,
) -> TemplateMatchResult:
    '''Compute class scores for one ROI crop.

    Runs :func:`_score_single` against every reference in both
    classes; the class score is the best match across the set.
    '''
    best_seated = float('-inf')
    best_empty = float('-inf')
    best_seated_name: str | None = None
    best_empty_name: str | None = None

    for fname, ref in refs.get(LABEL_SEATED, []):
        s = _score_single(roi_crop_bgr, ref, search_px)
        if s > best_seated:
            best_seated = s
            best_seated_name = fname
    for fname, ref in refs.get(LABEL_EMPTY, []):
        s = _score_single(roi_crop_bgr, ref, search_px)
        if s > best_empty:
            best_empty = s
            best_empty_name = fname

    # Normalize -inf to NaN-ish zero for JSON sanity. Callers
    # check the counts to distinguish "no refs" from "no match".
    if best_seated == float('-inf'):
        best_seated = 0.0
    if best_empty == float('-inf'):
        best_empty = 0.0

    return TemplateMatchResult(
        seated_score=best_seated,
        empty_score=best_empty,
        seated_count=len(refs.get(LABEL_SEATED, [])),
        empty_count=len(refs.get(LABEL_EMPTY, [])),
        seated_best_ref=best_seated_name,
        empty_best_ref=best_empty_name,
    )


def classify(
    match: TemplateMatchResult,
    *,
    min_score: float = 0.5,
    margin: float = 0.0,
) -> tuple[bool, str | None]:
    '''Turn scores into a present/absent verdict + reason.

    Args:
        match: Output of :func:`score_match`.
        min_score: The winning class score must meet this floor,
            otherwise the frame is considered ambiguous (returns
            ``absent`` with ``reason='template_low_confidence'``).
        margin: Optional gap ``seated_score - empty_score`` must
            exceed for a SEATED verdict. Useful when both classes
            score high (e.g. a partially-seated tube).

    Returns:
        ``(present, reason)``. ``reason`` is ``None`` on success.
    '''
    if match.seated_count == 0 or match.empty_count == 0:
        return (
            False,
            'template_insufficient_refs '
            f'(seated={match.seated_count}, '
            f'empty={match.empty_count})',
        )
    winning = max(match.seated_score, match.empty_score)
    if winning < min_score:
        return (
            False,
            'template_low_confidence '
            f'(best={winning:.3f} < {min_score:.3f})',
        )
    if match.seated_score >= match.empty_score + margin:
        return True, None
    return (
        False,
        'template_empty_match '
        f'(empty={match.empty_score:.3f} >= '
        f'seated={match.seated_score:.3f}+{margin:.2f})',
    )
