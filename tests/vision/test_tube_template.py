'''Unit tests for :mod:`ultra.vision.tube_template`.

Uses synthetic BGR patches written to a temporary directory so
every round-trip (save, list, load, score, classify) runs against
real files -- no mocks. Requires cv2; skips cleanly if absent.
'''
from __future__ import annotations

import pytest

cv2 = pytest.importorskip('cv2')
np = pytest.importorskip('numpy')

from ultra.vision import tube_template as tt  # noqa: E402


def _coloured_patch(
    bgr: tuple[int, int, int], shape: tuple[int, int] = (60, 80),
) -> np.ndarray:
    '''Uniform BGR patch at *bgr*.'''
    h, w = shape
    out = np.zeros((h, w, 3), dtype=np.uint8)
    out[:] = bgr
    return out


def _gray_patch(
    value: int, shape: tuple[int, int] = (60, 80),
) -> np.ndarray:
    h, w = shape
    return np.full((h, w, 3), value, dtype=np.uint8)


# ---------------------------------------------------------------
# save / list / load round-trip
# ---------------------------------------------------------------

def test_save_and_list_round_trip(tmp_path):
    ref_dir = str(tmp_path)
    p_seated = tt.save_reference(
        ref_dir, 'seated', _coloured_patch((200, 50, 50)),
    )
    p_empty = tt.save_reference(
        ref_dir, 'empty', _gray_patch(220),
    )
    assert p_seated.endswith('.png')
    assert p_empty.endswith('.png')

    items = tt.list_references(ref_dir)
    labels = sorted(it['label'] for it in items)
    assert labels == ['empty', 'seated']
    for it in items:
        assert it['width'] == 80 and it['height'] == 60


def test_invalid_label_raises(tmp_path):
    with pytest.raises(ValueError):
        tt.save_reference(
            str(tmp_path), 'garbage', _gray_patch(100),
        )


def test_empty_crop_raises(tmp_path):
    with pytest.raises(ValueError):
        tt.save_reference(
            str(tmp_path), 'seated',
            np.zeros((0, 0, 3), dtype=np.uint8),
        )


def test_load_filters_mismatched_size(tmp_path, caplog):
    # Save a 60x80 ref, then request load with a 40x40 size.
    tt.save_reference(
        str(tmp_path), 'seated', _coloured_patch((200, 50, 50)),
    )
    refs = tt.load_references(
        str(tmp_path), required_size=(40, 40),
    )
    assert refs['seated'] == []
    assert refs['empty'] == []


def test_load_accepts_matching_size(tmp_path):
    tt.save_reference(
        str(tmp_path), 'seated', _coloured_patch((200, 50, 50)),
    )
    refs = tt.load_references(
        str(tmp_path), required_size=(80, 60),
    )
    assert len(refs['seated']) == 1
    assert len(refs['empty']) == 0


def test_delete_reference_round_trip(tmp_path):
    path = tt.save_reference(
        str(tmp_path), 'empty', _gray_patch(240),
    )
    import os.path as op
    fname = op.basename(path)
    assert tt.delete_reference(str(tmp_path), fname) is True
    # Deleting again returns False (not an error).
    assert tt.delete_reference(str(tmp_path), fname) is False


def test_delete_rejects_path_traversal(tmp_path):
    with pytest.raises(ValueError):
        tt.delete_reference(str(tmp_path), '../etc/passwd')
    with pytest.raises(ValueError):
        tt.delete_reference(str(tmp_path), 'sub/bad.png')


# ---------------------------------------------------------------
# scoring
# ---------------------------------------------------------------

def test_score_match_prefers_right_class(tmp_path):
    ref_dir = str(tmp_path)
    # Two seated refs (strong blue), one empty ref (near-white).
    tt.save_reference(
        ref_dir, 'seated', _coloured_patch((220, 50, 50)),
    )
    tt.save_reference(
        ref_dir, 'seated', _coloured_patch((200, 60, 60)),
    )
    tt.save_reference(ref_dir, 'empty', _gray_patch(210))

    refs = tt.load_references(ref_dir, required_size=(80, 60))

    # Current frame: strong blue -> should score high against
    # seated refs, low against empty ref.
    match = tt.score_match(
        _coloured_patch((215, 55, 55)), refs, search_px=2,
    )
    assert match.seated_count == 2
    assert match.empty_count == 1
    assert match.seated_score > match.empty_score
    assert match.seated_best_ref is not None

    present, reason = tt.classify(match, min_score=0.5)
    assert present is True
    assert reason is None


def test_score_match_rejects_ambiguous(tmp_path):
    ref_dir = str(tmp_path)
    tt.save_reference(
        ref_dir, 'seated', _coloured_patch((220, 50, 50)),
    )
    tt.save_reference(ref_dir, 'empty', _gray_patch(200))
    refs = tt.load_references(ref_dir, required_size=(80, 60))

    # Random-ish patch that matches neither reference well.
    # NCC against both classes will be low.
    rng = np.random.default_rng(0)
    random_patch = rng.integers(
        0, 256, size=(60, 80, 3), dtype=np.uint8,
    )
    match = tt.score_match(random_patch, refs, search_px=0)
    present, reason = tt.classify(match, min_score=0.9)
    # With a strict floor of 0.9 the verdict must reject.
    assert present is False
    assert reason and 'low_confidence' in reason


def test_classify_needs_both_classes(tmp_path):
    # Only seated refs: classifier refuses to predict.
    match = tt.TemplateMatchResult(
        seated_score=0.9, empty_score=0.0,
        seated_count=2, empty_count=0,
    )
    present, reason = tt.classify(match, min_score=0.5)
    assert present is False
    assert reason and 'insufficient_refs' in reason


def test_classify_margin_enforced():
    # seated barely beats empty -> margin of 0.1 rejects,
    # margin of 0.01 accepts.
    match = tt.TemplateMatchResult(
        seated_score=0.80, empty_score=0.78,
        seated_count=1, empty_count=1,
    )
    present, _ = tt.classify(match, min_score=0.5, margin=0.10)
    assert present is False
    present, _ = tt.classify(match, min_score=0.5, margin=0.01)
    assert present is True


def test_score_mismatched_ref_size_is_safe():
    # A ref whose shape differs from the crop must not crash
    # matchTemplate -- _score_single should short-circuit.
    crop = _coloured_patch((200, 50, 50), shape=(60, 80))
    mismatched = _coloured_patch((200, 50, 50), shape=(40, 40))
    refs = {
        tt.LABEL_SEATED: [('m.png', mismatched)],
        tt.LABEL_EMPTY: [],
    }
    match = tt.score_match(crop, refs, search_px=0)
    # Mismatched ref treated as no match; empty class empty.
    assert match.seated_score == 0.0
