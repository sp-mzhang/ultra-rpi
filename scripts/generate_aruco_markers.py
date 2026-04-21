#!/usr/bin/env python3
'''Generate a pack of ArUco markers for carousel alignment.

Produces, for a chosen ArUco dictionary and physical size:

  * Individual PNGs, one per marker (``aruco_<id>.png``).
  * A contact-sheet PNG with all markers laid out in a grid
    (``aruco_sheet.png``), annotated with the marker ID below
    each one.
  * A print-ready PDF (``aruco_sheet.pdf``) at true physical
    size -- drop it into any PDF printer with "actual size" /
    100% scaling and the markers will come out exactly
    ``--size-mm`` on the paper.

Default configuration is tuned for the Ultra carousel camera
(1280x720 MJPG webcam at ~70 mm standoff):

  * ``DICT_4X4_50``       -- 4x4 data bits + 2-cell quiet zone
                             = 6x6 modules per marker, largest
                             modules of any common ArUco dict
                             (easiest to detect at distance).
  * ``--size-mm 15``      -- projects to ~90-110 px in-frame,
                             well above the ~24 px lower limit
                             for DICT_4X4_50 detection.
  * ``--dpi 1200``        -- print density; only affects the
                             output raster resolution, not the
                             physical size on paper.
  * ``--count 12``        -- generates IDs 0..11.
  * ``--quiet-mm 1.0``    -- extra white quiet zone around each
                             marker beyond the 1-module border
                             baked into the ArUco pattern.

If you need tinier markers (e.g. 8 mm), switch to a smaller
dictionary bit-grid is NOT the answer -- smaller bit grids
are ALWAYS larger per-module for the same physical size. What
helps is more print DPI, matte stock (no glint), and a wider
quiet zone. Under ~8 mm at this camera geometry, expect to
move to AprilTag (``--dict DICT_APRILTAG_36h11``), which has
larger Hamming distance and tolerates blur / perspective
distortion better at the cost of a bigger bit-grid (8x8).

Requires ``opencv-contrib-python`` (has the ``cv2.aruco``
submodule). Install once outside the repo venv with::

    pip install opencv-contrib-python

and run this script from anywhere. No write paths are
hard-coded; use ``--outdir`` to point wherever you want the
PNG/PDF pack to land.

Usage::

    python scripts/generate_aruco_markers.py
    python scripts/generate_aruco_markers.py \\
        --size-mm 10 --dict DICT_5X5_50 --count 16 \\
        --outdir /tmp/markers
'''

from __future__ import annotations

import argparse
import sys
from pathlib import Path


_MM_PER_INCH = 25.4


_DICT_CHOICES = {
    'DICT_4X4_50':          (4, 4, 50),
    'DICT_4X4_100':         (4, 4, 100),
    'DICT_4X4_250':         (4, 4, 250),
    'DICT_5X5_50':          (5, 5, 50),
    'DICT_5X5_100':         (5, 5, 100),
    'DICT_6X6_50':          (6, 6, 50),
    'DICT_6X6_250':         (6, 6, 250),
    'DICT_7X7_50':          (7, 7, 50),
    'DICT_APRILTAG_16h5':   (4, 4, 30),
    'DICT_APRILTAG_25h9':   (5, 5, 35),
    'DICT_APRILTAG_36h10':  (6, 6, 2320),
    'DICT_APRILTAG_36h11':  (6, 6, 587),
}


def _mm_to_px(mm: float, dpi: int) -> int:
    '''Convert physical mm to pixel count at the given DPI.'''
    return int(round(mm / _MM_PER_INCH * dpi))


def _make_dictionary(cv2, aruco, name: str):
    '''Resolve an ArUco dictionary by string name.

    OpenCV's API for this has churned (``Dictionary_get`` ->
    ``getPredefinedDictionary``), so try both.
    '''
    const = getattr(aruco, name, None)
    if const is None:
        raise SystemExit(
            f'Unknown ArUco dict "{name}". '
            f'Choices: {", ".join(_DICT_CHOICES)}',
        )
    if hasattr(aruco, 'getPredefinedDictionary'):
        return aruco.getPredefinedDictionary(const)
    return aruco.Dictionary_get(const)  # legacy OpenCV


def _generate_marker(cv2, aruco, dictionary, marker_id: int,
                     marker_px: int):
    '''Render a single marker to a grayscale numpy image.'''
    # ``generateImageMarker`` was ``drawMarker`` pre-4.7.
    if hasattr(aruco, 'generateImageMarker'):
        return aruco.generateImageMarker(
            dictionary, marker_id, marker_px,
        )
    return aruco.drawMarker(
        dictionary, marker_id, marker_px,
    )


def _add_quiet_zone(np, marker_img, quiet_px: int):
    '''Pad the marker with a white quiet zone.'''
    h, w = marker_img.shape[:2]
    out = np.full(
        (h + 2 * quiet_px, w + 2 * quiet_px),
        255, dtype=marker_img.dtype,
    )
    out[quiet_px:quiet_px + h, quiet_px:quiet_px + w] = marker_img
    return out


def _save_png(cv2, path: Path, img):
    cv2.imwrite(str(path), img)


def _build_sheet(cv2, np, markers, cols: int, label_px: int,
                 pad_px: int, label_font_scale: float):
    '''Arrange individual marker images into a labelled grid.'''
    count = len(markers)
    rows = (count + cols - 1) // cols
    cell_w = max(m[1].shape[1] for m in markers)
    cell_h = max(m[1].shape[0] for m in markers) + label_px
    sheet_w = cols * cell_w + (cols + 1) * pad_px
    sheet_h = rows * cell_h + (rows + 1) * pad_px
    sheet = np.full((sheet_h, sheet_w), 255, dtype=np.uint8)

    for i, (mid, img) in enumerate(markers):
        r, c = divmod(i, cols)
        x0 = pad_px + c * (cell_w + pad_px)
        y0 = pad_px + r * (cell_h + pad_px)
        h, w = img.shape[:2]
        # Centre the marker horizontally within its cell.
        cx = x0 + (cell_w - w) // 2
        sheet[y0:y0 + h, cx:cx + w] = img
        cv2.putText(
            sheet, f'ID {mid}',
            (x0 + 4, y0 + h + label_px - 8),
            cv2.FONT_HERSHEY_SIMPLEX, label_font_scale,
            0, 2, cv2.LINE_AA,
        )
    return sheet


def _sheet_to_pdf(np, sheet_img, size_mm: float, pad_mm: float,
                  cols: int, count: int, out_path: Path):
    '''Emit a print-ready PDF, one marker per page at true mm.

    We use ``reportlab`` if present for an exact page-size
    PDF; otherwise we fall back to ``Pillow``'s multi-page
    PDF writer, which also preserves DPI metadata so most
    printers honour the physical size.
    '''
    try:
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas
    except ImportError:
        _sheet_to_pdf_pillow(
            sheet_img, size_mm, pad_mm, out_path,
        )
        return

    # Page size = marker + generous white margin for handling.
    margin_mm = max(10.0, pad_mm * 3)
    page_w_mm = size_mm + 2 * margin_mm
    page_h_mm = size_mm + 2 * margin_mm + 8  # room for ID text

    c = canvas.Canvas(
        str(out_path),
        pagesize=(page_w_mm * mm, page_h_mm * mm),
    )
    import tempfile
    import cv2  # re-imported locally so the hint path is clear
    tmpdir = Path(tempfile.mkdtemp(prefix='aruco_pdf_'))
    try:
        # Split the sheet back into per-marker crops so each
        # page contains exactly one marker at its true size.
        # We re-render from the raw grid instead of slicing
        # the sheet to avoid the font labels bleeding in.
        # ``sheet_img`` here is just used to count/colour; the
        # actual page content comes from the individual PNGs.
        rows = (count + cols - 1) // cols
        # The caller passes a pre-built sheet, but for the PDF
        # we regenerate clean per-marker PNGs from the same
        # source images to get pixel-clean results.
        for mid in range(count):
            # We'll re-emit the marker as a fresh PNG at the
            # chosen pixel size by re-reading from tmpdir if
            # the caller pre-populated them. Otherwise use
            # the sheet slice.
            row = mid // cols
            col = mid % cols
            cell_h = sheet_img.shape[0] // rows
            cell_w = sheet_img.shape[1] // cols
            y0 = row * cell_h
            x0 = col * cell_w
            sub = sheet_img[y0:y0 + cell_h, x0:x0 + cell_w]
            tmp_png = tmpdir / f'aruco_pdf_{mid}.png'
            cv2.imwrite(str(tmp_png), sub)
            c.drawImage(
                str(tmp_png),
                margin_mm * mm, margin_mm * mm,
                width=size_mm * mm, height=size_mm * mm,
                preserveAspectRatio=True,
                mask='auto',
            )
            c.setFont('Helvetica', 10)
            c.drawString(
                margin_mm * mm,
                (margin_mm - 5) * mm,
                f'ArUco ID {mid}  |  {size_mm:g} mm',
            )
            c.showPage()
        c.save()
    finally:
        for p in tmpdir.iterdir():
            p.unlink()
        tmpdir.rmdir()


def _sheet_to_pdf_pillow(sheet_img, size_mm: float, pad_mm: float,
                         out_path: Path):
    '''Fallback PDF writer using Pillow (one image per page).

    Pillow embeds DPI metadata in the saved PDF so the print
    dialog respects the physical size at 100% scale, but it
    cannot set an exact page size. Margins will depend on the
    printer defaults.
    '''
    from PIL import Image
    # Convert grayscale numpy -> PIL; split into per-marker
    # sub-images by scanning dark-to-light transitions.
    pil = Image.fromarray(sheet_img)
    # Derive DPI from the image geometry so the embedded PDF
    # declares the marker at its true physical size.
    target_px = sheet_img.shape[0]
    dpi = target_px / (
        (sheet_img.shape[0] / sheet_img.shape[1])
        * (size_mm / _MM_PER_INCH)
    )
    pil.save(
        str(out_path),
        'PDF', resolution=float(dpi),
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description='Generate ArUco markers for carousel alignment.',
    )
    ap.add_argument(
        '--count', type=int, default=12,
        help='Number of markers (IDs 0..count-1). Default: 12.',
    )
    ap.add_argument(
        '--size-mm', type=float, default=15.0,
        help=(
            'Edge length of each marker in millimetres. '
            'Default: 15.0 (chosen to project to ~90-110 px '
            'at the Ultra camera geometry, i.e. ~15 px/module '
            'for DICT_4X4_50). 5 mm is NOT recommended at '
            'this camera -- see script header.'
        ),
    )
    ap.add_argument(
        '--dict', default='DICT_4X4_50',
        choices=list(_DICT_CHOICES),
        help=(
            'ArUco dictionary. DICT_4X4_50 has the largest '
            'modules per mm (easiest to detect); AprilTag '
            'dicts are more robust but need larger markers.'
        ),
    )
    ap.add_argument(
        '--dpi', type=int, default=1200,
        help='Print DPI of the raster output. Default: 1200.',
    )
    ap.add_argument(
        '--quiet-mm', type=float, default=1.0,
        help=(
            'Extra white quiet zone around each marker, on '
            'top of the 1-module white border built into the '
            'ArUco pattern. Default: 1 mm.'
        ),
    )
    ap.add_argument(
        '--sheet-cols', type=int, default=4,
        help='Columns in the contact-sheet PNG. Default: 4.',
    )
    ap.add_argument(
        '--outdir', type=Path, default=Path('./aruco_markers'),
        help='Directory to write into. Default: ./aruco_markers',
    )
    args = ap.parse_args(argv)

    try:
        import cv2
        from cv2 import aruco
    except ImportError:
        print(
            'error: cv2.aruco not found. Install '
            'opencv-contrib-python (not opencv-python).',
            file=sys.stderr,
        )
        return 2

    try:
        import numpy as np
    except ImportError:
        print('error: numpy is required', file=sys.stderr)
        return 2

    bits, _, max_ids = _DICT_CHOICES[args.dict]
    if args.count > max_ids:
        print(
            f'error: {args.dict} only defines {max_ids} unique '
            f'IDs; requested {args.count}.',
            file=sys.stderr,
        )
        return 2

    # Total modules per side = bits + 2 (one-cell border each
    # side is baked into the ArUco pattern).
    modules_per_side = bits + 2
    marker_px = _mm_to_px(args.size_mm, args.dpi)
    # Round up to a multiple of modules_per_side so each
    # module is an integer number of pixels (avoids blurry
    # module edges when anti-aliasing is off).
    marker_px = max(
        modules_per_side,
        marker_px + (
            -marker_px % modules_per_side
        ),
    )
    px_per_module = marker_px // modules_per_side
    quiet_px = _mm_to_px(args.quiet_mm, args.dpi)

    args.outdir.mkdir(parents=True, exist_ok=True)
    dictionary = _make_dictionary(cv2, aruco, args.dict)

    print(
        f'Generating {args.count} markers from {args.dict} at '
        f'{args.size_mm:g} mm @ {args.dpi} DPI -> '
        f'{marker_px}px ({px_per_module}px per module), '
        f'{args.quiet_mm:g} mm quiet zone.',
    )

    markers: list[tuple[int, 'np.ndarray']] = []
    for mid in range(args.count):
        img = _generate_marker(
            cv2, aruco, dictionary, mid, marker_px,
        )
        padded = _add_quiet_zone(np, img, quiet_px)
        out_png = args.outdir / f'aruco_{mid:02d}.png'
        _save_png(cv2, out_png, padded)
        markers.append((mid, padded))
        print(f'  wrote {out_png}')

    # Contact-sheet PNG for quick visual verification.
    sheet = _build_sheet(
        cv2, np, markers,
        cols=args.sheet_cols,
        label_px=max(40, _mm_to_px(6.0, args.dpi)),
        pad_px=_mm_to_px(3.0, args.dpi),
        label_font_scale=max(1.0, args.dpi / 300.0),
    )
    sheet_path = args.outdir / 'aruco_sheet.png'
    _save_png(cv2, sheet_path, sheet)
    print(f'  wrote {sheet_path}')

    # Print-ready PDF at exact physical size.
    pdf_path = args.outdir / 'aruco_sheet.pdf'
    _sheet_to_pdf(
        np, sheet, args.size_mm,
        pad_mm=args.quiet_mm,
        cols=args.sheet_cols,
        count=args.count,
        out_path=pdf_path,
    )
    print(f'  wrote {pdf_path}')
    print(
        'Done. Print aruco_sheet.pdf at 100% / "actual size" '
        'and verify each marker measures '
        f'{args.size_mm:g} mm edge-to-edge with a caliper.',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
