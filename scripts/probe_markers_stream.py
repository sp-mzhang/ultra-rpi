#!/usr/bin/env python3
"""scripts/probe_markers_stream.py

Live carousel-angle bring-up viewer.

Opens the on-board USB camera, runs pylibdmtx on every frame,
fuses the per-marker orientations into a single carousel angle,
and serves the annotated stream as MJPEG on
``http://<host>:<port>/`` so you can watch it from any browser
(no GUI / X11 required on the Pi).

The HUD overlay shows:

  - One green outline + payload label per detected marker
  - A red dot at each marker's center, with a blue arrow showing
    its in-plane orientation
  - The fused carousel angle (mean of per-marker orientations,
    unwrapped to a continuous frame), in the top-left corner
  - Frame number, decode count, and decode latency

Usage:

    ./scripts/probe_markers_stream.sh                  # /dev/video0
    ./scripts/probe_markers_stream.sh --device /dev/video1
    ./scripts/probe_markers_stream.sh --port 8765
    ./scripts/probe_markers_stream.sh --width 1280 --height 720
    ./scripts/probe_markers_stream.sh --record /tmp/cam_log

  --record DIR    save one annotated PNG per second to DIR for
                  offline review (capped at 600 frames; rolls over)

Stop with Ctrl-C.

Open in a browser:

    http://<rpi-host>:8765/

Or fetch a single still:

    curl -o frame.jpg http://<rpi-host>:8765/snapshot
"""
from __future__ import annotations

import argparse
import http.server
import io
import logging
import math
import os
import re
import signal
import socketserver
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

try:
    import numpy as np
except ImportError as err:
    print(f"ERROR: numpy not installed: {err}", file=sys.stderr)
    sys.exit(2)

try:
    import cv2
except ImportError as err:
    print(
        "ERROR: cv2 not installed (apt install python3-opencv "
        "or pip install opencv-python-headless): "
        f"{err}",
        file=sys.stderr,
    )
    sys.exit(2)

try:
    from pylibdmtx import pylibdmtx
except ImportError as err:
    print(
        "ERROR: pylibdmtx not installed -- run "
        f"./scripts/probe_markers_setup.sh first ({err})",
        file=sys.stderr,
    )
    sys.exit(2)

LOG = logging.getLogger('probe_stream')

# Decode every Nth frame -- pylibdmtx is CPU-heavy on the Pi
# (~150 ms per call at 720p). Annotated frames between decodes
# reuse the last marker set so the live view stays smooth.
DEFAULT_DECODE_EVERY = 3

# pylibdmtx scan window (ms). Lower = faster, less robust.
# Defaults; both can be overridden from the CLI.
DEFAULT_DECODE_TIMEOUT_MS = 1200
DEFAULT_MIN_MARKER_PX = 18
DEFAULT_MIN_PAYLOAD_LEN = 2


def _grab_devices() -> list[str]:
    import glob
    return sorted(glob.glob('/dev/video[0-9]*'))


_VIDEO_PATH_RE = re.compile(r'^/dev/video(\d+)$')


def _device_to_cv_index(device: str | int) -> int | str:
    """OpenCV's V4L2 backend wants an integer index, not a path
    ('/dev/video0' triggers the warning "backend is generally
    available but can't be used to capture by name"). Convert
    /dev/videoN -> N. Pure numeric strings ('0') -> int. Anything
    else is returned as-is so non-V4L2 backends still work."""
    if isinstance(device, int):
        return device
    s = str(device)
    if s.isdigit():
        return int(s)
    m = _VIDEO_PATH_RE.match(s)
    if m:
        return int(m.group(1))
    return s


def _device_path(device: str | int) -> str:
    """Inverse of _device_to_cv_index, used for `fuser` / log lines."""
    if isinstance(device, int):
        return f'/dev/video{device}'
    s = str(device)
    if s.isdigit():
        return f'/dev/video{s}'
    return s


def _device_holders(device: str) -> list[tuple[int, str]]:
    """Return [(pid, comm), ...] of processes currently holding
    the given /dev/video* node. Empty list if free or fuser is not
    available."""
    if not device.startswith('/dev/'):
        return []
    try:
        out = subprocess.run(
            ['fuser', device],
            capture_output=True, text=True, timeout=2,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return []
    pids = [
        int(p) for p in (out.stdout + ' ' + out.stderr).split()
        if p.isdigit()
    ]
    if not pids:
        return []
    holders: list[tuple[int, str]] = []
    for pid in pids:
        try:
            comm = Path(f'/proc/{pid}/comm').read_text().strip()
        except OSError:
            comm = '?'
        holders.append((pid, comm))
    return holders


def _release_device(
    device: str, holders: list[tuple[int, str]],
) -> None:
    """SIGTERM, then SIGKILL if needed, every process holding
    the device. Caller is responsible for the wait + retry."""
    for pid, comm in holders:
        LOG.warning(
            'sending SIGTERM to pid %d (%s) holding %s',
            pid, comm, device,
        )
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as e:
            LOG.warning('  SIGTERM failed: %s', e)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        time.sleep(0.2)
        if not _device_holders(device):
            return
    for pid, _ in _device_holders(device):
        LOG.warning('  pid %d still holding -- SIGKILL', pid)
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError as e:
            LOG.warning('  SIGKILL failed: %s', e)
    time.sleep(0.3)


def _try_open_one(
    device: str | int, width: int, height: int,
) -> cv2.VideoCapture | None:
    """Open exactly one V4L2 node. Returns the opened cap on
    success, None otherwise. Uses short timeouts so a non-capture
    node (metadata / M2M) fails fast instead of hanging 10s in
    select()."""
    cv_target = _device_to_cv_index(device)
    cap = cv2.VideoCapture(cv_target, cv2.CAP_V4L2)
    if not cap.isOpened():
        return None
    # OpenCV >=4.x: cap a slow device to <2s instead of 10s default.
    # These props are no-ops on unsupported builds, which is fine.
    try:
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 1500)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 1500)
    except Exception:
        pass
    if width:
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
    if height:
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
    # warmup: a non-capture node will return ok=False here
    ok = False
    for _ in range(3):
        ok, _ = cap.read()
        if ok:
            break
    if not ok:
        cap.release()
        return None
    actual_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    actual_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    LOG.info(
        'Camera open: %s (cv_idx=%r) @ %dx%d',
        _device_path(device), cv_target, actual_w, actual_h,
    )
    return cap


def _open_camera(
    device: str,
    width: int,
    height: int,
    scan: bool = False,
    release: bool = False,
) -> cv2.VideoCapture | None:
    """Open the requested V4L2 device. Only falls back to scanning
    every /dev/videoN when scan=True; otherwise an explicit
    --device that fails returns None immediately so the caller can
    print a clear error instead of silently iterating through 24
    metadata nodes.

    If `release` is True and another process is holding the device,
    SIGTERM/SIGKILL it first so we can take over.
    """
    primary_path = _device_path(device)
    holders = _device_holders(primary_path)
    if holders:
        descr = ', '.join(f'{c}({p})' for p, c in holders)
        if release:
            LOG.warning(
                '%s is held by %s -- releasing (--release)',
                primary_path, descr,
            )
            _release_device(primary_path, holders)
        else:
            LOG.error(
                '%s is currently held by %s. '
                'Re-run with --release to free it.',
                primary_path, descr,
            )
            return None

    cap = _try_open_one(device, width, height)
    if cap is not None:
        return cap

    if not scan:
        LOG.error(
            'Could not open %s (not a capture node, busy, or '
            'wrong format). Re-run with --scan to auto-detect, '
            'or check `v4l2-ctl --list-devices`.',
            primary_path,
        )
        return None

    LOG.warning(
        '%s did not yield frames -- scanning all /dev/video* '
        '(this can take a few seconds on RPi)', primary_path,
    )
    for d in _grab_devices():
        if d == primary_path:
            continue
        cap = _try_open_one(d, width, height)
        if cap is not None:
            return cap
    return None


def _decode_markers(
    frame_bgr: np.ndarray,
    timeout_ms: int = DEFAULT_DECODE_TIMEOUT_MS,
    min_marker_px: int = DEFAULT_MIN_MARKER_PX,
    min_payload_len: int = DEFAULT_MIN_PAYLOAD_LEN,
) -> tuple[list[dict], list[dict]]:
    """Run pylibdmtx and return (kept, rejected) marker dicts.

    `rejected` contains everything libdmtx returned that we filtered
    out (too small or payload too short). The streamer can draw
    these in a different colour so we can tell whether libdmtx is
    finding nothing vs finding noise we're throwing away."""
    h = frame_bgr.shape[0]
    rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    raw = pylibdmtx.decode(
        rgb, max_count=8, timeout=timeout_ms,
    )
    kept: list[dict] = []
    rejected: list[dict] = []
    for det in raw:
        r = det.rect
        left, bottom_from_bot, w, hh = (
            r.left, r.top, r.width, r.height,
        )
        payload = det.data.decode('utf-8', errors='replace')
        top = h - bottom_from_bot - hh
        bl = (left, top + hh)
        br = (left + w, top + hh)
        tr = (left + w, top)
        tl = (left, top)
        cx = (bl[0] + br[0] + tr[0] + tl[0]) / 4.0
        cy = (bl[1] + br[1] + tr[1] + tl[1]) / 4.0
        dx, dy = br[0] - bl[0], br[1] - bl[1]
        ori = math.degrees(math.atan2(dy, dx))
        marker = {
            'payload': payload,
            'corners_px': [bl, br, tr, tl],
            'center_px': (cx, cy),
            'orientation_deg': ori,
            'size_px': (w, hh),
        }
        if w < min_marker_px or hh < min_marker_px:
            marker['reject_reason'] = f'size<{min_marker_px}px'
            rejected.append(marker)
        elif len(payload.strip()) < min_payload_len:
            marker['reject_reason'] = (
                f'payload<{min_payload_len}ch'
            )
            rejected.append(marker)
        else:
            kept.append(marker)
    return kept, rejected


def _wrap180(deg: float) -> float:
    """Wrap to (-180, 180]."""
    while deg > 180.0:
        deg -= 360.0
    while deg <= -180.0:
        deg += 360.0
    return deg


def _fuse_carousel_angle(markers: list[dict]) -> float | None:
    """Return one carousel angle in degrees, or None if no
    markers. Uses the mean of per-marker orientations, unwrapped
    via complex-exponential averaging so wrap-around at +/-180
    doesn't bias the result.

    NOTE: this is an absolute angle in the camera frame, NOT in
    the carousel's own coordinate system. The full alignment math
    (subtract a per-marker theta_ref from a saved reference) is
    in carousel_angle.py once the vision module lands. This
    overlay just gives the operator a stable readout to confirm
    the markers track 1:1 with the carousel in real time.
    """
    if not markers:
        return None
    vecs = [
        complex(
            math.cos(math.radians(m['orientation_deg'])),
            math.sin(math.radians(m['orientation_deg'])),
        )
        for m in markers
    ]
    mean = sum(vecs) / len(vecs)
    return _wrap180(math.degrees(math.atan2(mean.imag, mean.real)))


def _annotate(
    frame_bgr: np.ndarray,
    markers: list[dict],
    carousel_deg: float | None,
    frame_no: int,
    decode_ms: float,
    decode_age: int,
    rejected: list[dict] | None = None,
) -> np.ndarray:
    """Draw HUD onto a copy of the frame and return it.

    `rejected` (when supplied, e.g. via --debug-decode) is drawn
    with a thin orange dashed-look outline so we can see what
    libdmtx found but our filter dropped."""
    out = frame_bgr.copy()
    h, w = out.shape[:2]

    if rejected:
        for j, m in enumerate(rejected):
            pts = np.array(
                m['corners_px'], dtype=np.int32,
            ).reshape(-1, 1, 2)
            cv2.polylines(
                out, [pts], isClosed=True,
                color=(0, 140, 255), thickness=1,
            )
            cx, cy = m['center_px']
            wpx, hpx = m.get('size_px', (0, 0))
            label = (
                f"x{j} {m['payload'][:8]} "
                f"({wpx}x{hpx} {m.get('reject_reason', '')})"
            )
            cv2.putText(
                out, label,
                (int(cx) + 6, int(cy) + 14),
                cv2.FONT_HERSHEY_SIMPLEX, 0.4,
                (0, 140, 255), 1, cv2.LINE_AA,
            )

    for i, m in enumerate(markers):
        pts = np.array(
            m['corners_px'], dtype=np.int32,
        ).reshape(-1, 1, 2)
        cv2.polylines(
            out, [pts], isClosed=True,
            color=(0, 255, 0), thickness=2,
        )
        cx, cy = m['center_px']
        cv2.circle(
            out, (int(cx), int(cy)), 4, (0, 0, 255), -1,
        )
        ang = math.radians(m['orientation_deg'])
        arrow_len = 40
        ax = int(cx + arrow_len * math.cos(ang))
        ay = int(cy + arrow_len * math.sin(ang))
        cv2.arrowedLine(
            out, (int(cx), int(cy)), (ax, ay),
            (255, 0, 0), 2, tipLength=0.3,
        )
        label = (
            f"#{i} {m['payload'][:16]} "
            f"{m['orientation_deg']:+.1f}\u00b0"
        )
        cv2.putText(
            out, label,
            (int(cx) + 8, int(cy) - 8),
            cv2.FONT_HERSHEY_SIMPLEX, 0.45,
            (0, 255, 255), 1, cv2.LINE_AA,
        )

    # --- top-left HUD panel ---
    panel_w, panel_h = 320, 110
    overlay = out.copy()
    cv2.rectangle(
        overlay, (8, 8), (8 + panel_w, 8 + panel_h),
        (0, 0, 0), -1,
    )
    out = cv2.addWeighted(overlay, 0.55, out, 0.45, 0)

    if carousel_deg is None:
        head = 'CAROUSEL ANGLE: --'
        head_color = (60, 60, 255)
    else:
        head = f'CAROUSEL ANGLE: {carousel_deg:+7.2f} deg'
        head_color = (0, 255, 0)
    cv2.putText(
        out, head, (16, 36),
        cv2.FONT_HERSHEY_SIMPLEX, 0.75, head_color,
        2, cv2.LINE_AA,
    )
    cv2.putText(
        out,
        f'markers={len(markers)}  '
        f'decode={decode_ms:5.1f}ms  age={decode_age}f',
        (16, 64),
        cv2.FONT_HERSHEY_SIMPLEX, 0.5,
        (220, 220, 220), 1, cv2.LINE_AA,
    )
    cv2.putText(
        out,
        f'frame#{frame_no}  {w}x{h}',
        (16, 86),
        cv2.FONT_HERSHEY_SIMPLEX, 0.5,
        (180, 180, 180), 1, cv2.LINE_AA,
    )
    payload_str = ','.join(
        m['payload'][:6] for m in markers
    ) or '-'
    cv2.putText(
        out, f'ids: {payload_str[:42]}',
        (16, 106),
        cv2.FONT_HERSHEY_SIMPLEX, 0.45,
        (180, 220, 255), 1, cv2.LINE_AA,
    )
    return out


# --------------------------------------------------------------
# Capture / decode worker
# --------------------------------------------------------------

class StreamWorker:
    """Background thread: capture, decode every Nth frame,
    annotate every frame. Latest annotated JPEG is published
    under self.lock for the HTTP handlers."""

    def __init__(
        self,
        device: str,
        width: int,
        height: int,
        decode_every: int,
        record_dir: Path | None,
        scan: bool = False,
        release: bool = False,
        decode_timeout_ms: int = DEFAULT_DECODE_TIMEOUT_MS,
        min_marker_px: int = DEFAULT_MIN_MARKER_PX,
        min_payload_len: int = DEFAULT_MIN_PAYLOAD_LEN,
        debug_decode: bool = False,
    ) -> None:
        self.device = device
        self.width = width
        self.height = height
        self.decode_every = max(1, decode_every)
        self.record_dir = record_dir
        self.scan = scan
        self.release = release
        self.decode_timeout_ms = decode_timeout_ms
        self.min_marker_px = min_marker_px
        self.min_payload_len = min_payload_len
        self.debug_decode = debug_decode
        if record_dir is not None:
            record_dir.mkdir(parents=True, exist_ok=True)

        self.lock = threading.Lock()
        self.latest_jpeg: bytes | None = None
        self.latest_summary: dict = {
            'frame_no': 0,
            'n_markers': 0,
            'carousel_deg': None,
            'decode_ms': 0.0,
            'payloads': [],
        }
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._record_history: deque[Path] = deque(maxlen=600)

    def start(self) -> bool:
        cap = _open_camera(
            self.device, self.width, self.height,
            scan=self.scan, release=self.release,
        )
        if cap is None:
            LOG.error('Could not open any camera')
            return False
        self._cap = cap
        self._thread = threading.Thread(
            target=self._loop, name='probe-stream', daemon=True,
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(2.0)
        try:
            self._cap.release()
        except Exception:
            pass

    def _loop(self) -> None:
        frame_no = 0
        last_markers: list[dict] = []
        last_rejected: list[dict] = []
        last_decode_at = 0
        last_decode_ms = 0.0
        last_record_sec = 0
        while not self._stop.is_set():
            ok, frame = self._cap.read()
            if not ok or frame is None:
                time.sleep(0.05)
                continue
            frame_no += 1

            do_decode = (
                frame_no % self.decode_every == 0
                or frame_no == 1
            )
            if do_decode:
                t0 = time.monotonic()
                last_markers, last_rejected = _decode_markers(
                    frame,
                    timeout_ms=self.decode_timeout_ms,
                    min_marker_px=self.min_marker_px,
                    min_payload_len=self.min_payload_len,
                )
                last_decode_ms = (
                    time.monotonic() - t0
                ) * 1000.0
                last_decode_at = frame_no
                if self.debug_decode and (
                    last_markers or last_rejected
                ):
                    LOG.info(
                        'decode#%d: kept=%d rejected=%d (%s)',
                        frame_no,
                        len(last_markers),
                        len(last_rejected),
                        ', '.join(
                            f"{m['payload'][:8]!r}"
                            f"@{m['size_px'][0]}x{m['size_px'][1]}"
                            f"[{m.get('reject_reason', 'kept')}]"
                            for m in (last_markers + last_rejected)
                        ),
                    )

            carousel_deg = _fuse_carousel_angle(last_markers)
            annot = _annotate(
                frame, last_markers, carousel_deg, frame_no,
                last_decode_ms, frame_no - last_decode_at,
                rejected=(
                    last_rejected if self.debug_decode else None
                ),
            )
            ok, buf = cv2.imencode(
                '.jpg', annot,
                [cv2.IMWRITE_JPEG_QUALITY, 78],
            )
            if not ok:
                continue
            jpeg = bytes(buf)

            with self.lock:
                self.latest_jpeg = jpeg
                self.latest_summary = {
                    'frame_no': frame_no,
                    'n_markers': len(last_markers),
                    'carousel_deg': carousel_deg,
                    'decode_ms': last_decode_ms,
                    'payloads': [
                        m['payload'] for m in last_markers
                    ],
                }

            if (
                self.record_dir is not None
                and int(time.time()) != last_record_sec
            ):
                last_record_sec = int(time.time())
                p = self.record_dir / (
                    f'cam_{last_record_sec}.png'
                )
                cv2.imwrite(str(p), annot)
                self._record_history.append(p)
                # drop oldest if maxlen rolled over
                while (
                    len(self._record_history)
                    >= self._record_history.maxlen  # type: ignore
                ):
                    old = self._record_history.popleft()
                    try:
                        old.unlink(missing_ok=True)
                    except Exception:
                        pass


# --------------------------------------------------------------
# HTTP server
# --------------------------------------------------------------

def _build_handler(worker: StreamWorker):
    class Handler(http.server.BaseHTTPRequestHandler):

        # quieter access log -- one per second is enough
        _last_log = [0.0]

        def log_message(self, fmt, *args):
            now = time.monotonic()
            if now - self._last_log[0] > 1.0:
                self._last_log[0] = now
                LOG.info(
                    '%s -- %s', self.address_string(),
                    fmt % args,
                )

        def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler API)
            if self.path in ('/', '/index.html'):
                self._send_index()
            elif self.path.startswith('/stream'):
                self._send_mjpeg()
            elif self.path == '/snapshot':
                self._send_snapshot()
            elif self.path == '/status':
                self._send_status()
            else:
                self.send_error(404)

        def _send_index(self) -> None:
            page = b"""<!doctype html>
<html><head><title>Carousel marker probe</title>
<style>
 body { background:#111; color:#eee; font-family: sans-serif;
        margin:0; padding:12px; }
 h1   { margin:0 0 8px 0; font-size:18px; }
 img  { max-width:100%; border:1px solid #333;
        background:#000; }
 pre  { background:#222; padding:8px; }
</style></head><body>
<h1>Carousel marker probe (live)</h1>
<img src="/stream" alt="live"/>
<pre id="s">loading...</pre>
<script>
async function refresh() {
 try {
  const r = await fetch('/status');
  document.getElementById('s').innerText =
    JSON.stringify(await r.json(), null, 2);
 } catch(e) {}
 setTimeout(refresh, 500);
}
refresh();
</script>
</body></html>
"""
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', str(len(page)))
            self.end_headers()
            self.wfile.write(page)

        def _send_mjpeg(self) -> None:
            boundary = b'frame'
            self.send_response(200)
            self.send_header(
                'Content-Type',
                'multipart/x-mixed-replace; '
                'boundary=' + boundary.decode(),
            )
            self.end_headers()
            try:
                while True:
                    with worker.lock:
                        jpeg = worker.latest_jpeg
                    if jpeg is None:
                        time.sleep(0.05)
                        continue
                    self.wfile.write(b'--' + boundary + b'\r\n')
                    self.wfile.write(
                        b'Content-Type: image/jpeg\r\n'
                        b'Content-Length: '
                        + str(len(jpeg)).encode()
                        + b'\r\n\r\n',
                    )
                    self.wfile.write(jpeg)
                    self.wfile.write(b'\r\n')
                    time.sleep(0.05)
            except (BrokenPipeError, ConnectionResetError):
                return

        def _send_snapshot(self) -> None:
            with worker.lock:
                jpeg = worker.latest_jpeg
            if jpeg is None:
                self.send_error(503, 'no frame yet')
                return
            self.send_response(200)
            self.send_header('Content-Type', 'image/jpeg')
            self.send_header('Content-Length', str(len(jpeg)))
            self.end_headers()
            self.wfile.write(jpeg)

        def _send_status(self) -> None:
            import json as _json
            with worker.lock:
                summary = dict(worker.latest_summary)
            body = _json.dumps(summary).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


class _ThreadedHTTPServer(
    socketserver.ThreadingMixIn, http.server.HTTPServer,
):
    daemon_threads = True
    allow_reuse_address = True


# --------------------------------------------------------------
# CLI
# --------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description='Live MJPEG viewer that overlays the '
                    'detected carousel rotation angle on the '
                    'on-board camera stream.',
    )
    ap.add_argument(
        '--device', default='/dev/video0',
        help='V4L2 device path or index (default /dev/video0)',
    )
    ap.add_argument(
        '--scan', action='store_true',
        help='if --device fails, auto-scan all /dev/video* nodes '
             '(off by default; on a Pi 5 this can take ~30s '
             'because libcamera registers ~24 video nodes)',
    )
    ap.add_argument(
        '--release', action='store_true',
        help='if another process is already holding the camera '
             '(checked with `fuser`), SIGTERM/SIGKILL it before '
             'opening. Useful when a previous probe_stream did '
             'not exit cleanly.',
    )
    ap.add_argument(
        '--decode-timeout-ms', type=int,
        default=DEFAULT_DECODE_TIMEOUT_MS,
        help=f'pylibdmtx scan budget per frame '
             f'(default {DEFAULT_DECODE_TIMEOUT_MS} ms). '
             f'Raise if real markers are missed.',
    )
    ap.add_argument(
        '--min-marker-px', type=int,
        default=DEFAULT_MIN_MARKER_PX,
        help=f'reject detections smaller than this on either '
             f'edge (default {DEFAULT_MIN_MARKER_PX} px). '
             f'Lower to accept smaller cassette codes; raise '
             f'to drop tiny noise hits.',
    )
    ap.add_argument(
        '--min-payload-len', type=int,
        default=DEFAULT_MIN_PAYLOAD_LEN,
        help=f'reject detections with payload shorter than this '
             f'(default {DEFAULT_MIN_PAYLOAD_LEN}).',
    )
    ap.add_argument(
        '--debug-decode', action='store_true',
        help='log every libdmtx hit and draw rejected ones in '
             'orange so you can see what the filter is dropping.',
    )
    ap.add_argument(
        '--port', type=int, default=8765,
        help='HTTP port to serve MJPEG on (default 8765)',
    )
    ap.add_argument(
        '--bind', default='0.0.0.0',
        help='HTTP bind address (default all interfaces)',
    )
    ap.add_argument(
        '--width', type=int, default=1280,
        help='requested capture width (default 1280)',
    )
    ap.add_argument(
        '--height', type=int, default=720,
        help='requested capture height (default 720)',
    )
    ap.add_argument(
        '--decode-every', type=int,
        default=DEFAULT_DECODE_EVERY,
        help='run pylibdmtx every Nth frame; intermediate '
             'frames reuse the last marker set '
             f'(default {DEFAULT_DECODE_EVERY})',
    )
    ap.add_argument(
        '--record',
        help='if set, save one annotated PNG/sec to this dir '
             '(rolls over after 600 frames)',
    )
    ap.add_argument(
        '-v', '--verbose', action='store_true',
        help='debug logging',
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=(
            logging.DEBUG if args.verbose else logging.INFO
        ),
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )

    record_dir = (
        Path(os.path.expanduser(args.record))
        if args.record else None
    )

    worker = StreamWorker(
        device=args.device,
        width=args.width,
        height=args.height,
        decode_every=args.decode_every,
        record_dir=record_dir,
        scan=args.scan,
        release=args.release,
        decode_timeout_ms=args.decode_timeout_ms,
        min_marker_px=args.min_marker_px,
        min_payload_len=args.min_payload_len,
        debug_decode=args.debug_decode,
    )
    if not worker.start():
        return 2

    server = _ThreadedHTTPServer(
        (args.bind, args.port), _build_handler(worker),
    )
    LOG.info(
        'Serving on http://%s:%d/   (Ctrl-C to stop)',
        args.bind, args.port,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOG.info('Stopping...')
    finally:
        server.shutdown()
        worker.stop()
    return 0


if __name__ == '__main__':
    sys.exit(main())
