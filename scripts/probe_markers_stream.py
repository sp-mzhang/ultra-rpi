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
import socketserver
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
DECODE_TIMEOUT_MS = 200


def _grab_devices() -> list[str]:
    import glob
    return sorted(glob.glob('/dev/video[0-9]*'))


def _open_camera(
    device: str, width: int, height: int,
) -> cv2.VideoCapture | None:
    """Open the requested device, falling back to auto-scan."""
    for d in [device, *(_grab_devices())]:
        try:
            idx = int(d) if str(d).isdigit() else d
        except (TypeError, ValueError):
            idx = d
        cap = cv2.VideoCapture(idx)
        if not cap.isOpened():
            continue
        if width:
            cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        if height:
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
        # warmup -- discard first 3 frames so auto-exposure
        # doesn't make the first decode useless
        for _ in range(3):
            cap.read()
        ok, _ = cap.read()
        if ok:
            actual_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            actual_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            LOG.info(
                'Camera open: %s @ %dx%d',
                d, actual_w, actual_h,
            )
            return cap
        cap.release()
    return None


def _decode_markers(frame_bgr: np.ndarray) -> list[dict]:
    """Run pylibdmtx, return marker dicts (same shape as
    probe_markers.py::decode_markers but with the y-flip
    handled here so callers get image-coord corners)."""
    h = frame_bgr.shape[0]
    rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    raw = pylibdmtx.decode(
        rgb, max_count=8, timeout=DECODE_TIMEOUT_MS,
    )
    out: list[dict] = []
    for det in raw:
        r = det.rect
        left, bottom_from_bot, w, hh = (
            r.left, r.top, r.width, r.height,
        )
        top = h - bottom_from_bot - hh
        bl = (left, top + hh)
        br = (left + w, top + hh)
        tr = (left + w, top)
        tl = (left, top)
        cx = (bl[0] + br[0] + tr[0] + tl[0]) / 4.0
        cy = (bl[1] + br[1] + tr[1] + tl[1]) / 4.0
        dx, dy = br[0] - bl[0], br[1] - bl[1]
        ori = math.degrees(math.atan2(dy, dx))
        out.append({
            'payload': det.data.decode(
                'utf-8', errors='replace',
            ),
            'corners_px': [bl, br, tr, tl],
            'center_px': (cx, cy),
            'orientation_deg': ori,
        })
    return out


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
) -> np.ndarray:
    """Draw HUD onto a copy of the frame and return it."""
    out = frame_bgr.copy()
    h, w = out.shape[:2]

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
    ) -> None:
        self.device = device
        self.width = width
        self.height = height
        self.decode_every = max(1, decode_every)
        self.record_dir = record_dir
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
        cap = _open_camera(self.device, self.width, self.height)
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
                last_markers = _decode_markers(frame)
                last_decode_ms = (
                    time.monotonic() - t0
                ) * 1000.0
                last_decode_at = frame_no

            carousel_deg = _fuse_carousel_angle(last_markers)
            annot = _annotate(
                frame, last_markers, carousel_deg, frame_no,
                last_decode_ms, frame_no - last_decode_at,
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
