'''ultra.hw.camera -- USB webcam MJPEG streaming service.

Captures frames from a USB (UVC) camera via OpenCV/V4L2
and yields them as an MJPEG multipart stream suitable for
an ``<img>`` tag or ``StreamingResponse``.

The capture loop runs in a background daemon thread; the
MJPEG generator reads the latest frame under a lock so
multiple HTTP clients can share a single capture device.

No frames are saved to disk.
'''
from __future__ import annotations

import fcntl
import glob
import logging
import os
import re
import struct
import threading
import time
from typing import Generator

# V4L2 VIDIOC_QUERYCAP ioctl: _IOR('V', 0, struct v4l2_capability)
# struct size is 104 bytes; encodes as (2<<30)|(104<<16)|('V'<<8)|0.
_VIDIOC_QUERYCAP = 0x80685600
_V4L2_CAP_VIDEO_CAPTURE = 0x00000001
_V4L2_CAP_VIDEO_CAPTURE_MPLANE = 0x00001000
_V4L2_CAP_DEVICE_CAPS = 0x80000000


def _supports_video_capture(dev_path: str) -> bool:
    '''Return True iff ``dev_path`` advertises VIDEO_CAPTURE via V4L2.

    Probes the device with ``VIDIOC_QUERYCAP`` -- completes in
    microseconds on both capture-capable and non-capture nodes,
    so we can skip the Pi's bcm2835 ISP / codec / rpivid pipeline
    devices without paying their ~10 s select() timeout when
    ``cv2.VideoCapture`` tries to grab a frame.
    '''
    try:
        fd = os.open(dev_path, os.O_RDWR | os.O_NONBLOCK)
    except OSError:
        return False
    try:
        buf = bytearray(104)
        fcntl.ioctl(fd, _VIDIOC_QUERYCAP, buf)
    except OSError:
        return False
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
    # Layout (linux/videodev2.h):
    #   u8 driver[16]; u8 card[32]; u8 bus_info[32];
    #   u32 version; u32 capabilities; u32 device_caps;
    #   u32 reserved[3];
    _drv, _card, _bus, _ver, caps, device_caps, *_r = struct.unpack(
        '<16s32s32sIII3I', bytes(buf),
    )
    # device_caps is only valid when the DEVICE_CAPS bit is set in
    # ``caps`` (newer kernels set it; older may not). Fall back to
    # the full driver caps otherwise.
    effective = (
        device_caps if caps & _V4L2_CAP_DEVICE_CAPS else caps
    )
    return bool(effective & (
        _V4L2_CAP_VIDEO_CAPTURE
        | _V4L2_CAP_VIDEO_CAPTURE_MPLANE
    ))

LOG = logging.getLogger(__name__)

try:
    import cv2
    HAS_CV2 = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    HAS_CV2 = False

_JPEG_QUALITY = 80
_CAPTURE_INTERVAL = 0.05  # ~20 fps
_MJPEG_BOUNDARY = b'frame'

# If cap.read() returns ret=False for this long, assume the
# underlying V4L2 handle is dead (USB bump, power glitch,
# driver hiccup) and force a re-detect on the next start().
_READ_STALL_TIMEOUT_S = 2.0


class CameraStream:
    '''Thread-safe USB camera capture with MJPEG output.

    Opens a V4L2 device via OpenCV in a background thread
    and provides a generator that yields MJPEG multipart
    chunks for HTTP streaming.

    Attributes:
        _device: V4L2 device path (e.g. ``/dev/video0``).
        _cap: OpenCV VideoCapture instance (or None).
        _frame: Latest JPEG-encoded frame bytes.
        _lock: Guards ``_frame`` access.
        _running: Whether the capture thread is alive.
    '''

    def __init__(self, device: str = '/dev/video0') -> None:
        '''Initialise the camera stream.

        Args:
            device: V4L2 device path or integer index.
                Defaults to ``/dev/video0``.
        '''
        self._device = device
        self._cap = None
        self._frame: bytes | None = None
        self._frame_bgr = None  # latest raw BGR ndarray (for CV consumers)
        self._frame_ts: float = 0.0  # monotonic ts of last successful read
        self._lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._running = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _try_open(device) -> 'cv2.VideoCapture | None':
        '''Try to open a device, return cap if it reads a frame.'''
        try:
            dev = int(device) if str(device).isdigit() else device
        except (ValueError, TypeError):
            dev = device
        cap = cv2.VideoCapture(dev)
        if not cap.isOpened():
            return None
        ret, _ = cap.read()
        if not ret:
            cap.release()
            return None
        return cap

    @staticmethod
    def _list_video_devices() -> list[tuple[str, bool]]:
        '''All /dev/videoN present, with a ``supports_capture`` flag.

        Returns a list of ``(path, capture_capable)`` tuples sorted
        numerically by the trailing device index. ``capture_capable``
        comes from V4L2 ``VIDIOC_QUERYCAP`` and is ``False`` for the
        Pi's internal pipeline nodes (bcm2835 ISP, codec, rpivid
        output, etc.), so those can be skipped without paying the
        ~10 s ``select()`` timeout that ``cv2.VideoCapture.read()``
        otherwise incurs on them.
        '''
        paths = glob.glob('/dev/video*')
        ordered: list[tuple[int, str]] = []
        for p in paths:
            m = re.match(r'/dev/video(\d+)$', p)
            if m:
                ordered.append((int(m.group(1)), p))
        return [
            (p, _supports_video_capture(p))
            for _, p in sorted(ordered)
        ]

    def _auto_detect(self) -> 'cv2.VideoCapture | None':
        '''Scan every /dev/videoN device in numeric order.

        Non-capture nodes are filtered out via V4L2 QUERYCAP so
        we never spend the ~10 s ``select()`` timeout on the Pi's
        internal ISP/codec pipeline devices.
        '''
        devices = self._list_video_devices()
        capture_devs = [p for p, ok in devices if ok]
        skipped = [p for p, ok in devices if not ok]
        LOG.info(
            'Camera scan: %d nodes found, %d capture-capable '
            '(skipping non-capture: %s)',
            len(devices), len(capture_devs),
            ', '.join(skipped) or '(none)',
        )
        if not capture_devs:
            LOG.warning(
                'No USB/UVC video-capture device present. '
                'All %d /dev/video* nodes belong to the SoC '
                'ISP/codec pipeline. Plug a USB camera in and '
                'confirm with `lsusb` + `ls /dev/video*`.',
                len(devices),
            )
            return None
        for dev in capture_devs:
            LOG.info('Probing camera: %s', dev)
            cap = self._try_open(dev)
            if cap is not None:
                self._device = dev
                LOG.info(
                    'Auto-detected camera: %s', dev,
                )
                return cap
            LOG.info('  -> %s: open or read failed', dev)
        return None

    def start(self) -> bool:
        '''Open the camera and start the capture thread.

        Idempotent and serialised: concurrent callers won't race
        each other into multiple parallel ``_auto_detect`` scans
        (each scan can take tens of seconds on a Pi with many
        pipeline nodes, so stacking them would block the UI).

        Returns:
            True if the camera opened successfully.
        '''
        if not HAS_CV2:
            LOG.warning(
                'OpenCV not installed -- camera disabled. '
                'Install via: sudo apt install python3-opencv'
                ' or pip install opencv-python-headless',
            )
            return False

        with self._start_lock:
            if self._running:
                return True

            cap = self._try_open(self._device)
            if cap is None:
                LOG.warning(
                    'Failed to open camera: %s -- scanning...',
                    self._device,
                )
                cap = self._auto_detect()
            if cap is None:
                LOG.warning('No working camera found.')
                return False

            self._cap = cap
            self._stop.clear()
            self._running = True
            self._thread = threading.Thread(
                target=self._capture_loop,
                name='camera-capture',
                daemon=True,
            )
            self._thread.start()
            LOG.info('Camera started: %s', self._device)
            return True

    def stop(self) -> None:
        '''Stop the capture thread and release the device.'''
        self._stop.set()
        if self._thread is not None:
            self._thread.join(5.0)
            self._thread = None
        if self._cap is not None:
            try:
                self._cap.release()
            except Exception:
                pass
            self._cap = None
        self._running = False
        LOG.info('Camera stopped')

    def _capture_loop(self) -> None:
        '''Background thread: read and JPEG-encode frames.

        If reads start failing for longer than ``_READ_STALL_TIMEOUT_S``
        we tear the capture down so the next ``start()`` re-runs
        ``_auto_detect``. Without this, a physically disturbed or
        re-enumerated USB cam leaves us holding a dead handle and
        serving whatever frame happened to be cached last.
        '''
        last_ok_ts = time.monotonic()
        while not self._stop.is_set():
            if self._cap is None:
                break
            ret, frame = self._cap.read()
            now = time.monotonic()
            if not ret:
                stalled_for = now - last_ok_ts
                if stalled_for >= _READ_STALL_TIMEOUT_S:
                    LOG.warning(
                        'Camera read stalled for %.1fs on %s; '
                        'releasing handle and stopping capture '
                        '(next request will re-detect).',
                        stalled_for, self._device,
                    )
                    # Drop the dead capture; mark not running so
                    # the next start() will reopen (and re-scan if
                    # the device was re-enumerated).
                    try:
                        self._cap.release()
                    except Exception:
                        pass
                    self._cap = None
                    self._running = False
                    with self._lock:
                        # Invalidate the cached frame so stale
                        # images don't keep getting served.
                        self._frame = None
                        self._frame_bgr = None
                        self._frame_ts = 0.0
                    return
                time.sleep(_CAPTURE_INTERVAL)
                continue
            last_ok_ts = now
            ok, buf = cv2.imencode(
                '.jpg', frame,
                [cv2.IMWRITE_JPEG_QUALITY, _JPEG_QUALITY],
            )
            if ok:
                with self._lock:
                    self._frame = bytes(buf)
                    self._frame_bgr = frame
                    self._frame_ts = now
            time.sleep(_CAPTURE_INTERVAL)

    def generate_mjpeg(
            self,
    ) -> Generator[bytes, None, None]:
        '''Yield MJPEG multipart chunks for HTTP streaming.

        Each chunk includes the multipart boundary, content
        headers, and one JPEG frame. The generator runs
        until the capture stops or the caller disconnects.

        Yields:
            Bytes of one MJPEG multipart frame.
        '''
        while self._running:
            with self._lock:
                frame = self._frame
            if frame is None:
                time.sleep(_CAPTURE_INTERVAL)
                continue
            yield (
                b'--' + _MJPEG_BOUNDARY + b'\r\n'
                b'Content-Type: image/jpeg\r\n'
                b'Content-Length: '
                + str(len(frame)).encode()
                + b'\r\n\r\n'
                + frame
                + b'\r\n'
            )
            time.sleep(_CAPTURE_INTERVAL)

    @property
    def is_running(self) -> bool:
        '''Whether the capture thread is active.'''
        return self._running

    def latest_frame_bgr(
            self,
            newer_than: float = 0.0,
            wait_s: float = 0.0,
    ) -> 'tuple':
        '''Return ``(frame_copy, monotonic_ts)`` or ``(None, 0.0)``.

        Args:
            newer_than: Only accept a frame whose capture timestamp
                is strictly greater than this monotonic value. Used
                by callers that must see a frame captured **after**
                some event (e.g. LED turning on).
            wait_s: Maximum time to block waiting for a fresher
                frame. Defaults to 0 (non-blocking snapshot).

        Notes:
            A copy is returned so callers can mutate / process
            without racing the capture thread.
        '''
        deadline = time.monotonic() + max(0.0, wait_s)
        while True:
            with self._lock:
                frame = self._frame_bgr
                ts = self._frame_ts
            if frame is not None and ts > newer_than:
                return frame.copy(), ts
            if time.monotonic() >= deadline:
                return None, 0.0
            time.sleep(0.02)

    def latest_frame_ts(self) -> float:
        '''Monotonic timestamp of the most recent successful capture.

        Returns 0.0 if no frame has been produced yet.
        '''
        with self._lock:
            return self._frame_ts
