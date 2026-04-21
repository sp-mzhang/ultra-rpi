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

import glob
import logging
import threading
import time
from typing import Generator

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
        self._lock = threading.Lock()
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

    def _auto_detect(self) -> 'cv2.VideoCapture | None':
        '''Scan /dev/video* for the first device that produces frames.'''
        candidates = sorted(glob.glob('/dev/video[0-9]*'))
        for dev in candidates:
            LOG.debug('Probing camera: %s', dev)
            cap = self._try_open(dev)
            if cap is not None:
                self._device = dev
                LOG.info(
                    'Auto-detected camera: %s', dev,
                )
                return cap
        return None

    def start(self) -> bool:
        '''Open the camera and start the capture thread.

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

        if self._running:
            return True

        cap = self._try_open(self._device)
        if cap is None:
            LOG.warning(
                'Failed to open camera: %s — scanning…',
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
        '''Background thread: read and JPEG-encode frames.'''
        while not self._stop.is_set():
            if self._cap is None:
                break
            ret, frame = self._cap.read()
            if not ret:
                time.sleep(_CAPTURE_INTERVAL)
                continue
            ok, buf = cv2.imencode(
                '.jpg', frame,
                [cv2.IMWRITE_JPEG_QUALITY, _JPEG_QUALITY],
            )
            if ok:
                with self._lock:
                    self._frame = bytes(buf)
                    self._frame_bgr = frame
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

    def latest_frame_bgr(self):
        '''Return the most recent raw BGR frame as an ndarray, or None.

        A copy is returned so callers can safely mutate / process without
        racing with the capture thread. Returns None if the camera has
        not yet produced its first frame.
        '''
        with self._lock:
            frame = self._frame_bgr
        if frame is None:
            return None
        return frame.copy()
