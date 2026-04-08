'''ultra.hw.camera -- USB webcam MJPEG streaming service.

Captures frames from a USB (UVC) camera via OpenCV/V4L2
and yields them as an MJPEG multipart stream suitable for
an ``<img>`` tag or ``StreamingResponse``.

The capture loop runs in a background daemon thread; the
MJPEG generator reads the latest frame under a lock so
multiple HTTP clients can share a single capture device.
'''
from __future__ import annotations

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


def _no_camera_jpeg() -> bytes:
    '''Generate a small black JPEG with "No Camera" text.

    Used as a placeholder when OpenCV is unavailable or the
    camera device cannot be opened.

    Returns:
        JPEG-encoded bytes of the placeholder image.
    '''
    if not HAS_CV2:
        return b''
    import numpy as np
    img = np.zeros((240, 320, 3), dtype=np.uint8)
    cv2.putText(
        img, 'No Camera', (60, 130),
        cv2.FONT_HERSHEY_SIMPLEX, 1.0,
        (255, 255, 255), 2,
    )
    ok, buf = cv2.imencode(
        '.jpg', img,
        [cv2.IMWRITE_JPEG_QUALITY, _JPEG_QUALITY],
    )
    return bytes(buf) if ok else b''


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
        '''Initialize the camera stream.

        Args:
            device: V4L2 device path or integer index.
                Defaults to ``/dev/video0``.
        '''
        self._device = device
        self._cap = None
        self._frame: bytes = _no_camera_jpeg()
        self._lock = threading.Lock()
        self._running = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> bool:
        '''Open the camera and start the capture thread.

        Returns:
            True if the camera opened successfully.
        '''
        if not HAS_CV2:
            LOG.warning(
                'OpenCV not installed -- camera disabled',
            )
            return False

        if self._running:
            return True

        cap = cv2.VideoCapture(self._device)
        if not cap.isOpened():
            LOG.warning(
                'Failed to open camera: %s', self._device,
            )
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
        '''Background thread: read frames continuously.'''
        placeholder = _no_camera_jpeg()
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
            time.sleep(_CAPTURE_INTERVAL)

        with self._lock:
            self._frame = placeholder

    def generate_mjpeg(
            self,
    ) -> Generator[bytes, None, None]:
        '''Yield MJPEG multipart chunks for HTTP streaming.

        Each chunk includes the multipart boundary, content
        headers, and one JPEG frame. The generator runs
        indefinitely until the caller disconnects.

        Yields:
            Bytes of one MJPEG multipart frame.
        '''
        while True:
            with self._lock:
                frame = self._frame
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
