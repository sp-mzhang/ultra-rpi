'''Process-wide :class:`CameraStream` singleton.

Both the GUI engineering API (live MJPEG preview, manual carousel
alignment) and the protocol step executor
(``align_to_carousel``) need to grab frames from the same physical
USB webcam. Opening two ``CameraStream`` instances against the
same ``/dev/video*`` node would conflict over the V4L2 device
handle, so we serialise access through a single shared object.

The singleton is created lazily on first ``get_camera`` call (so
configs that disable the camera incur no cost) and started on
demand. Both the FastAPI route closure in
``ultra.gui.api_stm32`` and the step in ``ultra.protocol.steps``
import from here.
'''
from __future__ import annotations

import threading
from typing import Any

from ultra.hw.camera import CameraStream

_state: dict[str, Any] = {'instance': None}
_lock = threading.Lock()


def get_camera(config: dict | None = None) -> CameraStream:
    '''Return the shared :class:`CameraStream`, starting it.

    Args:
        config: Full app config dict. The first call to
            ``get_camera`` reads ``config['camera']`` to
            construct the underlying ``CameraStream`` (device,
            width, height, fourcc). Subsequent calls ignore
            ``config`` and return the already-built instance --
            the singleton's resolution is fixed for the life of
            the process.

    Returns:
        A running ``CameraStream``. Caller does not need to
        call ``.start()``.
    '''
    with _lock:
        cam = _state['instance']
        if cam is None:
            cfg = (config or {}).get('camera', {}) or {}
            cam = CameraStream(
                device=cfg.get('device', '/dev/video0'),
                width=cfg.get('width'),
                height=cfg.get('height'),
                fourcc=cfg.get('fourcc'),
            )
            _state['instance'] = cam
        if not cam.is_running:
            cam.start()
        return cam


def get_existing_camera() -> CameraStream | None:
    '''Return the singleton if it exists, else None.

    Useful for code paths that want to *check* whether the
    camera has been touched without forcing it to open.
    '''
    return _state['instance']


def reset_for_tests() -> None:
    '''Clear the singleton -- intended for unit tests only.'''
    with _lock:
        cam = _state['instance']
        if cam is not None:
            try:
                cam.stop()
            except Exception:
                pass
        _state['instance'] = None
