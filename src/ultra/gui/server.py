'''ultra.gui.server -- FastAPI application + WebSocket.

Creates the FastAPI app with static file serving and
WebSocket endpoint for real-time event streaming. The
WebSocket broadcaster subscribes to the event bus and
pushes all events to connected clients.
'''
from __future__ import annotations

import asyncio
import json
import logging
import os.path as op
from collections import deque
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import (
    BaseHTTPMiddleware,
)
from starlette.requests import Request

if TYPE_CHECKING:
    from ultra.app import Application

LOG = logging.getLogger(__name__)

STATIC_DIR = op.join(
    op.dirname(op.abspath(__file__)), 'static',
)


_PEAK_BUFFER_MAX = 50_000


class WebSocketBroadcaster:
    '''Manages WebSocket connections and broadcasts events.

    Subscribes to the event bus and pushes all events to
    all connected WebSocket clients as JSON messages.
    Buffers recent peak_data and the latest sweep_data so
    newly connected clients can replay the full sensorgram.

    Attributes:
        _connections: Set of active WebSocket connections.
        _peak_buffer: Ring buffer of peak_data payloads.
        _last_sweep: Most recent sweep_data payload.
    '''

    # Diagnostic counters specific to the accel stream pipeline.
    # Compared against STM32Interface.accel_dispatched and the
    # browser's "Dropped" counter to localise where batches are
    # being lost when the GUI is active.
    accel_broadcast_calls = 0   # invocations of broadcast('accel_stream', …)
    accel_ws_sends        = 0   # successful per-client ws.send_text
    accel_ws_drops        = 0   # ws.send_text raised → connection dead

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._peak_buffer: deque[dict] = deque(
            maxlen=_PEAK_BUFFER_MAX,
        )
        self._last_sweep: dict[str, Any] | None = None
        self._marker_buffer: list[dict] = []
        self._pressure_buffer: list[dict] = []
        self._last_protocol_started: dict | None = None
        self._current_step_index: int = 0
        self._last_step_data: dict | None = None

    def connect(self, ws: WebSocket) -> None:
        '''Register a new WebSocket connection.

        Args:
            ws: FastAPI WebSocket instance.
        '''
        self._connections.add(ws)
        LOG.info(
            f'WebSocket connected '
            f'({len(self._connections)} total)',
        )

    def disconnect(self, ws: WebSocket) -> None:
        '''Remove a WebSocket connection.

        Args:
            ws: FastAPI WebSocket instance.
        '''
        self._connections.discard(ws)
        LOG.info(
            f'WebSocket disconnected '
            f'({len(self._connections)} total)',
        )

    async def broadcast(
            self,
            event_type: str,
            data: dict[str, Any],
    ) -> None:
        '''Send a JSON message to all connected clients.

        Also buffers peak_data and sweep_data so that newly
        connected clients can replay the full history.

        Args:
            event_type: Event type string.
            data: Event payload dict.
        '''
        if event_type == 'peak_data':
            self._peak_buffer.append(data)
        elif event_type == 'sweep_data':
            self._last_sweep = data
        elif event_type == 'timing_marker':
            self._marker_buffer.append(data)
        elif event_type == 'pressure_update':
            self._pressure_buffer.append(data)
        elif event_type == 'step_changed':
            idx = data.get(
                'step', data.get('step_index', 0),
            )
            if not data.get('completed'):
                self._current_step_index = idx
                self._last_step_data = dict(data)

        if event_type == 'accel_stream':
            WebSocketBroadcaster.accel_broadcast_calls += 1

        if not self._connections:
            return
        message = json.dumps({
            'type': event_type,
            'data': data,
        })
        dead: list[WebSocket] = []
        for ws in list(self._connections):
            try:
                await ws.send_text(message)
                if event_type == 'accel_stream':
                    WebSocketBroadcaster.accel_ws_sends += 1
            except Exception:
                dead.append(ws)
                if event_type == 'accel_stream':
                    WebSocketBroadcaster.accel_ws_drops += 1
        for ws in dead:
            self._connections.discard(ws)

    async def replay(self, ws: WebSocket) -> None:
        '''Send buffered state to a single client.

        Replays the last protocol_started (step manifest),
        all cached peak_data, the latest sweep_data, and
        timing markers so a newly connected browser rebuilds
        the full UI state.

        Args:
            ws: The newly connected WebSocket.
        '''
        if self._last_protocol_started is not None:
            msg = json.dumps({
                'type': 'protocol_started',
                'data': self._last_protocol_started,
            })
            try:
                await ws.send_text(msg)
            except Exception:
                return
        if self._last_step_data is not None:
            total = self._last_step_data.get('total', 0)
            for i in range(1, self._current_step_index):
                msg = json.dumps({
                    'type': 'step_changed',
                    'data': {
                        'step': i,
                        'total': total,
                        'completed': True,
                        'ok': True,
                    },
                })
                try:
                    await ws.send_text(msg)
                except Exception:
                    return
            msg = json.dumps({
                'type': 'step_changed',
                'data': self._last_step_data,
            })
            try:
                await ws.send_text(msg)
            except Exception:
                return
        for peak in self._peak_buffer:
            msg = json.dumps({
                'type': 'peak_data', 'data': peak,
            })
            try:
                await ws.send_text(msg)
            except Exception:
                return
        if self._last_sweep is not None:
            msg = json.dumps({
                'type': 'sweep_data',
                'data': self._last_sweep,
            })
            try:
                await ws.send_text(msg)
            except Exception:
                pass
        for marker in self._marker_buffer:
            msg = json.dumps({
                'type': 'timing_marker',
                'data': marker,
            })
            try:
                await ws.send_text(msg)
            except Exception:
                return
        for pr in self._pressure_buffer:
            msg = json.dumps({
                'type': 'pressure_update',
                'data': pr,
            })
            try:
                await ws.send_text(msg)
            except Exception:
                return

    def clear_buffers(
            self,
            protocol_started_data: dict | None = None,
    ) -> None:
        '''Discard buffered chart data and store new start.

        Called when a new protocol run starts so stale data
        from the previous run is not replayed.

        Args:
            protocol_started_data: The protocol_started
                payload to buffer for late-joining clients.
        '''
        self._peak_buffer.clear()
        self._last_sweep = None
        self._marker_buffer.clear()
        self._pressure_buffer.clear()
        self._current_step_index = 0
        self._last_step_data = None
        self._last_protocol_started = (
            protocol_started_data
        )


def create_app(application: 'Application') -> FastAPI:
    '''Create and configure the FastAPI application.

    Registers API routes, WebSocket endpoint, static files,
    and wires the event bus to the WebSocket broadcaster.

    Args:
        application: Top-level Application instance.

    Returns:
        Configured FastAPI app.
    '''
    app = FastAPI(title='Ultra RPi')

    class NoCacheMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            response = await call_next(request)
            if request.url.path.startswith('/static'):
                response.headers['Cache-Control'] = (
                    'no-cache, no-store, must-revalidate'
                )
            return response

    app.add_middleware(NoCacheMiddleware)
    broadcaster = WebSocketBroadcaster()

    from ultra.gui.api import create_api_router
    api_router = create_api_router(application)
    app.include_router(api_router, prefix='/api')

    _BROADCAST_EVENTS = [
        'step_changed', 'well_updated',
        'wells_initialized', 'tip_changed',
        'peak_data', 'sweep_data', 'timing_marker',
        'protocol_paused', 'protocol_resumed',
        'status_changed',
        'self_check_substate',
        'door_opened', 'door_closed',
        'pressure_update', 'temperature_update',
        'accel_stream',
        'centrifuge_rpm', 'stm32_error',
        'egress_started', 'egress_done',
        'egress_error',
        'analysis_complete',
    ]

    for event_name in _BROADCAST_EVENTS:
        _name = event_name

        async def _handler(
                data: dict,
                _evt: str = _name,
        ) -> None:
            await broadcaster.broadcast(_evt, data)

        application.event_bus.on(event_name, _handler)

    async def _on_protocol_started(
            data: dict,
    ) -> None:
        broadcaster.clear_buffers(
            protocol_started_data=data,
        )
        await broadcaster.broadcast(
            'protocol_started', data,
        )

    application.event_bus.on(
        'protocol_started', _on_protocol_started,
    )

    _TERMINAL_EVENTS = [
        'protocol_done', 'protocol_error',
        'protocol_aborted',
    ]

    for event_name in _TERMINAL_EVENTS:
        _tname = event_name

        async def _terminal_handler(
                data: dict,
                _evt: str = _tname,
        ) -> None:
            await broadcaster.broadcast(_evt, data)
            broadcaster.clear_buffers()

        application.event_bus.on(
            event_name, _terminal_handler,
        )

    from ultra.utils.logging import get_log_handler
    _lh = get_log_handler()
    if _lh is not None:
        import asyncio as _aio

        def _on_log_line(line: str) -> None:
            loop = _aio.get_event_loop()
            loop.call_soon_threadsafe(
                _aio.ensure_future,
                broadcaster.broadcast(
                    'log_line', {'line': line},
                ),
            )

        _lh.set_callback(_on_log_line)

    @app.websocket('/ws')
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        await broadcaster.replay(ws)
        broadcaster.connect(ws)
        try:
            while True:
                data = await ws.receive_text()
                LOG.debug(f'WS recv: {data}')
        except WebSocketDisconnect:
            broadcaster.disconnect(ws)
        except Exception:
            broadcaster.disconnect(ws)

    if op.isdir(STATIC_DIR):
        app.mount(
            '/static',
            StaticFiles(directory=STATIC_DIR),
            name='static',
        )

        @app.get('/')
        async def index():
            return FileResponse(
                op.join(STATIC_DIR, 'index.html'),
            )

    return app
