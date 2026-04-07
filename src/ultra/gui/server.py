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

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._peak_buffer: deque[dict] = deque(
            maxlen=_PEAK_BUFFER_MAX,
        )
        self._last_sweep: dict[str, Any] | None = None

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

        if not self._connections:
            return
        message = json.dumps({
            'type': event_type,
            'data': data,
        })
        dead: list[WebSocket] = []
        for ws in self._connections:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections.discard(ws)

    async def replay(self, ws: WebSocket) -> None:
        '''Send buffered chart data to a single client.

        Replays all cached peak_data events then the latest
        sweep_data so a newly connected browser rebuilds the
        full sensorgram and spectrum.

        Args:
            ws: The newly connected WebSocket.
        '''
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

    def clear_buffers(self) -> None:
        '''Discard all buffered chart data.

        Called when a new protocol run starts so stale data
        from the previous run is not replayed.
        '''
        self._peak_buffer.clear()
        self._last_sweep = None


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
    api_router = create_api_router(
        application, broadcaster,
    )
    app.include_router(api_router, prefix='/api')

    _BROADCAST_EVENTS = [
        'step_changed', 'well_updated',
        'wells_initialized', 'tip_changed',
        'peak_data', 'sweep_data', 'protocol_paused',
        'protocol_resumed',
        'protocol_done', 'protocol_error',
        'protocol_aborted', 'status_changed',
        'door_opened', 'door_closed',
        'pressure_update', 'temperature_update',
        'centrifuge_rpm', 'stm32_error',
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
        broadcaster.clear_buffers()
        await broadcaster.broadcast(
            'protocol_started', data,
        )

    application.event_bus.on(
        'protocol_started', _on_protocol_started,
    )

    @app.websocket('/ws')
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        broadcaster.connect(ws)
        await broadcaster.replay(ws)
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
