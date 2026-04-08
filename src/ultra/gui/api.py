'''ultra.gui.api -- REST API endpoints for the web GUI.

Provides endpoints for protocol control, status queries,
recipe listing, camera streaming, and state machine
management.
'''
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

if TYPE_CHECKING:
    from ultra.app import Application
    from ultra.gui.server import WebSocketBroadcaster

LOG = logging.getLogger(__name__)


class RunRequest(BaseModel):
    '''Request body for starting a protocol run.'''
    recipe: str
    chip_id: str = ''
    note: str = ''


class RestartRequest(BaseModel):
    '''Request body for restarting from a specific step.'''
    step_index: int


class SMRequest(BaseModel):
    '''Request body for state machine control.'''
    action: str = 'start'


def create_api_router(
        app: 'Application',
        broadcaster: 'WebSocketBroadcaster',
) -> APIRouter:
    '''Create the API router with all endpoints.

    Args:
        app: Application instance.
        broadcaster: WebSocket broadcaster.

    Returns:
        Configured APIRouter.
    '''
    router = APIRouter()
    _run_task: dict[str, asyncio.Task | None] = {
        'task': None,
    }

    @router.get('/status')
    async def get_status():
        '''Get current protocol status snapshot.'''
        runner = app.get_runner()
        snap = runner.tracker.snapshot()
        result = snap.to_dict()
        result['is_running'] = runner.is_running
        result['is_paused'] = runner.is_paused
        result['machine_name'] = app.config.get(
            'machine_name', '',
        )
        if app._state_machine:
            result['sm_state'] = (
                app._state_machine.state.value
            )
        else:
            result['sm_state'] = 'inactive'
        return result

    @router.get('/recipes')
    async def list_recipes():
        '''List available protocol recipes.'''
        from ultra.protocol.recipe_loader import (
            list_recipes as _list,
        )
        return _list()

    @router.get('/quick_run')
    async def get_quick_run():
        '''Get quick_run config defaults for the GUI.

        Returns the pre-fill values so the frontend can
        populate recipe, chip_id, and operator fields
        without the user typing them.
        '''
        qr = app.config.get('quick_run', {})
        return {
            'enabled': qr.get('enabled', False),
            'protocol': qr.get('protocol', ''),
            'chip_id': qr.get('chip_id', ''),
            'operator': qr.get('operator', ''),
        }

    @router.post('/run')
    async def run_protocol(req: RunRequest):
        '''Start a protocol run.

        Returns 409 if a protocol is already running or
        if the state machine is in RUNNING_PROTOCOL.
        '''
        runner = app.get_runner()
        if runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='Protocol already running',
            )
        if (
            app._state_machine
            and app._state_machine.state.value
            == 'running_protocol'
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    'State machine owns the protocol '
                    '-- use SM controls'
                ),
            )

        from ultra.hw.stm32_monitor import (
            STM32StatusMonitor,
        )
        STM32StatusMonitor.stop_active()
        await asyncio.sleep(0.5)

        stm32 = app._stm32
        if stm32 is None:
            from ultra.hw.stm32_interface import (
                STM32Interface,
            )
            stm32_cfg = app.config.get('stm32', {})
            stm32 = STM32Interface(
                port=stm32_cfg.get(
                    'port', '/dev/ttyAMA3',
                ),
                baud=stm32_cfg.get('baud', 921600),
            )
            if not stm32.connect():
                raise HTTPException(
                    status_code=500,
                    detail='Failed to connect to STM32',
                )
        runner.stm32 = stm32

        def _hw_init_and_run():
            '''Pre-flight init + protocol run in thread.'''
            stm32.send_command_wait_done(
                cmd={'cmd': 'pump_init'},
                timeout_s=30.0,
            )
            stm32.send_command_wait_done(
                cmd={'cmd': 'home_all'},
                timeout_s=60.0,
            )
            return runner._run_sync(
                req.recipe,
                chip_id=req.chip_id,
                note=req.note,
            )

        async def _run():
            try:
                loop = asyncio.get_running_loop()
                app.event_bus.set_loop(loop)
                await loop.run_in_executor(
                    None, _hw_init_and_run,
                )
            except Exception as err:
                LOG.error(
                    f'Protocol run error: {err}',
                )
            finally:
                if app._stm32 is None:
                    stm32.disconnect()
                if app._monitor:
                    app._monitor.start()

        _run_task['task'] = asyncio.ensure_future(
            _run(),
        )
        return {
            'status': 'started',
            'recipe': req.recipe,
        }

    @router.post('/pause')
    async def pause_protocol():
        '''Pause the running protocol.'''
        runner = app.get_runner()
        if not runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='No protocol running',
            )
        runner.pause()
        return {'status': 'pausing'}

    @router.post('/resume')
    async def resume_protocol():
        '''Resume a paused protocol.'''
        runner = app.get_runner()
        if not runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='No protocol running',
            )
        runner.resume()
        return {'status': 'resuming'}

    @router.post('/abort')
    async def abort_protocol():
        '''Abort the running protocol.'''
        runner = app.get_runner()
        if not runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='No protocol running',
            )
        runner.abort()
        return {'status': 'aborting'}

    @router.post('/restart_from')
    async def restart_from_step(req: RestartRequest):
        '''Restart protocol from a specific step.

        Only allowed when the protocol is paused. Reconciles
        tip state and resumes from the requested step.
        '''
        runner = app.get_runner()
        if not runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='No protocol running',
            )
        if not runner.is_paused:
            raise HTTPException(
                status_code=409,
                detail='Protocol must be paused first',
            )
        try:
            runner.restart_from(req.step_index)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            )
        return {
            'status': 'restarting',
            'from_step': req.step_index,
        }

    @router.get('/wells')
    async def get_wells():
        '''Get current well state map.'''
        runner = app.get_runner()
        snap = runner.tracker.snapshot()
        return {
            name: ws.to_dict()
            for name, ws in snap.wells.items()
        }

    @router.post('/state-machine/start')
    async def start_state_machine():
        '''Start the automated state machine.'''
        if app._state_machine and app._sm_task:
            if not app._sm_task.done():
                raise HTTPException(
                    status_code=409,
                    detail='State machine already running',
                )
        app._start_state_machine()
        return {'status': 'started'}

    @router.post('/state-machine/stop')
    async def stop_state_machine():
        '''Stop the state machine.'''
        if app._state_machine:
            app._state_machine.stop()
        return {'status': 'stopped'}

    @router.get('/state-machine/status')
    async def get_sm_status():
        '''Get state machine status.'''
        if app._state_machine:
            return {
                'active': True,
                'state': app._state_machine.state.value,
            }
        return {'active': False, 'state': 'inactive'}

    # ---- Egress status endpoints ----

    def _get_egress_db():
        '''Return the EgressDB if egress is enabled.'''
        svc = getattr(app, '_egress_svc', None)
        if svc is None:
            return None
        return getattr(svc, '_db', None)

    @router.get('/egress/status')
    async def egress_status():
        '''Return egress summary counts.

        Returns ``{total, egressed, pending, errored}``.
        When egress is disabled returns all zeros.
        '''
        db = _get_egress_db()
        if db is None:
            return {
                'total': 0, 'egressed': 0,
                'pending': 0, 'errored': 0,
            }
        return db.get_summary()

    @router.get('/egress/runs')
    async def egress_runs():
        '''Return all egress runs as JSON list.

        Each entry contains run metadata and egress state.
        Newest runs appear first.
        '''
        db = _get_egress_db()
        if db is None:
            return []
        rows = db.get_all_runs()
        return [
            {
                'rowid': r.rowid,
                'rundate_ts': r.rundate_ts,
                'run_uuid': r.run_uuid,
                'run_id': r.run_id,
                'rungroup_uuid': r.rungroup_uuid,
                'is_egressed': bool(r.is_egressed),
                'egress_ts': r.egress_ts,
                'run_dir_path': r.run_dir_path,
                'egress_errors': r.egress_errors,
            }
            for r in rows
        ]

    # ---- Camera MJPEG streaming ----

    _camera = None

    def _get_camera():
        nonlocal _camera
        if _camera is None:
            from ultra.hw.camera import CameraStream
            cam_cfg = app.config.get('camera', {})
            device = cam_cfg.get(
                'device', '/dev/video0',
            )
            _camera = CameraStream(device=device)
        if not _camera.is_running:
            _camera.start()
        return _camera

    @router.get('/camera/stream')
    async def camera_stream():
        '''Stream MJPEG frames from the USB camera.

        Returns a multipart/x-mixed-replace response
        that browsers render natively in an ``<img>`` tag.
        '''
        cam = _get_camera()
        return StreamingResponse(
            cam.generate_mjpeg(),
            media_type=(
                'multipart/x-mixed-replace; '
                'boundary=frame'
            ),
        )

    return router
