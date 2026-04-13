'''Protocol run-control endpoints.

Handles /status, /recipes, /quick_run, /run, /pause, /resume,
/abort, /restart_from, /wells, and /state-machine/*.
'''
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ultra.gui._eng_state import eng_stm32

if TYPE_CHECKING:
    from ultra.app import Application

LOG = logging.getLogger(__name__)


class RunRequest(BaseModel):
    '''Request body for starting a protocol run.'''
    recipe: str
    chip_id: str = ''
    note: str = ''


class RestartRequest(BaseModel):
    '''Request body for restarting from a specific step.'''
    step_index: int


def create_protocol_router(app: 'Application') -> APIRouter:
    router = APIRouter()

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
        '''Get quick_run config defaults for the GUI.'''
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

        eng = eng_stm32['iface']
        if eng is not None:
            try:
                eng.disconnect()
            except Exception:
                pass
            eng_stm32['iface'] = None

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

        asyncio.ensure_future(_run())
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
        '''Abort the running protocol.

        Always succeeds — also resets state if nothing is
        running so the UI can recover from a stuck state.
        '''
        runner = app.get_runner()
        if runner.is_running:
            runner.abort()
            return {'status': 'aborting'}

        runner._running = False
        runner._abort_event.clear()
        runner._pause_event.set()
        await app.event_bus.emit(
            'protocol_aborted', {'forced': True},
        )
        if app._monitor and not app._monitor._running:
            app._monitor.start()
        return {'status': 'reset'}

    @router.post('/restart_from')
    async def restart_from_step(req: RestartRequest):
        '''Restart protocol from a specific step (paused only).'''
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
        '''Stop the state machine.

        Cancels the background task so blocked awaits
        (drawer events, sleeps) are interrupted immediately.
        '''
        if app._state_machine:
            app._state_machine.stop()
        if app._sm_task and not app._sm_task.done():
            app._sm_task.cancel()
            try:
                await app._sm_task
            except asyncio.CancelledError:
                pass
        await app.event_bus.emit(
            'status_changed',
            {'state': 'inactive', 'message': 'Stopped'},
        )
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

    return router
