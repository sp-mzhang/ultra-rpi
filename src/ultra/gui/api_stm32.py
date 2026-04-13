'''STM32 engineering endpoints.

Handles /stm32/connect, /stm32/disconnect, /stm32/connected,
/stm32/command, /motor-status, /motor-telemetry/stream,
/stm32/status, /stm32/commands, /logs, and /camera/stream.
'''
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ultra.gui._eng_state import (
    camera as _camera_state,
    eng_stm32,
    get_eng_stm32,
)

if TYPE_CHECKING:
    from ultra.app import Application

LOG = logging.getLogger(__name__)


class Stm32CmdRequest(BaseModel):
    '''Request body for a raw STM32 command.'''
    cmd: str
    params: dict[str, Any] = {}
    timeout_s: float = 30.0
    wait_done: bool = True
    lock_timeout: float | None = None


def create_stm32_router(app: 'Application') -> APIRouter:
    router = APIRouter()

    @router.post('/stm32/connect')
    async def stm32_connect():
        '''Connect the engineering STM32 interface.'''
        if eng_stm32['iface'] is not None:
            return {'ok': True, 'detail': 'already connected'}

        runner = app.get_runner()
        if runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='Protocol running -- cannot '
                       'connect engineering',
            )

        from ultra.hw.stm32_monitor import (
            STM32StatusMonitor,
        )
        STM32StatusMonitor.stop_active()
        await asyncio.sleep(0.3)

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
        loop = asyncio.get_running_loop()
        ok = await loop.run_in_executor(
            None, stm32.connect,
        )
        if not ok:
            if app._monitor:
                app._monitor.start()
            raise HTTPException(
                status_code=500,
                detail='Failed to connect to STM32',
            )
        eng_stm32['iface'] = stm32

        version_str = '--'
        try:
            r = await loop.run_in_executor(
                None,
                lambda: stm32.send_command(
                    {'cmd': 'get_version'},
                    timeout_s=3.0,
                ),
            )
            if r:
                version_str = r.get(
                    'version', str(r),
                )
        except Exception:
            pass

        LOG.info('Engineering STM32 connected')
        return {
            'ok': True,
            'firmware': version_str,
        }

    @router.post('/stm32/disconnect')
    async def stm32_disconnect():
        '''Disconnect engineering STM32 and restart monitor.'''
        stm32 = eng_stm32['iface']
        if stm32 is not None:
            try:
                stm32.disconnect()
            except Exception:
                pass
            eng_stm32['iface'] = None
            LOG.info('Engineering STM32 disconnected')

        if app._monitor:
            app._monitor.start()
        return {'ok': True}

    @router.get('/stm32/connected')
    async def stm32_connected():
        '''Check if engineering STM32 is connected.'''
        return {
            'connected': eng_stm32['iface'] is not None,
        }

    @router.post('/stm32/command')
    async def stm32_command(req: Stm32CmdRequest):
        '''Send an arbitrary command to the STM32.'''
        from ultra.hw.frame_protocol import (
            CMD_NAME_TO_ID,
        )
        if req.cmd not in CMD_NAME_TO_ID:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown command: {req.cmd}',
            )
        runner = app.get_runner()
        if runner.is_running and req.cmd != 'abort':
            raise HTTPException(
                status_code=409,
                detail='Protocol running -- only abort '
                       'allowed',
            )
        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected -- click '
                       'Connect first',
            )
        cmd_dict: dict[str, Any] = {'cmd': req.cmd}
        cmd_dict.update(req.params)

        lt = req.lock_timeout
        loop = asyncio.get_running_loop()
        try:
            if req.wait_done:
                result = await loop.run_in_executor(
                    None,
                    lambda: stm32.send_command_wait_done(
                        cmd=cmd_dict,
                        timeout_s=req.timeout_s,
                    ),
                )
            else:
                result = await loop.run_in_executor(
                    None,
                    lambda: stm32.send_command(
                        cmd=cmd_dict,
                        timeout_s=req.timeout_s,
                        lock_timeout=lt,
                    ),
                )
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f'Command error: {exc}',
            )
        if result is None and lt is not None:
            raise HTTPException(
                status_code=503,
                detail='serial busy',
            )
        return {
            'ok': result is not None,
            'response': result or {},
        }

    @router.get('/motor-status')
    async def motor_status():
        '''One-shot snapshot of X/Y/Z motor driver status.'''
        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected',
            )
        loop = asyncio.get_running_loop()
        r = await loop.run_in_executor(
            None,
            lambda: stm32.send_command(
                cmd={'cmd': 'get_motor_status'},
                timeout_s=3.0,
            ),
        )
        if r is None:
            raise HTTPException(
                status_code=504,
                detail='Motor status timed out',
            )
        return r

    @router.get('/motor-telemetry/stream')
    async def motor_telemetry_stream():
        '''SSE stream of motor telemetry samples.'''
        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected',
            )

        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        loop = asyncio.get_running_loop()

        def _on_sample(d: dict):
            try:
                loop.call_soon_threadsafe(
                    q.put_nowait, d,
                )
            except (asyncio.QueueFull, RuntimeError):
                pass

        stm32.set_motor_telem_callback(_on_sample)
        rsp = await loop.run_in_executor(
            None,
            lambda: stm32.send_command(
                cmd={'cmd': 'set_motor_telem', 'enable': True},
                timeout_s=2.0,
            ),
        )
        if rsp is None:
            stm32.set_motor_telem_callback(None)
            raise HTTPException(
                status_code=503,
                detail='Firmware did not acknowledge '
                       'set_motor_telem — command may '
                       'not be supported on this FW',
            )
        stm32.start_telem_reader()

        async def _generate():
            import json as _json
            try:
                while True:
                    try:
                        sample = await asyncio.wait_for(
                            q.get(), timeout=5.0,
                        )
                    except asyncio.TimeoutError:
                        yield 'event: ping\ndata: {}\n\n'
                        continue
                    yield (
                        f'data: {_json.dumps(sample)}\n\n'
                    )
            except asyncio.CancelledError:
                pass
            finally:
                stm32.stop_telem_reader()

        return StreamingResponse(
            _generate(),
            media_type='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',
            },
        )

    @router.get('/stm32/status')
    async def stm32_status():
        '''Return combined hardware status.'''
        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected',
            )
        loop = asyncio.get_running_loop()

        LIFT_USTEPS_PER_MM = 16.0 / 0.0254

        def _query():
            out: dict[str, Any] = {}
            try:
                r = stm32.send_command(
                    {'cmd': 'get_gantry_status'},
                    timeout_s=2.0,
                )
                if r:
                    r['x_mm'] = round(
                        r.get('x', 0) / 1000.0, 3,
                    )
                    r['y_mm'] = round(
                        r.get('y', 0) / 1000.0, 3,
                    )
                    r['z_mm'] = round(
                        r.get('z', 0) / 1000.0, 3,
                    )
                    out['gantry'] = r
            except Exception:
                pass
            try:
                r = stm32.send_command(
                    {'cmd': 'door_status'},
                    timeout_s=2.0,
                )
                if r:
                    out['door'] = r
            except Exception:
                pass
            try:
                r = stm32.send_command(
                    {'cmd': 'lift_status'},
                    timeout_s=2.0,
                )
                if r:
                    steps = r.get('position_steps', 0)
                    r['position_mm'] = round(
                        steps / LIFT_USTEPS_PER_MM, 2,
                    )
                    r['homed'] = r.get(
                        'is_homed', False,
                    )
                    out['lift'] = r
            except Exception:
                pass
            try:
                r = stm32.send_command(
                    {'cmd': 'pump_get_status'},
                    timeout_s=2.0,
                )
                if r:
                    out['pump'] = r
            except Exception:
                pass
            try:
                r = stm32.send_command(
                    {'cmd': 'centrifuge_status'},
                    timeout_s=2.0,
                )
                if r:
                    out['centrifuge'] = r
            except Exception:
                pass
            return out

        result = await loop.run_in_executor(
            None, _query,
        )
        return result

    @router.get('/stm32/commands')
    async def stm32_commands():
        '''Return list of valid STM32 command names.'''
        from ultra.hw.frame_protocol import (
            CMD_NAME_TO_ID,
        )
        return sorted(CMD_NAME_TO_ID.keys())

    # ---- Logs ----

    @router.get('/logs')
    async def get_logs():
        '''Return recent log lines from the ring buffer.'''
        from ultra.utils.logging import get_log_handler
        handler = get_log_handler()
        if handler is None:
            return {'lines': []}
        return {'lines': handler.get_lines()}

    # ---- Camera MJPEG streaming ----

    def _get_camera():
        cam = _camera_state['instance']
        if cam is None:
            from ultra.hw.camera import CameraStream
            cam_cfg = app.config.get('camera', {})
            device = cam_cfg.get(
                'device', '/dev/video0',
            )
            cam = CameraStream(device=device)
            _camera_state['instance'] = cam
        if not cam.is_running:
            cam.start()
        return cam

    @router.get('/camera/stream')
    async def camera_stream():
        '''Stream MJPEG frames from the USB camera.'''
        cam = _get_camera()
        return StreamingResponse(
            cam.generate_mjpeg(),
            media_type=(
                'multipart/x-mixed-replace; '
                'boundary=frame'
            ),
        )

    return router
