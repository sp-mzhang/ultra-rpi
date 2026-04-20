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

        # Refuse while the state machine needs the MSG_STATUS
        # stream.  /stm32/connect stops the monitor and takes
        # the UART exclusively, which would wedge the SM in
        # IDLE / SELF_CHECK / AWAITING_PROTOCOL_START because
        # no drawer events would ever be seen.
        sm = app._state_machine
        if sm is not None:
            from ultra.services.state_machine import (
                SystemState,
            )
            _SM_MONITOR_STATES = {
                SystemState.IDLE,
                SystemState.DRAWER_OPEN_LOAD_CARTRIDGE,
                SystemState.SELF_CHECK,
                SystemState.AWAITING_PROTOCOL_START,
            }
            if sm.state in _SM_MONITOR_STATES:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        'State machine is using the STM32 '
                        f'UART (state={sm.state.value}) -- '
                        'stop the state machine first '
                        'before connecting engineering'
                    ),
                )

        # Defense-in-depth: a previous protocol run (especially one
        # driven by the state machine) may have leaked an
        # STM32Interface that's still holding /dev/ttyAMA3 via its
        # RX/TX worker threads.  Opening another fd on the same
        # device "succeeds" on Linux but the two readers race for
        # incoming bytes and every command from the engineering
        # panel then times out.  Evict any such stragglers before
        # we open our own.
        try:
            leaked = getattr(runner, 'stm32', None)
            if (
                leaked is not None
                and not runner.is_running
                and leaked is not eng_stm32.get('iface')
            ):
                LOG.warning(
                    'Engineering connect: evicting leaked '
                    'protocol-owned STM32Interface before '
                    'opening engineering iface',
                )
                try:
                    leaked.disconnect()
                except Exception as derr:
                    LOG.debug(
                        'leaked stm32.disconnect failed: %s',
                        derr,
                    )
                runner.stm32 = None
        except Exception as err:
            LOG.debug(
                'Could not check for leaked runner stm32: %s',
                err,
            )

        from ultra.hw.stm32_monitor import (
            STM32StatusMonitor,
        )
        STM32StatusMonitor.stop_active()
        # Give the kernel tty buffer + Pi UART hardware time to fully
        # settle after the monitor thread's read()/close() — otherwise
        # the engineering Serial open occasionally inherits a tty state
        # where poll() says "readable" but read() returns 0 bytes, and
        # every command from the web UI times out until the service is
        # reset. 1.2 s is empirical; 0.3 s wasn't enough.
        await asyncio.sleep(1.2)

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
        if hasattr(stm32, 'apply_motion_defaults_from_config'):
            stm32.apply_motion_defaults_from_config(app.config)
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

        # Drop any bytes the Pi's kernel buffered during the handoff
        # from the monitor thread. Without this, a stale half-frame
        # from the monitor era can mis-align the FrameParser and make
        # the first few commands appear to hang.
        try:
            stm32._ser.reset_input_buffer()
            stm32._ser.reset_output_buffer()
        except Exception:
            pass

        # Pipe accel stream batches onto the event bus so the
        # WebSocket broadcaster fans them out to the browser.
        # Callback fires from the interface reader thread, so
        # use emit_sync which bounces through call_soon_threadsafe.
        stm32.set_accel_stream_callback(
            lambda d: app.event_bus.emit_sync(
                'accel_stream', d,
            ),
        )

        # Start the background telemetry reader. Without this,
        # async push frames (accel stream 0x9E03, motor telemetry
        # 0xBxxx, etc.) stay stuck in the kernel RX buffer until
        # the next send_command call, which starves the browser
        # of stream samples. The reader coordinates with
        # send_command via the interface's internal _lock.
        stm32.start_telem_reader()

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
                stm32.stop_telem_reader()
            except Exception:
                pass
            try:
                stm32.set_accel_stream_callback(None)
            except Exception:
                pass
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

    @router.get('/stm32/position')
    async def stm32_position(
        lift: bool = False,
    ):
        '''Return gantry positions + homing flags (1 command).

        Uses get_gantry_status which returns X/Y/Z positions
        and homing flags in a single command.  When lift=true,
        also calls lift_status for lift position (+1 command).
        '''
        from ultra.hw.frame_protocol import (
            GANTRY_XY_USTEPS_PER_MM,
            LIFT_USTEPS_PER_MM,
            Z_USTEPS_PER_MM,
        )
        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected',
            )
        loop = asyncio.get_running_loop()
        include_lift = lift

        def _query_pos():
            out: dict[str, Any] = {}
            gantry: dict[str, Any] = {}
            try:
                r = stm32.send_command(
                    {'cmd': 'get_gantry_status'},
                    timeout_s=2.0,
                )
                if r:
                    gantry['x_mm'] = round(
                        r.get('x', 0)
                        / GANTRY_XY_USTEPS_PER_MM, 3,
                    )
                    gantry['y_mm'] = round(
                        r.get('y', 0)
                        / GANTRY_XY_USTEPS_PER_MM, 3,
                    )
                    gantry['z_mm'] = round(
                        r.get('z', 0)
                        / Z_USTEPS_PER_MM, 3,
                    )
                    gantry['x_homed'] = r.get(
                        'x_homed', False,
                    )
                    gantry['y_homed'] = r.get(
                        'y_homed', False,
                    )
                    gantry['z_homed'] = r.get(
                        'z_homed', False,
                    )
            except Exception:
                pass
            if include_lift:
                try:
                    r = stm32.send_command(
                        {'cmd': 'lift_status'},
                        timeout_s=2.0,
                    )
                    if r:
                        steps = r.get(
                            'position_steps', 0,
                        )
                        out['lift'] = {
                            'position_mm': round(
                                steps
                                / LIFT_USTEPS_PER_MM, 2,
                            ),
                            'homed': r.get(
                                'is_homed', False,
                            ),
                        }
                except Exception:
                    pass
            out['gantry'] = gantry
            return out

        return await loop.run_in_executor(
            None, _query_pos,
        )

    @router.get('/stm32/status')
    async def stm32_status():
        '''Return combined hardware status (all subsystems).'''
        from ultra.hw.frame_protocol import (
            GANTRY_XY_USTEPS_PER_MM,
            LIFT_USTEPS_PER_MM,
            Z_USTEPS_PER_MM,
        )
        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected',
            )
        loop = asyncio.get_running_loop()

        def _query():
            out: dict[str, Any] = {}
            gantry: dict[str, Any] = {}
            try:
                r = stm32.send_command(
                    {'cmd': 'get_gantry_status'},
                    timeout_s=2.0,
                )
                if r:
                    gantry['x_mm'] = round(
                        r.get('x', 0)
                        / GANTRY_XY_USTEPS_PER_MM, 3,
                    )
                    gantry['y_mm'] = round(
                        r.get('y', 0)
                        / GANTRY_XY_USTEPS_PER_MM, 3,
                    )
                    gantry['z_mm'] = round(
                        r.get('z', 0)
                        / Z_USTEPS_PER_MM, 3,
                    )
                    gantry['x_homed'] = r.get(
                        'x_homed', False,
                    )
                    gantry['y_homed'] = r.get(
                        'y_homed', False,
                    )
                    gantry['z_homed'] = r.get(
                        'z_homed', False,
                    )
            except Exception:
                pass
            try:
                r = stm32.send_command(
                    {'cmd': 'lift_status'},
                    timeout_s=2.0,
                )
                if r:
                    steps = r.get(
                        'position_steps', 0,
                    )
                    out['lift'] = {
                        'position_mm': round(
                            steps
                            / LIFT_USTEPS_PER_MM, 2,
                        ),
                        'homed': r.get(
                            'is_homed', False,
                        ),
                    }
            except Exception:
                pass
            out['gantry'] = gantry
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

    @router.get('/diag/accel_counters')
    async def diag_accel_counters():
        '''Snapshot of the accel-stream pipeline counters on the Pi.

        Compare with STM32 isr_seq delta (firmware sent N batches)
        and the browser's "Dropped" counter (N - decoded_in_browser):

          accel_dispatched      <-- stm32_interface decoded N batches
          accel_broadcast_calls <-- broadcaster.broadcast invoked N times
          accel_ws_sends        <-- successful ws.send_text per client
          accel_ws_drops        <-- ws.send_text raised (connection dead)
        '''
        from ultra.hw.stm32_interface import STM32Interface
        from ultra.gui.server import WebSocketBroadcaster
        return {
            'accel_dispatched':
                STM32Interface.accel_dispatched,
            'accel_cb_exceptions':
                STM32Interface.accel_cb_exceptions,
            'accel_broadcast_calls':
                WebSocketBroadcaster.accel_broadcast_calls,
            'accel_ws_sends':
                WebSocketBroadcaster.accel_ws_sends,
            'accel_ws_drops':
                WebSocketBroadcaster.accel_ws_drops,
        }

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
