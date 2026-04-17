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


class TubeRoiBody(BaseModel):
    '''Request body for ``POST /api/camera/tube-roi``.

    All four fields are in native-frame pixels. The zero
    sentinel ``(0, 0, 0, 0)`` means "clear ROI, detector falls
    back to full frame"; otherwise the detector clamps into the
    frame.
    '''
    x: int = 0
    y: int = 0
    w: int = 0
    h: int = 0


class TubeRefBody(BaseModel):
    '''Request body for ``POST /api/camera/tube-refs``.

    ``label`` is the class name (``seated`` or ``empty``) the
    captured ROI crop will be tagged with.
    '''
    label: str


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
        # The monitor's serial close can leave the Pi's PL011 tty
        # in a wedged state where subsequent writes silently fail
        # (bytes never reach the wire). Force-reset the tty via
        # stty before reopening so the driver reinitialises cleanly.
        import subprocess
        stm32_port = app.config.get('stm32', {}).get(
            'port', '/dev/ttyAMA3',
        )
        stm32_baud = app.config.get('stm32', {}).get(
            'baud', 921600,
        )
        await asyncio.sleep(0.5)
        try:
            subprocess.run(
                ['stty', '-F', stm32_port,
                 str(stm32_baud), 'cs8', '-cstopb',
                 '-parenb', '-crtscts', 'raw',
                 '-echo', '-echoe', '-echok'],
                timeout=2, check=False,
            )
        except Exception:
            pass
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
        # Singleton; shared with the protocol step
        # (align_to_carousel) so we don't fight over /dev/video0.
        from ultra.hw.camera_singleton import get_camera
        cam = get_camera(app.config)
        # Mirror the legacy ``_camera_state`` dict so other
        # closures in this module (and downstream code paths
        # still reading via ``_eng_state.camera``) see a
        # populated handle.
        _camera_state['instance'] = cam
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

    # ---- Carousel alignment (blister / pipette via markers) ----

    # Cache the most recent annotated alignment frame so the GUI
    # can display it after the POST returns. Kept in module-local
    # state (no persistence) -- survives only until the process
    # restarts. Includes a monotonic timestamp so the GUI can bust
    # its image cache with a query-string param.
    _last_align_frame: dict = {'jpeg': None, 'ts': 0.0}

    def _load_aligner():
        '''Build a CarouselAligner from the current app config.

        Thin wrapper around
        :func:`ultra.vision.align_runner.build_aligner_from_config`
        so both the GUI route and the recipe step see the same
        derivation of station angles, side configs, etc.
        '''
        from ultra.vision.align_runner import (
            build_aligner_from_config,
        )
        return build_aligner_from_config(app.config)

    def _cache_annotated_frame(frame_bgr, result):
        '''Render alignment overlay on ``frame_bgr`` and cache as
        JPEG. Failures are logged, not raised -- the alignment
        response is still useful without the preview image.'''
        try:
            import time as _time
            import cv2
            from ultra.vision.carousel_align import annotate
            overlay = annotate(frame_bgr, result)
            ok, buf = cv2.imencode(
                '.jpg', overlay, [cv2.IMWRITE_JPEG_QUALITY, 80],
            )
            if ok:
                _last_align_frame['jpeg'] = bytes(buf)
                _last_align_frame['ts'] = _time.time()
        except Exception as exc:
            LOG.warning(
                'annotate/encode failed: %s', exc,
            )

    def _grab_bgr_frame(settle_ms: int = 0):
        '''Start the camera if needed, return a *fresh* BGR frame.

        We capture a baseline timestamp BEFORE the settle, then
        wait for a frame captured strictly after ``baseline +
        settle_ms``. This guarantees the returned image reflects
        the world *after* LED turn-on and whatever else the caller
        did immediately before -- any frame that happened to be
        cached from a previous call is rejected.

        Returns None if the camera never produced a fresh frame
        within a ~5 s budget (covers the case where the USB cam
        got bumped and is mid-reconnect).
        '''
        import time
        cam = _get_camera()
        baseline_ts = cam.latest_frame_ts()
        settle_s = max(0.0, settle_ms / 1000.0)
        if settle_s > 0:
            time.sleep(settle_s)
        # At this point we want a frame captured *after* the LED
        # settle completed. If the capture thread stalled during
        # the settle we may have to wait a further few seconds
        # while start()/auto-detect recovers the device.
        # Total budget = settle + 5 s.
        frame, _ts = cam.latest_frame_bgr(
            newer_than=baseline_ts,
            wait_s=5.0,
        )
        return frame

    def _reading_to_dict(m) -> dict:
        return {
            'payload': m.payload,
            'angle_deg': round(m.angle_deg, 3),
            'center_px': [
                round(m.center_px[0], 1),
                round(m.center_px[1], 1),
            ],
            'size_px': [
                round(m.size_px[0], 1),
                round(m.size_px[1], 1),
            ],
        }

    def _result_to_dict(r, extras: dict | None = None) -> dict:
        out: dict = {
            'side': r.side,
            'avg_deg': (
                round(r.avg_deg, 3)
                if r.avg_deg is not None else None
            ),
            'reference_deg': r.reference_deg,
            'c_cw_deg': (
                round(r.c_cw_deg, 3)
                if r.c_cw_deg is not None else None
            ),
            'delta_motor_deg': (
                round(r.delta_motor_deg, 3)
                if r.delta_motor_deg is not None else None
            ),
            'polarity': r.polarity,
            'markers': [_reading_to_dict(m) for m in r.markers],
            'reason': r.reason,
        }
        if extras:
            out.update(extras)
        return out

    @router.get('/camera/last-alignment-frame')
    async def camera_last_alignment_frame():
        '''Return the most recent annotated alignment JPEG.

        Populated by POST /camera/align-carousel each time it
        runs. 404 until the first alignment has been attempted.
        '''
        from fastapi.responses import Response
        jpeg = _last_align_frame.get('jpeg')
        if not jpeg:
            raise HTTPException(
                status_code=404,
                detail='No alignment frame yet',
            )
        return Response(
            content=jpeg,
            media_type='image/jpeg',
            headers={'Cache-Control': 'no-store'},
        )

    @router.post('/camera/align-carousel')
    async def align_carousel():
        '''One-shot carousel alignment via toolhead camera markers.

        Home Z, move gantry to probe pose, LED on, detect markers,
        auto-classify side by decoded payload (L/T/R/U -> blister;
        other sets defined in ``carousel_align.sides`` -> that side),
        compute offset vs that side's reference angle, rotate the
        centrifuge to cancel, LED off. Refuses if a recipe is
        currently running.

        Body of the work is in
        :func:`ultra.vision.align_runner.run_alignment`; the recipe
        step ``align_to_carousel`` calls the same helper so both
        paths execute identical logic.
        '''
        from ultra.vision.align_runner import run_alignment

        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected -- click '
                       'Connect first',
            )
        runner = app.get_runner()
        if runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='Protocol running -- refusing alignment',
            )
        aligner, cfg = _load_aligner()

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_alignment(
                stm32=stm32,
                aligner=aligner,
                align_cfg=cfg,
                get_frame=_grab_bgr_frame,
                cache_frame=_cache_annotated_frame,
            ),
        )
        # Always include the cached frame timestamp so the GUI
        # can bust its <img> cache regardless of success.
        result.payload['frame_ts'] = _last_align_frame.get('ts', 0)
        return result.payload

    # ---- Cartridge QR + serum-tube debug endpoints ----
    #
    # Two small module-local caches keep the most recent annotated
    # preview from each check so the engineering-panel buttons can
    # render a thumbnail. Mirrors the ``_last_align_frame`` pattern
    # a few dozen lines up. No persistence; state dies with the
    # process.
    _last_qr_frame: dict = {'jpeg': None, 'ts': 0.0}
    # ``raw_bgr`` is kept alongside the annotated JPEG so the
    # reference-capture endpoint can crop the unmodified camera
    # frame (overlay pixels in the JPEG would contaminate NCC
    # scores). ``None`` until the first tube check runs.
    _last_tube_frame: dict = {
        'jpeg': None, 'ts': 0.0, 'raw_bgr': None,
    }

    def _cache_qr_frame(frame_bgr, det):
        '''Render QR overlay + cache as JPEG. Failures are logged.'''
        try:
            import time as _time
            import cv2
            from ultra.vision.qr_detect import annotate as qr_annotate
            overlay = qr_annotate(frame_bgr, det)
            ok, buf = cv2.imencode(
                '.jpg', overlay, [cv2.IMWRITE_JPEG_QUALITY, 80],
            )
            if ok:
                _last_qr_frame['jpeg'] = bytes(buf)
                _last_qr_frame['ts'] = _time.time()
        except Exception as exc:
            LOG.warning('qr annotate/encode failed: %s', exc)

    def _cache_tube_frame(frame_bgr, det):
        '''Cache both the annotated JPEG preview and the raw BGR.

        :class:`TubeDetection` already carries an ``annotated``
        image for the preview JPEG. We also stash a copy of
        ``frame_bgr`` so the reference-capture endpoint can
        re-crop the unmodified pixels without any overlay
        artefacts.
        '''
        try:
            import time as _time
            import cv2
            img = getattr(det, 'annotated', None)
            if img is None:
                img = frame_bgr
            ok, buf = cv2.imencode(
                '.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 80],
            )
            if ok:
                _last_tube_frame['jpeg'] = bytes(buf)
                _last_tube_frame['ts'] = _time.time()
                _last_tube_frame['raw_bgr'] = (
                    frame_bgr.copy()
                    if frame_bgr is not None else None
                )
        except Exception as exc:
            LOG.warning('tube annotate/encode failed: %s', exc)

    @router.get('/camera/last-qr-frame')
    async def camera_last_qr_frame():
        '''Return the most recent annotated cartridge-QR JPEG.'''
        from fastapi.responses import Response
        jpeg = _last_qr_frame.get('jpeg')
        if not jpeg:
            raise HTTPException(
                status_code=404,
                detail='No QR frame yet',
            )
        return Response(
            content=jpeg,
            media_type='image/jpeg',
            headers={'Cache-Control': 'no-store'},
        )

    @router.get('/camera/last-tube-frame')
    async def camera_last_tube_frame():
        '''Return the most recent annotated tube-presence JPEG.'''
        from fastapi.responses import Response
        jpeg = _last_tube_frame.get('jpeg')
        if not jpeg:
            raise HTTPException(
                status_code=404,
                detail='No tube frame yet',
            )
        return Response(
            content=jpeg,
            media_type='image/jpeg',
            headers={'Cache-Control': 'no-store'},
        )

    @router.post('/camera/check-cartridge-qr')
    async def check_cartridge_qr():
        '''One-shot cartridge-QR decode (engineering debug).

        Mirrors ``POST /camera/align-carousel``: drives
        :func:`ultra.vision.check_runner.run_cartridge_qr_check`
        with the engineering STM32 interface so the operator can
        validate label reading, LED timing, and probe pose
        without running the full state machine.
        '''
        from ultra.vision.check_runner import run_cartridge_qr_check

        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected -- click '
                       'Connect first',
            )
        runner = app.get_runner()
        if runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='Protocol running -- refusing QR check',
            )

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_cartridge_qr_check(
                stm32=stm32,
                config=app.config,
                get_frame=_grab_bgr_frame,
                cache_frame=_cache_qr_frame,
            ),
        )
        payload = {
            'ok': result.ok,
            'reason': result.reason,
            'payload': result.payload,
            'frame_ts': _last_qr_frame.get('ts', 0),
            **(result.extras or {}),
        }
        return payload

    @router.post('/camera/check-serum-tube')
    async def check_serum_tube():
        '''One-shot serum-tube presence check (engineering debug).

        Drives :func:`ultra.vision.check_runner.run_serum_tube_check`
        so the operator can tune the ROI, capture template
        references, or inspect saturation stats against a live
        cartridge. The annotated preview and raw BGR frame are
        cached for ``GET /camera/last-tube-frame`` and the
        reference-capture endpoint.
        '''
        from ultra.vision.check_runner import run_serum_tube_check

        stm32 = get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=503,
                detail='STM32 not connected -- click '
                       'Connect first',
            )
        runner = app.get_runner()
        if runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='Protocol running -- refusing tube check',
            )

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_serum_tube_check(
                stm32=stm32,
                config=app.config,
                get_frame=_grab_bgr_frame,
                cache_frame=_cache_tube_frame,
            ),
        )
        payload = {
            'ok': result.ok,
            'reason': result.reason,
            'frame_ts': _last_tube_frame.get('ts', 0),
            **(result.extras or {}),
        }
        return payload

    # ---- Tube ROI calibration (GUI picker) ----
    #
    # Workflow: operator clicks "Check Serum Tube" to capture a
    # preview with the current ROI drawn on it, drags a tighter
    # rectangle on the overlay, then clicks Save -- which POSTs
    # here. We update app.config in-memory (takes effect on the
    # next check) AND persist to /etc/ultra/machine.yaml (the
    # ULTRA_CONFIG overlay loaded on startup) so the value
    # survives a service restart. S3 upload piggybacks when a
    # device_sn is available, keeping the fleet copy in sync
    # without manual intervention. The config/ultra_default.yaml
    # defaults file is never touched -- only the overlay is
    # machine-specific, and the overlay is the only file that
    # should diverge between units.

    @router.get('/camera/tube-roi')
    async def camera_tube_roi_get():
        '''Return the current tube ROI from ``app.config``.

        Also reports the last cached tube frame's pixel dims so
        the GUI can scale the overlay correctly.
        '''
        tube_cfg = (
            (app.config.get('checks', {}) or {}).get('tube', {}) or {}
        )
        roi = tube_cfg.get('roi', {}) or {}
        frame_w = 0
        frame_h = 0
        jpeg = _last_tube_frame.get('jpeg')
        if jpeg:
            try:
                import cv2
                import numpy as np
                arr = np.frombuffer(jpeg, dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if img is not None:
                    frame_h, frame_w = img.shape[:2]
            except Exception as exc:
                LOG.debug('tube-roi: cannot probe frame dims: %s', exc)
        return {
            'roi': {
                'x': int(roi.get('x', 0) or 0),
                'y': int(roi.get('y', 0) or 0),
                'w': int(roi.get('w', 0) or 0),
                'h': int(roi.get('h', 0) or 0),
            },
            'frame_w': int(frame_w),
            'frame_h': int(frame_h),
            'frame_ts': _last_tube_frame.get('ts', 0),
        }

    @router.post('/camera/tube-roi')
    async def camera_tube_roi_set(body: TubeRoiBody):
        '''Persist a new tube ROI into ``app.config`` (in-memory).

        Returns the echoed ROI plus a ``persist_hint`` string so
        the UI can display the values for pasting into
        ``config/ultra_default.yaml`` (keys ``checks.tube.roi``).
        '''
        checks = app.config.setdefault('checks', {})
        tube = checks.setdefault('tube', {})
        roi = tube.setdefault('roi', {})
        x = max(0, int(body.x))
        y = max(0, int(body.y))
        w = max(0, int(body.w))
        h = max(0, int(body.h))
        roi['x'] = x
        roi['y'] = y
        roi['w'] = w
        roi['h'] = h
        LOG.info(
            'tube ROI updated in-memory: x=%d y=%d w=%d h=%d',
            x, y, w, h,
        )

        # Persist to /etc/ultra/machine.yaml so the value
        # survives a restart. Run in the default executor
        # because both local write and the S3 upload can block.
        from ultra.gui.api_config import persist_config_overlay
        loop = asyncio.get_running_loop()
        try:
            persisted = await loop.run_in_executor(
                None,
                lambda: persist_config_overlay(
                    ['checks', 'tube', 'roi'],
                    {'x': x, 'y': y, 'w': w, 'h': h},
                    upload_s3=True,
                    app_config=app.config,
                ),
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            ) from exc
        except Exception as exc:
            LOG.exception('persist tube ROI failed')
            raise HTTPException(
                status_code=500,
                detail=f'persist failed: {exc}',
            ) from exc

        # Build a concrete status string so the GUI can show
        # exactly where the value landed.
        bits = []
        if persisted['local_written']:
            bits.append(f"saved to {persisted['local_path']}")
        else:
            bits.append(
                f"could NOT write {persisted['local_path']} "
                '(service lacks permission? run as root or '
                'fix directory ownership)',
            )
        if persisted['s3_uploaded']:
            bits.append('uploaded to S3')
        elif persisted['s3_error']:
            bits.append(f"S3 skipped: {persisted['s3_error']}")
        status_msg = '; '.join(bits)

        return {
            'ok': True,
            'roi': {'x': x, 'y': y, 'w': w, 'h': h},
            'persisted': persisted,
            'status': status_msg,
            'persist_hint': (
                status_msg
                + '. Restart the service to reload from disk '
                'if you want to verify, but the change is '
                'already live in memory.'
            ),
        }

    # ---- Tube template references (GUI gallery) ----
    #
    # Saved PNG crops of the current ROI, labelled SEATED /
    # EMPTY. When both classes have >= 1 ref, the detector's
    # template path replaces the saturation-only verdict. These
    # endpoints are the only path to create / delete / view the
    # on-disk references; the detector loads them lazily on
    # every check.

    def _tube_refs_dir() -> str:
        from ultra.vision.check_runner import _resolve_refs_dir
        tube_cfg = (
            (app.config.get('checks', {}) or {}).get('tube', {})
            or {}
        )
        tmpl_cfg = tube_cfg.get('templates', {}) or {}
        return _resolve_refs_dir(tmpl_cfg.get('dir', 'tube_refs'))

    def _current_roi_crop_from_frame():
        '''Pull the raw-BGR frame from the tube cache + crop to ROI.

        Returns ``(crop_bgr, (x, y, w, h))`` on success or
        ``(None, reason_str)`` on any failure. The crop must match
        the pixels the detector actually sees, so we use the raw
        BGR cached at check time, not the annotated preview.
        '''
        raw = _last_tube_frame.get('raw_bgr')
        if raw is None:
            return (
                None,
                'no cached raw tube frame -- run Check Serum '
                'Tube first so the capture pipeline has a frame',
            )
        tube_cfg = (
            (app.config.get('checks', {}) or {}).get('tube', {})
            or {}
        )
        roi = tube_cfg.get('roi', {}) or {}
        x = int(roi.get('x', 0) or 0)
        y = int(roi.get('y', 0) or 0)
        w = int(roi.get('w', 0) or 0)
        h = int(roi.get('h', 0) or 0)
        fh, fw = raw.shape[:2]
        if w <= 0 or h <= 0:
            return (
                None,
                'ROI is unset (0,0,0,0) -- calibrate ROI before '
                'capturing references',
            )
        x = max(0, min(x, fw - 1))
        y = max(0, min(y, fh - 1))
        w = max(1, min(w, fw - x))
        h = max(1, min(h, fh - y))
        crop = raw[y:y + h, x:x + w].copy()
        return crop, (x, y, w, h)

    @router.get('/camera/tube-refs')
    async def camera_tube_refs_list():
        '''Return metadata for every saved reference ROI.'''
        from ultra.vision import tube_template
        try:
            items = tube_template.list_references(_tube_refs_dir())
        except Exception as exc:
            LOG.exception('tube-refs list failed')
            raise HTTPException(
                status_code=500,
                detail=f'list refs failed: {exc}',
            ) from exc
        tube_cfg = (
            (app.config.get('checks', {}) or {}).get('tube', {})
            or {}
        )
        roi = tube_cfg.get('roi', {}) or {}
        cur_w = int(roi.get('w', 0) or 0)
        cur_h = int(roi.get('h', 0) or 0)
        for it in items:
            it['matches_current_roi'] = (
                cur_w > 0 and cur_h > 0
                and it['width'] == cur_w
                and it['height'] == cur_h
            )
            # Strip the server path from the response; the GUI
            # only needs the filename.
            it.pop('path', None)
        return {
            'items': items,
            'current_roi': {
                'w': cur_w, 'h': cur_h,
            },
            'seated_count': sum(
                1 for i in items if i['label'] == 'seated'
            ),
            'empty_count': sum(
                1 for i in items if i['label'] == 'empty'
            ),
        }

    @router.post('/camera/tube-refs')
    async def camera_tube_refs_create(body: TubeRefBody):
        '''Capture the current ROI crop as a labelled reference.

        Requires a prior ``POST /camera/check-serum-tube`` so the
        frame cache is populated. The crop is taken from the raw
        camera frame (not the annotated preview) so overlay
        pixels don't contaminate the reference.
        '''
        from ultra.vision import tube_template
        label = (body.label or '').strip().lower()
        if label not in (
            tube_template.LABEL_SEATED, tube_template.LABEL_EMPTY,
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f'invalid label {body.label!r}; expected '
                    '"seated" or "empty"'
                ),
            )
        crop, roi_or_reason = _current_roi_crop_from_frame()
        if crop is None:
            raise HTTPException(
                status_code=409,
                detail=str(roi_or_reason),
            )
        try:
            path = tube_template.save_reference(
                _tube_refs_dir(), label, crop,
            )
        except (ValueError, OSError) as exc:
            raise HTTPException(
                status_code=500,
                detail=f'save failed: {exc}',
            ) from exc
        import os.path as op
        filename = op.basename(path)
        return {
            'ok': True,
            'filename': filename,
            'label': label,
            'roi': list(roi_or_reason),
        }

    @router.delete('/camera/tube-refs/{filename}')
    async def camera_tube_refs_delete(filename: str):
        '''Remove one saved reference by filename.'''
        from ultra.vision import tube_template
        try:
            removed = tube_template.delete_reference(
                _tube_refs_dir(), filename,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            ) from exc
        if not removed:
            raise HTTPException(
                status_code=404,
                detail=f'no such reference: {filename}',
            )
        return {'ok': True, 'filename': filename}

    @router.get('/camera/tube-refs/{filename}')
    async def camera_tube_refs_image(filename: str):
        '''Return the raw PNG bytes of one saved reference.'''
        from fastapi.responses import FileResponse
        import os.path as op
        if (
            not filename or '/' in filename or '\\' in filename
            or '..' in filename
        ):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename {filename!r}',
            )
        path = op.join(_tube_refs_dir(), filename)
        if not op.isfile(path):
            raise HTTPException(
                status_code=404,
                detail=f'no such reference: {filename}',
            )
        return FileResponse(
            path,
            media_type='image/png',
            headers={'Cache-Control': 'no-store'},
        )

    return router
