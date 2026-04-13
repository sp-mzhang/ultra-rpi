'''ultra.gui.api -- REST API endpoints for the web GUI.

Provides endpoints for protocol control, status queries,
recipe listing, camera streaming, and state machine
management.
'''
from __future__ import annotations

import asyncio
import logging
import threading
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Query
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


class FirmwareFlashRequest(BaseModel):
    '''Request body for flashing firmware.'''
    key: str


class YamlTextBody(BaseModel):
    '''YAML payload for machine settings or recipe save.'''
    yaml_text: str = ''


class FcLiquidSeqRequest(BaseModel):
    '''Request body for the FC liquid test sequence.'''
    source_well: str = 'M1'
    aspirate_vol_ul: float = 200
    cart_vol_ul: float = 80
    aspirate_speed_ul_s: float = 80
    cart_vel_ul_s: float = 1.0


def _machine_settings_effective_yaml(
        cfg: dict[str, Any],
) -> str:
    '''Serialize the full effective in-memory config as YAML.

    Used as the editor fallback when no S3 object exists yet.
    '''
    import yaml

    header = (
        '# machine_settings.yaml — full effective config.\n'
        '# Edit any keys and Save to S3.\n\n'
    )
    try:
        body = yaml.dump(
            cfg,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    except Exception:
        LOG.exception('Cannot serialize config to YAML')
        return header + '# Error: see server log.\n'
    return header + body


def _parse_and_merge_machine_yaml(
        yaml_text: str,
        app_config: dict[str, Any],
) -> dict[str, Any]:
    '''Parse *yaml_text*, deep-merge into *app_config*, return new config.

    Raises ``ValueError`` when the YAML is not a mapping.
    '''
    import yaml
    from ultra.config import merge_config

    try:
        parsed = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        raise ValueError(f'Invalid YAML: {exc}') from exc
    if parsed is None:
        parsed = {}
    if not isinstance(parsed, dict):
        raise ValueError(
            'Machine settings must be a YAML mapping, '
            'not a list or scalar.',
        )
    return merge_config(app_config, parsed)


class SMRequest(BaseModel):
    '''Request body for state machine control.'''
    action: str = 'start'


class Stm32CmdRequest(BaseModel):
    '''Request body for a raw STM32 command.'''
    cmd: str
    params: dict[str, Any] = {}
    timeout_s: float = 30.0
    wait_done: bool = True
    lock_timeout: float | None = None


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

        eng = _eng_stm32['iface']
        if eng is not None:
            try:
                eng.disconnect()
            except Exception:
                pass
            _eng_stm32['iface'] = None

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
        '''Abort the running protocol.

        Sends CMD_ABORT to the firmware, interrupts the
        current serial wait, and signals the runner to stop.
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

    @router.post('/egress/clear')
    async def egress_clear():
        '''Mark all pending runs as egressed.

        Prevents the startup scan from re-queuing cleared
        runs. Returns ``{cleared: N}`` with the count of
        rows that were marked.
        '''
        db = _get_egress_db()
        if db is None:
            raise HTTPException(
                status_code=503,
                detail='Egress not enabled',
            )
        n = db.clear_all()
        return {'cleared': n}

    @router.post('/egress/clear_uploaded')
    async def egress_clear_uploaded():
        '''Delete only successfully uploaded (egressed) runs.

        Keeps pending and errored rows so they can still be
        retried. Returns ``{deleted: N}``.
        '''
        db = _get_egress_db()
        if db is None:
            raise HTTPException(
                status_code=503,
                detail='Egress not enabled',
            )
        n = db.clear_egressed()
        return {'deleted': n}

    # ---- STM32 engineering endpoints ----

    _eng_stm32 = {'iface': None}

    def _get_eng_stm32():
        '''Return the engineering STM32 interface.

        Only available after POST /api/stm32/connect.
        Returns None when not connected.
        '''
        return _eng_stm32['iface']

    @router.post('/stm32/connect')
    async def stm32_connect():
        '''Connect the engineering STM32 interface.

        Stops the STM32StatusMonitor so the UART is
        free, then creates and connects an STM32Interface.
        Returns firmware version on success.
        '''
        if _eng_stm32['iface'] is not None:
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
        _eng_stm32['iface'] = stm32

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
        '''Disconnect engineering STM32 and restart monitor.

        Releases the UART and resumes the background
        STM32StatusMonitor for door/sensor listening.
        '''
        stm32 = _eng_stm32['iface']
        if stm32 is not None:
            try:
                stm32.disconnect()
            except Exception:
                pass
            _eng_stm32['iface'] = None
            LOG.info('Engineering STM32 disconnected')

        if app._monitor:
            app._monitor.start()
        return {'ok': True}

    @router.get('/stm32/connected')
    async def stm32_connected():
        '''Check if engineering STM32 is connected.'''
        return {
            'connected': _eng_stm32['iface'] is not None,
        }

    @router.post('/stm32/command')
    async def stm32_command(req: Stm32CmdRequest):
        '''Send an arbitrary command to the STM32.

        Validates the command name against the firmware
        vocabulary and rejects requests while a protocol
        is actively running (except ``abort``).
        '''
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
        stm32 = _get_eng_stm32()
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
        stm32 = _get_eng_stm32()
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
        '''SSE stream of motor telemetry samples.

        Enables firmware telemetry on connect, disables
        on disconnect. Yields JSON samples at ~10ms rate.
        '''
        from starlette.responses import StreamingResponse

        stm32 = _get_eng_stm32()
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
        '''Return combined hardware status.

        Queries gantry position, door, lift, pump, and
        centrifuge status in one call for the engineering
        UI position display.
        '''
        stm32 = _get_eng_stm32()
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

    # -------------------------------------------------------
    # Firmware OTA update
    # -------------------------------------------------------

    @router.get('/firmware/list')
    async def firmware_list():
        '''List available firmware builds from S3.

        Returns a JSON array of version objects with
        version, key, size, date, and is_latest fields.
        '''
        from ultra.services import fw_update
        loop = asyncio.get_running_loop()
        try:
            builds = await loop.run_in_executor(
                None, fw_update.list_firmware,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f'S3 error: {exc}',
            )
        return builds

    @router.post('/firmware/flash')
    async def firmware_flash(req: FirmwareFlashRequest):
        '''Download and flash a firmware binary.

        Stops the STM32 monitor and engineering interface
        to free the UART, then runs the download + flash
        sequence in a background thread.
        '''
        from ultra.services import fw_update

        status = fw_update.get_status()
        if status['state'] in (
            'downloading', 'flashing',
        ):
            raise HTTPException(
                status_code=409,
                detail='Flash already in progress',
            )

        runner = app.get_runner()
        if runner.is_running:
            raise HTTPException(
                status_code=409,
                detail='Protocol running',
            )

        from ultra.hw.stm32_monitor import (
            STM32StatusMonitor,
        )
        STM32StatusMonitor.stop_active()

        stm32 = _eng_stm32.get('iface')
        if stm32 is not None:
            try:
                stm32.disconnect()
            except Exception:
                pass
            _eng_stm32['iface'] = None

        if app._monitor:
            app._monitor.stop()
        await asyncio.sleep(0.5)

        import threading
        t = threading.Thread(
            target=fw_update.download_and_flash,
            args=(req.key,),
            daemon=True,
        )
        t.start()

        return {'ok': True, 'message': 'Flash started'}

    @router.get('/firmware/status')
    async def firmware_status(log_offset: int = 0):
        '''Return current firmware flash status.

        Args:
            log_offset: Only return log lines from this
                index onward.

        Returns:
            Dict with state, progress, message, log,
            log_total.
        '''
        from ultra.services import fw_update
        return fw_update.get_status(log_offset)

    # -------------------------------------------------------
    # Machine settings + global recipes (S3)
    # -------------------------------------------------------

    @router.get('/machine-settings')
    async def machine_settings_get(
            apply: bool = Query(
                False,
                description='Reload from S3 and merge into app.config.',
            ),
    ):
        '''Return YAML for the machine settings editor.

        Returns the raw S3 object when present; otherwise a dump of
        ``app.config``.  With ``apply=true`` (Reload button) the S3
        content is also deep-merged into ``app.config``.
        '''
        from ultra.services import config_store
        ds = app.config.get('device_sn', '')
        if not ds:
            raise HTTPException(
                status_code=400,
                detail='device_sn not set in config',
            )
        loop = asyncio.get_running_loop()

        def _load() -> tuple[str, str, bool]:
            raw = config_store.fetch_machine_settings_yaml(
                ds, force=apply,
            )
            if raw and raw.strip():
                applied = False
                if apply:
                    try:
                        app.config = _parse_and_merge_machine_yaml(
                            raw, app.config,
                        )
                        applied = True
                    except ValueError as exc:
                        LOG.warning('apply machine_settings: %s', exc)
                return raw, 's3', applied
            return (
                _machine_settings_effective_yaml(app.config),
                'defaults',
                False,
            )

        yaml_text, source, applied = await loop.run_in_executor(
            None, _load,
        )
        return {
            'device_sn': ds,
            'yaml_text': yaml_text,
            'source': source,
            'applied': applied,
        }

    @router.put('/machine-settings')
    @router.post('/machine-settings')
    async def machine_settings_put(req: YamlTextBody):
        '''Save machine_settings.yaml to S3 and merge into app.config.'''
        from ultra.services import config_store
        ds = app.config.get('device_sn', '')
        if not ds:
            raise HTTPException(
                status_code=400,
                detail='device_sn not set in config',
            )
        loop = asyncio.get_running_loop()

        def _save_and_apply() -> None:
            app.config = _parse_and_merge_machine_yaml(
                req.yaml_text, app.config,
            )
            config_store.put_machine_settings_yaml(
                ds, req.yaml_text,
            )

        try:
            await loop.run_in_executor(
                None, _save_and_apply,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            ) from exc
        except Exception as exc:
            LOG.exception('S3 put machine_settings failed')
            raise HTTPException(
                status_code=502,
                detail=f'S3 upload failed: {exc}',
            ) from exc
        return {
            'ok': True,
            'message': (
                'Saved to S3 and applied (no restart needed).'
            ),
        }

    @router.post('/config/sync-recipes')
    async def config_sync_recipes():
        '''Download global recipes and _shared/_common from S3.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            config_store.sync_recipes_and_shared_from_s3,
        )
        return {'ok': True}

    @router.get('/recipes/{slug}/yaml')
    async def recipe_yaml_get(slug: str):
        '''Return raw YAML for a recipe.

        Prefer S3 (download to cache) when the object exists; otherwise
        packaged ``recipes/{slug}.yaml``. ``source`` is ``s3`` or
        ``packaged`` so Reload matches machine-settings behavior.
        '''
        import os.path as op
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _read() -> tuple[str, str]:
            path = config_store.fetch_recipe_to_cache(slug)
            if path and op.isfile(path):
                with open(path, encoding='utf-8') as fh:
                    return fh.read(), 's3'
            pack = op.join(
                rl.RECIPES_DIR, f'{slug}.yaml',
            )
            if op.isfile(pack):
                with open(pack, encoding='utf-8') as fh:
                    return fh.read(), 'packaged'
            raise FileNotFoundError(slug)

        try:
            text, source = await loop.run_in_executor(
                None, _read,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404, detail='Recipe not found',
            )
        return {
            'slug': slug,
            'yaml_text': text,
            'source': source,
        }

    @router.put('/recipes/{slug}/yaml')
    @router.post('/recipes/{slug}/yaml')
    async def recipe_yaml_put(slug: str, req: YamlTextBody):
        '''Validate and save a global recipe to S3.'''
        import yaml
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _validate_and_save() -> None:
            raw = yaml.safe_load(req.yaml_text) or {}
            recipe = rl.recipe_from_raw_dict(raw, slug)
            rl.validate_recipe(recipe)
            rl.lint_global_recipe_keys(recipe)
            config_store.put_recipe_yaml(slug, req.yaml_text)

        try:
            await loop.run_in_executor(
                None, _validate_and_save,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            )
        return {
            'ok': True,
            'slug': slug,
            'message': f'Recipe "{slug}" saved to S3.',
        }

    @router.delete('/recipes/{slug}')
    async def recipe_delete(slug: str):
        '''Delete a recipe from S3.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, config_store.delete_recipe, slug,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f'Delete failed: {exc}',
            )
        return {
            'ok': True,
            'message': f'Recipe "{slug}" deleted.',
        }

    @router.get('/common-protocol/yaml')
    async def common_protocol_get():
        '''Return raw YAML for _common.yaml (shared protocol phases).'''
        import os.path as op
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _read() -> tuple[str, str]:
            path = config_store.fetch_shared_common_to_cache()
            if path and op.isfile(path):
                with open(path, encoding='utf-8') as fh:
                    return fh.read(), 's3'
            pack = op.join(rl.RECIPES_DIR, '_common.yaml')
            if op.isfile(pack):
                with open(pack, encoding='utf-8') as fh:
                    return fh.read(), 'packaged'
            raise FileNotFoundError('_common.yaml')

        try:
            text, source = await loop.run_in_executor(None, _read)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail='_common.yaml not found')
        return {'yaml_text': text, 'source': source}

    @router.put('/common-protocol/yaml')
    @router.post('/common-protocol/yaml')
    async def common_protocol_put(req: YamlTextBody):
        '''Save _common.yaml to S3.'''
        import yaml
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _validate_and_save() -> None:
            raw = yaml.safe_load(req.yaml_text)
            if not isinstance(raw, dict):
                raise ValueError('_common.yaml must be a YAML mapping')
            config_store.put_shared_common_yaml(req.yaml_text)

        try:
            await loop.run_in_executor(None, _validate_and_save)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {'ok': True, 'message': 'Common protocol saved to S3.'}

    @router.get('/protocol/step-types')
    async def protocol_step_types():
        '''List registered protocol step type names.'''
        from ultra.protocol.steps import STEP_REGISTRY
        return {'step_types': sorted(STEP_REGISTRY.keys())}

    @router.get('/protocol/step-schemas')
    async def protocol_step_schemas():
        '''Return step types with parameter schemas for the GUI builder.'''
        from ultra.protocol.steps import STEP_SCHEMAS, STEP_DESCRIPTIONS
        return {'schemas': STEP_SCHEMAS, 'descriptions': STEP_DESCRIPTIONS}

    # ── FC Liquid Test Sequence ────────────────────────
    _fc_seq_state = {
        'state': 'idle', 'step': '', 'thread': None,
    }

    WELL_NAME_TO_LOC = {
        'SERUM': 18,
        'S1': 21, 'S2': 22, 'S3': 23, 'S4': 24,
        'S5': 25, 'S6': 26, 'S7': 27, 'S8': 28, 'S9': 29,
        'M1': 33, 'M2': 34, 'M3': 35, 'M4': 36,
        'M5': 37, 'M6': 38, 'M7': 39, 'M8': 40,
        'M9': 41, 'M10': 42, 'M11': 43, 'M12': 44,
        'M13': 45, 'M14': 46, 'M15': 47,
        'PP1': 8, 'PP2': 9, 'PP3': 10, 'PP4': 11,
        'PP5': 12, 'PP6': 13, 'PP7': 14, 'PP8': 15,
    }

    @router.post('/fc-liquid-sequence')
    async def fc_liquid_sequence_start(
        req: FcLiquidSeqRequest,
    ):
        if _fc_seq_state['state'] == 'running':
            raise HTTPException(
                status_code=409,
                detail='Sequence already running',
            )
        src_name = req.source_well.upper()
        src_loc = WELL_NAME_TO_LOC.get(src_name)
        if src_loc is None:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown well: {src_name}',
            )
        pp4_loc = WELL_NAME_TO_LOC['PP4']
        stm32 = _get_eng_stm32()
        if stm32 is None:
            raise HTTPException(
                status_code=409,
                detail='STM32 not connected',
            )

        def _ok(r):
            if r is None:
                return False
            if isinstance(r, bool):
                return r
            return r.get('status') == 'OK'

        def _aborted():
            return stm32._abort_flag.is_set()

        def _set(step_label):
            _fc_seq_state['step'] = step_label
            LOG.info('FC liquid seq: %s', step_label)

        def _check(r, label):
            if _aborted():
                return False
            if not _ok(r):
                LOG.error(
                    'FC liquid seq FAILED at: %s  '
                    'resp=%s', label, r,
                )
                _fc_seq_state['state'] = 'error'
                _fc_seq_state['step'] = (
                    f'FAILED: {label}'
                )
                return False
            return True

        def _run():
            try:
                _fc_seq_state['state'] = 'running'
                stm32.clear_abort()

                _set('Home all')
                r = stm32.send_command_wait_done(
                    cmd={'cmd': 'home_all'},
                    timeout_s=120.0,
                )
                if not _check(r, 'Home all'):
                    return

                _set('Pump init')
                r = stm32.send_command_wait_done(
                    cmd={'cmd': 'pump_init'},
                    timeout_s=60.0,
                )
                if not _check(r, 'Pump init'):
                    return

                _set('Tip pickup (slot 4)')
                r = stm32.send_command_wait_done(
                    cmd={
                        'cmd': 'gantry_tip_swap',
                        'from_id': 0, 'to_id': 4,
                    },
                    timeout_s=120.0,
                )
                if not _check(r, 'Tip pickup'):
                    return

                _set('LLD — detect cartridge Z')
                from ultra.hw.stm32_interface import (
                    Z_USTEPS_PER_MM,
                )
                lld_r = stm32.perform_lld(threshold=20)
                cartridge_z = 0.0
                if lld_r and lld_r.get('detected'):
                    z_us = lld_r.get('z_position', 0)
                    cartridge_z = z_us / Z_USTEPS_PER_MM
                    LOG.info(
                        'FC seq LLD: z=%d usteps '
                        '= %.2f mm',
                        z_us, cartridge_z,
                    )
                else:
                    LOG.warning(
                        'FC seq LLD not detected, '
                        'using cartridge_z=0 (resp=%s)',
                        lld_r,
                    )
                if _aborted():
                    return

                label = (
                    f'Aspirate {req.aspirate_vol_ul} uL '
                    f'from {src_name}'
                )
                _set(label)
                r = stm32.smart_aspirate_at(
                    loc_id=src_loc,
                    volume_ul=int(req.aspirate_vol_ul),
                    speed_ul_s=req.aspirate_speed_ul_s,
                    piston_reset=True,
                    air_slug_ul=40,
                    timeout_s=120.0,
                )
                if not _check(r, label):
                    return

                label = (
                    f'Dispense {req.cart_vol_ul} uL '
                    f'to PP4 @ {req.cart_vel_ul_s} uL/s'
                )
                _set(label)
                r = stm32.cart_dispense_at(
                    loc_id=pp4_loc,
                    volume_ul=int(req.cart_vol_ul),
                    vel_ul_s=req.cart_vel_ul_s,
                    reasp_ul=12,
                    cartridge_z=cartridge_z,
                    timeout_s=300.0,
                )
                if not _check(r, label):
                    return

                reasp = 12
                remainder = int(
                    req.aspirate_vol_ul
                    - req.cart_vol_ul
                    + reasp
                )
                label = (
                    f'Return {remainder} uL to '
                    f'{src_name} (blowout)'
                )
                _set(label)
                r = stm32.well_dispense_at(
                    loc_id=src_loc,
                    volume_ul=remainder,
                    speed_ul_s=100.0,
                    blowout=True,
                    timeout_s=120.0,
                )
                if not _check(r, label):
                    return

                _set('Tip return (slot 4)')
                r = stm32.send_command_wait_done(
                    cmd={
                        'cmd': 'gantry_tip_swap',
                        'from_id': 4, 'to_id': 0,
                    },
                    timeout_s=120.0,
                )
                if not _check(r, 'Tip return'):
                    return

                _set('Home all (final)')
                stm32.send_command_wait_done(
                    cmd={'cmd': 'home_all'},
                    timeout_s=120.0,
                )

                _fc_seq_state['state'] = 'done'
                _fc_seq_state['step'] = 'Done'
            except Exception as exc:
                LOG.exception(
                    'FC liquid seq error: %s', exc,
                )
                _fc_seq_state['state'] = 'error'
                _fc_seq_state['step'] = str(exc)
            finally:
                if _aborted():
                    _fc_seq_state['state'] = 'aborted'
                    _fc_seq_state['step'] = 'Aborted'
                    stm32.clear_abort()

        t = threading.Thread(
            target=_run, daemon=True,
        )
        _fc_seq_state['thread'] = t
        t.start()
        return {'ok': True}

    @router.get('/fc-liquid-sequence/status')
    async def fc_liquid_sequence_status():
        return {
            'state': _fc_seq_state['state'],
            'step': _fc_seq_state['step'],
        }

    return router
