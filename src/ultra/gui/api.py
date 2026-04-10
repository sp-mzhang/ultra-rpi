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


class FirmwareFlashRequest(BaseModel):
    '''Request body for flashing firmware.'''
    key: str


class YamlTextBody(BaseModel):
    '''YAML payload for machine settings or recipe save.'''
    yaml_text: str = ''


def _machine_settings_effective_yaml(
        cfg: dict[str, Any],
) -> str:
    '''YAML for the machine settings editor: full effective merged config.

    Always serializes the in-memory dict (defaults + ULTRA_CONFIG + S3
    overlay applied at process start). The GUI always shows every key,
    not the raw short object that may still exist in S3.
    '''
    import yaml

    header = (
        '# machine_settings.yaml — full effective config for this process.\n'
        '# Built from: defaults, ULTRA_CONFIG, and S3 (merged at startup).\n'
        '# Edit any keys and Save to S3 to replace '
        'machines/{device_sn}/machine_settings.yaml.\n\n'
    )
    try:
        body = yaml.dump(
            cfg,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    except Exception:
        LOG.exception(
            'Could not serialize config dict to YAML for '
            'machine_settings editor',
        )
        return (
            header
            + '# Error: could not serialize config (see server log).\n'
        )
    return header + body


class SMRequest(BaseModel):
    '''Request body for state machine control.'''
    action: str = 'start'


class Stm32CmdRequest(BaseModel):
    '''Request body for a raw STM32 command.'''
    cmd: str
    params: dict[str, Any] = {}
    timeout_s: float = 30.0
    wait_done: bool = True


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
                    ),
                )
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f'Command error: {exc}',
            )
        return {
            'ok': result is not None,
            'response': result or {},
        }

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
    async def machine_settings_get():
        '''Return full effective merged config as YAML for editing.

        Always dumps ``app.config`` (everything the process loaded), not the
        raw S3 object text — so a legacy short upload in S3 does not hide
        stm32, reader, gui, etc. ``source`` indicates whether an S3 object
        exists for metadata only.
        '''
        from ultra.services import config_store
        ds = app.config.get('device_sn', '')
        if not ds:
            raise HTTPException(
                status_code=400,
                detail='device_sn not set in config',
            )
        loop = asyncio.get_running_loop()

        def _load() -> tuple[str, str]:
            exists = config_store.machine_settings_object_exists(
                ds,
            )
            text = _machine_settings_effective_yaml(app.config)
            return text, ('s3' if exists else 'defaults')

        yaml_text, source = await loop.run_in_executor(
            None, _load,
        )
        return {
            'device_sn': ds,
            'yaml_text': yaml_text,
            'source': source,
        }

    @router.put('/machine-settings')
    @router.post('/machine-settings')
    async def machine_settings_put(req: YamlTextBody):
        '''Write machine_settings.yaml to S3 for this device.

        POST is an alias for PUT (some reverse proxies block PUT).
        '''
        from ultra.services import config_store
        ds = app.config.get('device_sn', '')
        if not ds:
            raise HTTPException(
                status_code=400,
                detail='device_sn not set in config',
            )
        loop = asyncio.get_running_loop()

        def _save() -> None:
            config_store.put_machine_settings_yaml(
                ds, req.yaml_text,
            )

        try:
            await loop.run_in_executor(None, _save)
        except Exception as exc:
            LOG.exception('S3 put machine_settings failed')
            raise HTTPException(
                status_code=502,
                detail=f'S3 upload failed: {exc}',
            ) from exc
        return {
            'ok': True,
            'message': (
                'Saved to S3. Restart the app to load this YAML into '
                'memory; the editor shows what you saved until you Reload.'
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
        '''Return raw YAML for a recipe (S3 cache or packaged).'''
        import os.path as op
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _read() -> str:
            path = config_store.fetch_recipe_to_cache(slug)
            if path and op.isfile(path):
                with open(path, encoding='utf-8') as fh:
                    return fh.read()
            pack = op.join(
                rl.RECIPES_DIR, f'{slug}.yaml',
            )
            if op.isfile(pack):
                with open(pack, encoding='utf-8') as fh:
                    return fh.read()
            raise FileNotFoundError(slug)

        try:
            text = await loop.run_in_executor(None, _read)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404, detail='Recipe not found',
            )
        return {'slug': slug, 'yaml_text': text}

    @router.put('/recipes/{slug}/yaml')
    async def recipe_yaml_put(slug: str, req: YamlTextBody):
        '''Validate and save a global recipe to S3.'''
        import yaml
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _validate_and_save() -> None:
            raw = yaml.safe_load(req.yaml_text) or {}
            recipe = rl.recipe_from_raw_dict(raw, slug)
            rl._validate_recipe(recipe)
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
        return {'ok': True}

    @router.get('/protocol/step-types')
    async def protocol_step_types():
        '''List registered protocol step type names.'''
        from ultra.protocol.steps import STEP_REGISTRY
        return {'step_types': sorted(STEP_REGISTRY.keys())}

    return router
