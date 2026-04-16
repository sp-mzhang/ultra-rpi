'''ultra.protocol.runner -- Protocol execution engine.

Orchestrates recipe execution with pause/resume support,
state tracking, background reader acquisition, and
sway-compatible run data persistence.

The entire protocol loop runs in a dedicated OS thread
(``_run_sync``), so all STM32 serial calls block naturally
without starving the asyncio event loop. A thin
``async def run()`` wrapper launches the thread via
``run_in_executor`` to preserve the awaitable API contract
for callers (e.g. ``api.py``).
'''
from __future__ import annotations

import asyncio
import logging
import os
import os.path as op
import threading
import time
from typing import Any

from ultra.events import EventBus
from ultra.protocol.models import Recipe
from ultra.protocol.recipe_loader import (
    apply_machine_calibration,
    load_recipe,
    merge_protocol_config,
)
from ultra.protocol.state_tracker import (
    ProtocolStateTracker,
)
from ultra.protocol.steps import STEP_REGISTRY

LOG = logging.getLogger(__name__)

DEFAULT_DATA_DIR = '~/sway_runs'
_READER_WARMUP_BLOCKS = 2
_READER_WARMUP_TIMEOUT_S = 30


class ProtocolRunner:
    '''Orchestrates protocol recipe execution.

    Iterates through recipe phases and steps, delegating to
    registered StepExecutor classes.  The step loop runs in a
    dedicated OS thread so synchronous STM32 serial calls
    never block the asyncio event loop.  Supports pause/resume
    via threading.Event, tracks state via ProtocolStateTracker,
    runs a background reader acquisition loop, and persists
    run data in sway-compatible format.

    Attributes:
        stm32: STM32 hardware interface (or mock).
        tracker: Protocol state tracker.
        recipe: Currently loaded recipe (set during run).
        cartridge_z_mm: Last detected cartridge Z position.
    '''

    def __init__(
            self,
            stm32: Any,
            event_bus: EventBus,
            config: dict[str, Any] | None = None,
            acquisition: Any | None = None,
            pipeline: Any | None = None,
    ) -> None:
        '''Initialize the protocol runner.

        Args:
            stm32: STM32Interface or STM32Mock instance.
            event_bus: Application event bus for emitting
                protocol lifecycle events.
            config: Full application config dict (used for
                reader timing and egress.data_dir).
            acquisition: Optional AcquisitionService for
                background TLV capture.
            pipeline: Optional ReaderPipeline for live
                peak detection from TLV data.
        '''
        self.stm32 = stm32
        self._event_bus = event_bus
        self._config = config or {}
        self._acquisition = acquisition
        self._pipeline = pipeline
        self.tracker = ProtocolStateTracker(event_bus)
        self.recipe: Recipe | None = None
        self.cartridge_z_mm: float = 0.0

        self._pause_event = threading.Event()
        self._pause_event.set()
        self._abort_event = threading.Event()
        self._running = False
        self._reader_thread: threading.Thread | None = None
        self._reader_stop = threading.Event()

        self._flat_steps: list[tuple] = []
        self._tip_state_at: list[int] = []
        self._start_step: int = 1
        self._restart_requested = False
        self._run_dir: str | None = None
        self._protocol_config: dict[str, Any] | None = None

        self._pressure_csv_file: Any = None
        self._pressure_csv_writer: Any = None
        self._pressure_csv_path: str | None = None

    def _active_config(self) -> dict[str, Any]:
        '''Config merged with recipe reader/peak for current run.'''
        if self._protocol_config is not None:
            return self._protocol_config
        return self._config

    @property
    def is_running(self) -> bool:
        '''Whether a protocol is currently executing.'''
        return self._running

    @property
    def is_paused(self) -> bool:
        '''Whether the protocol is paused.'''
        return not self._pause_event.is_set()

    def pause(self) -> None:
        '''Pause the protocol at the next step boundary.

        The current in-flight step completes before pausing.
        '''
        if self._running:
            LOG.info('Protocol PAUSE requested')
            self._pause_event.clear()

    def resume(self) -> None:
        '''Resume a paused protocol.'''
        if self._running:
            LOG.info('Protocol RESUME requested')
            self._pause_event.set()

    def abort(self) -> None:
        '''Abort the running protocol immediately.

        Interrupts the current in-flight serial wait so the
        step exits within ~50ms, then sends CMD_ABORT to the
        firmware to halt all motors.
        '''
        if self._running:
            LOG.info('Protocol ABORT requested')
            self._abort_event.set()
            self._pause_event.set()
            if self.stm32 is not None:
                self.stm32.request_abort()

    def _send_firmware_abort(self) -> None:
        '''Send CMD_ABORT to firmware to halt all motors.'''
        if self.stm32 is None:
            return
        try:
            self.stm32.clear_abort()
            self.stm32.send_command(
                cmd={'cmd': 'abort'}, timeout_s=3.0,
            )
            LOG.info('Firmware CMD_ABORT sent')
        except Exception as exc:
            LOG.warning('Firmware abort failed: %s', exc)

    def check_pause(self) -> None:
        '''Check for pause at a step boundary.

        Called between steps from the protocol thread. Blocks
        the thread if paused until resume or abort is called.
        Events are emitted via ``emit_sync`` (thread-safe).
        '''
        if self._abort_event.is_set():
            return

        if not self._pause_event.is_set():
            self.tracker.is_paused = True
            self._event_bus.emit_sync(
                'protocol_paused',
                self.tracker.snapshot().to_dict(),
            )
            LOG.info('Protocol PAUSED -- waiting...')
            self._pause_event.wait()
            if not self._abort_event.is_set():
                self.tracker.is_paused = False
                self._event_bus.emit_sync(
                    'protocol_resumed',
                    self.tracker.snapshot().to_dict(),
                )
                LOG.info('Protocol RESUMED')

    def restart_from(self, step_index: int) -> None:
        '''Set the runner to restart from a specific step.

        Must be called while paused. Reconciles tip state
        with the expected state at the target step, updates
        the loop cursor, then resumes.

        Args:
            step_index: 1-based step index to restart from.

        Raises:
            ValueError: If step_index is out of range or
                the protocol is not paused.
        '''
        if not self._running or not self.is_paused:
            raise ValueError(
                'Protocol must be running and paused',
            )
        total = len(self._flat_steps) - 1
        if step_index < 1 or step_index > total:
            raise ValueError(
                f'step_index must be 1..{total}, '
                f'got {step_index}',
            )
        self._reconcile_tip(step_index)
        self._start_step = step_index
        self._restart_requested = True
        LOG.info(
            'Restart from step %d requested', step_index,
        )
        self.resume()

    def _reconcile_tip(self, target_step: int) -> None:
        '''Swap tips so the physical state matches what
        the target step expects.

        Args:
            target_step: 1-based step index about to run.
        '''
        current_tip = (
            self.tracker.snapshot().tip.current_tip_id
        )
        needed_tip = self._tip_state_at[target_step]
        if current_tip == needed_tip:
            return

        LOG.info(
            'Tip reconciliation: current=%d needed=%d',
            current_tip, needed_tip,
        )
        r = self.stm32.send_command_wait_done(
            cmd={
                'cmd': 'gantry_tip_swap',
                'from_id': current_tip,
                'to_id': needed_tip,
            },
            timeout_s=60.0,
        )
        if r and r.get('status') == 'ok':
            self.tracker.update_tip(needed_tip)
        else:
            LOG.error(
                'Tip reconciliation failed: %s', r,
            )

    # ----------------------------------------------------------
    # Background reader acquisition
    # ----------------------------------------------------------

    def _reader_loop(self) -> None:
        '''Continuously capture TLV blocks in a dedicated
        OS thread.

        Runs entirely outside the asyncio event loop so
        protocol steps (which block their own thread via
        synchronous STM32 serial calls) can never starve the
        serial reader. This mirrors sway's separate-process
        reader.

        ``capture_block()`` and ``process_tlv_file()`` are
        both synchronous and run back-to-back in this thread.
        Events are pushed to the GUI via ``emit_sync``
        (thread-safe).
        '''
        reader_cfg = self._active_config().get('reader', {})
        step_s = int(reader_cfg.get('acq_time_step_s', 3))
        total_s = int(
            reader_cfg.get('acq_time_total_s', 20000),
        )
        acq_mode = reader_cfg.get('acq_mode', 'continuous')
        block_count = 0

        LOG.info(
            'Reader thread started '
            '(mode=%s, step=%ds, cap=%ds)',
            acq_mode, step_s, total_s,
        )
        t0 = time.monotonic()
        try:
            while not self._reader_stop.is_set():
                if time.monotonic() - t0 > total_s:
                    LOG.info(
                        'Reader acq_time_total_s (%ds) '
                        'exceeded -- stopping',
                        total_s,
                    )
                    break

                path = self._acquisition.capture_block(
                    acq_seconds=step_s,
                )
                if path is None:
                    if self._reader_stop.wait(1.0):
                        break
                    continue

                block_count += 1
                elapsed = self.tracker.snapshot().elapsed_s

                if self._pipeline is not None:
                    try:
                        self._pipeline.process_tlv_file(
                            path, elapsed,
                        )
                    except Exception as exc:
                        LOG.warning(
                            'Pipeline error on block %d: '
                            '%s', block_count, exc,
                        )
        except Exception:
            LOG.exception('Reader thread crashed')
        finally:
            if self._acquisition is not None:
                self._acquisition.stop()
            LOG.info(
                'Reader thread exiting '
                '(%d blocks captured)',
                block_count,
            )

    def _start_reader(self) -> None:
        '''Start the background reader thread.

        Caches the asyncio event loop on the event bus so
        ``emit_sync()`` (called from both reader and protocol
        threads) can schedule callbacks on the correct loop.
        '''
        if self._acquisition and self._pipeline:
            self._pipeline.reset_baseline()
            self._reader_stop.clear()
            if self._event_bus._loop is None:
                LOG.warning(
                    'EventBus loop not cached -- '
                    'call set_loop() before _start_reader()',
                )
            self._reader_thread = threading.Thread(
                target=self._reader_loop,
                name='reader-acq',
                daemon=True,
            )
            self._reader_thread.start()
            LOG.info('Background reader thread started')
        else:
            LOG.warning(
                'Reader loop NOT started '
                '(acquisition=%s, pipeline=%s)',
                'ok' if self._acquisition else 'NONE',
                'ok' if self._pipeline else 'NONE',
            )

    def _wait_for_reader_data(self) -> None:
        '''Wait for initial reader blocks before protocol
        steps begin.

        Polls the acquisition block counter until
        ``_READER_WARMUP_BLOCKS`` have been captured or a
        timeout is reached. Runs inside the protocol thread
        so ``time.sleep`` is fine.
        '''
        if self._acquisition is None:
            return

        target = _READER_WARMUP_BLOCKS
        deadline = time.monotonic() + _READER_WARMUP_TIMEOUT_S
        LOG.info(
            'Waiting for %d reader blocks '
            'before starting protocol...',
            target,
        )

        while time.monotonic() < deadline:
            captured = self._acquisition._block_counter + 1
            if captured >= target:
                LOG.info(
                    'Reader warm-up complete '
                    '(%d blocks captured)',
                    captured,
                )
                return
            time.sleep(0.5)

        captured = self._acquisition._block_counter + 1
        LOG.warning(
            'Reader warm-up timeout after %ds '
            '(%d/%d blocks captured)',
            _READER_WARMUP_TIMEOUT_S,
            captured, target,
        )

    def _stop_reader(self) -> None:
        '''Signal the reader thread to stop and wait for it.

        Called from the protocol thread, so a direct join is
        safe (no event loop to block).
        '''
        if self._reader_thread is not None:
            self._reader_stop.set()
            self._reader_thread.join(10.0)
            if self._reader_thread.is_alive():
                LOG.warning(
                    'Reader thread did not exit within '
                    '10s timeout',
                )
            self._reader_thread = None

    # ----------------------------------------------------------
    # Run data persistence
    # ----------------------------------------------------------

    def _create_run_group(
            self,
            chip_id: str,
            note: str = '',
    ) -> tuple[Any, str]:
        '''Create a RunGroupWriter, run directory, and
        register the RunGroup + Run on Dollop so that
        ``run_id`` is assigned immediately.

        Args:
            chip_id: Chip ID for the run.
            note: User-provided run note.

        Returns:
            (RunGroupWriter, run_dir_path, RunDirTuple).
        '''
        from ultra.services.run_data import RunGroupWriter

        egress_cfg = self._config.get('egress', {})
        data_dir = os.path.expanduser(
            egress_cfg.get(
                'data_dir', DEFAULT_DATA_DIR,
            ),
        )
        device_sn = self._config.get(
            'device_sn',
            egress_cfg.get('device_sn', 'ultra-001'),
        )

        station_id = self._config.get('station_id', 100)
        protocol_mode = self._config.get(
            'protocol_mode', 'ultra',
        )
        qr_cfg = self._config.get('quick_run', {})
        operator = qr_cfg.get('operator', 'ultra_rpi')

        rg = RunGroupWriter(
            data_dir=data_dir,
            user=operator,
            name=self.recipe.name if self.recipe else 'run',
            device_sn=device_sn,
            station_id=station_id,
            protocol_mode=protocol_mode,
        )
        rg.mark_started()

        reader_sn = 'pproc-001'
        run_dir, rdt = rg.add_run(
            reader_sn=reader_sn,
            chip_id=chip_id or 'unknown',
            note=note,
        )

        rdt = self._register_on_dollop(rg, rdt)
        return rg, run_dir, rdt

    def _register_on_dollop(
            self, rg: Any, rdt: Any,
    ) -> Any:
        '''Register RunGroup and Run on Dollop at start time.

        Updates the on-disk JSON files with the Dollop-assigned
        IDs so the egress service can skip re-creation.
        Failures are non-fatal -- egress will retry later.

        Args:
            rg: RunGroupWriter instance.
            rdt: RunDirTuple from ``add_run``.

        Returns:
            Updated RunDirTuple with Dollop IDs (or original
            on failure).
        '''
        from ultra.services import (
            dollop_client as dollop,
        )
        from ultra.services.run_data import RunDirTuple

        rg_id = rg.rg_dict.get('rungroup_id', -1)
        if rg_id == -1:
            try:
                rg_id, _ = dollop.create_rungroup(
                    rg.rg_dict,
                )
            except Exception as exc:
                LOG.warning(
                    'Dollop RunGroup create failed: %s',
                    exc,
                )
                return rdt
            if rg_id != -1:
                rg.rg_dict['rungroup_id'] = rg_id
                rg._write_rg_json()
                LOG.info(
                    'Dollop RunGroup created: id=%d',
                    rg_id,
                )
            else:
                LOG.warning(
                    'Dollop RunGroup creation returned '
                    'default ID',
                )
                return rdt

        reader_cfg = self._active_config().get('reader', {})
        reader_dollop = reader_cfg.get(
            'dollop_name', 'reader7',
        )

        run_id = rdt.run_id
        if run_id == -1:
            try:
                run_dict = dollop.read_run_json(
                    rdt.run_dir_path,
                )
                run_dict['rungroup_id'] = rg_id
                run_dict['local_directory_path'] = (
                    rdt.run_dir_path
                )
                run_id = dollop.create_run(
                    run_dict,
                    reader_dollop_name=reader_dollop,
                )
            except Exception as exc:
                LOG.warning(
                    'Dollop Run create failed: %s', exc,
                )
                return rdt._replace(
                    rungroup_id=rg_id,
                )
            if run_id != -1:
                LOG.info(
                    'Dollop Run created: id=%d', run_id,
                )
                self._update_run_json_id(
                    rdt.run_dir_path, run_id, rg_id,
                )
            else:
                LOG.warning(
                    'Dollop Run creation returned '
                    'default ID',
                )

        new_rdt = RunDirTuple(
            run_uuid=rdt.run_uuid,
            run_id=run_id,
            rungroup_uuid=rdt.rungroup_uuid,
            rungroup_id=rg_id,
            run_dir_path=rdt.run_dir_path,
            rungroup_dir_path=rdt.rungroup_dir_path,
        )

        rdt_list = rg.rg_dict.get(
            'run_uuid_dir_list', [],
        )
        for i, entry in enumerate(rdt_list):
            uid = (
                entry[0]
                if isinstance(entry, (list, tuple))
                else ''
            )
            if uid == rdt.run_uuid:
                rdt_list[i] = new_rdt.to_list()
                break
        rg._write_rg_json()

        return new_rdt

    @staticmethod
    def _update_run_json_id(
            run_dir: str, run_id: int, rg_id: int,
    ) -> None:
        '''Patch run.json with Dollop-assigned IDs.

        Args:
            run_dir: Path to the run directory.
            run_id: Dollop-assigned Run ID.
            rg_id: Dollop-assigned RunGroup ID.
        '''
        import json
        fp = os.path.join(run_dir, 'run.json')
        try:
            with open(fp, 'r') as fh:
                d = json.load(fh)
            d['run_id'] = run_id
            d['rungroup_id'] = rg_id
            with open(fp, 'w') as fh:
                json.dump(
                    d, fh, sort_keys=True, indent=2,
                )
        except Exception as exc:
            LOG.warning(
                'Failed to update run.json: %s', exc,
            )

    # ----------------------------------------------------------
    # Main run -- synchronous core in protocol thread
    # ----------------------------------------------------------

    def _build_flat_steps(self) -> None:
        '''Flatten recipe phases into an indexed list and
        precompute the expected tip state at each step.

        Populates ``_flat_steps`` as a list of
        ``(phase, step_def)`` tuples (1-indexed: element 0
        is a sentinel) and ``_tip_state_at`` where entry *i*
        holds the ``current_tip_id`` expected at the *start*
        of step *i*.
        '''
        flat: list[tuple] = [('', None)]
        tip_at: list[int] = [0]
        cur_tip = 0
        for phase in self.recipe.phases:
            for step_def in phase.steps:
                tip_at.append(cur_tip)
                flat.append((phase, step_def))
                stype = step_def.type
                if stype == 'tip_pick':
                    cur_tip = step_def.params.get(
                        'tip_id', 0,
                    )
                elif stype == 'tip_swap':
                    cur_tip = step_def.params.get(
                        'to_id', 0,
                    )
                elif stype == 'tip_return':
                    cur_tip = 0
        self._flat_steps = flat
        self._tip_state_at = tip_at

    def _build_step_manifest(self) -> list[dict]:
        '''Build a JSON-serializable list of every step.

        Returns:
            List of dicts with index, phase, label, type,
            and expected_tip for each step.
        '''
        manifest = []
        for i in range(1, len(self._flat_steps)):
            phase, step_def = self._flat_steps[i]
            manifest.append({
                'index': i,
                'phase': phase.name,
                'label': step_def.label,
                'type': step_def.type,
                'expected_tip': self._tip_state_at[i],
            })
        return manifest

    def _run_sync(
            self,
            recipe_name: str,
            chip_id: str = '',
            note: str = '',
    ) -> list[dict]:
        '''Execute a complete protocol recipe (synchronous).

        Runs entirely in a dedicated OS thread. All STM32
        calls block naturally and events reach the GUI via
        ``emit_sync``.

        Args:
            recipe_name: Recipe name or path to YAML file.
            chip_id: Optional chip ID for tracking.
            note: User-provided run note persisted in
                run data.

        Returns:
            List of per-step result dicts.
        '''
        self.recipe = load_recipe(recipe_name)
        self.recipe = apply_machine_calibration(
            self._config, self.recipe,
        )
        self._protocol_config = merge_protocol_config(
            self._config, self.recipe,
        )
        if self._pipeline is not None:
            self._pipeline.set_peak_config(
                self._protocol_config.get('peak_detect', {}),
            )
        self.tracker.init_wells(
            self.recipe.wells,
            recipe_name=self.recipe.name,
        )
        self.cartridge_z_mm = 0.0
        self._abort_event.clear()
        self._pause_event.set()
        self._running = True
        if self.stm32 is not None:
            self.stm32.clear_abort()
        self._start_step = 1

        self._build_flat_steps()

        rg_writer = None
        run_dir = None
        run_dir_tup = None
        try:
            rg_writer, run_dir, run_dir_tup = (
                self._create_run_group(
                    chip_id, note=note,
                )
            )
            self._run_dir = run_dir
            self._chip_id = chip_id
            LOG.info('Run data dir: %s', run_dir)
        except Exception as exc:
            LOG.warning(
                'Failed to create run dir: %s', exc,
            )

        if run_dir:
            if self._acquisition is not None:
                tlv_dir = op.join(run_dir, 'tlv')
                self._acquisition.set_output_dir(tlv_dir)
            if self._pipeline is not None:
                self._pipeline.set_run_dir(run_dir)

        self._start_reader()
        self._wait_for_reader_data()

        self._event_bus.emit_sync(
            'protocol_started', {
                'recipe': recipe_name,
                'recipe_display': self.recipe.name,
                'chip_id': chip_id,
                'total_steps': self.recipe.total_steps,
                'run_dir': run_dir or '',
                'steps': self._build_step_manifest(),
            },
        )
        LOG.info(
            f'Protocol started: {self.recipe.name} '
            f'({self.recipe.total_steps} steps)',
        )

        total = self.recipe.total_steps
        try:
            while self._start_step <= total:
                self._restart_requested = False
                for si in range(
                    self._start_step,
                    len(self._flat_steps),
                ):
                    self.check_pause()
                    if self._abort_event.is_set():
                        LOG.warning('Protocol ABORTED')
                        self._send_firmware_abort()
                        self._event_bus.emit_sync(
                            'protocol_aborted',
                            self.tracker.snapshot(
                            ).to_dict(),
                        )
                        return self.tracker.results
                    if self._restart_requested:
                        LOG.info(
                            'Restarting from step %d',
                            self._start_step,
                        )
                        break

                    phase, step_def = (
                        self._flat_steps[si]
                    )
                    executor_cls = STEP_REGISTRY.get(
                        step_def.type,
                    )
                    if executor_cls is None:
                        LOG.error(
                            f'No executor for step type '
                            f'"{step_def.type}"',
                        )
                        self.tracker.end_step(
                            si, ok=False,
                        )
                        continue

                    if step_def.params.get('skip'):
                        LOG.info(
                            'Step %d "%s" SKIPPED',
                            si, step_def.label,
                        )
                        self.tracker.begin_step(
                            index=si,
                            total=total,
                            label=(
                                f'[SKIP] '
                                f'{step_def.label}'
                            ),
                            phase=phase.name,
                        )
                        self.tracker.end_step(
                            si, ok=True,
                        )
                        continue

                    self.tracker.begin_step(
                        index=si,
                        total=total,
                        label=step_def.label,
                        phase=phase.name,
                    )

                    executor = executor_cls()
                    ok = executor.execute(
                        step_def.params, self,
                    )
                    self.tracker.end_step(si, ok=ok)

                    if not ok:
                        LOG.error(
                            f'Step {si} '
                            f'"{step_def.label}" '
                            f'FAILED -- aborting',
                        )
                        self._event_bus.emit_sync(
                            'protocol_error', {
                                'step': si,
                                'label': step_def.label,
                            },
                        )
                        return self.tracker.results
                else:
                    break
        finally:
            if self._pressure_csv_file:
                self._pressure_csv_file.close()
                LOG.info(
                    'Pressure CSV closed: %s',
                    self._pressure_csv_path,
                )
                self._pressure_csv_file = None
                self._pressure_csv_writer = None
            self._stop_reader()
            self._protocol_config = None
            if self._pipeline is not None:
                self._pipeline.set_peak_config(
                    self._config.get('peak_detect', {}),
                )
            self._running = False
            if rg_writer is not None:
                try:
                    rg_writer.mark_completed()
                    rg_writer.write_spectrify_complete()
                    if run_dir:
                        rg_writer.copy_rg_files_to_run(
                            run_dir,
                        )
                except Exception as exc:
                    LOG.warning(
                        'RunGroup finalize error: %s', exc,
                    )

        LOG.info(
            f'Protocol completed: '
            f'{self.recipe.name} '
            f'({total} steps, '
            f'{self.tracker.snapshot().elapsed_s:.1f}s)',
        )
        done_data = self.tracker.snapshot().to_dict()
        if run_dir_tup is not None:
            done_data['run_uuid_dir_list'] = [
                run_dir_tup,
            ]
        self._event_bus.emit_sync(
            'protocol_done', done_data,
        )
        return self.tracker.results

    async def run(
            self,
            recipe_name: str,
            chip_id: str = '',
            note: str = '',
    ) -> list[dict]:
        '''Async wrapper that launches ``_run_sync`` in
        a thread pool.

        Preserves the ``await runner.run()`` API contract for
        callers. Caches the event loop first so both the
        protocol thread and reader thread can reach the GUI
        via ``emit_sync``.

        Args:
            recipe_name: Recipe name or path to YAML file.
            chip_id: Optional chip ID for tracking.
            note: User-provided run note.

        Returns:
            List of per-step result dicts.
        '''
        loop = asyncio.get_running_loop()
        self._event_bus.set_loop(loop)
        return await loop.run_in_executor(
            None,
            lambda: self._run_sync(
                recipe_name,
                chip_id=chip_id,
                note=note,
            ),
        )

    def trigger_analysis(self) -> None:
        '''Run concentration analysis in a background thread.

        Called when a ``timing_marker`` step has
        ``trigger_analysis: true``.
        '''
        import threading

        run_dir = self._run_dir
        if not run_dir:
            LOG.warning(
                'trigger_analysis: no run_dir set',
            )
            return

        cfg = self._config or {}
        analysis_cfg = cfg.get('analysis', {})
        assay = analysis_cfg.get('default_assay', 'crp')
        version = analysis_cfg.get(
            'default_calibration_version', 'v1.0',
        )

        calib_ver = getattr(self, '_calibration_version', '')
        if calib_ver and '/' in calib_ver:
            assay, version = calib_ver.split('/', 1)

        chip_id = getattr(self, '_chip_id', '')

        LOG.info(
            'trigger_analysis: %s/%s on %s (chip=%s)',
            assay, version, run_dir, chip_id or 'none',
        )

        def _run() -> None:
            try:
                from ultra.analysis import run_analysis
                LOG.info('analysis-worker: starting')
                result = run_analysis(
                    run_dir=run_dir,
                    assay=assay,
                    version=version,
                    chip_id=chip_id,
                )
                if result.ok:
                    self._event_bus.emit_sync(
                        'analysis_complete',
                        result.to_dict(),
                    )
                    LOG.info(
                        'Analysis complete: %d analytes',
                        len(result.analytes),
                    )
                else:
                    LOG.warning(
                        'Analysis returned no results: %s',
                        result.error,
                    )
                    self._event_bus.emit_sync(
                        'analysis_complete',
                        {'analytes': [], 'run_dir': run_dir,
                         'error': result.error},
                    )
            except Exception as exc:
                LOG.exception('Background analysis failed')
                self._event_bus.emit_sync(
                    'analysis_complete',
                    {'analytes': [], 'run_dir': run_dir,
                     'error': str(exc)},
                )

        t = threading.Thread(
            target=_run, daemon=True,
            name='analysis-worker',
        )
        t.start()

    def _open_pressure_csv(self) -> None:
        '''Lazily open the pressure CSV on first data.'''
        if self._pressure_csv_file is not None:
            return
        run_dir = self._run_dir
        if not run_dir:
            return
        import csv
        from datetime import datetime
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        fname = f'pressure_streaming_{ts}.csv'
        fpath = os.path.join(run_dir, fname)
        self._pressure_csv_path = fpath
        self._pressure_csv_file = open(
            fpath, 'w', newline='',
        )
        self._pressure_csv_writer = csv.writer(
            self._pressure_csv_file,
        )
        self._pressure_csv_writer.writerow([
            'reagent', 'operation', 'phase',
            'cycle', 'time_s', 'pressure_14bit',
            'position_steps', 'timestamp_us',
        ])

    def collect_pressure(
            self,
            resp: dict | None,
            label: str,
            operation: str = 'dispense',
            phase: str = 'cart_dispense',
    ) -> None:
        '''Extract, store, emit, and persist pressure samples.

        Stores samples in the tracker, emits a pressure_update
        event for the GUI, and appends rows to the CSV file
        incrementally.

        Args:
            resp: Command response dict that may contain
                '_pressure_samples'.
            label: Step label for annotation (CSV reagent).
            operation: 'aspirate' or 'dispense'.
            phase: 'LLF' or 'cart_dispense'.
        '''
        if resp is None:
            return
        samples = resp.get('_pressure_samples', [])
        if not samples:
            return
        elapsed = self.tracker.snapshot().elapsed_s
        for s in samples:
            s['label'] = label
            s['operation'] = operation
            s['phase'] = phase
        self.tracker.add_pressure_data(samples)
        t_last = samples[-1].get('ts', 0)
        gui_max = 1000
        src = samples
        if len(samples) > gui_max:
            step = len(samples) / gui_max
            src = [
                samples[int(i * step)]
                for i in range(gui_max)
            ]
            src.append(samples[-1])
        sample_list = []
        for s in src:
            dt = (
                s.get('ts', 0) - t_last
            ) / 1000.0
            sample_list.append({
                'pressure': s.get('p', 0),
                'position': s.get('pos', 0),
                'dt': round(dt, 4),
            })
        self._event_bus.emit_sync(
            'pressure_update', {
                'label': label,
                'timestamp_s': round(elapsed, 2),
                'samples': sample_list,
            },
        )

        self._open_pressure_csv()
        if self._pressure_csv_writer:
            for s in samples:
                raw_ts = s.get('ts', 0)
                ts_us = raw_ts * 1000
                time_s = ts_us / 1_000_000.0
                self._pressure_csv_writer.writerow([
                    s.get('label', ''),
                    s.get('operation', 'dispense'),
                    s.get('phase', 'cart_dispense'),
                    s.get('cycle', 0),
                    f'{time_s:.6f}',
                    s.get('p', 0),
                    s.get('pos', 0),
                    ts_us,
                ])
            self._pressure_csv_file.flush()
