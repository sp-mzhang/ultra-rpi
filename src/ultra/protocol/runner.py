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
from ultra.protocol.recipe_loader import load_recipe
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
        '''Abort the running protocol.

        The current in-flight step completes before aborting.
        '''
        if self._running:
            LOG.info('Protocol ABORT requested')
            self._abort_event.set()
            self._pause_event.set()

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
        reader_cfg = self._config.get('reader', {})
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
            self, chip_id: str,
    ) -> tuple[Any, str]:
        '''Create a RunGroupWriter and run directory.

        Args:
            chip_id: Chip ID for the run.

        Returns:
            (RunGroupWriter, run_dir_path) tuple.
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
        run_dir, _ = rg.add_run(
            reader_sn=reader_sn,
            chip_id=chip_id or 'unknown',
        )
        return rg, run_dir

    # ----------------------------------------------------------
    # Main run -- synchronous core in protocol thread
    # ----------------------------------------------------------

    def _run_sync(
            self,
            recipe_name: str,
            chip_id: str = '',
    ) -> list[dict]:
        '''Execute a complete protocol recipe (synchronous).

        Runs entirely in a dedicated OS thread. All STM32
        calls block naturally and events reach the GUI via
        ``emit_sync``.

        Args:
            recipe_name: Recipe name or path to YAML file.
            chip_id: Optional chip ID for tracking.

        Returns:
            List of per-step result dicts.
        '''
        self.recipe = load_recipe(recipe_name)
        self.tracker.init_wells(
            self.recipe.wells,
            recipe_name=self.recipe.name,
        )
        self.cartridge_z_mm = 0.0
        self._abort_event.clear()
        self._pause_event.set()
        self._running = True

        rg_writer = None
        run_dir = None
        try:
            rg_writer, run_dir = self._create_run_group(
                chip_id,
            )
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
                'recipe': self.recipe.name,
                'chip_id': chip_id,
                'total_steps': self.recipe.total_steps,
                'run_dir': run_dir or '',
            },
        )
        LOG.info(
            f'Protocol started: {self.recipe.name} '
            f'({self.recipe.total_steps} steps)',
        )

        step_index = 0
        try:
            for phase in self.recipe.phases:
                for step_def in phase.steps:
                    self.check_pause()
                    if self._abort_event.is_set():
                        LOG.warning('Protocol ABORTED')
                        self._event_bus.emit_sync(
                            'protocol_aborted',
                            self.tracker.snapshot(
                            ).to_dict(),
                        )
                        return self.tracker.results

                    step_index += 1
                    executor_cls = STEP_REGISTRY.get(
                        step_def.type,
                    )
                    if executor_cls is None:
                        LOG.error(
                            f'No executor for step type '
                            f'"{step_def.type}"',
                        )
                        self.tracker.end_step(
                            step_index, ok=False,
                        )
                        continue

                    self.tracker.begin_step(
                        index=step_index,
                        total=self.recipe.total_steps,
                        label=step_def.label,
                        phase=phase.name,
                    )

                    executor = executor_cls()
                    ok = executor.execute(
                        step_def.params, self,
                    )
                    self.tracker.end_step(
                        step_index, ok=ok,
                    )

                    if not ok:
                        LOG.error(
                            f'Step {step_index} '
                            f'"{step_def.label}" '
                            f'FAILED -- aborting',
                        )
                        self._event_bus.emit_sync(
                            'protocol_error', {
                                'step': step_index,
                                'label': step_def.label,
                            },
                        )
                        return self.tracker.results
        finally:
            self._stop_reader()
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
            f'({step_index} steps, '
            f'{self.tracker.snapshot().elapsed_s:.1f}s)',
        )
        self._event_bus.emit_sync(
            'protocol_done',
            self.tracker.snapshot().to_dict(),
        )
        return self.tracker.results

    async def run(
            self,
            recipe_name: str,
            chip_id: str = '',
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

        Returns:
            List of per-step result dicts.
        '''
        loop = asyncio.get_running_loop()
        self._event_bus.set_loop(loop)
        return await loop.run_in_executor(
            None, self._run_sync, recipe_name, chip_id,
        )

    def collect_pressure(
            self,
            resp: dict | None,
            label: str,
    ) -> None:
        '''Extract, store, and emit pressure samples.

        Stores samples in the tracker and emits a
        pressure_update event so the GUI can optionally
        display them.

        Args:
            resp: Command response dict that may contain
                '_pressure_samples'.
            label: Step label for annotation.
        '''
        if resp is None:
            return
        samples = resp.get('_pressure_samples', [])
        if not samples:
            return
        elapsed = self.tracker.snapshot().elapsed_s
        for s in samples:
            s['label'] = label
        self.tracker.add_pressure_data(samples)
        self._event_bus.emit_sync(
            'pressure_update', {
                'label': label,
                'timestamp_s': round(elapsed, 2),
                'samples': [
                    {
                        'pressure': s.get('pressure', 0),
                        'position': s.get('position', 0),
                    }
                    for s in samples
                ],
            },
        )
