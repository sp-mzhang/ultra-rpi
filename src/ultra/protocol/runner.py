'''ultra.protocol.runner -- Protocol execution engine.

Orchestrates recipe execution with pause/resume support,
state tracking, background reader acquisition, and
sway-compatible run data persistence.
'''
from __future__ import annotations

import asyncio
import logging
import os
import os.path as op
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


class ProtocolRunner:
    '''Orchestrates protocol recipe execution.

    Iterates through recipe phases and steps, delegating to
    registered StepExecutor classes. Supports pause/resume
    via asyncio.Event, tracks state via ProtocolStateTracker,
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

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._abort_event = asyncio.Event()
        self._running = False
        self._reader_task: asyncio.Task | None = None

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

    async def check_pause(self) -> None:
        '''Check for pause at a step boundary.

        Called between steps. Blocks if paused until resume
        or abort is called.
        '''
        if self._abort_event.is_set():
            return

        if not self._pause_event.is_set():
            self.tracker.is_paused = True
            await self._event_bus.emit(
                'protocol_paused',
                self.tracker.snapshot().to_dict(),
            )
            LOG.info('Protocol PAUSED -- waiting...')
            await self._pause_event.wait()
            if not self._abort_event.is_set():
                self.tracker.is_paused = False
                await self._event_bus.emit(
                    'protocol_resumed',
                    self.tracker.snapshot().to_dict(),
                )
                LOG.info('Protocol RESUMED')

    # ----------------------------------------------------------
    # Background reader acquisition
    # ----------------------------------------------------------

    async def _reader_loop(self) -> None:
        '''Continuously capture TLV blocks and run peak
        detection until cancelled or ``acq_time_total_s``
        is exceeded.

        Each iteration captures one block
        (``acq_time_step_s`` seconds of data) and feeds
        it through the ReaderPipeline. The loop runs as a
        background asyncio task alongside protocol steps.
        '''
        reader_cfg = self._config.get('reader', {})
        step_s = int(reader_cfg.get('acq_time_step_s', 3))
        total_s = int(
            reader_cfg.get('acq_time_total_s', 20000),
        )
        acq_mode = reader_cfg.get('acq_mode', 'continuous')
        block_count = 0

        LOG.info(
            'Reader loop started '
            '(mode=%s, step=%ds, cap=%ds)',
            acq_mode, step_s, total_s,
        )
        t0 = time.monotonic()
        try:
            while True:
                if time.monotonic() - t0 > total_s:
                    LOG.info(
                        'Reader acq_time_total_s (%ds) '
                        'exceeded -- stopping',
                        total_s,
                    )
                    break

                path = await self._acquisition.capture_block(
                    acq_seconds=step_s,
                )
                if path is None:
                    await asyncio.sleep(1.0)
                    continue

                block_count += 1
                elapsed = self.tracker.snapshot().elapsed_s

                if self._pipeline is not None:
                    try:
                        self._pipeline.process_tlv_file(
                            path,
                            timestamp_s=elapsed,
                        )
                    except Exception as exc:
                        LOG.warning(
                            'Pipeline error on block %d: %s',
                            block_count, exc,
                        )
        except asyncio.CancelledError:
            LOG.info(
                'Reader loop stopped after %d blocks',
                block_count,
            )
            raise

    def _start_reader(self) -> None:
        '''Start the background reader task if available.'''
        if self._acquisition and self._pipeline:
            self._pipeline.reset_baseline()
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
            )
            LOG.info('Background reader task created')
        else:
            LOG.warning(
                'Reader loop NOT started '
                '(acquisition=%s, pipeline=%s)',
                'ok' if self._acquisition else 'NONE',
                'ok' if self._pipeline else 'NONE',
            )

    async def _stop_reader(self) -> None:
        '''Cancel and await the background reader task.'''
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

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
    # Main run
    # ----------------------------------------------------------

    async def run(
            self,
            recipe_name: str,
            chip_id: str = '',
    ) -> list[dict]:
        '''Execute a complete protocol recipe.

        Loads the recipe, creates a sway-compatible run
        directory, starts background reader acquisition,
        iterates through all phases and steps, then
        persists results.

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

        await self._event_bus.emit(
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
                    await self.check_pause()
                    if self._abort_event.is_set():
                        LOG.warning('Protocol ABORTED')
                        await self._event_bus.emit(
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
                    ok = await executor.execute(
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
                        await self._event_bus.emit(
                            'protocol_error', {
                                'step': step_index,
                                'label': step_def.label,
                            },
                        )
                        return self.tracker.results
        finally:
            await self._stop_reader()
            self._running = False
            if rg_writer is not None:
                try:
                    rg_writer.mark_completed()
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
        await self._event_bus.emit(
            'protocol_done',
            self.tracker.snapshot().to_dict(),
        )
        return self.tracker.results

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
