'''ultra.protocol.runner -- Protocol execution engine.

Orchestrates recipe execution with pause/resume support
and state tracking. The runner iterates through recipe
phases and steps, delegating to registered StepExecutors.
'''
from __future__ import annotations

import asyncio
import logging
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


class ProtocolRunner:
    '''Orchestrates protocol recipe execution.

    Iterates through recipe phases and steps, delegating to
    registered StepExecutor classes. Supports pause/resume
    via asyncio.Event and tracks state via
    ProtocolStateTracker.

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
    ) -> None:
        '''Initialize the protocol runner.

        Args:
            stm32: STM32Interface or STM32Mock instance.
            event_bus: Application event bus for emitting
                protocol lifecycle events.
        '''
        self.stm32 = stm32
        self._event_bus = event_bus
        self.tracker = ProtocolStateTracker(event_bus)
        self.recipe: Recipe | None = None
        self.cartridge_z_mm: float = 0.0

        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._abort_event = asyncio.Event()
        self._running = False

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

    async def run(
            self,
            recipe_name: str,
            chip_id: str = '',
    ) -> list[dict]:
        '''Execute a complete protocol recipe.

        Loads the recipe, initializes wells, and iterates
        through all phases and steps. Emits lifecycle events
        on the event bus.

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

        await self._event_bus.emit(
            'protocol_started', {
                'recipe': self.recipe.name,
                'chip_id': chip_id,
                'total_steps': self.recipe.total_steps,
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
            self._running = False

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
