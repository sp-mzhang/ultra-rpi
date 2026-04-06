'''ultra.protocol.state_tracker -- Protocol state management.

Centralized model that tracks all observable protocol state.
Updated by the runner and step executors as the protocol
progresses. Consumed by the GUI (WebSocket), cloud (IoT),
and pause/resume logic.
'''
from __future__ import annotations

import logging
import time
from typing import Any

from ultra.events import EventBus
from ultra.protocol.models import (
    ProtocolSnapshot,
    TipState,
    WellState,
)

LOG = logging.getLogger(__name__)


class ProtocolStateTracker:
    '''Tracks and broadcasts protocol execution state.

    Maintains the current ProtocolSnapshot and emits events
    on the event bus whenever state changes.

    Attributes:
        _snapshot: Current protocol state snapshot.
        _event_bus: Event bus for broadcasting changes.
        _start_time: Protocol start timestamp.
    '''

    def __init__(self, event_bus: EventBus) -> None:
        '''Initialize the state tracker.

        Args:
            event_bus: Application event bus for emitting
                state change events.
        '''
        self._event_bus = event_bus
        self._snapshot = ProtocolSnapshot()
        self._start_time: float = 0.0

    @property
    def is_paused(self) -> bool:
        '''Whether the protocol is currently paused.'''
        return self._snapshot.is_paused

    @is_paused.setter
    def is_paused(self, value: bool) -> None:
        self._snapshot.is_paused = value

    def init_wells(
            self,
            wells_def: dict[str, dict[str, Any]],
            recipe_name: str = '',
    ) -> None:
        '''Initialize well states from recipe definition.

        Creates WellState for each well in the recipe and
        sets the initial protocol snapshot. Called once when
        a recipe starts.

        Args:
            wells_def: Well definitions from recipe YAML.
                Keys are well names, values are dicts with
                'loc', 'reagent', 'volume_ul'.
            recipe_name: Name of the loaded recipe.
        '''
        self._start_time = time.time()
        wells: dict[str, WellState] = {}
        for name, attrs in wells_def.items():
            vol = float(attrs.get('volume_ul', 0))
            wells[name] = WellState(
                name=name,
                loc_id=int(attrs['loc']),
                reagent=str(attrs.get('reagent', '')),
                initial_volume_ul=vol,
                current_volume_ul=vol,
            )

        self._snapshot = ProtocolSnapshot(
            tip=TipState(),
            wells=wells,
            recipe_name=recipe_name,
        )
        LOG.info(
            f'State tracker initialized: '
            f'{len(wells)} wells, '
            f'recipe={recipe_name}',
        )
        self._event_bus.emit_sync(
            'wells_initialized', {
                name: ws.to_dict()
                for name, ws in wells.items()
            },
        )

    def begin_step(
            self,
            index: int,
            total: int,
            label: str,
            phase: str,
    ) -> None:
        '''Mark a step as starting.

        Updates the snapshot and emits a step_changed event.

        Args:
            index: 1-based step index.
            total: Total number of steps.
            label: Human-readable step description.
            phase: Phase name (e.g. 'A').
        '''
        self._snapshot.step_index = index
        self._snapshot.step_total = total
        self._snapshot.step_label = label
        self._snapshot.phase = phase
        self._snapshot.elapsed_s = (
            time.time() - self._start_time
        )
        LOG.info(
            f'Step {index}/{total} [{phase}]: {label}',
        )
        self._event_bus.emit_sync(
            'step_changed', {
                'step': index,
                'total': total,
                'label': label,
                'phase': phase,
            },
        )

    def end_step(
            self,
            index: int,
            ok: bool = True,
    ) -> None:
        '''Mark a step as completed.

        Records the result, updates elapsed time, and emits
        a ``step_changed`` event so the GUI can advance the
        progress bar immediately on completion.

        Args:
            index: 1-based step index.
            ok: True if the step succeeded.
        '''
        self._snapshot.elapsed_s = (
            time.time() - self._start_time
        )
        self._snapshot.results.append({
            'step': index,
            'ok': ok,
            'elapsed_s': round(
                self._snapshot.elapsed_s, 1,
            ),
        })
        self._event_bus.emit_sync(
            'step_changed', {
                'step': index,
                'total': self._snapshot.step_total,
                'label': self._snapshot.step_label,
                'phase': self._snapshot.phase,
                'elapsed_s': round(
                    self._snapshot.elapsed_s, 1,
                ),
                'completed': True,
                'ok': ok,
            },
        )

    def update_tip(self, tip_id: int) -> None:
        '''Update the current tip.

        Args:
            tip_id: New active tip ID (0 = none).
        '''
        old_id = self._snapshot.tip.current_tip_id
        if old_id != 0 and old_id in (
            self._snapshot.tip.tip_slots
        ):
            self._snapshot.tip.tip_slots[old_id] = (
                'available'
            )
        self._snapshot.tip.current_tip_id = tip_id
        if tip_id != 0 and tip_id in (
            self._snapshot.tip.tip_slots
        ):
            self._snapshot.tip.tip_slots[tip_id] = (
                'in_use'
            )
        LOG.debug(f'Tip changed: {old_id} -> {tip_id}')
        self._event_bus.emit_sync(
            'tip_changed',
            self._snapshot.tip.to_dict(),
        )

    def update_well(
            self,
            well_name: str,
            delta_ul: float,
            operation: str = '',
    ) -> None:
        '''Update a well's liquid volume.

        Args:
            well_name: Well name (e.g. 'S1', 'PP4').
            delta_ul: Volume change in uL (negative =
                liquid removed, positive = liquid added).
            operation: Description (e.g. 'aspirate 110uL').
        '''
        well = self._snapshot.wells.get(well_name)
        if well is None:
            LOG.warning(
                f'update_well: unknown well {well_name}',
            )
            return
        well.current_volume_ul += delta_ul
        if operation:
            well.operations.append(operation)
        self._event_bus.emit_sync(
            'well_updated', {
                'name': well.name,
                'loc_id': well.loc_id,
                'reagent': well.reagent,
                'current_volume_ul': round(
                    well.current_volume_ul, 1,
                ),
                'delta': delta_ul,
                'operation': operation,
            },
        )

    def get_well(
            self, well_name: str,
    ) -> WellState | None:
        '''Look up a well by name.

        Args:
            well_name: Well name string.

        Returns:
            WellState or None if not found.
        '''
        return self._snapshot.wells.get(well_name)

    def add_pressure_data(
            self, samples: list[dict],
    ) -> None:
        '''Append pressure samples to the snapshot.

        Args:
            samples: List of pressure sample dicts.
        '''
        self._snapshot.pressure_data.extend(samples)

    def snapshot(self) -> ProtocolSnapshot:
        '''Return the current protocol snapshot.

        Updates elapsed_s before returning.

        Returns:
            Current ProtocolSnapshot (mutable reference).
        '''
        if self._start_time > 0:
            self._snapshot.elapsed_s = (
                time.time() - self._start_time
            )
        return self._snapshot

    @property
    def results(self) -> list[dict]:
        '''Per-step results list.'''
        return self._snapshot.results
