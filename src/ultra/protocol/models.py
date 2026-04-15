'''ultra.protocol.models -- Protocol data models.

Dataclasses for protocol state, step definitions, and
recipe structure. Used by the state tracker, runner,
and GUI.
'''
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TipState:
    '''Tracks current tip status during a protocol run.

    Attributes:
        current_tip_id: Active tip ID (0 = none, 4, 5).
        tip_slots: Map of tip slot ID to status string
            ('available', 'in_use', 'returned').
    '''
    current_tip_id: int = 0
    tip_slots: dict[int, str] = field(
        default_factory=lambda: {
            4: 'available', 5: 'available',
        },
    )

    def to_dict(self) -> dict:
        '''Serialize to a JSON-compatible dict.'''
        return {
            'current_tip_id': self.current_tip_id,
            'tip_slots': dict(self.tip_slots),
        }


@dataclass
class WellState:
    '''Tracks liquid state of a single well.

    Attributes:
        name: Well label (e.g. 'S1', 'M1', 'PP4').
        loc_id: Cartridge location ID.
        reagent: Reagent name string.
        initial_volume_ul: Starting volume in uL.
        current_volume_ul: Current volume in uL.
        operations: Log of operations performed on well.
        foil_punctured: True after any physical access
            (aspirate, dispense, mix) has punctured the
            foil seal on this well.
    '''
    name: str
    loc_id: int
    reagent: str
    initial_volume_ul: float
    current_volume_ul: float
    operations: list[str] = field(
        default_factory=list,
    )
    foil_punctured: bool = False

    def to_dict(self) -> dict:
        '''Serialize to a JSON-compatible dict.'''
        return {
            'name': self.name,
            'loc_id': self.loc_id,
            'reagent': self.reagent,
            'initial_volume_ul': self.initial_volume_ul,
            'current_volume_ul': self.current_volume_ul,
            'operations': list(self.operations),
            'foil_punctured': self.foil_punctured,
        }


@dataclass
class ProtocolSnapshot:
    '''Full observable state of a running protocol.

    Consumed by the GUI (WebSocket), cloud (IoT), and
    pause/resume logic.

    Attributes:
        phase: Current phase label (e.g. 'A', 'B', 'C').
        step_index: Current 1-based step index.
        step_total: Total number of steps in recipe.
        step_label: Human-readable step description.
        tip: Current tip state.
        wells: Map of well name to WellState.
        is_paused: True if protocol is paused.
        elapsed_s: Seconds since protocol started.
        pressure_data: Collected pressure samples.
        results: Per-step pass/fail results.
        recipe_name: Name of the loaded recipe.
    '''
    phase: str = ''
    step_index: int = 0
    step_total: int = 0
    step_label: str = ''
    tip: TipState = field(default_factory=TipState)
    wells: dict[str, WellState] = field(
        default_factory=dict,
    )
    is_paused: bool = False
    elapsed_s: float = 0.0
    pressure_data: list[dict] = field(
        default_factory=list,
    )
    results: list[dict] = field(default_factory=list)
    recipe_name: str = ''

    def to_dict(self) -> dict:
        '''Serialize to a JSON-compatible dict.'''
        return {
            'phase': self.phase,
            'step_index': self.step_index,
            'step_total': self.step_total,
            'step_label': self.step_label,
            'tip': self.tip.to_dict(),
            'wells': {
                name: ws.to_dict()
                for name, ws in self.wells.items()
            },
            'is_paused': self.is_paused,
            'elapsed_s': round(self.elapsed_s, 1),
            'results': list(self.results),
            'recipe_name': self.recipe_name,
        }


@dataclass
class StepDef:
    '''Parsed step definition from a YAML recipe.

    Attributes:
        type: Step type key (e.g. 'reagent_transfer').
        label: Human-readable step description.
        params: All parameters from the YAML step dict.
    '''
    type: str
    label: str
    params: dict[str, Any] = field(
        default_factory=dict,
    )


@dataclass
class PhaseDef:
    '''Parsed phase definition from a YAML recipe.

    Attributes:
        name: Phase identifier (e.g. 'A', 'B', 'C').
        label: Human-readable phase label.
        steps: Ordered list of step definitions.
    '''
    name: str
    label: str
    steps: list[StepDef] = field(
        default_factory=list,
    )


@dataclass
class Recipe:
    '''Fully loaded and validated protocol recipe.

    Attributes:
        name: Recipe display name.
        description: Recipe description text.
        constants: Shared constants (speeds, thresholds).
        wells: Well definitions (name -> attrs).
        phases: Ordered list of phase definitions.
        total_steps: Pre-computed total step count.
        reader: Optional reader acquisition overrides
            (merged with app config for the run).
        peak_detect: Optional peak detection overrides.
    '''
    name: str
    description: str = ''
    constants: dict[str, Any] = field(
        default_factory=dict,
    )
    wells: dict[str, dict[str, Any]] = field(
        default_factory=dict,
    )
    phases: list[PhaseDef] = field(
        default_factory=list,
    )
    total_steps: int = 0
    reader: dict[str, Any] = field(
        default_factory=dict,
    )
    peak_detect: dict[str, Any] = field(
        default_factory=dict,
    )
