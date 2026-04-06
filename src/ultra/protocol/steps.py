'''ultra.protocol.steps -- Step type executors.

Each step type is a class with an execute() method. The runner
dispatches by the 'type' field in the YAML step definition.
New step types can be added by decorating a class with
@step_type('name').

All execute() methods are synchronous -- the entire protocol
loop runs in a dedicated OS thread so STM32 serial calls
block naturally without starving the asyncio event loop.
'''
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ultra.protocol.runner import ProtocolRunner

LOG = logging.getLogger(__name__)

STEP_REGISTRY: dict[str, type[StepExecutor]] = {}


def step_type(name: str):
    '''Decorator to register a step executor class.

    Args:
        name: Step type key matching YAML 'type' field.
    '''
    def wrapper(cls: type[StepExecutor]):
        STEP_REGISTRY[name] = cls
        return cls
    return wrapper


class StepExecutor:
    '''Base class for all step executors.

    Subclasses must implement execute() which performs
    the hardware operations for a single protocol step.
    All methods are synchronous -- they run in the
    protocol thread.
    '''

    def execute(
            self,
            params: dict[str, Any],
            runner: 'ProtocolRunner',
    ) -> bool:
        '''Execute one protocol step.

        Args:
            params: Step parameters from YAML recipe.
            runner: Protocol runner providing hardware
                interfaces and state tracker.

        Returns:
            True if step succeeded, False on failure.
        '''
        raise NotImplementedError


@step_type('centrifuge_unlock')
class CentrifugeUnlockStep(StepExecutor):
    '''Unlock the cartridge holder.'''

    def execute(self, params, runner) -> bool:
        r = runner.stm32.send_command(
            cmd={'cmd': 'centrifuge_unlock'},
            timeout_s=600.0,
        )
        return _ok(r)


@step_type('centrifuge_lock')
class CentrifugeLockStep(StepExecutor):
    '''Lock the cartridge holder.'''

    def execute(self, params, runner) -> bool:
        r = runner.stm32.send_command(
            cmd={'cmd': 'centrifuge_lock'},
            timeout_s=600.0,
        )
        return _ok(r)


@step_type('centrifuge_spin')
class CentrifugeSpinStep(StepExecutor):
    '''Spin centrifuge at given RPM for duration.'''

    def execute(self, params, runner) -> bool:
        rpm = params.get('rpm', 500)
        duration_s = params.get('duration_s', 5)
        r = runner.stm32.send_command(
            cmd={
                'cmd': 'centrifuge_start',
                'rpm': rpm,
                'duration': duration_s,
            },
            timeout_s=float(duration_s) + 30.0,
        )
        if not _ok(r):
            return False
        ok = runner.stm32.wait_centrifuge_idle(
            timeout_s=float(duration_s) + 60.0,
        )
        return ok


@step_type('centrifuge_rotate')
class CentrifugeRotateStep(StepExecutor):
    '''Rotate centrifuge to a specific angle.'''

    def execute(self, params, runner) -> bool:
        angle = params.get('angle_001deg', 0)
        move_rpm = params.get('move_rpm', 1)
        r = runner.stm32.send_command(
            cmd={
                'cmd': 'centrifuge_move_angle',
                'angle_001deg': angle,
                'move_rpm': move_rpm,
            },
            timeout_s=60.0,
        )
        if not _ok(r):
            return False
        time.sleep(1.0)
        return True


@step_type('lift_move')
class LiftMoveStep(StepExecutor):
    '''Move lift to a target height in mm.'''

    def execute(self, params, runner) -> bool:
        target_mm = params.get('target_mm', 18.0)
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'lift_move',
                'target_mm': target_mm,
            },
            timeout_s=90.0,
        )
        if not _ok(r):
            return False
        ok = runner.stm32.wait_lift_idle(
            target_mm=target_mm,
            timeout_s=90.0,
        )
        return ok


@step_type('lid')
class LidStep(StepExecutor):
    '''Open or close the lid.'''

    def execute(self, params, runner) -> bool:
        open_lid = params.get('open', True)
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'lid_move',
                'open': open_lid,
            },
            timeout_s=30.0,
        )
        return _ok(r)


@step_type('tip_pick')
class TipPickStep(StepExecutor):
    '''Pick up a tip via gantry_tip_swap from_id=0.

    Sway uses gantry_tip_swap for all tip operations,
    not the raw tip_pickup command.
    '''

    def execute(self, params, runner) -> bool:
        tip_id = params.get('tip_id', 4)
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'gantry_tip_swap',
                'from_id': 0,
                'to_id': tip_id,
            },
            timeout_s=120.0,
        )
        if _ok(r):
            runner.tracker.update_tip(tip_id)
        return _ok(r)


@step_type('tip_swap')
class TipSwapStep(StepExecutor):
    '''Swap from one tip to another.'''

    def execute(self, params, runner) -> bool:
        from_id = params.get('from_id', 4)
        to_id = params.get('to_id', 5)
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'gantry_tip_swap',
                'from_id': from_id,
                'to_id': to_id,
            },
            timeout_s=120.0,
        )
        if _ok(r):
            runner.tracker.update_tip(to_id)
        return _ok(r)


@step_type('tip_return')
class TipReturnStep(StepExecutor):
    '''Return tip via gantry_tip_swap to_id=0.'''

    def execute(self, params, runner) -> bool:
        tip_id = params.get('tip_id', 5)
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'gantry_tip_swap',
                'from_id': tip_id,
                'to_id': 0,
            },
            timeout_s=120.0,
        )
        if _ok(r):
            runner.tracker.update_tip(0)
        return _ok(r)


@step_type('lld')
class LLDStep(StepExecutor):
    '''Detect cartridge Z via liquid level detection.

    Sends lld_perform to the STM32. On success, converts
    the returned z_position (µsteps) to mm and stores it
    in ``runner.cartridge_z_mm`` for subsequent
    ``cart_dispense_at`` calls.  Homes Z after detection,
    matching sway's sequence.
    '''

    def execute(self, params, runner) -> bool:
        from ultra.hw.stm32_interface import (
            Z_USTEPS_PER_MM,
        )
        threshold = params.get(
            'threshold',
            runner.recipe.constants.get(
                'lld_threshold', 20,
            ),
        )
        r = runner.stm32.perform_lld(
            threshold=threshold,
        )
        if r and r.get('detected'):
            z_usteps = r.get('z_position', 0)
            runner.cartridge_z_mm = (
                z_usteps / Z_USTEPS_PER_MM
            )
            LOG.info(
                'LLD detected: z=%d usteps = %.2f mm',
                z_usteps, runner.cartridge_z_mm,
            )
            runner.stm32.send_command_wait_done(
                cmd={'cmd': 'home_z_axis'},
                timeout_s=30.0,
            )
            return True
        LOG.warning('LLD failed: %s', r)
        return r is not None


@step_type('reagent_transfer')
class ReagentTransferStep(StepExecutor):
    '''Aspirate from source, cart-dispense to target,
    return remainder to source.

    Handles: piston reset, air slug, LLF aspiration,
    cartridge dispense with reasp, blowout, pressure
    collection, well state updates.
    '''

    def execute(self, params, runner) -> bool:
        source = runner.tracker.get_well(
            params['source'],
        )
        target = runner.tracker.get_well(
            params['target'],
        )
        if source is None or target is None:
            LOG.error(
                'reagent_transfer: unknown well ref',
            )
            return False

        asp_vol = params['asp_vol']
        cart_vol = params['cart_vol']
        consts = runner.recipe.constants
        reasp = consts.get('reasp_ul', 12)
        remainder = asp_vol - cart_vol + reasp

        sa = runner.stm32.smart_aspirate_at(
            loc_id=source.loc_id,
            volume_ul=asp_vol,
            speed_ul_s=consts.get(
                'aspirate_speed', 40.0,
            ),
            lld_threshold=consts.get(
                'lld_threshold', 20,
            ),
            piston_reset=True,
            air_slug_ul=consts.get('air_slug_ul', 40),
            stream=False,
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-asp_vol,
            operation=f'asp {asp_vol}uL',
        )
        runner.collect_pressure(sa, params['label'])

        cd_r = runner.stm32.cart_dispense_at(
            loc_id=target.loc_id,
            volume_ul=cart_vol,
            vel_ul_s=params.get(
                'cart_vel',
                consts.get('cart_disp_vel', 1.5),
            ),
            reasp_ul=reasp,
            cartridge_z=runner.cartridge_z_mm,
            stream=False,
        )
        if not cd_r:
            return False
        runner.tracker.update_well(
            target.name, delta_ul=cart_vol,
            operation=f'disp {cart_vol}uL',
        )
        if isinstance(cd_r, dict):
            runner.collect_pressure(
                cd_r, params['label'],
            )

        ok = runner.stm32.well_dispense_at(
            loc_id=source.loc_id,
            volume_ul=int(remainder),
            speed_ul_s=consts.get(
                'well_disp_speed', 100.0,
            ),
            blowout=True,
        )
        runner.tracker.update_well(
            source.name, delta_ul=remainder,
            operation=f'return {int(remainder)}uL',
        )
        return ok


@step_type('reagent_transfer_bf')
class ReagentTransferBFStep(StepExecutor):
    '''Back-and-forth variant for slow-binding reagents.

    Used for SampleDil1A and SA-GNP type steps that need
    prolonged incubation with mixing.
    '''

    def execute(self, params, runner) -> bool:
        source = runner.tracker.get_well(
            params['source'],
        )
        target = runner.tracker.get_well(
            params['target'],
        )
        if source is None or target is None:
            LOG.error(
                'reagent_transfer_bf: unknown well ref',
            )
            return False

        asp_vol = params['asp_vol']
        cart_vol = params['cart_vol']
        consts = runner.recipe.constants
        reasp = consts.get('reasp_ul', 12)
        remainder = asp_vol - cart_vol + reasp

        sa = runner.stm32.smart_aspirate_at(
            loc_id=source.loc_id,
            volume_ul=asp_vol,
            speed_ul_s=consts.get(
                'aspirate_speed', 40.0,
            ),
            lld_threshold=consts.get(
                'lld_threshold', 20,
            ),
            piston_reset=True,
            air_slug_ul=consts.get('air_slug_ul', 40),
            stream=False,
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-asp_vol,
            operation=f'asp {asp_vol}uL',
        )
        runner.collect_pressure(sa, params['label'])

        cd_r = runner.stm32.cart_dispense_bf_at(
            loc_id=target.loc_id,
            total_volume_ul=cart_vol,
            vel_ul_s=params.get(
                'cart_vel',
                consts.get('cart_disp_vel', 1.5),
            ),
            for_vol_ul=params.get('for_vol', 60),
            back_vol_ul=params.get('back_vol', 30),
            reasp_ul=reasp,
            sleep_s=params.get('sleep_s', 30),
            cartridge_z=runner.cartridge_z_mm,
            stream=False,
        )
        if not cd_r:
            return False
        runner.tracker.update_well(
            target.name, delta_ul=cart_vol,
            operation=f'disp_bf {cart_vol}uL',
        )
        if isinstance(cd_r, dict):
            runner.collect_pressure(
                cd_r, params['label'],
            )

        ok = runner.stm32.well_dispense_at(
            loc_id=source.loc_id,
            volume_ul=int(remainder),
            speed_ul_s=consts.get(
                'well_disp_speed', 100.0,
            ),
            blowout=True,
        )
        runner.tracker.update_well(
            source.name, delta_ul=remainder,
            operation=f'return {int(remainder)}uL',
        )
        return ok


@step_type('well_transfer')
class WellToWellStep(StepExecutor):
    '''Direct well-to-well transfer (no cartridge).'''

    def execute(self, params, runner) -> bool:
        source = runner.tracker.get_well(
            params['source'],
        )
        dest = runner.tracker.get_well(params['dest'])
        if source is None or dest is None:
            LOG.error(
                'well_transfer: unknown well ref',
            )
            return False

        volume = params['volume']
        ok = runner.stm32.aspirate_at(
            loc_id=source.loc_id,
            volume_ul=volume,
            piston_reset=True,
        )
        if not ok:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-volume,
            operation=f'asp {volume}uL',
        )

        ok = runner.stm32.dispense_at(
            loc_id=dest.loc_id,
            volume_ul=volume,
        )
        if not ok:
            return False
        runner.tracker.update_well(
            dest.name, delta_ul=volume,
            operation=f'disp {volume}uL',
        )
        return True


@step_type('well_transfer_return')
class WellTransferReturnStep(StepExecutor):
    '''Aspirate from source, dispense to dest, return
    remainder to source with blowout.

    Used by salt protocol where asp_vol > disp_vol and
    the overshoot is returned to the source well.
    '''

    def execute(self, params, runner) -> bool:
        source = runner.tracker.get_well(
            params['source'],
        )
        dest = runner.tracker.get_well(params['dest'])
        if source is None or dest is None:
            LOG.error(
                'well_transfer_return: unknown well ref',
            )
            return False

        asp_vol = params['asp_vol']
        disp_vol = params['disp_vol']
        vel = params.get('vel', 20.0)
        consts = runner.recipe.constants

        sa = runner.stm32.smart_aspirate_at(
            loc_id=source.loc_id,
            volume_ul=asp_vol,
            speed_ul_s=consts.get(
                'aspirate_speed', 40.0,
            ),
            lld_threshold=consts.get(
                'lld_threshold', 20,
            ),
            piston_reset=True,
            air_slug_ul=consts.get(
                'air_slug_ul', 50,
            ),
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-asp_vol,
            operation=f'asp {asp_vol}uL',
        )

        ok = runner.stm32.well_dispense_at(
            loc_id=dest.loc_id,
            volume_ul=disp_vol,
            speed_ul_s=vel,
            blowout=False,
        )
        if not ok:
            return False
        runner.tracker.update_well(
            dest.name, delta_ul=disp_vol,
            operation=f'disp {disp_vol}uL',
        )

        remainder = asp_vol - disp_vol
        if remainder > 0:
            ok = runner.stm32.well_dispense_at(
                loc_id=source.loc_id,
                volume_ul=remainder,
                speed_ul_s=consts.get(
                    'well_disp_speed', 100.0,
                ),
                blowout=True,
            )
            if not ok:
                return False
            runner.tracker.update_well(
                source.name, delta_ul=remainder,
                operation=f'return {remainder}uL',
            )
        return True


@step_type('well_to_chip')
class WellToChipStep(StepExecutor):
    '''Aspirate from well, cart-dispense to chip through
    PP4, return small remainder to source.

    Used by salt protocol chip transfer phase. Aspirates
    chip_vol + overshoot_ul from source, cart-dispenses
    chip_vol through PP4, returns overshoot to source.
    '''

    def execute(self, params, runner) -> bool:
        source = runner.tracker.get_well(
            params['source'],
        )
        target = runner.tracker.get_well(
            params.get('target', 'PP4'),
        )
        if source is None or target is None:
            LOG.error(
                'well_to_chip: unknown well ref',
            )
            return False

        chip_vol = params['chip_vol']
        overshoot = params.get('overshoot_ul', 10)
        asp_vol = chip_vol + overshoot
        consts = runner.recipe.constants

        sa = runner.stm32.smart_aspirate_at(
            loc_id=source.loc_id,
            volume_ul=asp_vol,
            speed_ul_s=consts.get(
                'aspirate_speed', 40.0,
            ),
            lld_threshold=consts.get(
                'lld_threshold', 20,
            ),
            piston_reset=True,
            air_slug_ul=consts.get(
                'air_slug_ul', 50,
            ),
            stream=params.get('stream', True),
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-asp_vol,
            operation=f'asp {asp_vol}uL',
        )
        runner.collect_pressure(sa, params['label'])

        cd_r = runner.stm32.cart_dispense_at(
            loc_id=target.loc_id,
            volume_ul=chip_vol,
            vel_ul_s=params.get(
                'chip_vel',
                consts.get('chip_disp_vel', 1.0),
            ),
            reasp_ul=consts.get('reasp_ul', 15),
            cartridge_z=runner.cartridge_z_mm,
            stream=params.get('stream', True),
        )
        if not cd_r:
            return False
        runner.tracker.update_well(
            target.name, delta_ul=chip_vol,
            operation=f'disp {chip_vol}uL',
        )
        if isinstance(cd_r, dict):
            runner.collect_pressure(
                cd_r, params['label'],
            )

        ok = runner.stm32.well_dispense_at(
            loc_id=source.loc_id,
            volume_ul=overshoot,
            speed_ul_s=consts.get(
                'well_disp_speed', 100.0,
            ),
            blowout=True,
        )
        runner.tracker.update_well(
            source.name, delta_ul=overshoot,
            operation=f'return {overshoot}uL',
        )
        return ok


@step_type('tip_mix')
class TipMixStep(StepExecutor):
    '''Mix reagent in a well by repeated asp/disp cycles.'''

    def execute(self, params, runner) -> bool:
        well = runner.tracker.get_well(params['well'])
        if well is None:
            LOG.error('tip_mix: unknown well ref')
            return False
        return runner.stm32.tip_mix_at(
            loc_id=well.loc_id,
            mix_vol_ul=params.get('mix_vol', 150),
            speed_ul_s=params.get('speed', 100.0),
            cycles=params.get('cycles', 4),
            pull_vol_ul=params.get('pull_vol', 0),
        )


@step_type('home_all')
class HomeAllStep(StepExecutor):
    '''Home all axes.'''

    def execute(self, params, runner) -> bool:
        r = runner.stm32.send_command_wait_done(
            cmd={'cmd': 'home_all'},
            timeout_s=60.0,
        )
        return _ok(r)


@step_type('home_close')
class HomeCloseStep(StepExecutor):
    '''Home all axes and close lid.'''

    def execute(self, params, runner) -> bool:
        r = runner.stm32.send_command_wait_done(
            cmd={'cmd': 'home_all'},
            timeout_s=60.0,
        )
        if not _ok(r):
            return False
        r = runner.stm32.send_command_wait_done(
            cmd={'cmd': 'lid_move', 'open': False},
            timeout_s=30.0,
        )
        return _ok(r)


@step_type('pump_init')
class PumpInitStep(StepExecutor):
    '''Initialize the pump.'''

    def execute(self, params, runner) -> bool:
        r = runner.stm32.send_command_wait_done(
            cmd={'cmd': 'pump_init'},
            timeout_s=30.0,
        )
        return _ok(r)


@step_type('led_pattern')
class LEDPatternStep(StepExecutor):
    '''Set LED pattern.'''

    def execute(self, params, runner) -> bool:
        pattern = params.get('pattern', 0)
        stage = params.get('stage', 0)
        r = runner.stm32.send_command(
            cmd={
                'cmd': 'led_set_pattern',
                'pattern': pattern,
                'stage': stage,
            },
        )
        return _ok(r)


@step_type('delay')
class DelayStep(StepExecutor):
    '''Wait for a specified duration.'''

    def execute(self, params, runner) -> bool:
        seconds = params.get('seconds', 1.0)
        time.sleep(seconds)
        return True


def _ok(resp: dict | None) -> bool:
    '''Check if a command response indicates success.'''
    return bool(resp and resp.get('status') == 'OK')
