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


@step_type('set_loc_offset')
class SetLocOffsetStep(StepExecutor):
    '''Send a global calibration offset to the firmware.

    Shifts every named cartridge location by (dx, dy, dz).
    Reads defaults from recipe constants ``loc_offset_x_um``,
    ``loc_offset_y_um``, ``loc_offset_z_um``; per-step
    overrides via params.
    '''

    def execute(self, params, runner) -> bool:
        consts = runner.recipe.constants
        dx = int(params.get(
            'dx_um',
            consts.get('loc_offset_x_um', 0),
        ))
        dy = int(params.get(
            'dy_um',
            consts.get('loc_offset_y_um', 0),
        ))
        dz = int(params.get(
            'dz_um',
            consts.get('loc_offset_z_um', 0),
        ))
        r = runner.stm32.send_command(
            cmd={
                'cmd': 'set_loc_offset',
                'dx_um': dx,
                'dy_um': dy,
                'dz_um': dz,
            },
            timeout_s=10.0,
        )
        if _ok(r):
            LOG.info(
                f'set_loc_offset: '
                f'dx={dx} dy={dy} dz={dz} um',
            )
        return _ok(r)


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
    '''Spin centrifuge at given RPM for duration.

    Matches sway: send centrifuge_start, sleep for the
    full spin duration, then poll centrifuge_status until
    READY.  Continues even if the idle check times out
    (sway just warns).
    '''

    _MAX_RETRIES = 5
    _RETRY_DELAY_S = 1.0

    def execute(self, params, runner) -> bool:
        consts = runner.recipe.constants
        if consts.get('skip_centrifuge_spin'):
            LOG.info('centrifuge_spin SKIPPED (constant)')
            return True

        rpm = params.get('rpm', 500)
        duration_s = params.get('duration_s', 5)
        r = None
        for attempt in range(1, self._MAX_RETRIES + 1):
            r = runner.stm32.send_command(
                cmd={
                    'cmd': 'centrifuge_start',
                    'rpm': rpm,
                    'duration': duration_s,
                },
                timeout_s=10.0,
            )
            if _ok(r):
                break
            LOG.warning(
                'centrifuge_start failed (attempt %d/%d)',
                attempt, self._MAX_RETRIES,
            )
            if attempt < self._MAX_RETRIES:
                time.sleep(self._RETRY_DELAY_S)
        if not _ok(r):
            LOG.error(
                'centrifuge_start failed after %d attempts',
                self._MAX_RETRIES,
            )
            return False
        time.sleep(float(duration_s))
        r_stop = runner.stm32.send_command(
            cmd={'cmd': 'centrifuge_stop'},
            timeout_s=10.0,
        )
        if not _ok(r_stop):
            LOG.warning('centrifuge_stop failed')
        ok = runner.stm32.wait_centrifuge_idle(
            timeout_s=10.0,
        )
        if not ok:
            LOG.warning(
                'Centrifuge not idle after spin -- '
                'waiting for firmware cleanup',
            )
            time.sleep(5.0)

        LOG.info('Power-cycling BLDC after spin')
        runner.stm32.send_command(
            cmd={'cmd': 'centrifuge_power', 'enable': False},
            timeout_s=5.0,
        )
        time.sleep(1.0)
        runner.stm32.send_command(
            cmd={'cmd': 'centrifuge_power', 'enable': True},
            timeout_s=5.0,
        )
        time.sleep(1.0)

        LOG.info('Re-homing BLDC encoder after power cycle')
        r_home = runner.stm32.send_command(
            cmd={'cmd': 'centrifuge_home'},
            timeout_s=60.0,
        )
        if not _ok(r_home):
            LOG.warning('centrifuge_home after spin failed')

        return True


@step_type('centrifuge_rotate')
class CentrifugeRotateStep(StepExecutor):
    '''Rotate centrifuge carousel to a specific angle.

    Matches sway: send_command (ACK only, no DONE wait),
    then a 1-second settle sleep.  On BLDC move_angle
    failure, resets the driver and retries up to 3 times.
    '''

    _MAX_RETRIES = 3

    def execute(self, params, runner) -> bool:
        angle = params.get('angle_001deg', 0)
        cmd = {
            'cmd': 'centrifuge_move_angle',
            'angle_001deg': angle,
            'move_rpm': runner.recipe.constants.get(
                'move_rpm', 1,
            ),
        }
        for attempt in range(1, self._MAX_RETRIES + 1):
            r = runner.stm32.send_command(
                cmd=cmd, timeout_s=120.0,
            )
            if _ok(r):
                time.sleep(1.0)
                return True
            LOG.warning(
                'centrifuge_move_angle failed '
                '(attempt %d/%d)',
                attempt, self._MAX_RETRIES,
            )
            if attempt < self._MAX_RETRIES:
                _bldc_reset(runner.stm32)
        return False


@step_type('centrifuge_shake')
class CentrifugeShakeStep(StepExecutor):
    '''Shake the carousel back and forth around a centre
    angle to agitate the sample after lowering to the
    carousel.

    Each cycle rotates to (centre + amplitude) then to
    (centre - amplitude). After all cycles the carousel
    returns to the centre angle.

    Params (YAML):
        centre_angle_001deg: Centre position in 0.01 deg
            units. Defaults to angle_open_initial_deg * 100
            from recipe constants (i.e. 290 deg = 29000).
        shake_angle_deg: Amplitude in degrees (default
            from recipe constant ``shake_angle_deg``, or
            45 if unset).
        cycles: Number of full back-and-forth cycles
            (default from recipe constant
            ``shake_cycles``, or 3 if unset).
    '''

    def execute(self, params, runner) -> bool:
        consts = runner.recipe.constants
        if consts.get('skip_centrifuge_shake'):
            LOG.info('centrifuge_shake SKIPPED (constant)')
            return True

        open_deg = consts.get(
            'angle_open_initial_deg', 290,
        )
        centre = params.get(
            'centre_angle_001deg',
            int(open_deg * 100),
        )
        amp_deg = params.get(
            'shake_angle_deg',
            consts.get('shake_angle_deg', 45),
        )
        cycles = params.get(
            'cycles',
            consts.get('shake_cycles', 3),
        )
        amp = int(amp_deg * 100)
        move_rpm = consts.get('move_rpm', 1)

        def _move(angle: int) -> bool:
            for attempt in range(1, 4):
                r = runner.stm32.send_command(
                    cmd={
                        'cmd': 'centrifuge_move_angle',
                        'angle_001deg': angle,
                        'move_rpm': move_rpm,
                    },
                    timeout_s=120.0,
                )
                if _ok(r):
                    time.sleep(1.0)
                    return True
                LOG.warning(
                    'shake move_angle(%d) failed '
                    '(attempt %d/3)', angle, attempt,
                )
                if attempt < 3:
                    _bldc_reset(runner.stm32)
            return False

        for i in range(int(cycles)):
            for target in (centre + amp, centre - amp):
                if not _move(target):
                    return False

        return _move(centre)


def _bldc_reset(stm32: Any) -> None:
    '''Stop the BLDC motor and clear any error state.

    Sends BLDC_STOP_MOTOR then BLDC_CLEAR_ERROR via the
    centrifuge_bldc_cmd pass-through.  Used before retrying
    a failed centrifuge move_angle.
    '''
    from ultra.hw import frame_protocol as fp
    LOG.info('BLDC reset: stopping motor + clearing error')
    stm32.send_command(
        cmd={
            'cmd': 'centrifuge_bldc_cmd',
            'bldc_cmd': fp.BLDC_STOP_MOTOR,
        },
        timeout_s=5.0,
    )
    time.sleep(0.5)
    stm32.send_command(
        cmd={
            'cmd': 'centrifuge_bldc_cmd',
            'bldc_cmd': fp.BLDC_CLEAR_ERROR,
        },
        timeout_s=5.0,
    )
    time.sleep(0.5)


def _centrifuge_goto_with_retry(
        cmd_name: str,
        runner: Any,
        max_retries: int = 3,
) -> bool:
    '''Send a centrifuge goto command with BLDC reset on failure.

    If the first attempt fails (BLDC move_angle error), resets
    the BLDC driver and retries up to ``max_retries`` times.
    '''
    consts = runner.recipe.constants
    cmd = {
        'cmd': cmd_name,
        'angle_open_initial_deg': consts.get(
            'angle_open_initial_deg', 290,
        ),
        'move_rpm': consts.get('move_rpm', 1),
    }
    for attempt in range(1, max_retries + 1):
        r = runner.stm32.send_command(
            cmd=cmd, timeout_s=120.0,
        )
        if _ok(r):
            time.sleep(1.0)
            return True
        LOG.warning(
            '%s failed (attempt %d/%d)',
            cmd_name, attempt, max_retries,
        )
        if attempt < max_retries:
            _bldc_reset(runner.stm32)
    return False


@step_type('centrifuge_goto_serum')
class CentrifugeGotoSerumStep(StepExecutor):
    '''Rotate centrifuge to serum-access position.

    Reads ``angle_open_initial_deg`` from recipe constants
    (default 290).  Firmware derives the target angle as
    open_init - 180 (i.e. 110 deg with the default).

    On BLDC move_angle failure, resets the driver and
    retries up to 3 times.
    '''

    def execute(self, params, runner) -> bool:
        return _centrifuge_goto_with_retry(
            'centrifuge_goto_serum', runner,
        )


@step_type('centrifuge_goto_pipette')
class CentrifugeGotoPipetteStep(StepExecutor):
    '''Rotate centrifuge to pipette-access position.

    Reads ``angle_open_initial_deg`` and ``move_rpm``
    from recipe constants.  Firmware derives the target
    angle as open_init - 90 (i.e. 200 deg with default
    290).

    On BLDC move_angle failure, resets the driver and
    retries up to 3 times.
    '''

    def execute(self, params, runner) -> bool:
        return _centrifuge_goto_with_retry(
            'centrifuge_goto_pipette', runner,
        )


@step_type('lift_move')
class LiftMoveStep(StepExecutor):
    '''Move lift to a target height in mm.

    Matches sway: send_command_wait_done (DONE response
    confirms the move finished), then a short settle
    sleep.  No extra position-polling -- the DONE message
    from firmware is sufficient.
    '''

    def execute(self, params, runner) -> bool:
        target_mm = params.get('target_mm', 18.0)
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'lift_move',
                'target_mm': target_mm,
            },
            timeout_s=120.0,
        )
        if not _ok(r):
            return False
        time.sleep(1.0)
        return True


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


@step_type('move_to_location')
class MoveToLocationStep(StepExecutor):
    '''Move gantry XY to a well/port location.

    Pure positioning command -- moves the gantry above
    the target location without any pump operation.
    Used to pre-position the tip before centrifuge
    rotation (e.g. serum port access).
    '''

    def execute(self, params, runner) -> bool:
        well = runner.tracker.get_well(params['well'])
        if well is None:
            LOG.error(
                'move_to_location: unknown well ref',
            )
            return False
        speed = int(params.get('speed_01mms', 250))
        r = runner.stm32.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': well.loc_id,
                'speed_01mms': speed,
            },
            timeout_s=120.0,
        )
        if not _ok(r):
            return False
        time.sleep(0.5)
        return True


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
    ``cart_dispense_at`` calls.

    If LLD fails (e.g. dry cartridge on first run), falls
    back to ``default_cartridge_z_mm`` from recipe
    constants so the dispense can still reach the port.

    Always homes Z after the probe, matching sway.
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
        else:
            default_z = runner.recipe.constants.get(
                'default_cartridge_z_mm', 0.0,
            )
            runner.cartridge_z_mm = default_z
            LOG.warning(
                'LLD did not detect liquid -- '
                'using default_cartridge_z_mm=%.2f '
                '(resp=%s)',
                default_z, r,
            )
        runner.stm32.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=30.0,
        )
        return True


@step_type('reagent_transfer')
class ReagentTransferStep(StepExecutor):
    '''Aspirate from source, cart-dispense to target,
    return remainder to source.

    Handles: piston reset, air slug, LLF aspiration,
    cartridge dispense with reasp, blowout, pressure
    collection, well state updates.

    Set ``skip_aspirate: true`` when a preceding tip_mix
    with ``pull_vol`` already loaded the liquid into the
    tip so the LLF aspiration is unnecessary.
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
        reasp = params.get(
            'reasp_ul',
            consts.get('reasp_ul', 12),
        )
        remainder = asp_vol - cart_vol + reasp

        if params.get('skip_aspirate'):
            LOG.info(
                'reagent_transfer: skip_aspirate '
                '(liquid already in tip)',
            )
            runner.tracker.update_well(
                source.name, delta_ul=-asp_vol,
                operation=f'asp {asp_vol}uL (pre-pulled)',
            )
        else:
            stream = params.get('stream', False)
            sa = runner.stm32.smart_aspirate_at(
                loc_id=source.loc_id,
                volume_ul=asp_vol,
                speed_ul_s=params.get(
                    'asp_speed',
                    consts.get('aspirate_speed', 40.0),
                ),
                lld_threshold=consts.get(
                    'lld_threshold', 20,
                ),
                piston_reset=True,
                air_slug_ul=params.get(
                    'air_slug',
                    consts.get('air_slug_ul', 40),
                ),
                stream=stream,
                foil_detect=not source.foil_punctured,
            )
            if sa is None:
                return False
            runner.tracker.update_well(
                source.name, delta_ul=-asp_vol,
                operation=f'asp {asp_vol}uL',
            )
            runner.collect_pressure(
                sa, params['label'],
                operation='aspirate', phase='LLF',
            )

        label = params.get('label', 'reagent_transfer')

        cd_r = runner.stm32.cart_dispense_at(
            loc_id=target.loc_id,
            volume_ul=cart_vol,
            vel_ul_s=params.get(
                'cart_vel',
                consts.get('cart_disp_vel', 1.5),
            ),
            reasp_ul=reasp,
            cartridge_z=runner.cartridge_z_mm,
            stream=params.get('stream', False),
            pre_dispense_cb=lambda: _emit_timing_marker(
                runner, label, 'start',
            ),
        )
        if not cd_r:
            return False

        _emit_timing_marker(runner, label, 'stop')

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
            speed_ul_s=params.get(
                'return_speed',
                consts.get('well_disp_speed', 100.0),
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

    The firmware runs the back-and-forth for a fixed
    ``duration_s`` rather than a target volume. The
    ``cart_vol`` parameter is still required for volume
    bookkeeping (remainder return and well state tracking).

    Set ``skip_aspirate: true`` when a preceding tip_mix
    with ``pull_vol`` already loaded the liquid into the
    tip so the LLF aspiration is unnecessary.
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
        duration_s = params['duration_s']
        consts = runner.recipe.constants
        reasp = params.get(
            'reasp_ul',
            consts.get('reasp_ul', 12),
        )
        remainder = asp_vol - cart_vol + reasp

        if params.get('skip_aspirate'):
            LOG.info(
                'reagent_transfer_bf: skip_aspirate '
                '(liquid already in tip)',
            )
            runner.tracker.update_well(
                source.name, delta_ul=-asp_vol,
                operation=f'asp {asp_vol}uL (pre-pulled)',
            )
        else:
            stream = params.get('stream', False)
            sa = runner.stm32.smart_aspirate_at(
                loc_id=source.loc_id,
                volume_ul=asp_vol,
                speed_ul_s=params.get(
                    'asp_speed',
                    consts.get('aspirate_speed', 40.0),
                ),
                lld_threshold=consts.get(
                    'lld_threshold', 20,
                ),
                piston_reset=True,
                air_slug_ul=params.get(
                    'air_slug',
                    consts.get('air_slug_ul', 40),
                ),
                stream=stream,
                foil_detect=not source.foil_punctured,
            )
            if sa is None:
                return False
            runner.tracker.update_well(
                source.name, delta_ul=-asp_vol,
                operation=f'asp {asp_vol}uL',
            )
            runner.collect_pressure(
                sa, params['label'],
                operation='aspirate', phase='LLF',
            )

        label = params.get('label', 'reagent_transfer_bf')

        cd_r = runner.stm32.cart_dispense_bf_at(
            loc_id=target.loc_id,
            duration_s=duration_s,
            vel_ul_s=params.get(
                'cart_vel',
                consts.get('cart_disp_vel', 1.5),
            ),
            for_vol_ul=params.get('for_vol', 60),
            back_vol_ul=params.get('back_vol', 30),
            reasp_ul=reasp,
            sleep_s=params.get('sleep_s', 30),
            cartridge_z=runner.cartridge_z_mm,
            stream=params.get('stream', False),
            pre_dispense_cb=lambda: _emit_timing_marker(
                runner, label, 'start',
            ),
        )
        if not cd_r:
            return False

        _emit_timing_marker(runner, label, 'stop')

        runner.tracker.update_well(
            target.name, delta_ul=cart_vol,
            operation=f'disp_bf ~{cart_vol}uL ({duration_s}s)',
        )
        if isinstance(cd_r, dict):
            runner.collect_pressure(
                cd_r, params['label'],
            )

        ok = runner.stm32.well_dispense_at(
            loc_id=source.loc_id,
            volume_ul=int(remainder),
            speed_ul_s=params.get(
                'return_speed',
                consts.get('well_disp_speed', 100.0),
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
            foil_detect=not source.foil_punctured,
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
            foil_detect=not source.foil_punctured,
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-asp_vol,
            operation=f'asp {asp_vol}uL',
        )
        runner.collect_pressure(
            sa, params['label'],
            operation='aspirate', phase='LLF',
        )

        label = params.get('label', 'well_to_chip')

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
            pre_dispense_cb=lambda: _emit_timing_marker(
                runner, label, 'start',
            ),
        )
        if not cd_r:
            return False

        _emit_timing_marker(runner, label, 'stop')

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


@step_type('home_z')
class HomeZStep(StepExecutor):
    '''Home Z axis only (retract tip before rotation).'''

    def execute(self, params, runner) -> bool:
        r = runner.stm32.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=120.0,
        )
        return _ok(r)


@step_type('well_dispense')
class WellDispenseStep(StepExecutor):
    '''Dispense into a well at the given location.

    Used for targeted dispenses (e.g. serum to dilution
    well, waste) where volume, speed, and blowout are
    specified per-step rather than derived from a
    reagent_transfer flow.
    '''

    def execute(self, params, runner) -> bool:
        well = runner.tracker.get_well(params['well'])
        if well is None:
            LOG.error('well_dispense: unknown well ref')
            return False
        volume = params['volume']
        speed = params.get('speed', 100.0)
        blowout = params.get('blowout', True)
        ok = runner.stm32.well_dispense_at(
            loc_id=well.loc_id,
            volume_ul=volume,
            speed_ul_s=speed,
            blowout=blowout,
        )
        if ok:
            runner.tracker.update_well(
                well.name, delta_ul=volume,
                operation=f'disp {volume}uL',
            )
        return ok


@step_type('smart_aspirate')
class SmartAspirateStep(StepExecutor):
    '''Smart-aspirate from a well with per-step overrides.

    Uses LLD + air slug like reagent_transfer but only
    performs the aspiration (no cart dispense). Used for
    serum aspiration where dispensing is a separate step.
    '''

    def execute(self, params, runner) -> bool:
        well = runner.tracker.get_well(params['well'])
        if well is None:
            LOG.error(
                'smart_aspirate: unknown well ref',
            )
            return False
        consts = runner.recipe.constants
        volume = params['volume']
        sa = runner.stm32.smart_aspirate_at(
            loc_id=well.loc_id,
            volume_ul=volume,
            speed_ul_s=params.get(
                'speed',
                consts.get('aspirate_speed', 40.0),
            ),
            lld_threshold=params.get(
                'lld_threshold',
                consts.get('lld_threshold', 20),
            ),
            piston_reset=params.get(
                'piston_reset', True,
            ),
            air_slug_ul=params.get(
                'air_slug',
                consts.get('air_slug_ul', 40),
            ),
            stream=params.get('stream', False),
            foil_detect=not well.foil_punctured,
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            well.name, delta_ul=-volume,
            operation=f'asp {volume}uL',
        )
        runner.collect_pressure(
            sa,
            params.get('label', 'smart_aspirate'),
            operation='aspirate', phase='LLF',
        )
        return True


@step_type('dilution_transfer')
class DilutionTransferStep(StepExecutor):
    '''Smart-aspirate from source, dispense to dest.

    Combines smart_aspirate_at + well_dispense_at for
    dilution steps (e.g. buffer to serum well). Differs
    from reagent_transfer in that there is no cartridge
    dispense -- liquid goes directly well-to-well.
    '''

    def execute(self, params, runner) -> bool:
        source = runner.tracker.get_well(
            params['source'],
        )
        dest = runner.tracker.get_well(params['dest'])
        if source is None or dest is None:
            LOG.error(
                'dilution_transfer: unknown well ref',
            )
            return False

        consts = runner.recipe.constants
        volume = params['volume']

        sa = runner.stm32.smart_aspirate_at(
            loc_id=source.loc_id,
            volume_ul=volume,
            speed_ul_s=params.get(
                'asp_speed',
                consts.get('aspirate_speed', 40.0),
            ),
            lld_threshold=consts.get(
                'lld_threshold', 20,
            ),
            piston_reset=True,
            air_slug_ul=params.get(
                'air_slug',
                consts.get('air_slug_ul', 40),
            ),
            stream=params.get('stream', True),
            foil_detect=not source.foil_punctured,
        )
        if sa is None:
            return False
        runner.tracker.update_well(
            source.name, delta_ul=-volume,
            operation=f'asp {volume}uL',
        )
        runner.collect_pressure(
            sa,
            params.get('label', 'dilution_transfer'),
            operation='aspirate', phase='LLF',
        )

        ok = runner.stm32.well_dispense_at(
            loc_id=dest.loc_id,
            volume_ul=volume,
            speed_ul_s=params.get(
                'disp_speed',
                consts.get('well_disp_speed', 100.0),
            ),
            blowout=params.get('blowout', True),
        )
        if ok:
            runner.tracker.update_well(
                dest.name, delta_ul=volume,
                operation=f'disp {volume}uL',
            )
        return ok


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


_SAMPLE_LOG_HEADER = (
    'datetime,start_time,well_name,'
    'volume,flow_rate,sample,rpm,tecan_port_num\n'
)


def _emit_timing_marker(
        runner, label: str, event_type: str,
) -> None:
    '''Emit a timing marker event and write to sample.log.

    Shared helper used by reagent transfer and chip dispense
    steps to automatically bracket cartridge dispenses with
    start/stop markers.

    Args:
        runner: Protocol runner instance.
        label: Marker label (e.g. reagent name).
        event_type: ``start`` or ``stop``.
    '''
    from datetime import datetime
    import os.path as op

    snap = runner.tracker.snapshot()
    elapsed = round(snap.elapsed_s, 2)
    now_str = datetime.now().strftime(
        '%Y/%m/%d-%H:%M:%S',
    )
    sample_name = f'TIMING: {label} ({event_type})'

    LOG.info(
        'timing_marker: %s at %.2fs',
        sample_name, elapsed,
    )

    run_dir = getattr(runner, '_run_dir', None)
    if run_dir:
        path = op.join(run_dir, 'sample.log')
        try:
            with open(path, 'a') as fh:
                if fh.tell() == 0:
                    fh.write(_SAMPLE_LOG_HEADER)
                fh.write(
                    f'{now_str},{elapsed},'
                    f'Timing_Marker,0,0,'
                    f'{sample_name},0,\n',
                )
        except Exception as exc:
            LOG.warning(
                'Failed to write sample.log: %s', exc,
            )

    runner._event_bus.emit_sync(
        'timing_marker', {
            'elapsed_s': elapsed,
            'label': label,
            'event_type': event_type,
            'trigger_analysis': False,
        },
    )


@step_type('timing_marker')
class TimingMarkerStep(StepExecutor):
    '''Record a timing marker in sample.log and emit a
    ``timing_marker`` event for the GUI sensorgram.

    Writes a sway-compatible ``sample.log`` row with the
    ``TIMING:`` prefix so downstream analysis can identify
    marker rows. The ``start_time`` column uses the same
    ``tracker.elapsed_s`` clock as peak data, keeping the
    marker x-position aligned with the sensorgram.

    Optionally triggers analysis (stub).
    '''

    def execute(self, params, runner) -> bool:
        '''Write timing to sample.log and broadcast.

        Args:
            params: Step params from YAML recipe.
                ``label`` (str): marker event name.
                ``event_type`` (str): ``start`` or ``stop``
                    (controls vertical line colour).
                ``trigger_analysis`` (bool): whether to
                    invoke the analysis stub.
            runner: Protocol runner instance.

        Returns:
            True (always succeeds).
        '''
        label = params.get('label', '')
        event_type = params.get('event_type', 'start')
        trigger = params.get(
            'trigger_analysis', False,
        )

        _emit_timing_marker(runner, label, event_type)

        LOG.info(
            'TimingMarker %s: trigger_analysis=%r',
            label, trigger,
        )
        if trigger:
            LOG.info('Calling runner.trigger_analysis()')
            runner.trigger_analysis()

        return True


def _ok(resp: dict | None) -> bool:
    '''Check if a command response indicates success.'''
    return bool(resp and resp.get('status') == 'OK')


# ---------------------------------------------------------------------------
# Parameter schemas for the GUI recipe builder.
# Each entry: {name, type, default, well_ref, required}
# well_ref=True means the param is a well name (rendered as dropdown).
# volume_out/volume_in hint which params move liquid for the simulator.
# ---------------------------------------------------------------------------

def _p(
    name, typ='number', default=None, *,
    well_ref=False, required=False,
    volume_out=False, volume_in=False,
):
    d = {'name': name, 'type': typ}
    if default is not None:
        d['default'] = default
    if well_ref:
        d['well_ref'] = True
    if required:
        d['required'] = True
    if volume_out:
        d['volume_out'] = True
    if volume_in:
        d['volume_in'] = True
    return d


STEP_DESCRIPTIONS: dict[str, str] = {
    'set_loc_offset':          'Send a global calibration offset to the firmware',
    'centrifuge_unlock':       'Unlock the cartridge holder',
    'centrifuge_lock':         'Lock the cartridge holder',
    'centrifuge_spin':         'Spin centrifuge at given RPM for duration',
    'centrifuge_rotate':       'Rotate centrifuge carousel to a specific angle',
    'centrifuge_shake':        'Shake the carousel back and forth',
    'centrifuge_goto_serum':   'Rotate centrifuge to serum-access position',
    'centrifuge_goto_pipette': 'Rotate centrifuge to pipette-access position',
    'lift_move':               'Move lift to a target height in mm',
    'lid':                     'Open or close the lid',
    'move_to_location':        'Move gantry XY to a well/port location',
    'tip_pick':                'Pick up a tip',
    'tip_swap':                'Swap from one tip to another',
    'tip_return':              'Return tip to rack',
    'lld':                     'Detect cartridge Z via liquid level detection',
    'reagent_transfer':        'Aspirate from source, cart-dispense to target',
    'reagent_transfer_bf':     'Back-and-forth reagent transfer for slow-binding reagents',
    'well_transfer':           'Direct well-to-well transfer (no cartridge)',
    'well_transfer_return':    'Aspirate from source, dispense to dest, return tip',
    'well_to_chip':            'Aspirate from well, cart-dispense to chip',
    'tip_mix':                 'Mix reagent in a well by repeated asp/disp cycles',
    'home_z':                  'Home Z axis only (retract tip before rotation)',
    'well_dispense':           'Dispense into a well at the given location',
    'smart_aspirate':          'Smart-aspirate from a well with LLD and piston reset',
    'dilution_transfer':       'Smart-aspirate from source, dispense to dest',
    'home_all':                'Home all axes',
    'home_close':              'Home all axes and close lid',
    'pump_init':               'Initialize the pump',
    'led_pattern':             'Set LED pattern',
    'delay':                   'Wait for a specified duration',
    'timing_marker':           'Record a timing marker in sample log',
}


STEP_SCHEMAS: dict[str, list[dict]] = {
    'set_loc_offset': [
        _p('dx_um', default=0),
        _p('dy_um', default=0),
        _p('dz_um', default=0),
    ],
    'centrifuge_unlock': [],
    'centrifuge_lock': [],
    'centrifuge_spin': [
        _p('rpm', default=500),
        _p('duration_s', default=5),
    ],
    'centrifuge_rotate': [
        _p('angle_001deg', default=0),
    ],
    'centrifuge_shake': [
        _p('centre_angle_001deg', default=29000),
        _p('shake_angle_deg', default=45),
        _p('cycles', default=3),
    ],
    'centrifuge_goto_serum': [],
    'centrifuge_goto_pipette': [],
    'lift_move': [
        _p('target_mm', default=18.0),
    ],
    'lid': [
        _p('open', 'boolean', default=True),
    ],
    'move_to_location': [
        _p('well', 'string', well_ref=True, required=True),
        _p('speed_01mms', default=250),
    ],
    'tip_pick': [
        _p('tip_id', default=4),
    ],
    'tip_swap': [
        _p('from_id', default=4),
        _p('to_id', default=5),
    ],
    'tip_return': [
        _p('tip_id', default=5),
    ],
    'lld': [
        _p('threshold', default=20),
    ],
    'reagent_transfer': [
        _p('source', 'string', well_ref=True, required=True),
        _p('target', 'string', well_ref=True, required=True),
        _p('asp_vol', required=True, volume_out=True),
        _p('cart_vol', required=True, volume_in=True),
        _p('asp_speed', default=40.0),
        _p('cart_vel', default=1.5),
        _p('air_slug', default=40),
        _p('reasp_ul', default=12),
        _p('return_speed', default=100.0),
        _p('skip_aspirate', 'boolean', default=False),
        _p('stream', 'boolean', default=False),
    ],
    'reagent_transfer_bf': [
        _p('source', 'string', well_ref=True, required=True),
        _p('target', 'string', well_ref=True, required=True),
        _p('asp_vol', required=True, volume_out=True),
        _p('cart_vol', required=True, volume_in=True),
        _p('duration_s', required=True),
        _p('asp_speed', default=40.0),
        _p('cart_vel', default=1.5),
        _p('air_slug', default=40),
        _p('reasp_ul', default=12),
        _p('for_vol', default=60),
        _p('back_vol', default=30),
        _p('sleep_s', default=30),
        _p('return_speed', default=100.0),
        _p('skip_aspirate', 'boolean', default=False),
        _p('stream', 'boolean', default=False),
    ],
    'well_transfer': [
        _p('source', 'string', well_ref=True, required=True),
        _p('dest', 'string', well_ref=True, required=True),
        _p('volume', required=True, volume_out=True, volume_in=True),
    ],
    'well_transfer_return': [
        _p('source', 'string', well_ref=True, required=True),
        _p('dest', 'string', well_ref=True, required=True),
        _p('asp_vol', required=True, volume_out=True),
        _p('disp_vol', required=True, volume_in=True),
        _p('vel', default=20.0),
    ],
    'well_to_chip': [
        _p('source', 'string', well_ref=True, required=True),
        _p('target', 'string', default='PP4', well_ref=True),
        _p('chip_vol', required=True, volume_out=True, volume_in=True),
        _p('overshoot_ul', default=10),
        _p('chip_vel', default=1.0),
        _p('stream', 'boolean', default=True),
    ],
    'tip_mix': [
        _p('well', 'string', well_ref=True, required=True),
        _p('mix_vol', default=150),
        _p('speed', default=100.0),
        _p('cycles', default=4),
        _p('pull_vol', default=0),
    ],
    'home_z': [],
    'well_dispense': [
        _p('well', 'string', well_ref=True, required=True),
        _p('volume', required=True, volume_in=True),
        _p('speed', default=100.0),
        _p('blowout', 'boolean', default=True),
    ],
    'smart_aspirate': [
        _p('well', 'string', well_ref=True, required=True),
        _p('volume', required=True, volume_out=True),
        _p('speed', default=40.0),
        _p('lld_threshold', default=20),
        _p('piston_reset', 'boolean', default=True),
        _p('air_slug', default=40),
        _p('stream', 'boolean', default=False),
    ],
    'dilution_transfer': [
        _p('source', 'string', well_ref=True, required=True),
        _p('dest', 'string', well_ref=True, required=True),
        _p('volume', required=True, volume_out=True, volume_in=True),
        _p('asp_speed', default=80.0),
        _p('disp_speed', default=100.0),
        _p('air_slug', default=40),
        _p('blowout', 'boolean', default=True),
        _p('stream', 'boolean', default=True),
    ],
    'home_all': [],
    'home_close': [],
    'pump_init': [],
    'led_pattern': [
        _p('pattern', default=0),
        _p('stage', default=0),
    ],
    'delay': [
        _p('seconds', default=1.0),
    ],
    'timing_marker': [
        _p('label', 'string', default=''),
        _p('event_type', 'string', default='start'),
        _p('trigger_analysis', 'boolean', default=False),
    ],
}
