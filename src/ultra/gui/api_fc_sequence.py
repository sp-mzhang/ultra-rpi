'''FC liquid test sequence endpoints.

Handles /fc-liquid-sequence POST and /fc-liquid-sequence/status GET.
'''
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ultra.gui._eng_state import get_eng_stm32

if TYPE_CHECKING:
    from ultra.app import Application

LOG = logging.getLogger(__name__)

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


class FcLiquidSeqRequest(BaseModel):
    '''Request body for the FC liquid test sequence.'''
    source_well: str = 'M1'
    aspirate_vol_ul: float = 200
    cart_vol_ul: float = 80
    aspirate_speed_ul_s: float = 80
    cart_vel_ul_s: float = 1.0


def create_fc_sequence_router(
        app: 'Application',
) -> APIRouter:
    router = APIRouter()

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
        stm32 = get_eng_stm32()
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
                cfg_offset = float(
                    app.config.get('liquid', {})
                    .get('cartridge_dispense', {})
                    .get('lld_offset_mm', 0.0),
                )
                default_z = float(
                    app.config.get('calibration', {})
                    .get('default_cartridge_z_mm', -23.8),
                )
                lld_r = stm32.perform_lld(threshold=20)
                if lld_r and lld_r.get('detected'):
                    z_us = lld_r.get('z_position', 0)
                    z_detected = z_us / Z_USTEPS_PER_MM
                    cartridge_z = z_detected + cfg_offset
                    LOG.info(
                        'FC seq LLD: z=%d usteps = %.2f mm, '
                        'offset=%.2f mm -> cartridge_z=%.2f mm',
                        z_us, z_detected,
                        cfg_offset, cartridge_z,
                    )
                else:
                    cartridge_z = default_z
                    LOG.warning(
                        'FC seq LLD not detected, '
                        'using default_cartridge_z=%.2f mm '
                        '(resp=%s)',
                        cartridge_z, lld_r,
                    )
                # Home Z so the next XY move is safe regardless
                # of where LLD left the tip.
                r = stm32.send_command_wait_done(
                    cmd={'cmd': 'home_z_axis'},
                    timeout_s=30.0,
                )
                if not _check(r, 'Home Z (post-LLD)'):
                    return
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
