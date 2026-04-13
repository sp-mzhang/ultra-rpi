'''Firmware OTA update endpoints.

Handles /firmware/list, /firmware/flash, /firmware/status.
'''
from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ultra.gui._eng_state import eng_stm32

if TYPE_CHECKING:
    from ultra.app import Application


class FirmwareFlashRequest(BaseModel):
    '''Request body for flashing firmware.'''
    key: str


def create_firmware_router(app: 'Application') -> APIRouter:
    router = APIRouter()

    @router.get('/firmware/list')
    async def firmware_list():
        '''List available firmware builds from S3.'''
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
        '''Download and flash a firmware binary.'''
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

        stm32 = eng_stm32.get('iface')
        if stm32 is not None:
            try:
                stm32.disconnect()
            except Exception:
                pass
            eng_stm32['iface'] = None

        if app._monitor:
            app._monitor.stop()
        await asyncio.sleep(0.5)

        t = threading.Thread(
            target=fw_update.download_and_flash,
            args=(req.key,),
            daemon=True,
        )
        t.start()

        return {'ok': True, 'message': 'Flash started'}

    @router.get('/firmware/status')
    async def firmware_status(log_offset: int = 0):
        '''Return current firmware flash status.'''
        from ultra.services import fw_update
        return fw_update.get_status(log_offset)

    return router
