'''ultra.gui.api -- REST API router orchestrator.

Assembles sub-routers from the api_* modules into a single
APIRouter that server.py mounts under ``/api``.
'''
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter

if TYPE_CHECKING:
    from ultra.app import Application


def create_api_router(
        app: 'Application',
) -> APIRouter:
    '''Create the combined API router from sub-routers.'''
    router = APIRouter()

    from ultra.gui.api_protocol import create_protocol_router
    from ultra.gui.api_egress import create_egress_router
    from ultra.gui.api_stm32 import create_stm32_router
    from ultra.gui.api_firmware import create_firmware_router
    from ultra.gui.api_config import create_config_router
    from ultra.gui.api_fc_sequence import create_fc_sequence_router

    router.include_router(create_protocol_router(app))
    router.include_router(create_egress_router(app))
    router.include_router(create_stm32_router(app))
    router.include_router(create_firmware_router(app))
    router.include_router(create_config_router(app))
    router.include_router(create_fc_sequence_router(app))

    return router
