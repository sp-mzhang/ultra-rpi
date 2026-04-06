'''ultra.services.nfc_service -- NFC tag service.

Placeholder for NFC tag provisioning. Will broadcast the
device setup URL via NFC when the hardware is available.
'''
from __future__ import annotations

import logging

LOG = logging.getLogger(__name__)


class NFCService:
    '''NFC tag provisioning service (placeholder).

    Will program the NFC tag with a setup URL containing
    the device serial number.
    '''

    def __init__(
            self,
            device_sn: str = 'unknown',
    ) -> None:
        self._device_sn = device_sn
        self._running = False

    def run(self) -> None:
        '''Start NFC tag broadcasting (placeholder).'''
        LOG.info(
            'NFC service placeholder -- '
            'not yet implemented',
        )

    def stop(self) -> None:
        '''Stop NFC tag.'''
        self._running = False
