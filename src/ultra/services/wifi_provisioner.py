'''ultra.services.wifi_provisioner -- BLE WiFi provisioning.

Placeholder for BLE-based WiFi credential provisioning.
Will be implemented when the BLE stack is ported from
sway's wifi_provisioning module.
'''
from __future__ import annotations

import logging

LOG = logging.getLogger(__name__)


class WiFiProvisioner:
    '''BLE WiFi provisioning service (placeholder).

    Will advertise via BLE, accept WiFi credentials from
    the SiPhox mobile app, and configure the network.
    '''

    def __init__(
            self,
            device_name: str = 'SiPhox-Ultra',
    ) -> None:
        self._device_name = device_name
        self._running = False

    def run(self) -> None:
        '''Start BLE advertising (placeholder).'''
        LOG.info(
            'WiFi provisioner placeholder -- '
            'not yet implemented',
        )

    def stop(self) -> None:
        '''Stop BLE advertising.'''
        self._running = False
