'''ultra.services.nfc_provisioning.provisioner

NFC WiFi provisioning orchestrator using I2C-based NFC service.
'''
import logging
import time
from typing import (
    Callable,
    Optional,
)

from ultra.services.nfc_provisioning.nfc_service import (
    I2C_AVAILABLE,
    STATUS_CONNECTED,
    STATUS_CONNECTING,
    STATUS_FAILED,
    STATUS_IDLE,
    NFCService,
)
from ultra.services.wifi_provisioning.wifi_manager import WiFiManager

LOG = logging.getLogger(__name__)

CONNECTION_WAIT_S = 5


class NFCWiFiProvisioner:
    '''Orchestrates NFC-based WiFi provisioning.

    Starts NFCService in I2C card emulation mode, broadcasts
    available networks, and waits for WiFi credentials from
    a phone tap.
    '''

    def __init__(
            self,
            device_sn: str,
            test_mode: bool = False,
            advertise_while_connected: bool = False,
            on_tap: Callable[[], None] | None = None,
            i2c_bus: int = 2,
            i2c_addr: int = 0x50,
    ) -> None:
        '''Create an NFC WiFi provisioner.

        Args:
            device_sn: Serial number embedded in the setup URL.
            test_mode: If True, never exit early when WiFi is up and
                never auto-stop on connect (script ``--test`` mode).
            advertise_while_connected: If True, start NFC emulation even
                when WiFi is already connected so taps keep working.
                Does not disable real ``WiFiManager.connect`` when
                credentials arrive.
            on_tap: Called (no arguments) each time a phone connects to
                the NFC tag. Forwarded to ``NFCService``. Must be
                non-blocking.
            i2c_bus: Linux I2C bus number for the NFC front end.
            i2c_addr: I2C address of the NFC device.
        '''
        self._device_sn = device_sn
        self._test_mode = test_mode
        self._advertise_while_connected = advertise_while_connected
        self._on_tap = on_tap
        self._i2c_bus = i2c_bus
        self._i2c_addr = i2c_addr
        self._nfc_service: Optional[NFCService] = None
        self._running = False

    def is_wifi_connected(self) -> bool:
        connected, _ = WiFiManager.is_connected()
        return connected

    def _on_credentials_received(
            self, ssid: str, password: str,
    ) -> None:
        if self._nfc_service:
            self._nfc_service.set_wifi_status(STATUS_CONNECTING)
        ok, msg = WiFiManager.connect(ssid, password)
        if self._nfc_service:
            self._nfc_service.set_wifi_status(
                STATUS_CONNECTED if ok else STATUS_FAILED,
            )
        if ok:
            LOG.info('WiFi connected via NFC credentials')
        else:
            LOG.warning(f'WiFi connect failed: {msg}')
        time.sleep(CONNECTION_WAIT_S)

    def run(self) -> bool:
        '''Run NFC WiFi provisioning.

        Returns:
            True if WiFi is or becomes connected, or if this instance
            exits cleanly after ``stop()``. False on missing I2C or
            unexpected failure paths that return False explicitly.
        '''
        if (
                not self._test_mode and
                not self._advertise_while_connected and
                self.is_wifi_connected()
        ):
            return True
        if not I2C_AVAILABLE:
            LOG.error(
                'smbus2 not available; '
                'NFC provisioning requires I2C hardware',
            )
            return False
        self._running = True
        self._nfc_service = NFCService(
            device_sn=self._device_sn,
            on_wifi_credentials=self._on_credentials_received,
            on_tap=self._on_tap,
            i2c_bus=self._i2c_bus,
            i2c_addr=self._i2c_addr,
        )
        self._nfc_service.set_wifi_status(STATUS_IDLE)
        self._nfc_service.set_wifi_networks(
            [n['ssid'] for n in WiFiManager.scan_networks()],
        )
        self._nfc_service.start_async()
        try:
            while self._running:
                time.sleep(1)
                if (
                        not self._test_mode and
                        not self._advertise_while_connected and
                        self.is_wifi_connected()
                ):
                    return True
        finally:
            self.stop()
        return False

    def stop(self) -> None:
        self._running = False
        if self._nfc_service:
            self._nfc_service.stop()
            self._nfc_service = None
        LOG.info('NFC WiFi provisioner stopped')


if __name__ == '__main__':
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    test_mode = '--test' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    device_sn = args[0] if args else 'ULTRA-001'
    print(f'Starting NFC WiFi provisioning (SN: {device_sn})')
    if test_mode:
        print('TEST MODE — will not connect WiFi')

    provisioner = NFCWiFiProvisioner(
        device_sn=device_sn, test_mode=test_mode,
    )
    try:
        result = provisioner.run()
        print(f'Provisioning result: {result}')
    except KeyboardInterrupt:
        print('\nStopped.')
    finally:
        provisioner.stop()
