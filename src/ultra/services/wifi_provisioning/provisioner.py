'''ultra.services.wifi_provisioning.provisioner

WiFi provisioning orchestrator combining BLE service and WiFi manager.

Ported from ultra-companion-linux (working implementation).
'''
import logging
import threading
import time
from typing import (
    Callable,
    Optional,
)

from ultra.services.wifi_provisioning.ble_service import (
    BLUEZERO_AVAILABLE,
    STATUS_CONNECTED,
    STATUS_CONNECTING,
    STATUS_FAILED,
    STATUS_IDLE,
    BLEWiFiProvisioningService,
)
from ultra.services.wifi_provisioning.wifi_manager import WiFiManager

LOG = logging.getLogger(__name__)

NETWORK_SCAN_INTERVAL_S = 15
CONNECTION_WAIT_S = 5


class WiFiProvisioner:
    '''WiFi provisioning orchestrator.

    Starts BLE advertising, scans networks, waits for credentials
    from the mobile app, connects to WiFi, stops BLE.
    '''

    def __init__(
            self,
            device_name: str = 'SiPhox-Setup',
            test_mode: bool = False,
            advertise_while_connected: bool = False,
            on_wifi_connected: Callable[[str, str], None] | None = None,
    ):
        '''Create a BLE WiFi provisioner.

        Args:
            device_name: BLE advertised device name.
            test_mode: If True, skip real WiFi connects from BLE and
                keep the service up (script ``--test`` mode).
            advertise_while_connected: If True, start BLE even when
                WiFi is already connected so phones can still pair.
                Real credential handling stays enabled unless
                ``test_mode`` is True.
            on_wifi_connected: Called with (ssid, ip_address) when
                credentials from the app are successfully applied and
                WiFi connects. Runs on the BLE background thread --
                must be non-blocking.
        '''
        self.device_name = device_name
        self.test_mode = test_mode
        self.advertise_while_connected = advertise_while_connected
        self._on_wifi_connected = on_wifi_connected
        self.wifi_manager = WiFiManager()
        self.ble_service: Optional[
            BLEWiFiProvisioningService
        ] = None

        self._running = False
        self._provisioned = False
        self._scan_thread: Optional[threading.Thread] = None

        LOG.info(
            f'WiFi Provisioner initialized: {device_name} '
            f'(test_mode={test_mode})',
        )

    def is_wifi_connected(self) -> bool:
        connected, _ = self.wifi_manager.is_connected()
        return connected

    def _on_credentials_received(
            self, ssid: str, password: str,
    ) -> None:
        LOG.info(f'Credentials received for: {ssid}')

        if self.test_mode:
            LOG.info('=== TEST MODE: Not connecting to WiFi ===')
            if self.ble_service:
                self.ble_service.set_status(
                    STATUS_CONNECTED,
                    f'TEST MODE: Would connect to {ssid}',
                    ip_address='0.0.0.0',
                )
                self.ble_service.clear_credentials()
            return

        if self.ble_service:
            self.ble_service.set_status(
                STATUS_CONNECTING,
                f'Connecting to {ssid}...',
            )

        success, message = self.wifi_manager.connect(
            ssid, password,
        )

        if success:
            time.sleep(CONNECTION_WAIT_S)
            ip_address = self.wifi_manager.get_ip_address()
            LOG.info(f'WiFi connected! IP: {ip_address}')
            if self.ble_service:
                self.ble_service.set_status(
                    STATUS_CONNECTED,
                    f'Connected to {ssid}',
                    ip_address=ip_address,
                )
            self._provisioned = True
            if self._on_wifi_connected is not None:
                try:
                    self._on_wifi_connected(ssid, ip_address or '')
                except Exception as err:
                    LOG.warning(
                        f'on_wifi_connected callback error: {err}',
                    )
        else:
            LOG.error(f'WiFi connection failed: {message}')
            if self.ble_service:
                self.ble_service.set_status(
                    STATUS_FAILED,
                    f'Failed: {message}',
                )
                self.ble_service.clear_credentials()

    def _network_scan_loop(self) -> None:
        LOG.info('Network scan loop started')
        while self._running and not self._provisioned:
            try:
                networks = self.wifi_manager.scan_networks()
                if self.ble_service:
                    self.ble_service.set_networks(networks)
                LOG.debug(f'Scanned {len(networks)} networks')
            except Exception as err:
                LOG.error(f'Network scan error: {err}')
            for _ in range(NETWORK_SCAN_INTERVAL_S):
                if not self._running or self._provisioned:
                    break
                time.sleep(1)
        LOG.info('Network scan loop stopped')

    def run(self) -> bool:
        '''Run WiFi provisioning (blocking).

        Returns True if WiFi connected.
        '''
        if (
                not self.test_mode and
                not self.advertise_while_connected and
                self.is_wifi_connected()
        ):
            connected, status = self.wifi_manager.is_connected()
            LOG.info(
                f'Already connected to WiFi: '
                f'{status.get("ssid")}, '
                f'IP: {status.get("ip")}',
            )
            return True
        if self.advertise_while_connected and self.is_wifi_connected():
            connected, status = self.wifi_manager.is_connected()
            LOG.info(
                f'WiFi connected; BLE stays up '
                f'(ssid={status.get("ssid")}, '
                f'ip={status.get("ip")})',
            )

        if not BLUEZERO_AVAILABLE:
            LOG.error(
                'bluezero not available - '
                'cannot start BLE provisioning',
            )
            return False

        LOG.info('Starting WiFi provisioning via BLE...')
        self._running = True
        self._provisioned = False

        try:
            self.ble_service = BLEWiFiProvisioningService(
                device_name=self.device_name,
                on_credentials_received=(
                    self._on_credentials_received
                ),
            )
            self.ble_service.set_status(
                STATUS_IDLE, 'Waiting for credentials',
            )

            self._scan_thread = threading.Thread(
                target=self._network_scan_loop,
                name='WiFiScanThread',
                daemon=True,
            )
            self._scan_thread.start()

            networks = self.wifi_manager.scan_networks()
            self.ble_service.set_networks(networks)

            LOG.info(f'BLE advertising as: {self.device_name}')
            self.ble_service.start_async()

            # Give the BLE thread a moment to run preflight + spin
            # up the GATT server so we can detect early failure
            # instead of silently looping.
            time.sleep(2.0)
            ble_thread = self.ble_service._run_thread
            if not self.ble_service.is_running and (
                ble_thread is None or not ble_thread.is_alive()
            ):
                LOG.error(
                    'BLE thread died during start-up; '
                    'aborting provisioning (see earlier '
                    'BLE error log for the root cause).',
                )
                return False

            while self._running and not self._provisioned:
                time.sleep(1)
                # Detect mid-run BLE thread death (e.g. bluetoothd
                # crashed or the adapter was unplugged); otherwise
                # run() would spin forever advertising nothing.
                if not self.ble_service.is_running:
                    LOG.error(
                        'BLE thread stopped unexpectedly; '
                        'exiting provisioning loop.',
                    )
                    return False
                if (
                        not self.test_mode and
                        not self.advertise_while_connected and
                        self.is_wifi_connected()
                ):
                    self._provisioned = True
                    break

            if self._provisioned:
                LOG.info(
                    'Waiting 10s for phone to read '
                    'BLE status...',
                )
                time.sleep(10)
                LOG.info('WiFi provisioning successful!')
                return True
            else:
                LOG.warning(
                    'WiFi provisioning stopped '
                    'without connecting',
                )
                return False

        except Exception as err:
            LOG.error(f'Provisioning error: {err}')
            return False

        finally:
            self._running = False
            if self.ble_service:
                self.ble_service.stop()

    def stop(self) -> None:
        LOG.info('Stopping WiFi provisioning...')
        self._running = False
        if self.ble_service:
            self.ble_service.stop()


def check_and_provision(
        station_id: str = 'Setup',
        skip_if_connected: bool = True,
) -> bool:
    '''Check WiFi and provision if needed.'''
    wifi_manager = WiFiManager()
    connected, status = wifi_manager.is_connected()
    if connected and skip_if_connected:
        LOG.info(
            f'WiFi already connected: '
            f'{status.get("ssid")}, '
            f'IP: {status.get("ip")}',
        )
        return True
    device_name = f'SiPhox-{station_id}'
    provisioner = WiFiProvisioner(device_name=device_name)
    return provisioner.run()


if __name__ == '__main__':
    import logging
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    test_mode = '--test' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    station_id = args[0] if args else 'TEST-001'

    print(f'\nStation ID: {station_id}')
    print(f'Device name: SiPhox-{station_id}')
    print(f'Test mode: {test_mode}')

    try:
        provisioner = WiFiProvisioner(
            device_name=f'SiPhox-{station_id}',
            test_mode=test_mode,
        )
        success = provisioner.run()
        print(
            f'\nResult: '
            f'{"Success" if success else "Failed"}',
        )
    except KeyboardInterrupt:
        print('\nStopped.')
