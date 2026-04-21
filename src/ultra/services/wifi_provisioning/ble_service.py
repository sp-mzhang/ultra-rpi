'''ultra.services.wifi_provisioning.ble_service

BLE GATT server for WiFi provisioning using bluezero.
Exposes characteristics for WiFi network scanning and credential input.

Ported from ultra-companion-linux (working implementation).
'''
import json
import logging
import shutil
import subprocess
import threading
import time
from typing import (
    Any,
    Callable,
    Optional,
)

LOG = logging.getLogger(__name__)

try:
    from bluezero import (
        adapter,
        peripheral,
    )
    BLUEZERO_AVAILABLE = True
except ImportError:
    BLUEZERO_AVAILABLE = False
    LOG.warning(
        'bluezero not installed. BLE provisioning disabled. '
        'Install with: pip install bluezero',
    )

try:
    import dbus
    import dbus.mainloop.glib
    import dbus.service
    DBUS_AVAILABLE = True
except ImportError:
    DBUS_AVAILABLE = False
    LOG.warning(
        'dbus not available. '
        'Pairing may require manual confirmation.',
    )


# Bluetooth Agent for auto-accepting pairing (NoInputNoOutput)
AGENT_INTERFACE = 'org.bluez.Agent1'
AGENT_PATH = '/com/siphox/agent'


if DBUS_AVAILABLE:
    class NoInputNoOutputAgent(dbus.service.Object):
        '''Bluetooth agent that auto-accepts all pairing requests.'''

        def __init__(self, bus: Any, path: str):
            super().__init__(bus, path)
            LOG.info('NoInputNoOutput Bluetooth agent initialized')

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='', out_signature='',
        )
        def Release(self) -> None:
            LOG.debug('Agent released')

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='os', out_signature='',
        )
        def AuthorizeService(
                self, device: str, uuid: str,
        ) -> None:
            LOG.info(
                f'Auto-authorizing service {uuid} '
                f'for {device}',
            )

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='o', out_signature='s',
        )
        def RequestPinCode(self, device: str) -> str:
            LOG.info(f'Auto-providing PIN for {device}')
            return '0000'

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='o', out_signature='u',
        )
        def RequestPasskey(self, device: str) -> int:
            LOG.info(f'Auto-providing passkey for {device}')
            return 0

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='ouq', out_signature='',
        )
        def DisplayPasskey(
                self, device: str, passkey: int, entered: int,
        ) -> None:
            LOG.debug(f'Passkey display: {passkey} for {device}')

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='os', out_signature='',
        )
        def DisplayPinCode(
                self, device: str, pincode: str,
        ) -> None:
            LOG.debug(f'PIN display: {pincode} for {device}')

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='ou', out_signature='',
        )
        def RequestConfirmation(
                self, device: str, passkey: int,
        ) -> None:
            LOG.info(
                f'Auto-confirming passkey {passkey} '
                f'for {device}',
            )

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='o', out_signature='',
        )
        def RequestAuthorization(self, device: str) -> None:
            LOG.info(f'Auto-authorizing {device}')

        @dbus.service.method(
            AGENT_INTERFACE,
            in_signature='', out_signature='',
        )
        def Cancel(self) -> None:
            LOG.debug('Agent cancelled')


def _register_auto_accept_agent() -> Any:
    '''Register a Bluetooth agent that auto-accepts pairing.'''
    if not DBUS_AVAILABLE:
        LOG.warning(
            'D-Bus not available, '
            'cannot register auto-accept agent',
        )
        return None

    try:
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        bus = dbus.SystemBus()
        agent = NoInputNoOutputAgent(bus, AGENT_PATH)
        manager = dbus.Interface(
            bus.get_object('org.bluez', '/org/bluez'),
            'org.bluez.AgentManager1',
        )
        manager.RegisterAgent(AGENT_PATH, 'NoInputNoOutput')
        manager.RequestDefaultAgent(AGENT_PATH)
        LOG.info('Auto-accept Bluetooth agent registered')
        return agent
    except Exception as err:
        LOG.error(f'Failed to register auto-accept agent: {err}')
        return None


def _get_adapter_address() -> Optional[str]:
    '''Get the default Bluetooth adapter address.'''
    if not BLUEZERO_AVAILABLE:
        return None
    try:
        adapters = adapter.list_adapters()
        if adapters:
            return str(adapters[0])
        return None
    except Exception as err:
        LOG.error(f'Failed to get adapter address: {err}')
        return None


def _run(cmd: list[str], timeout: float = 3.0) -> tuple[int, str]:
    '''Run a shell command and capture stdout+stderr. Returns
    (exit_code, combined_output). Safe across environments where
    the binary might be missing.'''
    if not shutil.which(cmd[0]):
        return (127, f'{cmd[0]}: not found on PATH')
    try:
        res = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        return (
            res.returncode, (res.stdout or '') + (res.stderr or ''),
        )
    except Exception as err:
        return (-1, f'{cmd[0]} failed: {err}')


def preflight_bluetooth(
    try_fix: bool = True,
) -> tuple[bool, list[str]]:
    '''Check that the local Bluetooth stack is ready to advertise.

    Returns (ok, messages). When try_fix is True and we detect a
    fixable problem (rfkill soft block, adapter powered off), we
    attempt to remediate and re-check once.

    Checks performed:
      - bluetoothd systemd unit running (via `systemctl is-active`)
      - `rfkill list bluetooth` shows no soft/hard blocks
      - at least one adapter reported by `bluetoothctl list`
      - default adapter reports Powered: yes

    Each failure adds a one-line human-friendly message that the
    caller should relay to the user.
    '''
    msgs: list[str] = []
    ok = True

    # 1) bluetoothd running?
    rc, out = _run(['systemctl', 'is-active', 'bluetooth'])
    state = out.strip().splitlines()[0] if out.strip() else '?'
    if rc != 0 or state != 'active':
        ok = False
        msgs.append(
            f'bluetoothd is not running (systemctl is-active: '
            f'{state!r}). Fix: `sudo systemctl start bluetooth`',
        )

    # 2) rfkill block?
    rc, out = _run(['rfkill', 'list', 'bluetooth'])
    soft = 'Soft blocked: yes' in out
    hard = 'Hard blocked: yes' in out
    if soft or hard:
        ok = False
        msgs.append(
            f'Bluetooth rfkill: soft={soft} hard={hard}. '
            f'Fix: `sudo rfkill unblock bluetooth`',
        )
        if try_fix and soft and not hard:
            LOG.warning('rfkill soft-blocked; unblocking...')
            _run(['rfkill', 'unblock', 'bluetooth'])

    # 3) adapter present?
    rc, out = _run(['bluetoothctl', 'list'])
    adapters_seen = [
        line for line in out.splitlines()
        if line.startswith('Controller ')
    ]
    if not adapters_seen:
        ok = False
        msgs.append(
            'No Bluetooth controller listed by `bluetoothctl '
            'list`. Is the adapter wired up / kernel module '
            'loaded?',
        )
    else:
        LOG.info(
            'Bluetooth controllers: %s',
            '; '.join(a.strip() for a in adapters_seen),
        )

    # 4) adapter powered on?
    rc, out = _run(['bluetoothctl', 'show'])
    powered_on = 'Powered: yes' in out
    if not powered_on:
        if try_fix:
            LOG.warning(
                'Default adapter Powered: no -- attempting to '
                'power on via bluetoothctl',
            )
            _run(['bluetoothctl', 'power', 'on'])
            time.sleep(0.5)
            rc, out = _run(['bluetoothctl', 'show'])
            powered_on = 'Powered: yes' in out
        if not powered_on:
            ok = False
            msgs.append(
                'Default Bluetooth adapter is not powered on. '
                'Fix: `bluetoothctl power on` (or reset the '
                'adapter via `sudo hciconfig hci0 up`)',
            )

    return ok, msgs


# BLE UUIDs for WiFi Provisioning Service
WIFI_PROV_SERVICE_UUID = '12345678-1234-5678-1234-56789abcdef0'
WIFI_NETWORKS_CHAR_UUID = '12345678-1234-5678-1234-56789abcdef1'
WIFI_SSID_CHAR_UUID = '12345678-1234-5678-1234-56789abcdef2'
WIFI_PASSWORD_CHAR_UUID = '12345678-1234-5678-1234-56789abcdef3'
WIFI_STATUS_CHAR_UUID = '12345678-1234-5678-1234-56789abcdef4'

# Status values
STATUS_IDLE = 'idle'
STATUS_CONNECTING = 'connecting'
STATUS_CONNECTED = 'connected'
STATUS_FAILED = 'failed'


class BLEWiFiProvisioningService:
    '''BLE GATT server for WiFi provisioning.

    Characteristics:
      - WIFI_NETWORKS (Read/Notify): compact JSON network list
      - WIFI_SSID (Write): SSID from mobile app
      - WIFI_PASSWORD (Write): password; triggers connect
      - WIFI_STATUS (Read/Notify): connection status JSON
    '''

    def __init__(
            self,
            device_name: str = 'SiPhox-Setup',
            on_credentials_received: Optional[
                Callable[[str, str], None]
            ] = None,
    ):
        if not BLUEZERO_AVAILABLE:
            raise RuntimeError(
                'bluezero library not available. '
                'Install with: pip install bluezero',
            )

        self.device_name = device_name
        self.on_credentials_received = on_credentials_received

        self._networks: list[dict[str, Any]] = []
        self._selected_ssid: Optional[str] = None
        self._password: Optional[str] = None
        self._status: str = STATUS_IDLE
        self._status_message: str = ''
        self._connected_ip: Optional[str] = None

        self._peripheral: Any = None
        self._agent: Any = None
        self._is_running: bool = False
        self._run_thread: Optional[threading.Thread] = None

        LOG.info(
            f'BLE WiFi Provisioning Service initialized: '
            f'{device_name}',
        )

    def set_networks(
            self, networks: list[dict[str, Any]],
    ) -> None:
        self._networks = networks
        LOG.debug(f'Updated network list: {len(networks)} networks')

    def set_status(
            self,
            status: str,
            message: str = '',
            ip_address: Optional[str] = None,
    ) -> None:
        self._status = status
        self._status_message = message
        self._connected_ip = ip_address
        LOG.info(f'Status updated: {status} - {message}')

    def get_selected_ssid(self) -> Optional[str]:
        return self._selected_ssid

    def get_password(self) -> Optional[str]:
        return self._password

    def clear_credentials(self) -> None:
        self._selected_ssid = None
        self._password = None

    def _read_networks(self) -> list[int]:
        '''Read callback for WIFI_NETWORKS characteristic.

        Format: {"n":[{"s":"SSID","g":signal,"c":"security"},...]}
        '''
        MAX_BYTES = 500
        MAX_SSID_LEN = 20

        compact_networks = []
        for net in self._networks:
            ssid = net.get('ssid', '')[:MAX_SSID_LEN]
            signal = net.get('signal', 0)
            security = net.get('security', '')[:8]
            compact_networks.append({
                's': ssid,
                'g': signal,
                'c': security,
            })

        result_networks: list[dict[str, Any]] = []
        for net in compact_networks:
            test_data = {'n': result_networks + [net]}
            test_json = json.dumps(
                test_data, separators=(',', ':'),
            )
            if len(test_json.encode('utf-8')) > MAX_BYTES:
                break
            result_networks.append(net)

        data = {'n': result_networks}
        json_str = json.dumps(data, separators=(',', ':'))
        LOG.debug(
            f'Networks read: '
            f'{len(result_networks)}/{len(self._networks)} '
            f'networks, {len(json_str)} bytes',
        )
        return list(json_str.encode('utf-8'))

    def _write_ssid(
            self, value: list[int], options: dict,
    ) -> None:
        try:
            ssid = bytes(value).decode('utf-8').strip()
            self._selected_ssid = ssid
            LOG.info(f'SSID received: {ssid}')
            self._check_credentials_complete()
        except Exception as err:
            LOG.error(f'Error decoding SSID: {err}')

    def _write_password(
            self, value: list[int], options: dict,
    ) -> None:
        try:
            password = bytes(value).decode('utf-8')
            self._password = password
            LOG.info('Password received (hidden)')
            self._check_credentials_complete()
        except Exception as err:
            LOG.error(f'Error decoding password: {err}')

    def _check_credentials_complete(self) -> None:
        if self._selected_ssid and self._password is not None:
            LOG.info(
                f'Credentials complete for: '
                f'{self._selected_ssid}',
            )
            if self.on_credentials_received:
                self.on_credentials_received(
                    self._selected_ssid,
                    self._password,
                )

    def _read_status(self) -> list[int]:
        '''Read callback for WIFI_STATUS characteristic.

        Format: {"st":"status","m":"message","s":"ssid","ip":"..."}
        '''
        data = {
            'st': self._status,
            'm': (self._status_message or '')[:50],
            's': self._selected_ssid,
            'ip': self._connected_ip,
        }
        json_str = json.dumps(data, separators=(',', ':'))
        return list(json_str.encode('utf-8'))

    def _on_connect(self, device_address: str) -> None:
        LOG.info(f'BLE client connected: {device_address}')

    def _on_disconnect(self, device_address: str) -> None:
        LOG.info(f'BLE client disconnected: {device_address}')

    def start(self) -> None:
        '''Start BLE advertising and GATT server (blocking).'''
        if self._is_running:
            LOG.warning('BLE service already running')
            return

        LOG.info(
            'BLE start requested: local_name=%r',
            self.device_name,
        )

        # Pre-flight the Bluetooth stack. If anything is wrong we
        # want a clear human-readable message in the log instead of
        # a confusing bluezero / dbus exception minutes later.
        ok, msgs = preflight_bluetooth(try_fix=True)
        for m in msgs:
            LOG.warning('BLE preflight: %s', m)
        if not ok:
            raise RuntimeError(
                'Bluetooth stack not ready: '
                + ' | '.join(msgs),
            )
        LOG.info('BLE preflight OK')

        try:
            LOG.debug('Registering BLE auto-accept agent...')
            self._agent = _register_auto_accept_agent()

            adapter_addr = _get_adapter_address()
            if not adapter_addr:
                raise RuntimeError(
                    'No Bluetooth adapter found by bluezero '
                    '(adapter.list_adapters() returned empty). '
                    'Check `bluetoothctl list` and dbus access.',
                )
            LOG.info(f'Using Bluetooth adapter: {adapter_addr}')

            self._peripheral = peripheral.Peripheral(
                adapter_address=adapter_addr,
                local_name=self.device_name,
            )

            self._peripheral.on_connect = self._on_connect
            self._peripheral.on_disconnect = self._on_disconnect

            self._peripheral.add_service(
                srv_id=1,
                uuid=WIFI_PROV_SERVICE_UUID,
                primary=True,
            )

            self._peripheral.add_characteristic(
                srv_id=1, chr_id=1,
                uuid=WIFI_NETWORKS_CHAR_UUID,
                value=[], notifying=False,
                flags=['read', 'notify'],
                read_callback=self._read_networks,
            )
            self._peripheral.add_characteristic(
                srv_id=1, chr_id=2,
                uuid=WIFI_SSID_CHAR_UUID,
                value=[], notifying=False,
                flags=['write'],
                write_callback=self._write_ssid,
            )
            self._peripheral.add_characteristic(
                srv_id=1, chr_id=3,
                uuid=WIFI_PASSWORD_CHAR_UUID,
                value=[], notifying=False,
                flags=['write'],
                write_callback=self._write_password,
            )
            self._peripheral.add_characteristic(
                srv_id=1, chr_id=4,
                uuid=WIFI_STATUS_CHAR_UUID,
                value=[], notifying=False,
                flags=['read', 'notify'],
                read_callback=self._read_status,
            )

            self._is_running = True
            LOG.info('BLE GATT server starting...')
            self._peripheral.publish()

        except Exception as err:
            LOG.error(f'BLE service error: {err}')
            self._is_running = False
            raise

    def start_async(self) -> None:
        '''Start BLE service in a background thread. The thread
        wrapper logs exceptions (otherwise a failure inside
        `start()` would silently kill the thread and the caller
        would never know BLE never came up).'''
        if self._run_thread and self._run_thread.is_alive():
            LOG.warning('BLE service thread already running')
            return

        def _runner() -> None:
            try:
                self.start()
            except Exception as err:
                LOG.error(
                    'BLE thread exiting with error: %s',
                    err, exc_info=True,
                )
                self._is_running = False

        self._run_thread = threading.Thread(
            target=_runner,
            name='BLEProvisioningThread',
            daemon=True,
        )
        self._run_thread.start()
        LOG.info('BLE service started in background thread')

    def stop(self) -> None:
        '''Stop BLE advertising and GATT server.'''
        if not self._is_running:
            return
        LOG.info('Stopping BLE service...')
        self._is_running = False
        LOG.info('BLE service stop requested')

    @property
    def is_running(self) -> bool:
        return self._is_running
