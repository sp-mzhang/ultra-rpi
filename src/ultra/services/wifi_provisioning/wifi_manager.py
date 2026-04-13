'''ultra.services.wifi_provisioning.wifi_manager

WiFi manager using nmcli for Raspberry Pi NetworkManager control.
'''
import hashlib
import logging
import subprocess
import time
from typing import Any

LOG = logging.getLogger(__name__)

NMCLI_TIMEOUT_S = 15
NMCLI_CONNECT_TIMEOUT_S = 45
RADIO_ENABLE_WAIT_S = 3


class WiFiManager:
    '''Manages WiFi connections via nmcli on Raspberry Pi.'''

    @staticmethod
    def _ensure_wifi_radio_on() -> bool:
        '''Ensure WiFi radio is enabled.

        Checks nmcli radio wifi state and re-enables if off.

        Returns:
            True if radio is on (or was successfully turned on).
        '''
        try:
            result = subprocess.run(
                ['nmcli', 'radio', 'wifi'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            state = result.stdout.strip().lower()
            if state == 'enabled':
                return True
            subprocess.run(
                ['nmcli', 'radio', 'wifi', 'on'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            time.sleep(RADIO_ENABLE_WAIT_S)
            check = subprocess.run(
                ['nmcli', 'radio', 'wifi'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            return check.stdout.strip().lower() == 'enabled'
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            LOG.warning(f'Could not ensure WiFi radio on: {e}')
            return False

    @staticmethod
    def _connection_profile_exists(ssid: str) -> bool:
        '''Check if a connection profile exists for the given SSID.

        Args:
            ssid: Network SSID to look for.

        Returns:
            True if a matching connection profile exists.
        '''
        try:
            result = subprocess.run(
                ['nmcli', '-t', '-f', 'NAME', 'connection', 'show'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            for line in result.stdout.strip().splitlines():
                if line.strip() == ssid:
                    return True
            return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    @staticmethod
    def scan_networks() -> list[dict[str, Any]]:
        '''Scan for available WiFi networks.

        Runs nmcli wifi rescan then lists networks. Returns dicts with
        ssid, signal, security keys, sorted by signal descending.

        Returns:
            List of dicts with keys: ssid, signal, security.
        '''
        WiFiManager._ensure_wifi_radio_on()
        try:
            subprocess.run(
                ['nmcli', 'dev', 'wifi', 'rescan'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        try:
            result = subprocess.run(
                [
                    'nmcli', '-t', '-f',
                    'SSID,SIGNAL,SECURITY',
                    'dev', 'wifi', 'list',
                ],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            networks: list[dict[str, Any]] = []
            for line in result.stdout.strip().splitlines():
                if not line:
                    continue
                parts = line.split(':')
                ssid = parts[0] if len(parts) > 0 else ''
                signal_str = parts[1] if len(parts) > 1 else '0'
                security = parts[2] if len(parts) > 2 else ''
                try:
                    signal = int(signal_str)
                except ValueError:
                    signal = 0
                if ssid:
                    networks.append({
                        'ssid': ssid,
                        'signal': signal,
                        'security': security,
                    })
            networks.sort(key=lambda n: n['signal'], reverse=True)
            return networks
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            LOG.warning(f'WiFi scan failed: {e}')
            return []

    @staticmethod
    def _profile_name(ssid: str) -> str:
        '''Stable nmcli connection name for an SSID.

        Args:
            ssid: Network SSID.

        Returns:
            A unique deterministic name string safe for nmcli.
        '''
        digest = hashlib.sha256(ssid.encode()).hexdigest()[:14]
        return f'ultra-{digest}'

    @staticmethod
    def _nmcli_cmd(privileged: bool = False) -> list[str]:
        '''Return nmcli command prefix, with sudo when privileged.

        ``nmcli connection add/delete`` require root on most Pi setups.
        ``nmcli device wifi connect`` works for ``netdev`` group members.

        Args:
            privileged: If True, prepend ``sudo`` to the command.

        Returns:
            List containing command prefix tokens.
        '''
        return ['sudo', 'nmcli'] if privileged else ['nmcli']

    @staticmethod
    def _delete_profiles(ssid: str) -> None:
        '''Delete all saved nmcli profiles for ssid.

        Removes both the stable sway-* profile and any profile whose
        NAME matches the SSID exactly (the default NM naming). Tries
        with and without sudo so it works regardless of user privileges.

        Args:
            ssid: Network SSID whose saved profiles to delete.
        '''
        for name in (WiFiManager._profile_name(ssid), ssid):
            for privileged in (False, True):
                try:
                    result = subprocess.run(
                        WiFiManager._nmcli_cmd(privileged) + [
                            'connection', 'delete', name,
                        ],
                        capture_output=True,
                        text=True,
                        timeout=NMCLI_TIMEOUT_S,
                    )
                    if result.returncode == 0:
                        break
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    break

    @staticmethod
    def _connect_with_profile(
            ssid: str,
            password: str,
            key_mgmt: str,
    ) -> tuple[bool, str]:
        '''Create a fresh nmcli connection profile and bring it up.

        Uses ``sudo nmcli connection add`` because ``connection add``
        requires root privileges on Raspberry Pi. Mirrors how
        ``test_provisioning_local.sh`` runs the provisioner under sudo.

        Args:
            ssid: Network SSID.
            password: Pre-shared key.
            key_mgmt: ``wpa-psk`` (WPA2) or ``sae`` (WPA3).

        Returns:
            Tuple of (success, message).
        '''
        name = WiFiManager._profile_name(ssid)
        WiFiManager._delete_profiles(ssid)
        try:
            add = subprocess.run(
                WiFiManager._nmcli_cmd(privileged=True) + [
                    'connection', 'add',
                    'type', 'wifi',
                    'con-name', name,
                    'ssid', ssid,
                    'wifi-sec.key-mgmt', key_mgmt,
                    'wifi-sec.psk', password,
                    'connection.autoconnect', 'yes',
                ],
                capture_output=True,
                text=True,
                timeout=NMCLI_CONNECT_TIMEOUT_S,
            )
            if add.returncode != 0:
                err = add.stderr.strip() or add.stdout.strip()
                return False, err
            up = subprocess.run(
                WiFiManager._nmcli_cmd(privileged=True) + [
                    'connection', 'up', name,
                ],
                capture_output=True,
                text=True,
                timeout=NMCLI_CONNECT_TIMEOUT_S,
            )
            if up.returncode == 0:
                return True, 'Connected'
            err = up.stderr.strip() or up.stdout.strip()
            return False, err
        except subprocess.TimeoutExpired:
            return False, 'Connection timed out'
        except FileNotFoundError:
            return False, 'nmcli not found'

    @staticmethod
    def connect(
            ssid: str,
            password: str,
    ) -> tuple[bool, str]:
        '''Connect to a WiFi network.

        Tries ``nmcli device wifi connect`` first. If that fails
        (including the ``802-11-wireless-security.key-mgmt: property
        is missing`` error caused by a corrupt saved NM profile),
        deletes old profiles and retries with an explicit ``wpa-psk``
        (WPA2) profile, then ``sae`` (WPA3) as a final fallback.

        Args:
            ssid: Network SSID.
            password: Network password.

        Returns:
            Tuple of (success, message).
        '''
        WiFiManager._ensure_wifi_radio_on()
        try:
            result = subprocess.run(
                [
                    'nmcli', 'device', 'wifi', 'connect', ssid,
                    'password', password,
                ],
                capture_output=True,
                text=True,
                timeout=NMCLI_CONNECT_TIMEOUT_S,
            )
            if result.returncode == 0:
                return True, 'Connected'

            first_err = result.stderr.strip() or result.stdout.strip()
            LOG.warning(
                f'nmcli device wifi connect failed for {ssid!r}: '
                f'{first_err[:200]} -- retrying with explicit profile',
            )

            ok, msg = WiFiManager._connect_with_profile(
                ssid, password, 'wpa-psk',
            )
            if ok:
                return True, msg

            ok2, msg2 = WiFiManager._connect_with_profile(
                ssid, password, 'sae',
            )
            if ok2:
                return True, msg2

            return False, (
                f'{first_err}; wpa-psk: {msg}; sae: {msg2}'
            )
        except subprocess.TimeoutExpired:
            return False, 'Connection timed out'
        except FileNotFoundError:
            return False, 'nmcli not found'

    @staticmethod
    def is_connected() -> tuple[bool, dict[str, str | None]]:
        '''Check if WiFi is connected.

        Uses nmcli dev status to determine wifi connection state.

        Returns:
            Tuple of (connected, dict with ssid and ip keys).
        '''
        try:
            result = subprocess.run(
                [
                    'nmcli', '-t', '-f',
                    'TYPE,STATE,CONNECTION',
                    'dev', 'status',
                ],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            for line in result.stdout.strip().splitlines():
                parts = line.split(':')
                if len(parts) < 3:
                    continue
                dev_type, state, conn = parts[0], parts[1], parts[2]
                if dev_type == 'wifi' and state == 'connected':
                    ip = WiFiManager.get_ip_address()
                    return True, {'ssid': conn, 'ip': ip}
            return False, {'ssid': None, 'ip': None}
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False, {'ssid': None, 'ip': None}

    @staticmethod
    def get_ip_address() -> str | None:
        '''Get the primary IP address.

        Runs hostname -I and returns first address.

        Returns:
            IP address string or None if unavailable.
        '''
        try:
            result = subprocess.run(
                ['hostname', '-I'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            addrs = result.stdout.strip().split()
            return addrs[0] if addrs else None
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

    @staticmethod
    def disconnect() -> bool:
        '''Disconnect from current WiFi.

        Runs nmcli con down on active connection.

        Returns:
            True if disconnect succeeded.
        '''
        try:
            active = subprocess.run(
                ['nmcli', '-t', '-f', 'NAME', 'con', 'show', '--active'],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            conn_name = active.stdout.strip().split('\n')[0].strip()
            if not conn_name:
                return True
            result = subprocess.run(
                ['nmcli', 'con', 'down', conn_name],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    @staticmethod
    def forget_network(ssid: str) -> bool:
        '''Remove a saved network profile.

        Args:
            ssid: SSID of network to forget.

        Returns:
            True if profile was deleted.
        '''
        try:
            result = subprocess.run(
                ['nmcli', 'con', 'delete', ssid],
                capture_output=True,
                text=True,
                timeout=NMCLI_TIMEOUT_S,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
