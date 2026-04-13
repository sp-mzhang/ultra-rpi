'''ultra.utils.network -- Network utility functions.

Provides WiFi connectivity checks using nmcli (NetworkManager).
'''
from __future__ import annotations

import logging
import socket
import subprocess

LOG = logging.getLogger(__name__)


def check_wifi_connected() -> tuple[bool, dict]:
    '''Check if device is connected to WiFi.

    Uses NetworkManager (nmcli) to check WiFi connection status.

    Returns:
        Tuple of (is_connected, status_dict) where status_dict
        has keys: ssid, ip.
    '''
    try:
        import shutil
        nmcli = shutil.which('nmcli') or '/usr/bin/nmcli'
        radio_result = subprocess.run(
            [nmcli, 'radio', 'wifi'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        radio_state = radio_result.stdout.strip()
        if radio_state != 'enabled':
            LOG.debug('WiFi radio is %s', radio_state)
            return False, {'ssid': None, 'ip': None}

        result = subprocess.run(
            [
                nmcli, '-t', '-f', 'TYPE,STATE,CONNECTION',
                'dev', 'status',
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        for line in result.stdout.strip().split('\n'):
            parts = line.split(':')
            if len(parts) >= 3:
                dev_type = parts[0]
                state = parts[1]
                connection = parts[2]

                if (
                    dev_type == 'wifi'
                    and state == 'connected'
                ):
                    ip_result = subprocess.run(
                        ['hostname', '-I'],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    ip_addr = None
                    if ip_result.stdout.strip():
                        ip_addr = (
                            ip_result.stdout.strip().split()[0]
                        )

                    LOG.debug(
                        'WiFi connected: %s, IP: %s',
                        connection, ip_addr,
                    )
                    return True, {
                        'ssid': connection,
                        'ip': ip_addr,
                    }

        LOG.debug('WiFi not connected')
        return False, {'ssid': None, 'ip': None}

    except FileNotFoundError:
        LOG.warning(
            'nmcli not available -- falling back to '
            'socket connectivity check',
        )
        try:
            s = socket.create_connection(
                ('8.8.8.8', 53), timeout=3,
            )
            ip = s.getsockname()[0]
            s.close()
            LOG.info(
                'Network reachable (no nmcli), IP: %s', ip,
            )
            return True, {
                'ssid': 'unknown (nmcli unavailable)',
                'ip': ip,
            }
        except OSError:
            return False, {'ssid': None, 'ip': None}
    except subprocess.TimeoutExpired:
        LOG.error('WiFi status check timeout')
        return False, {'ssid': None, 'ip': None}
    except Exception as err:
        LOG.error('WiFi status check error: %s', err)
        return False, {'ssid': None, 'ip': None}
