'''ultra.hw.stm32_mock -- Mock STM32 interface for testing.

Provides a drop-in replacement for STM32Interface that
simulates command responses without requiring physical
hardware. Useful for GUI development, recipe validation,
and integration testing.

All methods are synchronous to match the real
STM32Interface contract (protocol thread calls these
directly).
'''
from __future__ import annotations

import logging
import random
import time
from typing import Optional

LOG = logging.getLogger(__name__)


class STM32Mock:
    '''Mock STM32 interface that simulates hardware responses.

    All commands succeed by default with configurable delays.
    Can be configured to inject failures for testing error
    handling paths.

    Attributes:
        _connected: Whether the mock is "connected".
        _seq: Rolling sequence number.
        _delay_s: Simulated command execution delay.
        _fail_commands: Set of command names that should fail.
    '''

    def __init__(
            self,
            port: str = '/dev/null',
            baud: int = 921600,
            delay_s: float = 0.1,
    ) -> None:
        '''Initialize the mock interface.

        Args:
            port: Ignored (kept for API compatibility).
            baud: Ignored (kept for API compatibility).
            delay_s: Simulated delay per command in seconds.
        '''
        self._port = port
        self._baud = baud
        self._connected = False
        self._seq = 0
        self._delay_s = delay_s
        self._fail_commands: set[str] = set()
        LOG.info(
            'STM32Mock created (simulated hardware)',
        )

    def connect(self) -> bool:
        '''Simulate serial port connection.

        Returns:
            Always True.
        '''
        self._connected = True
        LOG.info('STM32Mock connected (simulated)')
        return True

    def disconnect(self) -> None:
        '''Simulate serial port disconnection.'''
        self._connected = False
        LOG.info('STM32Mock disconnected')

    def set_fail_commands(
            self, commands: set[str],
    ) -> None:
        '''Configure commands that should return errors.

        Args:
            commands: Set of command name strings that
                will return ERROR status.
        '''
        self._fail_commands = commands

    def send_command(
            self,
            cmd: dict,
            timeout_s: float = 30.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Simulate sending a command.

        Args:
            cmd: Command dict (e.g. {'cmd': 'ping'}).
            timeout_s: Ignored.
            collect_pressure: If True, return mock
                pressure samples.

        Returns:
            Simulated response dict.
        '''
        if not self._connected:
            LOG.error('STM32Mock: not connected')
            return None

        cmd_name = cmd.get('cmd', '')
        self._seq += 1
        time.sleep(self._delay_s)

        if cmd_name in self._fail_commands:
            LOG.info(
                f'STM32Mock TX: {cmd_name} -> ERROR',
            )
            return {
                'seq': self._seq,
                'status': 'ERROR',
                'error_code': 1,
            }

        result: dict = {
            'seq': self._seq,
            'status': 'OK',
            'error_code': 0,
        }

        if cmd_name == 'ping':
            result['timestamp'] = int(
                time.time() * 1000,
            )
        elif cmd_name == 'get_version':
            result['version'] = {
                'major': 1, 'minor': 0,
                'patch': 0, 'build': 'mock',
            }
        elif cmd_name == 'lld_perform':
            result['detected'] = True
            result['z_position'] = -8000
            result['time_ms'] = 500
            result['pressure_delta'] = 25
        elif cmd_name == 'fan_get_status':
            result['duty_pct'] = 50
            result['rpm'] = 2400
        elif cmd_name == 'temp_get_status':
            result['board_temp_c'] = 35.0
            result['ambient_temp_c'] = 25.0
        elif cmd_name == 'read_z_drv':
            result['z_position_usteps'] = 0
            result['z_current_ma'] = 120
        elif cmd_name == 'centrifuge_status':
            result['driver_online'] = True
            result['state'] = 1
            result['rpm'] = 0
            result['angle_001deg'] = 0
            result['vbus_01v'] = 240
            result['temp_001c'] = 3500
            result['error_flags'] = '0x0000'
        elif cmd_name == 'centrifuge_bldc_cmd':
            result['bldc_cmd'] = f'0x{cmd.get("bldc_cmd", 0):04X}'
            result['ok'] = True
            result['data'] = '01'

        if collect_pressure:
            result['_pressure_samples'] = [
                {
                    'timestamp_ms': i * 10,
                    'pressure': 500 + random.randint(
                        -5, 5,
                    ),
                    'position': 100 + i,
                }
                for i in range(10)
            ]

        LOG.info(f'STM32Mock TX: {cmd_name} -> OK')
        return result

    def send_command_wait_done(
            self,
            cmd: dict,
            timeout_s: float = 120.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Simulate sending a long-running command.

        Args:
            cmd: Command dict.
            timeout_s: Ignored.
            collect_pressure: If True, return mock
                pressure samples.

        Returns:
            Simulated response dict.
        '''
        return self.send_command(
            cmd=cmd,
            timeout_s=timeout_s,
            collect_pressure=collect_pressure,
        )

    def ping(self) -> bool:
        '''Simulate a ping.

        Returns:
            True unless 'ping' is in fail_commands.
        '''
        r = self.send_command({'cmd': 'ping'})
        return bool(r and r.get('status') == 'OK')

    def smart_aspirate(
            self,
            volume_ul: int,
            speed_ul_s: float,
            **kwargs,
    ) -> Optional[dict]:
        '''Simulate smart aspirate.

        Args:
            volume_ul: Volume in microliters.
            speed_ul_s: Speed in uL/s.

        Returns:
            Simulated response dict.
        '''
        return self.send_command_wait_done(
            cmd={
                'cmd': 'smart_aspirate',
                'volume': volume_ul,
                'speed': speed_ul_s,
            },
            collect_pressure=kwargs.get(
                'stream', False,
            ),
        )

    def centrifuge_status(
            self,
            timeout_s: float = 5.0,
    ) -> dict | None:
        '''Simulate centrifuge status query.

        Returns:
            Mock status with READY state.
        '''
        return {
            'seq': self._seq,
            'status': 'OK',
            'error_code': 0,
            'state': 1,
            'rpm': 0,
            'angle_001deg': 0,
            'driver_online': True,
            'error_flags': '0x0000',
        }

    def lift_status(
            self,
            timeout_s: float = 5.0,
    ) -> dict | None:
        '''Simulate lift status query.

        Returns:
            Mock status with homed position.
        '''
        return {
            'seq': self._seq,
            'status': 'OK',
            'error_code': 0,
            'position_steps': 0,
            'is_homed': True,
        }

    def wait_centrifuge_idle(
            self,
            timeout_s: float = 60.0,
            poll_interval_s: float = 0.2,
    ) -> bool:
        '''Simulate waiting for centrifuge idle.

        Returns:
            Always True after a short delay.
        '''
        time.sleep(0.5)
        return True

    def wait_lift_idle(
            self,
            target_mm: float,
            tol_mm: float = 1.5,
            timeout_s: float = 90.0,
            poll_interval_s: float = 0.25,
    ) -> bool:
        '''Simulate waiting for lift idle.

        Returns:
            Always True after a short delay.
        '''
        time.sleep(0.3)
        return True

    def aspirate_at(
            self, loc_id: int, volume_ul: int,
            **kwargs,
    ) -> bool:
        '''Simulate aspirate at location.

        Returns:
            True unless configured to fail.
        '''
        time.sleep(self._delay_s)
        return 'pump_aspirate' not in self._fail_commands

    def smart_aspirate_at(
            self, loc_id: int, volume_ul: int,
            **kwargs,
    ) -> dict | None:
        '''Simulate smart aspirate at location.

        Returns:
            Mock response dict.
        '''
        time.sleep(self._delay_s)
        if 'smart_aspirate' in self._fail_commands:
            return None
        result: dict = {
            'status': 'OK', 'error_code': 0,
        }
        if kwargs.get('stream'):
            result['_pressure_samples'] = []
        return result

    def dispense_at(
            self, loc_id: int, volume_ul: int,
            **kwargs,
    ) -> bool:
        '''Simulate dispense at location.'''
        time.sleep(self._delay_s)
        return 'pump_dispense' not in self._fail_commands

    def well_dispense_at(
            self, loc_id: int, volume_ul: int,
            **kwargs,
    ) -> bool:
        '''Simulate well dispense at location.'''
        time.sleep(self._delay_s)
        return 'well_dispense' not in self._fail_commands

    def cart_dispense_at(
            self, loc_id: int, volume_ul: int,
            **kwargs,
    ) -> dict | bool:
        '''Simulate cart dispense at location.'''
        time.sleep(self._delay_s)
        cb = kwargs.get('pre_dispense_cb')
        if callable(cb):
            cb()
        if 'cart_dispense' in self._fail_commands:
            return False
        if kwargs.get('stream'):
            return {'ok': True, '_pressure_samples': []}
        return True

    def cart_dispense_bf_at(
            self, loc_id: int, total_volume_ul: int,
            **kwargs,
    ) -> dict | bool:
        '''Simulate back-and-forth cart dispense.'''
        time.sleep(self._delay_s * 3)
        cb = kwargs.get('pre_dispense_cb')
        if callable(cb):
            cb()
        if 'cart_dispense_bf' in self._fail_commands:
            return False
        if kwargs.get('stream'):
            return {'ok': True, '_pressure_samples': []}
        return True

    def tip_mix_at(
            self, loc_id: int, **kwargs,
    ) -> bool:
        '''Simulate tip mix at location.'''
        time.sleep(self._delay_s)
        return 'tip_mix' not in self._fail_commands
