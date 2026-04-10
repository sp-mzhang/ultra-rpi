'''ultra.hw.stm32_interface

UART serial interface to the Ultra STM32 controller. Uses the
SOH-framed binary protocol defined in frame_protocol for all
communication over /dev/ttyAMA3.

Ported from sway.instruments.ultra.ultra_interface with sway
dependencies removed.
'''
from __future__ import annotations

import logging
import struct
import threading
import time
from typing import Optional

import serial

from ultra.hw import frame_protocol as fp

LOG = logging.getLogger(__name__)

DEFAULT_PORT = '/dev/ttyAMA3'
DEFAULT_BAUD = 921600
POLL_INTERVAL_S = 0.001

Z_DEFAULT_START = 0
Z_DEFAULT_BOTTOM = 40000

DEFAULT_THRESHOLD = 10
DEFAULT_TIMEOUT_MS = 20000
DEFAULT_VOLUME_UL = 100
DEFAULT_SPEED_UL_S = 50

Z_USTEPS_PER_MM = 400.0 / 0.6096

GANTRY_Z_MIN_MM = fp.GANTRY_Z_MIN_POS / Z_USTEPS_PER_MM


class STM32Interface:
    '''UART client for communicating with the Ultra STM32.

    Uses binary frame protocol (SOH-framed) over serial UART.
    Provides high-level methods for Z-axis, pump, and liquid
    level detection operations.

    Attributes:
        _port: Serial port path string.
        _baud: Baud rate integer.
        _ser: pyserial Serial instance or None.
        _seq: Rolling sequence number counter.
        _parser: FrameParser state machine instance.
    '''

    def __init__(
            self,
            port: str = DEFAULT_PORT,
            baud: int = DEFAULT_BAUD,
    ) -> None:
        '''Initialize the STM32 interface.

        Args:
            port: Serial port path (e.g. /dev/ttyAMA3).
            baud: Baud rate (default 921600).
        '''
        self._port = port
        self._baud = baud
        self._ser: Optional[serial.Serial] = None
        self._seq = 0
        self._parser = fp.FrameParser()
        self._lock = threading.Lock()
        self._abort_flag = threading.Event()
        self._motor_telem_cb = None
        LOG.info(
            f'STM32Interface created: '
            f'{port=} {baud=}',
        )

    def connect(self) -> bool:
        '''Open the serial port.

        Returns:
            True if the port was opened successfully,
                False otherwise.
        '''
        try:
            self._ser = serial.Serial(
                port=self._port,
                baudrate=self._baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.05,
            )
            LOG.info(
                f'Connected: UART {self._port} '
                f'@ {self._baud}',
            )
            return True
        except serial.SerialException as err:
            LOG.error(f'Connection failed: {err}')
            return False

    def disconnect(self) -> None:
        '''Close the serial port.'''
        if self._ser:
            self._ser.close()
            self._ser = None
            LOG.info('Disconnected')

    def _dispatch_motor_telem(
            self, rsp_cmd: int, rsp_data: bytes,
    ) -> None:
        '''Forward motor telemetry to registered callback.'''
        telem_low = fp.MSG_MOTOR_TELEMETRY & 0x00FF
        if (rsp_cmd & 0x00FF) != telem_low:
            return
        cb = self._motor_telem_cb
        if cb is None:
            return
        try:
            d = fp.unpack_msg_motor_telemetry(rsp_data)
            cb(d)
        except Exception:
            pass

    def set_motor_telem_callback(self, cb) -> None:
        '''Register a callback for motor telemetry samples.

        Args:
            cb: Callable(dict) or None to unregister.
        '''
        self._motor_telem_cb = cb

    def start_telem_reader(self) -> None:
        '''Start a background thread that reads UART frames
        and dispatches motor telemetry to the callback.

        Must be called while no command is in flight (the
        reader acquires _lock in short bursts).
        '''
        if getattr(self, '_telem_stop', None) is not None:
            return
        self._telem_stop = threading.Event()

        def _loop():
            while not self._telem_stop.is_set():
                if not self._ser or not self._ser.is_open:
                    self._telem_stop.wait(0.2)
                    continue
                if self._ser.in_waiting == 0:
                    self._telem_stop.wait(0.02)
                    continue
                acquired = self._lock.acquire(timeout=0.05)
                if not acquired:
                    self._telem_stop.wait(0.01)
                    continue
                try:
                    result = self._recv_frame(
                        timeout_s=0.02,
                    )
                finally:
                    self._lock.release()
                if result is None:
                    continue
                rsp_cmd, rsp_data = result
                if not fp.is_async_msg(rsp_cmd):
                    LOG.debug(
                        'telem-reader: dropped non-async '
                        'frame 0x%04X (%d bytes)',
                        rsp_cmd, len(rsp_data),
                    )
                    continue
                self._dispatch_motor_telem(
                    rsp_cmd, rsp_data,
                )

        self._telem_thread = threading.Thread(
            target=_loop, daemon=True,
            name='telem-reader',
        )
        self._telem_thread.start()
        LOG.info('Telemetry reader started')

    def stop_telem_reader(self) -> None:
        '''Stop the background telemetry reader thread and
        disable firmware telemetry.'''
        stop = getattr(self, '_telem_stop', None)
        if stop is None:
            return
        stop.set()
        t = getattr(self, '_telem_thread', None)
        if t is not None:
            t.join(timeout=2.0)
        self._telem_stop = None
        self._telem_thread = None
        self._motor_telem_cb = None
        try:
            self.send_command(
                cmd={
                    'cmd': 'set_motor_telem',
                    'enable': False,
                },
                timeout_s=1.0,
            )
        except Exception:
            pass
        LOG.info('Telemetry reader stopped')

    def request_abort(self) -> None:
        '''Signal all in-flight waits to exit immediately.

        Thread-safe — can be called from any thread while
        send_command or send_command_wait_done is blocked.
        After the wait exits the caller should send the
        firmware CMD_ABORT separately.
        '''
        self._abort_flag.set()

    def clear_abort(self) -> None:
        '''Clear the abort flag for the next operation.'''
        self._abort_flag.clear()

    def _next_seq(self) -> int:
        '''Get next sequence number.

        Returns:
            Current sequence number, then increments.
        '''
        seq = self._seq
        self._seq = (self._seq + 1) & 0xFFFFFFFF
        return seq

    def _send_frame(
            self,
            cmd_id: int,
            payload: bytes,
    ) -> None:
        '''Build and send a binary frame over UART.

        Args:
            cmd_id: 16-bit command identifier.
            payload: Packed payload bytes.
        '''
        if not self._ser:
            return
        frame = fp.build_frame(
            command=cmd_id,
            data=payload,
        )
        self._ser.write(frame)
        self._ser.flush()

    def _recv_frame(
            self,
            timeout_s: float = 0.05,
    ) -> tuple[int, bytes] | None:
        '''Receive one binary frame from UART.

        Args:
            timeout_s: Timeout in seconds.

        Returns:
            (command_id, data) tuple or None on timeout.
        '''
        if not self._ser:
            return None

        old_timeout = self._ser.timeout
        self._ser.timeout = timeout_s

        deadline = time.time() + timeout_s
        try:
            while time.time() < deadline:
                raw = self._ser.read(1)
                if not raw:
                    continue
                result = self._parser.feed(raw[0])
                if result is not None:
                    return result
        finally:
            self._ser.timeout = old_timeout
        return None

    def _drain_rx(self) -> None:
        '''Non-blocking drain of any pending UART data.'''
        if not self._ser:
            return
        self._parser.reset()
        while self._ser.in_waiting > 0:
            self._recv_frame(timeout_s=0.005)
        self._parser.reset()

    def send_command(
            self,
            cmd: dict,
            timeout_s: float = 30.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Send binary command and wait for response.

        Converts the command dict to binary frame format, sends
        it, and waits for a matching response. Async messages
        (status, pressure, events) are consumed silently.

        Thread-safe: acquires _lock for the entire
        send/receive cycle.

        Args:
            cmd: Command dict (e.g. {'cmd': 'ping'}).
            timeout_s: Timeout in seconds.
            collect_pressure: If True, collect pressure
                telemetry during waiting.

        Returns:
            Response dict or None on timeout.
        '''
        if not self._ser:
            LOG.error('Not connected')
            return None

        with self._lock:
            return self._send_command_inner(
                cmd, timeout_s, collect_pressure,
            )

    def _send_command_inner(
            self,
            cmd: dict,
            timeout_s: float = 30.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Inner send_command (caller must hold _lock).'''
        self._drain_rx()

        cmd_name = cmd.get('cmd', '')
        cmd_id = fp.CMD_NAME_TO_ID.get(cmd_name)
        if cmd_id is None:
            LOG.error(f'Unknown command: {cmd_name}')
            return None

        seq = self._next_seq()
        payload = self._pack_command(
            cmd_name=cmd_name,
            seq=seq,
            cmd=cmd,
        )

        expected_rsp = fp.cmd_to_rsp(cmd_id)

        LOG.info(
            f'TX: {cmd_name} (0x{cmd_id:04X}) '
            f'{seq=}',
        )
        self._send_frame(
            cmd_id=cmd_id,
            payload=payload,
        )

        start_time = time.time()
        pressure_samples: list[dict] = []

        while (time.time() - start_time) < timeout_s:
            if self._abort_flag.is_set():
                LOG.warning(
                    f'ABORT: cancelling wait for '
                    f'{cmd_name}',
                )
                return None
            remaining = timeout_s - (
                time.time() - start_time
            )
            result = self._recv_frame(
                timeout_s=min(remaining, 0.05),
            )
            if result is None:
                continue

            rsp_cmd, rsp_data = result

            if fp.is_async_msg(rsp_cmd):
                if (
                    collect_pressure
                    and rsp_cmd == fp.MSG_PRESSURE
                ):
                    d = fp.unpack_msg_pressure(rsp_data)
                    pressure_samples.append({
                        'ts': d.get('timestamp_ms', 0),
                        'p': d.get('pressure_raw', 0),
                        'pos': d.get(
                            'pump_position', 0,
                        ),
                    })
                self._dispatch_motor_telem(
                    rsp_cmd, rsp_data,
                )
                continue

            if rsp_cmd != expected_rsp:
                LOG.warning(
                    f'{cmd_name}: unexpected rsp_cmd '
                    f'0x{rsp_cmd:04X} (expected '
                    f'0x{expected_rsp:04X}) -- skipping',
                )
                continue

            decoded = self._decode_response(
                rsp_cmd=rsp_cmd,
                data=rsp_data,
                cmd_name=cmd_name,
            )

            rsp_seq = decoded.get('seq', -1)
            if rsp_seq != seq:
                LOG.warning(
                    f'{cmd_name}: seq mismatch '
                    f'got={rsp_seq} expected={seq} '
                    f'-- skipping stale response',
                )
                continue

            LOG.info(f'RX: {cmd_name} {decoded}')
            if collect_pressure and pressure_samples:
                decoded['_pressure_samples'] = (
                    pressure_samples
                )
            return decoded

        LOG.warning(
            f'Command {cmd_name} timed out after '
            f'{timeout_s:.1f}s',
        )
        return None

    def send_command_wait_done(
            self,
            cmd: dict,
            timeout_s: float = 120.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Send a command and wait for ACK then DONE.

        All long-running commands follow the same firmware
        pattern after the liquid_service unification:
          1. Immediate ACK (RSP_xxx) -- command accepted.
          2. Optional data RSP -- same cmd_id but larger
             payload (e.g. lld_perform returns z_position).
          3. Async DONE broadcast (MSG_PUMP_DONE,
             MSG_GANTRY_DONE, or MSG_LIFT_DONE).

        DONE routing:
          - lift_*  -> MSG_LIFT_DONE  (0xA009)
          - pump_* and liquid cmds (smart_aspirate,
            well_dispense, cart_dispense, cart_dispense_bf,
            tip_mix, lld_perform) -> MSG_PUMP_DONE (0xA007)
          - everything else (home_*, lid_move,
            gantry_tip_swap, move_*) -> MSG_GANTRY_DONE
            (0xA008)

        Thread-safe: acquires _lock for the entire
        send/ACK/DONE cycle.

        Args:
            cmd: Command dict (e.g. {'cmd': 'home_all'}).
            timeout_s: Total timeout covering both phases.
            collect_pressure: If True, accumulate
                MSG_PRESSURE (0xA004) async messages during
                the wait loop and attach them to the result
                dict as '_pressure_samples'.

        Returns:
            Dict with at least 'status' and 'error_code',
            plus any rich data from an intermediate RSP
            (e.g. 'z_position' for lld_perform).
            When collect_pressure is True, includes
            '_pressure_samples' list of dicts with keys
            timestamp_ms, pressure, position.
            None on timeout.
        '''
        if not self._ser:
            LOG.error('Not connected')
            return None

        with self._lock:
            return self._send_command_wait_done_inner(
                cmd, timeout_s, collect_pressure,
            )

    def _send_command_wait_done_inner(
            self,
            cmd: dict,
            timeout_s: float = 120.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Inner wait_done (caller must hold _lock).'''
        self._drain_rx()

        cmd_name = cmd.get('cmd', '')
        cmd_id = fp.CMD_NAME_TO_ID.get(cmd_name)
        if cmd_id is None:
            LOG.error(f'Unknown command: {cmd_name}')
            return None

        _PUMP_DONE_CMDS = {
            'pump_init', 'pump_aspirate', 'pump_dispense',
            'pump_prime', 'pump_blowout',
            'pump_piston_reset', 'pump_lld_start',
            'smart_aspirate', 'well_dispense',
            'cart_dispense', 'cart_dispense_bf', 'tip_mix',
            'lld_perform',
        }

        is_lift = cmd_name.startswith('lift_')
        is_pump_done = cmd_name in _PUMP_DONE_CMDS
        if is_lift:
            done_msg = fp.MSG_LIFT_DONE
            unpack_done = fp.unpack_msg_lift_done
        elif is_pump_done:
            done_msg = fp.MSG_PUMP_DONE
            unpack_done = fp.unpack_msg_pump_done
        else:
            done_msg = fp.MSG_GANTRY_DONE
            unpack_done = fp.unpack_msg_gantry_done

        seq = self._next_seq()
        payload = self._pack_command(
            cmd_name=cmd_name, seq=seq, cmd=cmd,
        )

        LOG.debug(
            f'TX(wait_done): {cmd_name} '
            f'(0x{cmd_id:04X}) {seq=}',
        )
        self._send_frame(cmd_id=cmd_id, payload=payload)

        start_time = time.time()
        got_ack = False
        data_rsp: Optional[dict] = None
        pressure_samples: list[dict] = []

        while (time.time() - start_time) < timeout_s:
            if self._abort_flag.is_set():
                LOG.warning(
                    f'ABORT: cancelling wait_done '
                    f'for {cmd_name}',
                )
                return None
            remaining = timeout_s - (
                time.time() - start_time
            )
            result = self._recv_frame(
                timeout_s=min(remaining, 0.05),
            )
            if result is None:
                continue

            rsp_cmd, rsp_data = result

            if fp.is_async_msg(rsp_cmd):
                rsp_low = rsp_cmd & 0x00FF
                pressure_low = fp.MSG_PRESSURE & 0x00FF
                if (
                    collect_pressure
                    and rsp_low == pressure_low
                ):
                    d = fp.unpack_msg_pressure(rsp_data)
                    pressure_samples.append({
                        'timestamp_ms': d.get(
                            'timestamp_ms', 0,
                        ),
                        'pressure': d.get(
                            'pressure_raw', 0,
                        ),
                        'position': d.get(
                            'pump_position', 0,
                        ),
                    })
                    continue
                self._dispatch_motor_telem(
                    rsp_cmd, rsp_data,
                )
                done_low = done_msg & 0x00FF
                if rsp_low == done_low:
                    msg = unpack_done(rsp_data)
                    if msg.get('cmd_id') == cmd_id:
                        err = msg.get('error', 0xFF)
                        status = (
                            'OK' if err == 0
                            else 'ERROR'
                        )
                        LOG.debug(
                            f'DONE({cmd_name}): '
                            f'error={err}',
                        )
                        done_result: dict = {
                            'status': status,
                            'error_code': err,
                        }
                        if collect_pressure:
                            done_result[
                                '_pressure_samples'
                            ] = pressure_samples
                        if data_rsp:
                            data_rsp.update(done_result)
                            return data_rsp
                        return done_result
                continue

            expected_rsp = fp.cmd_to_rsp(cmd_id)
            if rsp_cmd != expected_rsp:
                continue

            decoded = self._decode_response(
                rsp_cmd=rsp_cmd,
                data=rsp_data,
                cmd_name=cmd_name,
            )
            dec_err = decoded.get('error_code', 0xFF)

            if not got_ack:
                if dec_err != 0:
                    LOG.warning(
                        f'ACK({cmd_name}): '
                        f'error={dec_err}',
                    )
                    return decoded
                got_ack = True
                LOG.debug(f'ACK({cmd_name}): OK')
                continue

            data_rsp = decoded
            LOG.debug(
                f'DATA_RSP({cmd_name}): '
                f'{decoded}',
            )

        tag = 'ACK' if not got_ack else 'DONE'
        LOG.warning(
            f'{cmd_name} timed out waiting for {tag} '
            f'after {timeout_s:.1f}s',
        )
        return None

    def _pack_command(
            self,
            cmd_name: str,
            seq: int,
            cmd: dict,
    ) -> bytes:
        '''Pack a command dict into binary payload.

        Args:
            cmd_name: String name of the command.
            seq: Sequence number.
            cmd: Full command dict with parameters.

        Returns:
            Packed payload bytes.
        '''
        if cmd_name in (
                'ping', 'get_status', 'get_version',
                'get_flags', 'get_position', 'get_sensors',
                'reset', 'abort', 'pause', 'resume',
                'home_all', 'home_gantry',
                'home_x_axis', 'home_y_axis',
                'home_z_axis', 'get_gantry_status',
                'z_axis_test_gpio', 'pump_init',
                'pump_get_status', 'pump_test_move',
                'pump_lld_stop', 'pump_piston_reset',
                'centrifuge_stop', 'centrifuge_home',
                'door_open', 'door_close',
                'door_status',
                'lift_home', 'lift_stop',
                'lift_move_top',
                'led_set_all_off',
                'accel_get_status',
                'fc_heater_get_status',
        ):
            return fp.pack_seq(seq)

        if cmd_name == 'move_z_axis':
            return fp.pack_move_z_axis(
                seq=seq,
                position_mm=float(
                    cmd.get(
                        'position_mm',
                        cmd.get('position', 0),
                    ),
                ),
                speed=float(cmd.get('speed', 0.0)),
            )
        if cmd_name == 'move_gantry':
            return fp.pack_move_gantry(
                seq=seq,
                x_mm=cmd.get('x_mm'),
                y_mm=cmd.get('y_mm'),
                z_mm=cmd.get('z_mm'),
                speed=float(cmd.get('speed', 0.0)),
            )
        if cmd_name == 'lift_move':
            return fp.pack_lift_move(
                seq=seq,
                target_mm=float(
                    cmd.get('target_mm', 0.0),
                ),
                speed=float(
                    cmd.get('speed', 0.0),
                ),
            )
        if cmd_name == 'move_to_well':
            return fp.pack_move_to_well(
                seq=seq,
                well_id=cmd.get('well', 0),
            )
        if cmd_name == 'move_to_location':
            return fp.pack_move_to_location(
                seq=seq,
                location_id=int(
                    cmd.get('location_id', 0),
                ),
                speed_01mms=int(
                    cmd.get('speed_01mms', 0),
                ),
            )
        if cmd_name == 'th_power_en':
            return fp.pack_th_power_en(
                seq=seq,
                enable=bool(cmd.get('enable', False)),
            )
        if cmd_name == 'set_loc_centre':
            return fp.pack_set_loc_centre(
                seq=seq,
                x_um=int(cmd.get('x_um', 0)),
                y_um=int(cmd.get('y_um', 0)),
                z_um=int(cmd.get('z_um', 0)),
            )
        if cmd_name == 'set_loc_offset':
            return fp.pack_set_loc_offset(
                seq=seq,
                dx_um=int(cmd.get('dx_um', 0)),
                dy_um=int(cmd.get('dy_um', 0)),
                dz_um=int(cmd.get('dz_um', 0)),
            )
        if cmd_name == 'gantry_tip_swap':
            return fp.pack_tip_swap(
                seq=seq,
                from_id=int(cmd.get('from_id', 0)),
                to_id=int(cmd.get('to_id', 0)),
            )
        if cmd_name == 'lid_move':
            return fp.pack_lid_move(
                seq=seq,
                open=bool(cmd.get('open', True)),
                z_engage_um=int(
                    cmd.get('z_engage_um', 0),
                ),
                xy_speed_01mms=int(
                    cmd.get('xy_speed_01mms', 250),
                ),
                z_speed_01mms=int(
                    cmd.get('z_speed_01mms', 60),
                ),
                x_open_extra_um=int(
                    cmd.get('x_open_extra_um', 0),
                ),
            )
        if cmd_name in (
                'pump_aspirate', 'pump_dispense',
        ):
            return fp.pack_pump_transfer(
                seq=seq,
                volume_ul=cmd.get('volume', 0),
                speed_ul_s=cmd.get('speed', 0),
                cutoff_ul_s=cmd.get('cutoff', 0),
                streaming=cmd.get('streaming', False),
                wait=cmd.get('wait', True),
                validate=cmd.get('validate', False),
                integrate=cmd.get('integrate', False),
            )
        if cmd_name == 'pump_move_absolute':
            return fp.pack_pump_move_abs(
                seq=seq,
                position=cmd.get('position', 0),
                wait=cmd.get('wait', True),
            )
        if cmd_name == 'pump_enable_streaming':
            return fp.pack_pump_streaming(
                seq=seq,
                mode=cmd.get('mode', 0),
            )
        if cmd_name == 'pump_wait_idle':
            return fp.pack_pump_wait_idle(
                seq=seq,
                timeout_ms=cmd.get('timeout', 30000),
            )
        if cmd_name == 'pump_raw':
            return fp.pack_pump_raw(
                seq=seq,
                command=cmd.get('command', ''),
                query=cmd.get('query', False),
                timeout_ms=cmd.get('timeout', 5000),
            )
        if cmd_name == 'pump_lld_start':
            return fp.pack_pump_lld_start(
                seq=seq,
                threshold=cmd.get('threshold', 50),
                wait_ms=cmd.get('wait_ms', 500),
                dispense_s=cmd.get('dispense_s', 3),
                save_samples=cmd.get(
                    'save_samples', False,
                ),
            )
        if cmd_name == 'pump_stream_test':
            return fp.pack_pump_stream_test(
                seq=seq,
                duration_ms=cmd.get(
                    'duration_ms', 1000,
                ),
                steps=cmd.get('steps', 1000),
                dispense=cmd.get('dispense', False),
            )
        if cmd_name == 'pump_set_resolution':
            return fp.pack_pump_resolution(
                seq=seq,
                mode=cmd.get('mode', 0),
            )
        if cmd_name == 'pump_set_pressure_gain':
            return fp.pack_pump_gain(
                seq=seq,
                gain=cmd.get('gain', 0),
            )
        if cmd_name == 'set_state':
            return fp.pack_set_state(
                seq=seq,
                state=cmd.get('state', 0),
            )
        if cmd_name == 'set_control_mode':
            mode = cmd.get('mode', 0)
            if mode == 'direct':
                mode = 0
            elif mode == 'autonomous':
                mode = 1
            return fp.pack_set_control_mode(
                seq=seq,
                mode=mode,
            )
        if cmd_name == 'centrifuge_start':
            return fp.pack_centrifuge_start(
                seq=seq,
                rpm=cmd.get('rpm', 0),
                duration_s=cmd.get('duration', 0),
            )
        if cmd_name in (
            'centrifuge_unlock', 'centrifuge_lock',
            'centrifuge_reverse',
        ):
            return fp.pack_centrifuge_sequence(
                seq,
                angle_open_initial_deg=int(
                    cmd.get(
                        'angle_open_initial_deg', 290,
                    ),
                ),
            )
        if cmd_name == 'centrifuge_move_angle':
            return fp.pack_centrifuge_move_angle(
                seq=seq,
                angle_001deg=int(
                    cmd.get('angle_001deg', 0),
                ),
                move_rpm=int(
                    cmd.get('move_rpm', 500),
                ),
            )
        if cmd_name == 'centrifuge_bldc_cmd':
            bldc_cmd = int(cmd.get('bldc_cmd', 0))
            if bldc_cmd == fp.BLDC_SET_POS_PID:
                return fp.pack_bldc_pos_pid(
                    seq,
                    p_gain=int(
                        cmd.get('p_gain', 0),
                    ),
                    p_shift=int(
                        cmd.get('p_shift', 0),
                    ),
                    i_gain=int(
                        cmd.get('i_gain', 0),
                    ),
                    i_shift=int(
                        cmd.get('i_shift', 0),
                    ),
                )
            if bldc_cmd == fp.BLDC_SET_SOFT_CURR_LIMIT:
                return fp.pack_bldc_set_soft_curr_limit(
                    seq,
                    limit_01a=int(
                        cmd.get('data_u16', 0),
                    ),
                )
            if bldc_cmd == fp.BLDC_SET_MAX_CURRENT:
                return fp.pack_bldc_set_max_current(
                    seq,
                    max_01a=int(
                        cmd.get('data_u16', 0),
                    ),
                )
            data_u16 = cmd.get('data_u16')
            data_u32 = cmd.get('data_u32')
            if data_u32 is not None:
                inner = struct.pack(
                    '<I', int(data_u32),
                )
            elif data_u16 is not None:
                inner = struct.pack(
                    '<H', int(data_u16),
                )
            else:
                inner = cmd.get('data', b'')
                if isinstance(inner, str):
                    inner = bytes.fromhex(inner)
            return fp.pack_centrifuge_bldc_cmd(
                seq, bldc_cmd, inner,
            )
        if cmd_name in (
            'centrifuge_goto_serum',
            'centrifuge_goto_pipette',
            'centrifuge_goto_blister',
        ):
            return fp.pack_centrifuge_goto(
                seq=seq,
                angle_open_initial_deg=int(
                    cmd.get(
                        'angle_open_initial_deg', 290,
                    ),
                ),
                move_rpm=int(
                    cmd.get('move_rpm', 1),
                ),
            )
        if cmd_name == 'fan_set_duty':
            return fp.pack_fan_set_duty(
                seq=seq,
                pct=int(cmd.get('pct', 0)),
            )
        if cmd_name == 'fan_get_status':
            return fp.pack_fan_get_status(seq)
        if cmd_name == 'temp_get_status':
            return fp.pack_temp_get_status(seq)
        if cmd_name == 'read_z_drv':
            return fp.pack_seq(seq)
        if cmd_name == 'get_motor_status':
            return fp.pack_seq(seq)
        if cmd_name == 'set_motor_telem':
            return fp.pack_set_motor_telem(
                seq, cmd.get('enable', False),
            )
        if cmd_name == 'lld_perform':
            return fp.pack_lld_perform(
                seq=seq,
                threshold=cmd.get('threshold', 10),
                z_start=cmd.get('z_start', 0),
                z_bottom=cmd.get(
                    'z_bottom', fp.GANTRY_Z_MIN_POS,
                ),
                timeout_ms=cmd.get(
                    'timeout_ms', 20000,
                ),
                z_speed_sps=cmd.get('z_speed_sps', 0),
            )
        if cmd_name == 'llf_start':
            return fp.pack_llf_start(
                seq=seq,
                well_id=cmd.get(
                    'well_id', fp.WELL_ID_AUTO,
                ),
                z_speed_sps=cmd.get('z_speed_sps', 0),
            )
        if cmd_name == 'smart_aspirate':
            return fp.pack_smart_aspirate(
                seq=seq,
                volume_ul=cmd.get('volume', 100),
                pump_speed_ul_s=float(
                    cmd.get('speed', 50),
                ),
                lld_threshold=cmd.get(
                    'lld_threshold', 20,
                ),
                z_entry=cmd.get('z_entry', 0),
                z_bottom=cmd.get(
                    'z_bottom', fp.GANTRY_Z_MIN_POS,
                ),
                z_speed_sps=cmd.get('z_speed_sps', 0),
                well_id=cmd.get(
                    'well_id', fp.WELL_ID_AUTO,
                ),
                air_slug_ul=cmd.get('air_slug_ul', 0),
                stream=cmd.get('stream', False),
            )
        if cmd_name == 'well_dispense':
            return fp.pack_well_dispense(
                seq=seq,
                z_depth_mm=cmd.get('z_depth_mm', 0),
                volume_ul=cmd.get('volume', 0),
                speed_ul_s=float(
                    cmd.get('speed', 100.0),
                ),
                z_retract_mm=cmd.get(
                    'z_retract_mm', 5,
                ),
                blowout=cmd.get('blowout', True),
            )
        if cmd_name == 'cart_dispense_bf':
            return fp.pack_cart_dispense_bf(
                seq=seq,
                duration_s=cmd.get('duration_s', 170),
                vel_ul_s=float(
                    cmd.get('vel', 1.0),
                ),
                for_vol_ul=cmd.get('for_vol', 60),
                back_vol_ul=cmd.get('back_vol', 30),
                reasp_ul=cmd.get('reasp', 12),
                sleep_s=cmd.get('sleep_s', 30),
                z_retract_mm=cmd.get(
                    'z_retract_mm', 2,
                ),
                stream=cmd.get('stream', False),
            )
        if cmd_name == 'cart_dispense':
            return fp.pack_cart_dispense(
                seq=seq,
                volume_ul=cmd.get('volume', 0),
                vel_ul_s=float(
                    cmd.get('vel', 1.0),
                ),
                reasp_ul=cmd.get('reasp', 12),
                sleep_s=cmd.get('sleep_s', 0),
                z_retract_mm=cmd.get(
                    'z_retract_mm', 2,
                ),
                stream=cmd.get('stream', False),
            )
        if cmd_name == 'tip_mix':
            return fp.pack_tip_mix(
                seq=seq,
                mix_vol_ul=cmd.get('mix_vol', 150),
                speed_ul_s=float(
                    cmd.get('speed', 100.0),
                ),
                cycles=cmd.get('cycles', 4),
                pull_vol_ul=cmd.get('pull_vol', 0),
            )
        if cmd_name == 'centrifuge_power':
            return fp.pack_centrifuge_power(
                seq=seq,
                enable=bool(cmd.get('enable', False)),
            )
        if cmd_name == 'led_set_pixel':
            return fp.pack_led_set_pixel(
                seq=seq,
                idx=cmd.get('idx', 0xFF),
                r=cmd.get('r', 0), g=cmd.get('g', 0),
                b=cmd.get('b', 0), w=cmd.get('w', 0),
            )
        if cmd_name == 'led_set_button':
            return fp.pack_led_set_button(
                seq=seq,
                on=bool(cmd.get('on', False)),
            )
        if cmd_name == 'led_set_pixel_off':
            return fp.pack_led_set_pixel_off(
                seq=seq,
                idx=cmd.get('idx', 0xFF),
            )
        if cmd_name == 'led_set_pattern':
            return fp.pack_led_set_pattern(
                seq=seq,
                pattern=cmd.get('pattern', 0),
                stage=cmd.get('stage', 0),
            )
        if cmd_name == 'air_heater_set_duty':
            return fp.pack_air_heater_set_duty(
                seq=seq,
                pct=int(cmd.get('duty_pct', 0)),
            )
        if cmd_name == 'air_heater_set_en':
            return fp.pack_air_heater_set_en(
                seq=seq,
                enable=bool(cmd.get('enable', False)),
            )
        if cmd_name == 'air_heater_set_fan':
            return fp.pack_air_heater_set_fan(
                seq=seq,
                pct=int(cmd.get('duty_pct', 0)),
            )
        if cmd_name == 'air_heater_get_status':
            return fp.pack_air_heater_get_status(seq)
        if cmd_name == 'air_heater_set_ctrl':
            return fp.pack_air_heater_set_ctrl(
                seq=seq,
                setpoint_c=float(
                    cmd.get('setpoint_c', 37.0),
                ),
                hysteresis_c=float(
                    cmd.get('hysteresis_c', 1.0),
                ),
                heater_duty=int(
                    cmd.get('heater_duty', 100),
                ),
                fan_duty=int(
                    cmd.get('fan_duty', 100),
                ),
                enable=bool(
                    cmd.get('enable', False),
                ),
            )
        if cmd_name == 'fc_heater_set_duty':
            return fp.pack_fc_heater_set_duty(
                seq=seq,
                pct=int(cmd.get('pct', 0)),
            )
        if cmd_name == 'fc_heater_set_en':
            return fp.pack_fc_heater_set_en(
                seq=seq,
                enable=bool(cmd.get('enable', False)),
            )
        if cmd_name == 'fc_heater_set_ctrl':
            return fp.pack_fc_heater_set_ctrl(
                seq=seq,
                setpoint_x10=int(
                    cmd.get('setpoint_x10', 320),
                ),
                kp_x1000=int(
                    cmd.get('kp_x1000', 30),
                ),
                ki_x1000=int(
                    cmd.get('ki_x1000', 30000),
                ),
                kd_x1000=int(
                    cmd.get('kd_x1000', 6400),
                ),
                enable=bool(
                    cmd.get('enable', False),
                ),
            )

        return fp.pack_seq(seq)

    def _decode_response(
            self,
            rsp_cmd: int,
            data: bytes,
            cmd_name: str,
    ) -> dict:
        '''Decode a response frame into a dict.

        Args:
            rsp_cmd: Response command ID.
            data: Raw response payload bytes.
            cmd_name: Original command name string.

        Returns:
            Decoded response dict with seq, status, and
                command-specific fields.
        '''
        common = fp.unpack_rsp_common(data)
        error_code = common.get('error', 0)
        status = 'OK' if error_code == 0 else 'ERROR'
        result: dict = {
            'seq': common.get('seq', 0),
            'status': status,
            'error_code': error_code,
        }

        if cmd_name == 'ping':
            d = fp.unpack_rsp_ping(data)
            result['timestamp'] = d.get(
                'timestamp_ms', 0,
            )
        elif cmd_name == 'get_version':
            d = fp.unpack_rsp_version(data)
            result['version'] = {
                'major': d.get('major', 0),
                'minor': d.get('minor', 0),
                'patch': d.get('patch', 0),
                'build': d.get('build', ''),
            }
        elif cmd_name == 'get_flags':
            d = fp.unpack_rsp_flags(data)
            result['flags'] = d.get('flags', 0)
        elif cmd_name == 'get_position':
            d = fp.unpack_rsp_position(data)
            result['x'] = d.get('x', 0)
            result['y'] = d.get('y', 0)
            result['z'] = d.get('z', 0)
        elif cmd_name in (
                'pump_aspirate', 'pump_dispense',
        ):
            if len(data) >= 15:
                d = fp.unpack_rsp_pump_data(data)
                result['batch_integral'] = d.get(
                    'batch_integral', 0,
                )
                result['rt_integral'] = d.get(
                    'rt_integral', 0,
                )
                result['pump_rate'] = d.get(
                    'pump_rate_hz', 0,
                )
        elif cmd_name == 'lld_perform':
            if rsp_cmd == fp.RSP_LLD_PERFORM:
                d = fp.unpack_rsp_lld_result(data)
                result['detected'] = d.get(
                    'detected', False,
                )
                result['z_position'] = d.get(
                    'z_position', -1,
                )
                result['time_ms'] = d.get(
                    'time_ms', 0,
                )
                result['pressure_delta'] = d.get(
                    'pressure_delta', 0,
                )
        elif cmd_name == 'smart_aspirate':
            if rsp_cmd == fp.RSP_SMART_ASPIRATE:
                d = fp.unpack_rsp_smart_aspirate(data)
                result['lld_z'] = d.get('lld_z', 0)
                result['final_z'] = d.get(
                    'final_z', 0,
                )
                result['sample_count'] = d.get(
                    'sample_count', 0,
                )
        elif cmd_name == 'centrifuge_move_angle':
            d = fp.unpack_rsp_centrifuge_angle(data)
            result['error_code'] = d.get(
                'error', 0xFF,
            )
            result['status'] = (
                'OK' if result['error_code'] == 0
                else 'ERROR'
            )
            result['actual_deg'] = d.get(
                'actual_deg', 0.0,
            )
        elif cmd_name in ('lift_move', 'lift_status'):
            d = (
                fp.unpack_rsp_lift_status(data)
                if cmd_name == 'lift_status'
                else fp.unpack_rsp_lift_move(data)
            )
            result['error_code'] = d.get(
                'error', 0xFF,
            )
            result['status'] = (
                'OK' if result['error_code'] == 0
                else 'ERROR'
            )
            result['position_steps'] = d.get(
                'position_steps', 0,
            )
            if cmd_name == 'lift_status':
                result['is_homed'] = d.get(
                    'is_homed', False,
                )
                result['at_home'] = d.get(
                    'at_home', False,
                )
                result['at_top'] = d.get(
                    'at_top', False,
                )
                result['current_pct'] = d.get(
                    'current_pct', 0,
                )
        elif cmd_name == 'lift_home':
            d = fp.unpack_rsp_lift_home(data)
            result['error_code'] = d.get(
                'error', 0xFF,
            )
            result['status'] = (
                'OK' if result['error_code'] == 0
                else 'ERROR'
            )
            result['success'] = d.get('success', False)
            result['position_steps'] = d.get(
                'position_steps', 0,
            )
        elif cmd_name == 'get_gantry_status':
            d = fp.unpack_rsp_gantry_status(data)
            result.update(d)
        elif cmd_name in (
            'centrifuge_unlock', 'centrifuge_lock',
            'centrifuge_reverse',
            'centrifuge_goto_serum',
            'centrifuge_goto_pipette',
            'centrifuge_goto_blister',
        ):
            d = fp.unpack_rsp_centrifuge_sequence(data)
            result['error_code'] = d.get(
                'error', 0xFF,
            )
            result['status'] = (
                'OK' if d.get('ok') else 'ERROR'
            )
        elif cmd_name == 'air_heater_get_status':
            d = fp.unpack_air_heater_status(data)
            result.update(d)
        elif cmd_name == 'fc_heater_get_status':
            d = fp.unpack_fc_heater_status(data)
            result.update(d)
        elif cmd_name == 'fan_get_status':
            d = fp.unpack_fan_status(data)
            result.update(d)
        elif cmd_name == 'accel_get_status':
            d = fp.unpack_accel_status(data)
            result.update(d)
        elif cmd_name == 'temp_get_status':
            d = fp.unpack_temp_status(data)
            result.update(d)
        elif cmd_name == 'read_z_drv':
            d = fp.unpack_rsp_read_z_drv(data)
            result.update(d)
        elif cmd_name == 'get_motor_status':
            d = fp.unpack_rsp_motor_status(data)
            result.update(d)
        elif cmd_name == 'set_motor_telem':
            pass
        elif cmd_name == 'centrifuge_bldc_cmd':
            d = fp.unpack_rsp_centrifuge_bldc(data)
            result['error_code'] = d.get('error', 0xFF)
            result['status'] = (
                'OK' if d.get('ok') else 'ERROR'
            )
            result['bldc_cmd'] = d.get('bldc_cmd', '')
            result['data'] = d.get('data', '')
        elif cmd_name == 'centrifuge_status':
            d = fp.unpack_rsp_centrifuge_status(data)
            result['error_code'] = d.get(
                'error', 0xFF,
            )
            result['status'] = (
                'OK'
                if d.get('error', 0xFF) == 0
                else 'ERROR'
            )
            result['state'] = d.get('state', -1)
            result['rpm'] = d.get('rpm', 0)
            result['angle_001deg'] = d.get(
                'angle_001deg', 0,
            )
            result['driver_online'] = d.get(
                'driver_online', False,
            )
            result['error_flags'] = d.get(
                'error_flags', '0x0000',
            )
        elif cmd_name == 'well_dispense':
            if rsp_cmd == fp.RSP_WELL_DISPENSE:
                d = fp.unpack_rsp_well_dispense(data)
                result['error_code'] = d.get(
                    'error', 0xFF,
                )
                result['status'] = (
                    'OK'
                    if d.get('error', 0xFF) == 0
                    else 'ERROR'
                )
                result['final_z'] = d.get(
                    'final_z', 0,
                )
        elif cmd_name == 'cart_dispense_bf':
            if rsp_cmd == fp.RSP_CART_DISPENSE_BF:
                d = fp.unpack_rsp_cart_dispense_bf(data)
                result['error_code'] = d.get(
                    'error', 0xFF,
                )
                result['status'] = (
                    'OK'
                    if d.get('error', 0xFF) == 0
                    else 'ERROR'
                )
                result['final_z'] = d.get(
                    'final_z', 0,
                )
        elif cmd_name == 'cart_dispense':
            if rsp_cmd == fp.RSP_CART_DISPENSE:
                d = fp.unpack_rsp_cart_dispense(data)
                result['error_code'] = d.get(
                    'error', 0xFF,
                )
                result['status'] = (
                    'OK'
                    if d.get('error', 0xFF) == 0
                    else 'ERROR'
                )
                result['final_z'] = d.get(
                    'final_z', 0,
                )
        elif cmd_name == 'tip_mix':
            d = fp.unpack_rsp_tip_mix(data)
            result['error_code'] = d.get(
                'error', 0xFF,
            )
            result['status'] = (
                'OK'
                if d.get('error', 0xFF) == 0
                else 'ERROR'
            )

        return result

    # ----------------------------------------------------------------
    # Public command methods
    # ----------------------------------------------------------------

    def ping(self) -> bool:
        '''Test connection with a ping command.

        Returns:
            True if the device responded with OK status.
        '''
        resp = self.send_command(
            cmd={'cmd': 'ping'},
            timeout_s=5.0,
        )
        ok = bool(
            resp and (resp.get('status') == 'OK'),
        )
        LOG.info(f'Ping: {ok=}')
        return ok

    def perform_lld(
            self,
            z_start: int = 0,
            z_bottom: int = 0,
            threshold: int = DEFAULT_THRESHOLD,
            timeout_ms: int = DEFAULT_TIMEOUT_MS,
            z_speed_sps: int = 0,
    ) -> Optional[dict]:
        '''Perform liquid level detection scan.

        Args:
            z_start: Starting Z position in usteps from home.
            z_bottom: Hard bottom limit in usteps.
                0 = GANTRY_Z_MIN_POS default.
            threshold: Pump pLLD threshold (0-255).
            timeout_ms: Timeout in milliseconds.
            z_speed_sps: Z descent speed in steps/s
                (0 = firmware default).

        Returns:
            Response dict with detected, z_position,
                time_ms, pressure_delta, sample_count,
                or None on timeout.
        '''
        cmd: dict = {
            'cmd': 'lld_perform',
            'threshold': threshold,
            'z_start': z_start,
            'z_bottom': z_bottom or fp.GANTRY_Z_MIN_POS,
            'timeout_ms': timeout_ms,
            'z_speed_sps': z_speed_sps,
        }

        timeout_s = (timeout_ms / 1000.0) + 10.0
        resp = self.send_command_wait_done(
            cmd=cmd,
            timeout_s=timeout_s,
        )
        LOG.info(f'LLD result: {resp}')
        return resp

    def smart_aspirate(
            self,
            volume_ul: int,
            speed_ul_s: float,
            lld_threshold: int = DEFAULT_THRESHOLD,
            z_entry: int = 0,
            z_bottom: int = 0,
            z_speed_sps: int = 0,
            well_id: int = fp.WELL_ID_AUTO,
            air_slug_ul: int = 0,
            stream: bool = False,
            timeout_s: float = 120.0,
    ) -> Optional[dict]:
        '''LLD + aspirate in a single firmware command.

        Args:
            volume_ul: Volume to aspirate in microliters.
            speed_ul_s: Aspiration speed in uL/s (float).
            lld_threshold: pLLD pressure-rise threshold.
            z_entry: Z position to start LLD descent.
            z_bottom: Hard bottom limit (usteps).
            z_speed_sps: Z descent speed in steps/s.
            well_id: Well geometry lookup index.
                0xFF = auto-detect from last gantry location.
            air_slug_ul: Air slug volume in uL to aspirate
                before LLD; 0 = skip.
            stream: Enable real-time pressure streaming.
            timeout_s: Total timeout in seconds.

        Returns:
            Response dict with status and error_code,
            or None on timeout.
        '''
        cmd: dict = {
            'cmd': 'smart_aspirate',
            'volume': volume_ul,
            'speed': float(speed_ul_s),
            'lld_threshold': lld_threshold,
            'z_entry': z_entry,
            'z_bottom': z_bottom or fp.GANTRY_Z_MIN_POS,
            'z_speed_sps': z_speed_sps,
            'well_id': well_id,
            'air_slug_ul': air_slug_ul,
            'stream': stream,
        }

        resp = self.send_command_wait_done(
            cmd=cmd,
            timeout_s=timeout_s,
            collect_pressure=stream,
        )
        _summary = (
            {
                k: v for k, v in resp.items()
                if k != '_pressure_samples'
            }
            if resp else resp
        )
        LOG.info(
            'smart_aspirate %d uL: %s',
            volume_ul, _summary,
        )
        return resp

    def pump_dispense(
            self,
            volume_ul: int,
            speed_ul_s: int,
            streaming: bool = True,
            integrate: bool = True,
    ) -> Optional[dict]:
        '''Dispense with streaming and integration.

        Args:
            volume_ul: Volume to dispense in microliters.
            speed_ul_s: Dispense speed in uL/s.
            streaming: Enable CAN pressure streaming.
            integrate: Enable real-time pressure integration.

        Returns:
            Response dict with rt_integral,
                batch_integral, pump_rate, and optional
                _pressure_samples, or None on timeout.
        '''
        cmd: dict = {
            'cmd': 'pump_dispense',
            'volume': volume_ul,
            'speed': speed_ul_s,
            'wait': True,
            'streaming': streaming,
            'integrate': integrate,
        }
        timeout_s = (
            (volume_ul / max(speed_ul_s, 1)) + 15.0
        )
        resp = self.send_command(
            cmd=cmd,
            timeout_s=timeout_s,
            collect_pressure=streaming,
        )
        _summary = (
            {
                k: v for k, v in resp.items()
                if k != '_pressure_samples'
            }
            if resp else resp
        )
        _n = (
            len(resp.get('_pressure_samples', []))
            if resp else 0
        )
        LOG.info(
            f'pump_dispense {volume_ul=} uL: '
            f'{_summary} ({_n} pressure samples)',
        )
        LOG.debug(f'pump_dispense full: {resp}')
        return resp

    # ============================================================
    # Centrifuge helpers
    # ============================================================

    CFUGE_ST_IDLE = 0
    CFUGE_ST_READY = 1
    CFUGE_ST_STARTING = 3
    CFUGE_ST_RUNNING = 4
    CFUGE_ST_ERROR = 7

    _RPM_IDLE_MAX = 50

    def centrifuge_status(
            self,
            timeout_s: float = 5.0,
    ) -> dict | None:
        '''Query current centrifuge status.

        Returns:
            Dict with state, rpm, angle_001deg,
            driver_online, error_flags, status, error_code
            -- or None on timeout.
        '''
        return self.send_command(
            cmd={'cmd': 'centrifuge_status'},
            timeout_s=timeout_s,
        )

    def wait_centrifuge_idle(
            self,
            timeout_s: float = 60.0,
            poll_interval_s: float = 0.2,
    ) -> bool:
        '''Poll centrifuge_status until READY and |rpm| is low.

        Used after a timed spin to ensure the BLDC motor has
        fully stopped before issuing a centrifuge_move_angle.

        Args:
            timeout_s: Maximum seconds to wait (default 60).
            poll_interval_s: Seconds between polls
                (default 0.2).

        Returns:
            True if centrifuge reached READY with low rpm,
            False on timeout or ERROR state.
        '''
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            r = self.centrifuge_status(timeout_s=5.0)
            if r is None:
                time.sleep(poll_interval_s)
                continue
            state = r.get('state', -1)
            if state == self.CFUGE_ST_ERROR:
                LOG.error(
                    'Centrifuge in ERROR state: '
                    f'flags={r.get("error_flags")}',
                )
                return False
            rpm_abs = abs(int(r.get('rpm', 0)))
            if (
                    state in (
                        self.CFUGE_ST_IDLE,
                        self.CFUGE_ST_READY,
                    )
                    and rpm_abs <= self._RPM_IDLE_MAX
            ):
                LOG.info(
                    f'Centrifuge idle: state={state} '
                    f'rpm={r.get("rpm", 0)}',
                )
                return True
            time.sleep(poll_interval_s)

        LOG.warning(
            'Timed out waiting for centrifuge idle '
            f'after {timeout_s}s',
        )
        return False

    # ============================================================
    # Lift helpers
    # ============================================================

    def lift_status(
            self,
            timeout_s: float = 5.0,
    ) -> dict | None:
        '''Query current lift status.

        Returns:
            Dict with is_homed, position_steps, at_home,
            at_top, current_pct, status, error_code -- or
            None on timeout.
        '''
        return self.send_command(
            cmd={'cmd': 'lift_status'},
            timeout_s=timeout_s,
        )

    _LIFT_USTEPS_PER_MM = 16.0 / 0.0254  # ~629.92

    def wait_lift_idle(
            self,
            target_mm: float,
            tol_mm: float = 1.5,
            timeout_s: float = 90.0,
            poll_interval_s: float = 0.25,
    ) -> bool:
        '''Poll lift_status until position is near target_mm.

        Args:
            target_mm: Expected final lift height in mm.
            tol_mm: Acceptable distance from target
                (default 1.5).
            timeout_s: Maximum seconds to wait (default 90).
            poll_interval_s: Seconds between polls
                (default 0.25).

        Returns:
            True if lift reached target, False on timeout.
        '''
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            r = self.lift_status(timeout_s=5.0)
            if r is None:
                time.sleep(poll_interval_s)
                continue
            pos_steps = r.get('position_steps', 0)
            pos_mm = round(
                pos_steps / self._LIFT_USTEPS_PER_MM, 2,
            )
            if abs(pos_mm - target_mm) <= tol_mm:
                LOG.info(
                    f'Lift done: pos={pos_mm:.1f} mm '
                    f'(target={target_mm} mm)',
                )
                return True
            time.sleep(poll_interval_s)

        LOG.warning(
            'Timed out waiting for lift to reach '
            f'{target_mm} mm after {timeout_s}s',
        )
        return False

    # ============================================================
    # High-level pipetting helpers
    # ============================================================

    def aspirate_at(
            self,
            loc_id: int,
            volume_ul: int,
            speed_ul_s: int = 100,
            move_speed_01mms: int = 250,
            z_mm: float = -23.0,
            piston_reset: bool = False,
            timeout_s: float = 120.0,
    ) -> bool:
        '''Move to a location, lower Z, aspirate, then home Z.

        Sequence: move_to_location -> (piston_reset) -> Z down
        -> aspirate -> home Z.

        Args:
            loc_id: Cartridge location ID.
            volume_ul: Volume to aspirate in microliters.
            speed_ul_s: Aspiration speed in ul/s (default 100).
            move_speed_01mms: XY move speed in 0.1 mm/s units
                (default 250).
            z_mm: Z depth in mm (default -23.0).
            piston_reset: If True, reset piston before
                aspirating.
            timeout_s: Per-sub-step timeout in seconds
                (default 120).

        Returns:
            True if every sub-step succeeded.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'aspirate_at: move_to_location'
                f'({loc_id}) FAILED',
            )
            return False
        time.sleep(0.3)

        if piston_reset:
            r = self.send_command_wait_done(
                cmd={'cmd': 'pump_piston_reset'},
                timeout_s=60.0,
            )
            if not _resp_ok(r):
                LOG.error(
                    'aspirate_at: piston reset FAILED',
                )
                return False

        r = self.send_command_wait_done(
            cmd={'cmd': 'move_gantry', 'z_mm': z_mm},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'aspirate_at: Z down to {z_mm} mm '
                f'FAILED',
            )
            return False
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={
                'cmd': 'pump_aspirate',
                'volume': volume_ul,
                'speed': speed_ul_s,
            },
            timeout_s=60.0,
        )
        if not _resp_ok(r):
            LOG.error(
                f'aspirate_at: aspirate {volume_ul} ul '
                f'FAILED',
            )
            return False
        LOG.info(f'aspirate_at: {volume_ul} ul OK')
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error('aspirate_at: home Z FAILED')
            return False
        time.sleep(0.3)
        return True

    def smart_aspirate_at(
            self,
            loc_id: int,
            volume_ul: int,
            speed_ul_s: float = 100.0,
            move_speed_01mms: int = 250,
            well_id: int = fp.WELL_ID_AUTO,
            lld_threshold: int = 20,
            piston_reset: bool = True,
            air_slug_ul: int = 0,
            stream: bool = False,
            timeout_s: float = 120.0,
    ) -> dict | None:
        '''Move to a location, then LLF-aspirate in one step.

        Uses CMD_SMART_ASPIRATE so the firmware handles
        liquid-level detection, descent, and aspiration
        automatically.

        Sequence: move_to_location -> (piston_reset) ->
        smart_aspirate -> home_z_axis.

        Args:
            loc_id: Cartridge location ID.
            volume_ul: Volume to aspirate in microliters.
            speed_ul_s: Aspiration speed in ul/s
                (default 100).
            move_speed_01mms: XY move speed in 0.1 mm/s
                units (default 250).
            well_id: Well geometry index. 0xFF = auto-detect
                from gantry location.
            lld_threshold: pLLD pressure-rise threshold
                (default 20).
            piston_reset: If True, reset piston before
                aspirating (default True).
            air_slug_ul: Air slug volume in uL to aspirate
                before LLD; 0 = skip.
            stream: Enable real-time pressure streaming.
            timeout_s: Per-sub-step timeout in seconds
                (default 120).

        Returns:
            smart_aspirate response dict with status and
            error_code -- or None on failure.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'smart_aspirate_at: move_to_location'
                f'({loc_id}) FAILED',
            )
            return None
        time.sleep(0.3)

        if piston_reset:
            r = self.send_command_wait_done(
                cmd={'cmd': 'pump_piston_reset'},
                timeout_s=60.0,
            )
            if not _resp_ok(r):
                LOG.error(
                    'smart_aspirate_at: '
                    'piston reset FAILED',
                )
                return None

        resp = self.smart_aspirate(
            volume_ul=volume_ul,
            speed_ul_s=speed_ul_s,
            lld_threshold=lld_threshold,
            well_id=well_id,
            air_slug_ul=air_slug_ul,
            stream=stream,
        )
        if (
                resp is None
                or resp.get('error_code', 0xFF) != 0
        ):
            LOG.error(
                f'smart_aspirate_at: '
                f'smart_aspirate {volume_ul} ul '
                f'FAILED: {resp}',
            )
            return None

        LOG.info(
            f'smart_aspirate_at: {volume_ul} ul OK',
        )
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                'smart_aspirate_at: home Z FAILED',
            )
            return None
        time.sleep(0.3)
        return resp

    def dispense_at(
            self,
            loc_id: int,
            volume_ul: int,
            speed_ul_s: int = 1000,
            move_speed_01mms: int = 250,
            z_mm: float = -23.0,
            timeout_s: float = 120.0,
    ) -> bool:
        '''Move to a location, lower Z, dispense, then home Z.

        Sequence: move_to_location -> Z down -> dispense ->
        home Z.

        Args:
            loc_id: Cartridge location ID.
            volume_ul: Volume to dispense in microliters.
            speed_ul_s: Dispense speed in ul/s (default 1000).
            move_speed_01mms: XY move speed in 0.1 mm/s units
                (default 250).
            z_mm: Z depth in mm (default -23.0).
            timeout_s: Per-sub-step timeout in seconds
                (default 120).

        Returns:
            True if every sub-step succeeded.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'dispense_at: move_to_location'
                f'({loc_id}) FAILED',
            )
            return False
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={'cmd': 'move_gantry', 'z_mm': z_mm},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'dispense_at: Z down to {z_mm} mm '
                f'FAILED',
            )
            return False
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={
                'cmd': 'pump_dispense',
                'volume': volume_ul,
                'speed': speed_ul_s,
            },
            timeout_s=60.0,
        )
        if not _resp_ok(r):
            LOG.error(
                f'dispense_at: dispense {volume_ul} ul '
                f'FAILED',
            )
            return False
        LOG.info(f'dispense_at: {volume_ul} ul OK')
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error('dispense_at: home Z FAILED')
            return False
        time.sleep(0.3)
        return True

    def well_dispense_at(
            self,
            loc_id: int,
            volume_ul: int,
            speed_ul_s: float = 100.0,
            z_depth_mm: int = 0,
            z_retract_mm: int = 5,
            blowout: bool = True,
            move_speed_01mms: int = 250,
            timeout_s: float = 120.0,
    ) -> bool:
        '''Move to a well location, then firmware well-dispense.

        Sequence: move_to_location -> well_dispense (wait
        DONE) -> home Z.

        Args:
            loc_id: Cartridge location ID.
            volume_ul: Volume to dispense in uL.
            speed_ul_s: Dispense speed in uL/s.
            z_depth_mm: Depth into well in mm (0 = default).
            z_retract_mm: Retract height after dispense in mm.
            blowout: If True, include blowout after dispense.
            move_speed_01mms: XY move speed in 0.1 mm/s.
            timeout_s: Per-sub-step timeout in seconds.

        Returns:
            True if every sub-step succeeded.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'well_dispense_at: move_to({loc_id})'
                f' FAILED',
            )
            return False
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={
                'cmd': 'well_dispense',
                'z_depth_mm': z_depth_mm,
                'volume': volume_ul,
                'speed': speed_ul_s,
                'z_retract_mm': z_retract_mm,
                'blowout': blowout,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'well_dispense_at: well_dispense'
                f' {volume_ul} ul FAILED',
            )
            return False
        LOG.info(
            f'well_dispense_at loc={loc_id}: '
            f'{volume_ul} ul OK',
        )
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                'well_dispense_at: home Z FAILED',
            )
            return False
        time.sleep(0.3)
        return True

    def cart_dispense_at(
            self,
            loc_id: int,
            volume_ul: int,
            vel_ul_s: float = 1.0,
            reasp_ul: int = 12,
            sleep_s: int = 0,
            z_retract_mm: int = 2,
            cartridge_z: float = 0.0,
            move_speed_01mms: int = 250,
            stream: bool = False,
            timeout_s: float = 120.0,
    ) -> dict | bool:
        '''Move to a location and firmware cart-dispense.

        Sequence: move_to_location -> (Z to cartridge_z) ->
        cart_dispense (wait DONE) -> home Z.

        Args:
            loc_id: Cartridge location ID (e.g. PP4).
            volume_ul: Volume to dispense in uL.
            vel_ul_s: Dispense velocity in uL/s.
            reasp_ul: Re-aspiration volume in uL.
            sleep_s: Post-dispense sleep in seconds.
            z_retract_mm: Retract height after dispense in mm.
            cartridge_z: Pre-detected Z position in mm.
            move_speed_01mms: XY move speed in 0.1 mm/s.
            stream: Enable real-time pressure streaming.
            timeout_s: Per-sub-step timeout in seconds.

        Returns:
            When stream is False: True on success.
            When stream is True: dict with 'ok' and
            '_pressure_samples' on success. False on failure.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'cart_dispense_at: move_to({loc_id})'
                f' FAILED',
            )
            return False
        time.sleep(0.3)

        if cartridge_z:
            r = self.send_command_wait_done(
                cmd={
                    'cmd': 'move_z_axis',
                    'position_mm': cartridge_z,
                },
                timeout_s=timeout_s,
            )
            if not _resp_ok(r):
                LOG.error(
                    'cart_dispense_at: Z move FAILED',
                )
                return False
            time.sleep(0.3)

        dispense_time = (
            float(volume_ul) / max(vel_ul_s, 0.01)
        )
        long_timeout = max(
            timeout_s,
            dispense_time + float(sleep_s) + 60.0,
        )
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'cart_dispense',
                'volume': volume_ul,
                'vel': vel_ul_s,
                'reasp': reasp_ul,
                'sleep_s': sleep_s,
                'z_retract_mm': z_retract_mm,
                'stream': stream,
            },
            timeout_s=long_timeout,
            collect_pressure=stream,
        )
        if not _resp_ok(r):
            LOG.error(
                f'cart_dispense_at: cart_dispense'
                f' {volume_ul} ul FAILED',
            )
            return False
        pressure = (
            r.get('_pressure_samples', [])
            if stream and r else []
        )
        LOG.info(
            f'cart_dispense_at loc={loc_id}: '
            f'{volume_ul} ul OK',
        )
        time.sleep(0.3)

        rh = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(rh):
            LOG.error(
                'cart_dispense_at: home Z FAILED',
            )
            return False
        time.sleep(0.3)
        if stream:
            return {
                'ok': True,
                '_pressure_samples': pressure,
            }
        return True

    def cart_dispense_bf_at(
            self,
            loc_id: int,
            duration_s: int = 170,
            vel_ul_s: float = 1.0,
            for_vol_ul: int = 60,
            back_vol_ul: int = 30,
            reasp_ul: int = 12,
            sleep_s: int = 30,
            z_retract_mm: int = 2,
            cartridge_z: float = 0.0,
            move_speed_01mms: int = 250,
            stream: bool = False,
            timeout_s: float = 120.0,
    ) -> dict | bool:
        '''Move to a location and firmware back-and-forth
        dispense for a fixed duration.

        Sequence: move_to_location -> (Z to cartridge_z) ->
        cart_dispense_bf (wait DONE) -> home Z.

        Args:
            loc_id: Cartridge location ID (e.g. PP4).
            duration_s: Total back-and-forth duration in
                seconds. Firmware runs forward/back cycles
                for this long.
            vel_ul_s: Dispense velocity in uL/s.
            for_vol_ul: Forward dispense volume per cycle
                in uL.
            back_vol_ul: Back-aspirate volume per cycle
                in uL.
            reasp_ul: Re-aspiration volume in uL.
            sleep_s: Post-dispense dwell in seconds.
            z_retract_mm: Retract height after dispense
                in mm.
            cartridge_z: Pre-detected Z position in mm.
            move_speed_01mms: XY move speed in 0.1 mm/s.
            stream: Enable real-time pressure streaming.
            timeout_s: Per-sub-step timeout in seconds.

        Returns:
            When stream is False: True on success.
            When stream is True: dict with 'ok' and
            '_pressure_samples'. False on failure.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'cart_dispense_bf_at: '
                f'move_to({loc_id}) FAILED',
            )
            return False
        time.sleep(0.3)

        if cartridge_z:
            r = self.send_command_wait_done(
                cmd={
                    'cmd': 'move_z_axis',
                    'position_mm': cartridge_z,
                },
                timeout_s=timeout_s,
            )
            if not _resp_ok(r):
                LOG.error(
                    'cart_dispense_bf_at: Z move FAILED',
                )
                return False
            time.sleep(0.3)

        long_timeout = (
            float(duration_s) + float(sleep_s) + 60.0
        )
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'cart_dispense_bf',
                'duration_s': duration_s,
                'vel': vel_ul_s,
                'for_vol': for_vol_ul,
                'back_vol': back_vol_ul,
                'reasp': reasp_ul,
                'sleep_s': sleep_s,
                'z_retract_mm': z_retract_mm,
                'stream': stream,
            },
            timeout_s=long_timeout,
            collect_pressure=stream,
        )
        if not _resp_ok(r):
            LOG.error(
                f'cart_dispense_bf_at: '
                f'cart_dispense_bf '
                f'{duration_s}s FAILED',
            )
            return False
        pressure = (
            r.get('_pressure_samples', [])
            if stream and r else []
        )
        LOG.info(
            f'cart_dispense_bf_at loc={loc_id}: '
            f'{duration_s}s OK',
        )
        time.sleep(0.3)

        rh = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(rh):
            LOG.error(
                'cart_dispense_bf_at: home Z FAILED',
            )
            return False
        time.sleep(0.3)
        if stream:
            return {
                'ok': True,
                '_pressure_samples': pressure,
            }
        return True

    def tip_mix_at(
            self,
            loc_id: int,
            mix_vol_ul: int = 150,
            speed_ul_s: float = 100.0,
            cycles: int = 4,
            pull_vol_ul: int = 0,
            z_depth_mm: float = 0.0,
            move_speed_01mms: int = 250,
            timeout_s: float = 120.0,
    ) -> bool:
        '''Move to a well, descend Z, and firmware tip-mix.

        Sequence: move_to_location -> move_gantry(z) ->
        tip_mix (wait DONE) -> home Z.

        Args:
            loc_id: Cartridge location ID.
            mix_vol_ul: Volume per mix cycle in uL.
            speed_ul_s: Mix speed in uL/s.
            cycles: Number of aspirate/dispense cycles.
            pull_vol_ul: Final pull volume in uL (0 = none).
            z_depth_mm: Z depth into well in mm (negative
                = down). 0 = go to firmware Z min position.
            move_speed_01mms: XY move speed in 0.1 mm/s.
            timeout_s: Per-sub-step timeout in seconds.

        Returns:
            True if every sub-step succeeded.
        '''
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_to_location',
                'location_id': loc_id,
                'speed_01mms': move_speed_01mms,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'tip_mix_at: move_to({loc_id})'
                f' FAILED',
            )
            return False
        time.sleep(0.3)

        z_target = (
            z_depth_mm if z_depth_mm != 0.0
            else GANTRY_Z_MIN_MM
        )
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'move_gantry',
                'z_mm': z_target,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'tip_mix_at: Z down to '
                f'{z_target} mm FAILED',
            )
            return False
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={
                'cmd': 'tip_mix',
                'mix_vol': mix_vol_ul,
                'speed': speed_ul_s,
                'cycles': cycles,
                'pull_vol': pull_vol_ul,
            },
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error(
                f'tip_mix_at: tip_mix'
                f' {mix_vol_ul} ul x{cycles} FAILED',
            )
            return False
        LOG.info(
            f'tip_mix_at loc={loc_id}: '
            f'{mix_vol_ul} ul x{cycles} OK',
        )
        time.sleep(0.3)

        r = self.send_command_wait_done(
            cmd={'cmd': 'home_z_axis'},
            timeout_s=timeout_s,
        )
        if not _resp_ok(r):
            LOG.error('tip_mix_at: home Z FAILED')
            return False
        time.sleep(0.3)
        return True


def _resp_ok(resp: dict | None) -> bool:
    '''Return True if a command response indicates success.'''
    return bool(resp and resp.get('status') == 'OK')
