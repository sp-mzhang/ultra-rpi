'''ultra.hw.stm32_monitor -- STM32 status monitor.

Listens for periodic MSG_STATUS frames broadcast by the STM32
over UART and dispatches the parsed 36-byte payload to
registered handlers. Generalized from the old DoorMonitor to
decode all status fields.

The STM32 broadcasts MSG_STATUS every ~1 s via
Proto_SendResponse which adds 0x1000 to the command ID, so
MSG_STATUS (0xA001) arrives on the wire as 0xB001.
'''
from __future__ import annotations

import asyncio
import logging
import struct
import threading
import time
from typing import Callable, Optional

import serial

from ultra.events import EventBus
from ultra.hw import frame_protocol as fp

LOG = logging.getLogger(__name__)

DEFAULT_PORT = '/dev/ttyAMA3'
DEFAULT_BAUD = 921600

_MSG_STATUS_WIRE = fp.cmd_to_rsp(fp.MSG_STATUS)
_MSG_PRESSURE_WIRE = fp.cmd_to_rsp(fp.MSG_PRESSURE)

_STATUS_STRUCT = struct.Struct('<IBBBIiiiBHHHHBBBB')


def _parse_status_payload(data: bytes) -> dict | None:
    '''Decode the 36-byte proto_msg_status_t payload.

    Returns:
        Parsed status dict or None if payload is too short.
    '''
    if len(data) < _STATUS_STRUCT.size:
        return None
    (
        timestamp_ms,
        main_state, sub_state, progress,
        flags_raw,
        gantry_x, gantry_y, lift_z,
        motion_flags,
        pressure_raw, pump_position,
        temp_c_x10, centrifuge_rpm,
        tip_f, door_f,
        last_error, error_count,
    ) = _STATUS_STRUCT.unpack_from(data)

    return {
        'timestamp_ms': timestamp_ms,
        'main_state': main_state,
        'sub_state': sub_state,
        'progress': progress,
        'flags_raw': flags_raw,
        'gantry_x': gantry_x,
        'gantry_y': gantry_y,
        'lift_z': lift_z,
        'motion_flags': motion_flags,
        'gantry_moving': bool(motion_flags & 0x01),
        'lift_moving': bool(motion_flags & 0x02),
        'gantry_homed': bool(motion_flags & 0x04),
        'lift_homed': bool(motion_flags & 0x08),
        'pressure_raw': pressure_raw,
        'pump_position': pump_position,
        'temp_c': round(temp_c_x10 / 10.0, 1),
        'centrifuge_rpm': centrifuge_rpm,
        'tip_attached': bool(tip_f & 0x01),
        'tip_well_id': (tip_f >> 1) & 0x7F,
        'door_open': bool(door_f & 0x01),
        'door_closed': bool(door_f & 0x02),
        'door_locked': bool(door_f & 0x04),
        'last_error': last_error,
        'error_count': error_count,
    }


StatusHandler = Callable[[dict], None]


class STM32StatusMonitor:
    '''Listens for periodic MSG_STATUS from STM32 and dispatches
    parsed status dicts to registered handlers.

    Replaces the old DoorMonitor which only looked at door_f.
    Now decodes all 36 bytes and fires callbacks for any
    field change.

    Attributes:
        _active_instance: Singleton reference for UART
            sharing protocol.
    '''

    _active_instance: 'STM32StatusMonitor | None' = None

    @classmethod
    def stop_active(cls) -> None:
        '''Stop the currently active monitor instance.

        Called before protocol execution takes the UART port
        to avoid read contention.
        '''
        inst = cls._active_instance
        if inst is not None and inst._running:
            LOG.info(
                'Stopping active monitor '
                '(requested by protocol)',
            )
            inst.stop()

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            event_bus: EventBus,
            port: str = DEFAULT_PORT,
            baud: int = DEFAULT_BAUD,
    ) -> None:
        '''Create a status monitor.

        Args:
            loop: The running asyncio event loop for
                cross-thread scheduling.
            event_bus: Application event bus for emitting
                status events.
            port: Serial port path (e.g. /dev/ttyAMA3).
            baud: Baud rate matching firmware UART config.
        '''
        self._loop = loop
        self._event_bus = event_bus
        self._port = port
        self._baud = baud

        self._ser: Optional[serial.Serial] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._write_lock = threading.Lock()
        self._seq = 0

        self._handlers: dict[str, StatusHandler] = {}
        self._prev_status: dict | None = None

        self.drawer_opened_event = asyncio.Event()
        self.drawer_closed_event = asyncio.Event()

        self._register_builtin_handlers()

    def _register_builtin_handlers(self) -> None:
        '''Register default status field handlers.'''
        self.add_handler('door', self._door_handler)
        self.add_handler('motion', self._motion_handler)
        self.add_handler(
            'pressure', self._pressure_handler,
        )
        self.add_handler(
            'temperature', self._temperature_handler,
        )
        self.add_handler(
            'centrifuge', self._centrifuge_handler,
        )
        self.add_handler('tip', self._tip_handler)
        self.add_handler('error', self._error_handler)

    def add_handler(
            self,
            name: str,
            handler: StatusHandler,
    ) -> None:
        '''Register a handler called on every status update.

        Handlers run on the reader thread -- must be
        non-blocking. Use event_bus for async consumers.

        Args:
            name: Unique handler name (replaces if exists).
            handler: Callable accepting a status dict.
        '''
        self._handlers[name] = handler

    def remove_handler(self, name: str) -> None:
        '''Remove a registered handler by name.

        Args:
            name: Handler name to remove.
        '''
        self._handlers.pop(name, None)

    # ----------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------

    def is_door_open(self) -> bool:
        '''Return True if the last MSG_STATUS reported door_open.

        Safe to call from any thread -- just reads the last
        parsed status dict. Returns False if no status has
        been received yet.
        '''
        s = self._prev_status or {}
        return bool(s.get('door_open'))

    def is_door_closed(self) -> bool:
        '''Return True if the last MSG_STATUS reported door_closed.'''
        s = self._prev_status or {}
        return bool(s.get('door_closed'))

    def current_status(self) -> dict | None:
        '''Snapshot of the most recent MSG_STATUS, or None.'''
        return dict(self._prev_status) if self._prev_status else None

    def start(self) -> bool:
        '''Open serial and start the background reader thread.

        Returns:
            True if serial opened successfully.
        '''
        # Reset the rising-edge baseline so the first status
        # after a (re)start always has a chance to fire the
        # drawer_opened / drawer_closed events.  Without this,
        # a stop()/start() cycle inherits the previous run's
        # door state and the next 'True is not True' edge check
        # silently drops the event.
        self._prev_status = None
        try:
            self._ser = serial.Serial(
                port=self._port,
                baudrate=self._baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.1,
            )
            self._ser.reset_input_buffer()
            LOG.info(
                f'STM32StatusMonitor connected: '
                f'{self._port} @ {self._baud}',
            )
        except serial.SerialException as err:
            LOG.error(
                f'STM32StatusMonitor serial open failed: '
                f'{err}',
            )
            return False

        self._running = True
        self._thread = threading.Thread(
            target=self._reader_loop,
            name='STM32StatusMonitorThread',
            daemon=True,
        )
        self._thread.start()
        STM32StatusMonitor._active_instance = self
        LOG.info(
            'STM32StatusMonitor started -- listening '
            'for MSG_STATUS (wire ID 0x%04X)',
            _MSG_STATUS_WIRE,
        )
        return True

    def stop(self) -> None:
        '''Stop the reader thread and close the serial port.'''
        self._running = False
        if self._ser and self._ser.is_open:
            try:
                self._ser.close()
            except Exception:
                pass
            self._ser = None
        if self._thread:
            self._thread.join(timeout=3.0)
            self._thread = None
        if STM32StatusMonitor._active_instance is self:
            STM32StatusMonitor._active_instance = None
        LOG.info('STM32StatusMonitor stopped')

    # ----------------------------------------------------------------
    # LED command helpers
    # ----------------------------------------------------------------

    def send_led_pattern(
            self,
            pattern: int,
            stage: int = 0,
    ) -> None:
        '''Send CMD_LED_SET_PATTERN to the STM32.

        Thread-safe fire-and-forget. The STM32 will ACK but
        the reader thread discards non-MSG_STATUS frames.

        Args:
            pattern: Pattern ID (0=none, 1=waiting blue,
                2=ready green, 3=error red, 4=progress orange,
                5=scanning green).
            stage: Progress stage 1-5 (pattern=4 only).
        '''
        if not self._ser or not self._ser.is_open:
            LOG.warning(
                'send_led_pattern: serial not open',
            )
            return

        self._seq = (self._seq + 1) & 0xFFFFFFFF
        payload = fp.pack_led_set_pattern(
            seq=self._seq,
            pattern=pattern,
            stage=stage,
        )
        frame = fp.build_frame(
            command=fp.CMD_LED_SET_PATTERN,
            data=payload,
        )
        with self._write_lock:
            try:
                self._ser.write(frame)
                self._ser.flush()
                LOG.debug(
                    f'LED pattern sent: '
                    f'pattern={pattern} stage={stage}',
                )
            except serial.SerialException as err:
                LOG.warning(
                    f'send_led_pattern write failed: '
                    f'{err}',
                )

    # ----------------------------------------------------------------
    # Reader thread
    # ----------------------------------------------------------------

    def _reader_loop(self) -> None:
        '''Continuously read SOH frames and dispatch status.'''
        parser = fp.FrameParser()
        while self._running:
            if not self._ser or not self._ser.is_open:
                time.sleep(0.05)
                continue
            try:
                raw = self._ser.read(1)
            except (
                serial.SerialException,
                TypeError,
                OSError,
            ) as err:
                if self._running:
                    LOG.warning(f'Reader error: {err}')
                    time.sleep(0.1)
                continue

            if not raw:
                continue

            result = parser.feed(raw[0])
            if result is None:
                continue

            cmd, data = result

            if cmd == _MSG_STATUS_WIRE:
                status = _parse_status_payload(data)
                if status:
                    self._dispatch_status(status)
            elif cmd == _MSG_PRESSURE_WIRE:
                self._dispatch_pressure(data)

    def _dispatch_status(self, status: dict) -> None:
        '''Invoke all registered handlers with status dict.'''
        for name, handler in self._handlers.items():
            try:
                handler(status)
            except Exception:
                LOG.exception(
                    f'Error in status handler '
                    f'"{name}"',
                )
        self._prev_status = status

    def _dispatch_pressure(self, data: bytes) -> None:
        '''Decode and emit pressure async message.'''
        try:
            d = fp.unpack_msg_pressure(data)
            self._event_bus.emit_sync(
                'pressure_update', d,
            )
        except Exception:
            LOG.debug(
                'Failed to unpack pressure message',
            )

    # ----------------------------------------------------------------
    # Built-in handlers
    # ----------------------------------------------------------------

    def _door_handler(self, status: dict) -> None:
        '''Detect door open/close rising edges.'''
        prev = self._prev_status
        door_open = status.get('door_open', False)
        door_closed = status.get('door_closed', False)

        prev_open = (
            prev.get('door_open', False) if prev
            else None
        )
        prev_closed = (
            prev.get('door_closed', False) if prev
            else None
        )

        if door_open and prev_open is not True:
            LOG.info('Drawer OPENED')
            self._loop.call_soon_threadsafe(
                self.drawer_opened_event.set,
            )
            self._event_bus.emit_sync(
                'door_opened', status,
            )

        if door_closed and prev_closed is not True:
            LOG.info('Drawer CLOSED')
            self._loop.call_soon_threadsafe(
                self.drawer_closed_event.set,
            )
            self._event_bus.emit_sync(
                'door_closed', status,
            )

    def _motion_handler(self, status: dict) -> None:
        '''Emit gantry/lift position events.'''
        self._event_bus.emit_sync(
            'gantry_position', {
                'x': status.get('gantry_x', 0),
                'y': status.get('gantry_y', 0),
                'z': status.get('lift_z', 0),
                'gantry_moving': status.get(
                    'gantry_moving', False,
                ),
                'lift_moving': status.get(
                    'lift_moving', False,
                ),
            },
        )

    def _pressure_handler(self, status: dict) -> None:
        '''Emit pressure from periodic status.'''
        self._event_bus.emit_sync(
            'pressure_status', {
                'raw': status.get('pressure_raw', 0),
                'pump_pos': status.get(
                    'pump_position', 0,
                ),
            },
        )

    def _temperature_handler(self, status: dict) -> None:
        '''Emit temperature events.'''
        self._event_bus.emit_sync(
            'temperature_update', {
                'temp_c': status.get('temp_c', 0.0),
            },
        )

    def _centrifuge_handler(self, status: dict) -> None:
        '''Emit centrifuge RPM events.'''
        self._event_bus.emit_sync(
            'centrifuge_rpm', {
                'rpm': status.get('centrifuge_rpm', 0),
            },
        )

    def _tip_handler(self, status: dict) -> None:
        '''Emit tip status events on change.'''
        prev = self._prev_status
        if prev is None or (
            status.get('tip_attached')
            != prev.get('tip_attached')
            or status.get('tip_well_id')
            != prev.get('tip_well_id')
        ):
            self._event_bus.emit_sync(
                'tip_status', {
                    'attached': status.get(
                        'tip_attached', False,
                    ),
                    'well_id': status.get(
                        'tip_well_id', 0,
                    ),
                },
            )

    def _error_handler(self, status: dict) -> None:
        '''Emit error events on new errors.'''
        prev = self._prev_status
        if prev is None or (
            status.get('last_error')
            != prev.get('last_error')
            or status.get('error_count')
            != prev.get('error_count')
        ):
            err = status.get('last_error', 0)
            cnt = status.get('error_count', 0)
            if err != 0:
                LOG.warning(
                    f'STM32 error: code={err} '
                    f'count={cnt}',
                )
                self._event_bus.emit_sync(
                    'stm32_error', {
                        'error_code': err,
                        'error_count': cnt,
                    },
                )
