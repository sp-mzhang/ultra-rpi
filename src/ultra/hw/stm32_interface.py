'''ultra.hw.stm32_interface

UART serial interface to the Ultra STM32 controller. Uses the
SOH-framed binary protocol defined in frame_protocol for all
communication over /dev/ttyAMA3.

Ported from sway.instruments.ultra.ultra_interface with sway
dependencies removed.
'''
from __future__ import annotations

import logging
import queue
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

Z_USTEPS_PER_MM = fp.Z_USTEPS_PER_MM

GANTRY_Z_MIN_MM = fp.GANTRY_Z_MIN_POS / Z_USTEPS_PER_MM


class _PendingResult:
    '''Single-shot rendezvous between RX thread and a send_command
    caller. wait() blocks the caller; set_result() unblocks it from
    the RX thread. abort_evt lets request_abort() jolt the wait
    loose without waiting for the timeout.'''

    __slots__ = ('_evt', '_value')

    def __init__(self) -> None:
        self._evt   = threading.Event()
        self._value = None

    def set_result(self, value) -> None:
        self._value = value
        self._evt.set()

    def wait(
            self,
            timeout_s: float,
            abort_evt: Optional[threading.Event] = None,
    ):
        '''Block up to timeout_s. Returns the stored value, or
        None on timeout / abort. Polled in 50 ms slices so abort
        is responsive without sacrificing CPU.'''
        deadline = time.time() + timeout_s
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            if self._evt.wait(timeout=min(remaining, 0.05)):
                return self._value
            if abort_evt is not None and abort_evt.is_set():
                return None


# Map of MSG_*_DONE id → unpack function. Used by _dispatch_rx to
# decode the matching async event for send_command_wait_done.
_DONE_UNPACKERS: dict[int, callable] = {
    fp.MSG_PUMP_DONE:   fp.unpack_msg_pump_done,
    fp.MSG_GANTRY_DONE: fp.unpack_msg_gantry_done,
    fp.MSG_LIFT_DONE:   fp.unpack_msg_lift_done,
}


def _unpack_done(done_msg_id: int, data: bytes) -> dict:
    fn = _DONE_UNPACKERS.get(done_msg_id)
    if fn is None:
        return {}
    return fn(data)


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
        self._abort_flag = threading.Event()
        self._motor_telem_cb = None

        # Gantry motion defaults (mm/s). Applied when a caller issues
        # a move command without an explicit speed field. Zero = fall
        # back to the firmware axis default (cfg->default_speed_sps).
        # Overwritten at startup by set_motion_defaults() from YAML
        # config (see gantry.motion in ultra_default.yaml).
        self._mot_x_mms: float = 0.0
        self._mot_y_mms: float = 0.0
        self._mot_z_mms: float = 0.0

        # ---- Two-thread serial pipeline ----
        # The previous design held a single self._lock around every
        # send/receive cycle, which starved the always-on telem
        # reader and let the kernel TTY input buffer overrun under
        # load. New design: one TX worker drains a queue of frames
        # to write; one RX worker continuously reads bytes, parses
        # frames, and either fans them out to async callbacks or
        # fulfils a pending Future keyed by (rsp_cmd, seq). No lock
        # is held for the duration of a send_command wait, so RX
        # never stops draining.
        self._stop_workers = threading.Event()
        self._tx_queue: queue.Queue = queue.Queue(maxsize=256)
        self._tx_thread: Optional[threading.Thread] = None
        self._rx_thread: Optional[threading.Thread] = None
        self._tx_write_lock = threading.Lock()  # short, just around write()

        # Pending request matchers. Keyed by (rsp_cmd_id, seq).
        # Filled by send_command, fulfilled by the RX thread when
        # the matching response arrives.
        self._pending_ack: dict[tuple[int, int], '_PendingResult'] = {}
        # Done-event matchers for send_command_wait_done. Keyed by
        # (done_msg_id, original_cmd_id). Fulfilled by RX when the
        # async DONE broadcast carrying matching cmd_id arrives.
        self._pending_done: dict[tuple[int, int], '_PendingResult'] = {}
        # Active pressure-sample collectors, one per in-flight
        # send_command(*, collect_pressure=True). RX appends to
        # every list when a MSG_PRESSURE arrives.
        self._pressure_collectors: list[list[dict]] = []
        self._pending_lock = threading.Lock()

        LOG.info(
            f'STM32Interface created: '
            f'{port=} {baud=}',
        )

    def connect(self) -> bool:
        '''Open the serial port and start the TX/RX worker threads.

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
            time.sleep(0.1)
            self._ser.reset_input_buffer()
            self._parser = fp.FrameParser()
            LOG.info(
                f'Connected: UART {self._port} '
                f'@ {self._baud}',
            )
        except serial.SerialException as err:
            LOG.error(f'Connection failed: {err}')
            return False

        # Start the dedicated RX/TX worker threads. RX must come up
        # first so any bytes that arrive while we're starting TX
        # don't fill the kernel buffer.
        self._stop_workers.clear()
        # Drain any stale items from a previous session.
        while True:
            try:
                self._tx_queue.get_nowait()
            except queue.Empty:
                break

        self._rx_thread = threading.Thread(
            target=self._rx_loop, daemon=True,
            name='stm32-rx',
        )
        self._tx_thread = threading.Thread(
            target=self._tx_loop, daemon=True,
            name='stm32-tx',
        )
        self._rx_thread.start()
        self._tx_thread.start()
        LOG.info('STM32 RX/TX worker threads started')
        return True

    def disconnect(self) -> None:
        '''Stop the worker threads and close the serial port.'''
        # Signal both workers to exit. A poison-pill enqueue makes
        # sure the TX worker wakes from its blocking get() promptly
        # even if no command was pending.
        self._stop_workers.set()
        try:
            self._tx_queue.put_nowait(None)
        except queue.Full:
            pass

        for t_attr in ('_rx_thread', '_tx_thread'):
            t = getattr(self, t_attr, None)
            if t is not None:
                t.join(timeout=2.0)
                setattr(self, t_attr, None)

        # Cancel any pending Futures so callers waiting on them
        # don't hang past disconnect.
        with self._pending_lock:
            for fut in list(self._pending_ack.values()):
                fut.set_result(None)
            for fut in list(self._pending_done.values()):
                fut.set_result(None)
            self._pending_ack.clear()
            self._pending_done.clear()
            self._pressure_collectors.clear()

        if self._ser:
            try:
                self._ser.close()
            except Exception:
                pass
            self._ser = None
            LOG.info('Disconnected')

    # ----------------------------------------------------------------
    # Worker threads
    # ----------------------------------------------------------------

    def _tx_loop(self) -> None:
        '''Dedicated writer. Drains _tx_queue → serial.write().

        Frames built by send_command land here as bytes objects. A
        sentinel of None means "shut down" (used by disconnect()).
        Writes are guarded by a tiny mutex so a future caller that
        bypasses the queue and writes directly can't interleave
        bytes mid-frame; in normal operation only this thread holds
        it.
        '''
        while not self._stop_workers.is_set():
            try:
                frame = self._tx_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if frame is None:
                break
            ser = self._ser
            if ser is None:
                continue
            try:
                with self._tx_write_lock:
                    ser.write(frame)
                    ser.flush()
            except Exception as err:
                LOG.warning(f'TX write failed: {err}')

    def _rx_loop(self) -> None:
        '''Dedicated reader. Continuously parses frames and either
        dispatches them to async callbacks or fulfils a pending
        request Future. NEVER takes a lock that send_command holds,
        so the kernel TTY input buffer can't accumulate during a
        command wait.

        Drains the kernel buffer in one syscall per wake so the
        per-byte GIL thrashing doesn't delay ACK futures when the
        firmware's 500 ms MSG_STATUS broadcast and motor telemetry
        streams are running. ser.read(1) blocks up to the port
        timeout for the first byte; in_waiting picks up everything
        else that has already arrived.
        '''
        parser = fp.FrameParser()
        while not self._stop_workers.is_set():
            ser = self._ser
            if ser is None or not ser.is_open:
                self._stop_workers.wait(0.05)
                continue
            try:
                raw = ser.read(1)
                if raw:
                    pending = 0
                    try:
                        pending = ser.in_waiting
                    except (serial.SerialException, OSError):
                        pending = 0
                    if pending:
                        more = ser.read(pending)
                        if more:
                            raw = raw + more
            except (serial.SerialException, OSError) as err:
                if not self._stop_workers.is_set():
                    LOG.debug(f'RX read error: {err}')
                self._stop_workers.wait(0.05)
                continue
            if not raw:
                continue
            for b in raw:
                result = parser.feed(b)
                if result is None:
                    continue
                cmd, data = result
                try:
                    self._dispatch_rx(cmd, data)
                except Exception as err:
                    LOG.warning(f'RX dispatch raised: {err}')

    def _dispatch_rx(self, cmd: int, data: bytes) -> None:
        '''Route one parsed frame. Called only from _rx_loop.'''
        # 1. Accel stream push frames (share wire id with their ACK
        #    — disambiguate by length).
        if cmd == fp.RSP_ACCEL_STREAM and len(data) >= 8:
            self._dispatch_accel_stream(data)
            return

        # 2. Async push frames (0xA0xx → 0xB0xx range).
        if fp.is_async_msg(cmd):
            # Pressure samples are collected by any in-flight
            # send_command(*, collect_pressure=True).
            if (cmd & 0xFF) == (fp.MSG_PRESSURE & 0xFF):
                try:
                    d = fp.unpack_msg_pressure(data)
                except Exception:
                    d = {}
                with self._pending_lock:
                    collectors = list(self._pressure_collectors)
                if collectors:
                    sample = {
                        'ts':  d.get('timestamp_ms', 0),
                        'p':   d.get('pressure_raw', 0),
                        'pos': d.get('pump_position', 0),
                    }
                    for c in collectors:
                        c.append(sample)

            # Motor telemetry callback (filters on its own).
            self._dispatch_motor_telem(cmd, data)

            # DONE-event matchers for send_command_wait_done.
            with self._pending_lock:
                pending_done_items = list(self._pending_done.items())
            for key, fut in pending_done_items:
                done_msg_id, expected_cmd_id = key
                if (cmd & 0xFF) != (done_msg_id & 0xFF):
                    continue
                try:
                    msg = _unpack_done(done_msg_id - 0x1000, data)
                except Exception:
                    continue
                if msg.get('cmd_id') != expected_cmd_id:
                    continue
                with self._pending_lock:
                    self._pending_done.pop(key, None)
                fut.set_result(msg)
            return

        # 3. Sync responses (0x9xxx range, len ≥ 4 → seq prefix).
        if len(data) < 4:
            return
        seq = int.from_bytes(data[0:4], 'little')
        key = (cmd, seq)
        with self._pending_lock:
            fut = self._pending_ack.pop(key, None)
        if fut is not None:
            fut.set_result((cmd, data))

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

    # Diagnostic counters for the accel stream pipeline. Read via
    # the /api/diag/accel_counters endpoint so we can compare:
    #   STM32 isr_seq delta   = batches the firmware sent
    #   accel_dispatched      = batches stm32_interface decoded and
    #                            handed to the registered callback
    #   accel_cb_exceptions   = callback raised (would silently drop)
    accel_dispatched     = 0
    accel_cb_exceptions  = 0

    def _dispatch_accel_stream(
            self, rsp_data: bytes,
    ) -> None:
        '''Forward accel-stream batch to registered callback.

        Accel stream frames carry wire id RSP_ACCEL_STREAM
        (0x9E03), which is in the response-id range rather
        than the 0xA0xx/0xB0xx async range is_async_msg
        checks, so dispatch has to be explicit.
        '''
        STM32Interface.accel_dispatched += 1
        cb = getattr(self, '_accel_stream_cb', None)
        if cb is None:
            return
        try:
            cb(fp.unpack_accel_stream(rsp_data))
        except Exception:
            STM32Interface.accel_cb_exceptions += 1

    def set_accel_stream_callback(self, cb) -> None:
        '''Register a callback for accel stream batches.

        Fires ~25 Hz once accel_stream_start is sent. Each
        invocation receives the dict from unpack_accel_stream:
        seq, tick_ms, count, buf_used, samples=[(x,y,z), ...].

        Args:
            cb: Callable(dict) or None to unregister.
        '''
        self._accel_stream_cb = cb

    def set_motion_defaults(
            self,
            x_mms: float = 0.0,
            y_mms: float = 0.0,
            z_mms: float = 0.0,
    ) -> None:
        '''Set default gantry cruise speeds for unqualified moves.

        Any move command (``move_gantry``, ``move_to_location``,
        ``move_z_axis``) that omits an explicit ``speed`` / ``z_speed``
        field will fall back to these defaults when the frame is
        packed. Zero means "leave it as 0 and let the firmware pick
        its own axis default" (``cfg->default_speed_sps`` in
        ``gantry_hw.h``).

        Typically called once at startup from ``app.py`` using the
        ``gantry.motion`` block of ``config/ultra_default.yaml``.

        Args:
            x_mms: X-axis cruise speed in mm/s (0 = firmware default).
            y_mms: Y-axis cruise speed in mm/s (0 = firmware default).
            z_mms: Z-axis cruise speed in mm/s (0 = firmware default).
                   Note: firmware still clamps Z at GANTRY_Z_MAX_SPS
                   (~18 mm/s); higher requests are saturated there.
        '''
        self._mot_x_mms = max(0.0, float(x_mms))
        self._mot_y_mms = max(0.0, float(y_mms))
        self._mot_z_mms = max(0.0, float(z_mms))
        LOG.info(
            'Gantry motion defaults: '
            f'x={self._mot_x_mms:g} mm/s '
            f'y={self._mot_y_mms:g} mm/s '
            f'z={self._mot_z_mms:g} mm/s',
        )

    def apply_motion_defaults_from_config(
            self,
            config: dict | None,
    ) -> None:
        '''Apply gantry.motion cruise-speed defaults from a config dict.

        Reads the ``gantry.motion`` block of ``config`` (typically
        ``config/ultra_default.yaml`` or a merged runtime config) and
        forwards it to ``set_motion_defaults``. Missing keys default
        to 0 (firmware axis default).

        Args:
            config: Full Ultra config dict, or None to skip.
        '''
        motion = (
            (config or {}).get('gantry', {}).get('motion', {}) or {}
        )
        self.set_motion_defaults(
            x_mms=float(motion.get('x_speed_mms', 0.0) or 0.0),
            y_mms=float(motion.get('y_speed_mms', 0.0) or 0.0),
            z_mms=float(motion.get('z_speed_mms', 0.0) or 0.0),
        )

    def _default_xy_mms(self) -> float:
        '''Return the default XY cruise speed (mm/s).

        The firmware protocol carries a single XY speed for
        coordinated moves, so when per-axis defaults differ we fall
        back to the smaller one (whichever axis is the slow axis
        wins, keeping the motion within both limits). Returns 0 when
        neither X nor Y default is set so the firmware axis default
        kicks in.
        '''
        x, y = self._mot_x_mms, self._mot_y_mms
        if x <= 0.0:
            return y
        if y <= 0.0:
            return x
        return min(x, y)

    def start_telem_reader(self) -> None:
        '''No-op in the two-thread design.

        Kept for API compatibility with callers (api_stm32.py)
        that used to start a separate background reader after
        /connect. The dedicated RX worker thread now starts in
        connect() and runs continuously, so there's nothing to
        kick off here.
        '''
        LOG.debug('start_telem_reader: no-op (RX worker always on)')

    def stop_telem_reader(self) -> None:
        '''Disable firmware motor telemetry; clear the callback.

        Used to also tear down the background reader thread, which
        is now owned by connect()/disconnect() — so this only sends
        the firmware "set_motor_telem enable=False" command and
        unhooks the user callback.
        '''
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
        n = self._ser.write(frame)
        self._ser.flush()
        LOG.debug(
            'TX frame: %d bytes written, '
            'in_waiting=%d',
            n, self._ser.in_waiting,
        )

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
        pending = self._ser.in_waiting
        if pending:
            LOG.debug(
                'drain_rx: %d bytes pending', pending,
            )
        self._parser.reset()
        drained = 0
        while self._ser.in_waiting > 0:
            self._recv_frame(timeout_s=0.005)
            drained += 1
        if drained:
            LOG.debug(
                'drain_rx: consumed %d frames', drained,
            )
        self._parser.reset()

    def send_command(
            self,
            cmd: dict,
            timeout_s: float = 30.0,
            collect_pressure: bool = False,
            lock_timeout: float | None = None,   # accepted but ignored
    ) -> Optional[dict]:
        '''Send binary command, wait for the matching response.

        Implementation: builds the frame, registers a Future keyed
        by (rsp_cmd_id, seq), enqueues the frame on the TX worker,
        then blocks on the Future. The RX worker resolves it. No
        lock is held during the wait, so concurrent commands and
        the always-on async stream don't starve each other.

        Args:
            cmd: Command dict (e.g. {'cmd': 'ping'}).
            timeout_s: Wait timeout in seconds.
            collect_pressure: If True, attach every MSG_PRESSURE
                that arrives during the wait as
                response['_pressure_samples'].
            lock_timeout: Kept for API compatibility with the old
                serial-lock design. The new implementation has no
                blocking lock to wait on, so this parameter is
                ignored.

        Returns:
            Decoded response dict, or None on timeout / abort.
        '''
        del lock_timeout  # no-op in the new design
        if self._ser is None:
            LOG.error('Not connected')
            return None

        cmd_name = cmd.get('cmd', '')
        cmd_id = fp.CMD_NAME_TO_ID.get(cmd_name)
        if cmd_id is None:
            LOG.error(f'Unknown command: {cmd_name}')
            return None

        seq = self._next_seq()
        payload = self._pack_command(
            cmd_name=cmd_name, seq=seq, cmd=cmd,
        )
        rsp_id = fp.cmd_to_rsp(cmd_id)
        key    = (rsp_id, seq)
        fut    = _PendingResult()
        pressure: list[dict] = []

        with self._pending_lock:
            self._pending_ack[key] = fut
            if collect_pressure:
                self._pressure_collectors.append(pressure)

        try:
            frame = fp.build_frame(command=cmd_id, data=payload)
            try:
                self._tx_queue.put(frame, timeout=1.0)
            except queue.Full:
                LOG.error('TX queue full — dropping send_command')
                return None

            LOG.info(
                f'TX: {cmd_name} (0x{cmd_id:04X}) {seq=}',
            )

            result = fut.wait(timeout_s, abort_evt=self._abort_flag)
            if result is None:
                if self._abort_flag.is_set():
                    LOG.warning(
                        f'ABORT: cancelling wait for {cmd_name}',
                    )
                else:
                    LOG.warning(
                        f'Command {cmd_name} timed out after '
                        f'{timeout_s:.1f}s',
                    )
                return None

            rsp_cmd, rsp_data = result
            decoded = self._decode_response(
                rsp_cmd=rsp_cmd, data=rsp_data, cmd_name=cmd_name,
            )
            LOG.info(f'RX: {cmd_name} {decoded}')
            if collect_pressure and pressure:
                decoded['_pressure_samples'] = pressure
            return decoded
        finally:
            with self._pending_lock:
                self._pending_ack.pop(key, None)
                if collect_pressure:
                    try:
                        self._pressure_collectors.remove(pressure)
                    except ValueError:
                        pass

    def send_command_wait_done(
            self,
            cmd: dict,
            timeout_s: float = 120.0,
            collect_pressure: bool = False,
    ) -> Optional[dict]:
        '''Send a command, await its ACK, then await its async DONE.

        Long-running firmware commands ack synchronously then
        broadcast a DONE event when the underlying motion finishes:
          - lift_*  → MSG_LIFT_DONE
          - pump_* and liquid cmds → MSG_PUMP_DONE
          - everything else        → MSG_GANTRY_DONE

        Implementation: registers two Futures up front (ACK and
        DONE) so the RX worker can fulfil whichever arrives first
        without races. Releases both on every exit path.

        Args:
            cmd: Command dict (e.g. {'cmd': 'home_all'}).
            timeout_s: Total timeout covering both phases.
            collect_pressure: If True, accumulate MSG_PRESSURE
                async events into result['_pressure_samples'].

        Returns:
            Dict with 'status' and 'error_code', merged with any
            intermediate data RSP (e.g. 'z_position' from
            lld_perform). None on timeout / abort.
        '''
        if self._ser is None:
            LOG.error('Not connected')
            return None

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
        if cmd_name.startswith('lift_'):
            done_msg = fp.MSG_LIFT_DONE
        elif cmd_name in _PUMP_DONE_CMDS:
            done_msg = fp.MSG_PUMP_DONE
        else:
            done_msg = fp.MSG_GANTRY_DONE
        # The DONE broadcast wire ID has +0x1000 applied like every
        # other Proto_SendResponse, so the matcher key must use the
        # 0xB-prefixed wire form.
        done_wire = done_msg + 0x1000

        seq      = self._next_seq()
        payload  = self._pack_command(
            cmd_name=cmd_name, seq=seq, cmd=cmd,
        )
        rsp_id   = fp.cmd_to_rsp(cmd_id)
        ack_key  = (rsp_id, seq)
        done_key = (done_wire, cmd_id)

        ack_fut  = _PendingResult()
        done_fut = _PendingResult()
        pressure: list[dict] = []

        with self._pending_lock:
            self._pending_ack[ack_key]   = ack_fut
            self._pending_done[done_key] = done_fut
            if collect_pressure:
                self._pressure_collectors.append(pressure)

        try:
            frame = fp.build_frame(command=cmd_id, data=payload)
            try:
                self._tx_queue.put(frame, timeout=1.0)
            except queue.Full:
                LOG.error(
                    'TX queue full — dropping send_command_wait_done',
                )
                return None
            LOG.debug(
                f'TX(wait_done): {cmd_name} (0x{cmd_id:04X}) '
                f'{seq=}',
            )

            deadline   = time.time() + timeout_s
            data_rsp: Optional[dict] = None
            got_ack    = False

            # Two-phase wait. ACK comes first (immediate), then DONE
            # arrives later as an async broadcast. We poll both
            # Futures with a short slice so we can interleave them
            # without holding the GIL for long.
            while time.time() < deadline:
                remaining = deadline - time.time()
                if not got_ack:
                    rsp = ack_fut.wait(
                        min(remaining, 0.05),
                        abort_evt=self._abort_flag,
                    )
                    if rsp is not None:
                        rsp_cmd, rsp_data = rsp
                        decoded = self._decode_response(
                            rsp_cmd=rsp_cmd, data=rsp_data,
                            cmd_name=cmd_name,
                        )
                        dec_err = decoded.get('error_code', 0xFF)
                        if dec_err != 0:
                            LOG.warning(
                                f'ACK({cmd_name}): error={dec_err}',
                            )
                            return decoded
                        got_ack = True
                        LOG.debug(f'ACK({cmd_name}): OK')
                        # If the ACK had non-trivial payload, treat
                        # it as a data RSP (e.g. lld_perform).
                        if len(rsp_data) > 5:
                            data_rsp = decoded
                        continue
                    if self._abort_flag.is_set():
                        return None
                    continue

                msg = done_fut.wait(
                    min(remaining, 0.05),
                    abort_evt=self._abort_flag,
                )
                if msg is None:
                    if self._abort_flag.is_set():
                        return None
                    continue
                err    = msg.get('error', 0xFF)
                status = 'OK' if err == 0 else 'ERROR'
                LOG.debug(f'DONE({cmd_name}): error={err}')
                done_result: dict = {
                    'status': status,
                    'error_code': err,
                }
                if collect_pressure:
                    done_result['_pressure_samples'] = pressure
                if data_rsp:
                    data_rsp.update(done_result)
                    return data_rsp
                return done_result

            tag = 'ACK' if not got_ack else 'DONE'
            LOG.warning(
                f'{cmd_name} timed out waiting for {tag} '
                f'after {timeout_s:.1f}s',
            )
            return None
        finally:
            with self._pending_lock:
                self._pending_ack.pop(ack_key, None)
                self._pending_done.pop(done_key, None)
                if collect_pressure:
                    try:
                        self._pressure_collectors.remove(pressure)
                    except ValueError:
                        pass

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
                'accel_stream_start',
                'accel_stream_stop',
                'accel_reset',
                'fc_heater_get_status',
        ):
            return fp.pack_seq(seq)

        if cmd_name == 'move_z_axis':
            z_speed = float(cmd.get('speed', 0.0))
            if z_speed <= 0.0:
                z_speed = self._mot_z_mms
            return fp.pack_move_z_axis(
                seq=seq,
                position_mm=float(
                    cmd.get(
                        'position_mm',
                        cmd.get('position', 0),
                    ),
                ),
                speed=z_speed,
            )
        if cmd_name == 'move_gantry':
            xy_speed = float(cmd.get('speed', 0.0))
            if xy_speed <= 0.0:
                xy_speed = self._default_xy_mms()
            z_speed = float(cmd.get('z_speed', 0.0))
            if z_speed <= 0.0:
                z_speed = self._mot_z_mms
            return fp.pack_move_gantry(
                seq=seq,
                x_mm=cmd.get('x_mm'),
                y_mm=cmd.get('y_mm'),
                z_mm=cmd.get('z_mm'),
                speed=xy_speed,
                z_speed=z_speed,
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
            speed_01mms = int(cmd.get('speed_01mms', 0))
            if speed_01mms <= 0:
                default_xy_mms = self._default_xy_mms()
                if default_xy_mms > 0.0:
                    speed_01mms = int(round(default_xy_mms * 10.0))
            return fp.pack_move_to_location(
                seq=seq,
                location_id=int(
                    cmd.get('location_id', 0),
                ),
                speed_01mms=speed_01mms,
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
                x_eject_um=int(cmd.get('x_eject_um', 0)),
                pick_depth_um=int(
                    cmd.get('pick_depth_um', 0),
                ),
                retract_um=int(cmd.get('retract_um', 0)),
                xy_speed_01mms=int(
                    cmd.get('xy_speed_01mms', 250),
                ),
                z_speed_01mms=int(
                    cmd.get('z_speed_01mms', 60),
                ),
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
        if cmd_name in ('centrifuge_start', 'centrifuge_rock'):
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
                foil_detect=cmd.get(
                    'foil_detect', True,
                ),
                foil_pierce_um=int(
                    cmd.get('foil_pierce_um', 0),
                ),
                foil_pierce_speed_sps=int(
                    cmd.get('foil_pierce_speed_sps', 0),
                ),
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
            result['z_axis'] = d.get('z_axis', 0)
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
            foil_detect: bool = True,
            foil_pierce_um: int = 0,
            foil_pierce_speed_sps: int = 0,
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
            foil_detect: When True, firmware assumes foil
                is intact and always punctures + re-detects.
                Default True (safe for unaccessed wells).
            foil_pierce_um: Puncture stroke depth in µm (below
                foil-contact Z). 0 = firmware default.
            foil_pierce_speed_sps: Puncture Z speed in steps/s.
                0 = firmware default.
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
            'foil_detect': foil_detect,
            'foil_pierce_um': int(foil_pierce_um),
            'foil_pierce_speed_sps': int(foil_pierce_speed_sps),
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
            driver_online = r.get('driver_online', 0)
            if not driver_online:
                time.sleep(poll_interval_s)
                continue
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
            foil_detect: bool = True,
            foil_pierce_um: int = 0,
            foil_pierce_speed_sps: int = 0,
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
            foil_detect: When True, firmware assumes foil
                is intact and always punctures + re-detects.
                Default True (safe for unaccessed wells).
            foil_pierce_um: Puncture stroke depth in µm
                (below foil-contact Z).  0 = firmware default
                (FOIL_PIERCE_MM, typically 2.0 mm).
            foil_pierce_speed_sps: Puncture Z speed in steps/s.
                0 = firmware default (FOIL_PIERCE_SPEED_SPS,
                typically 2000 sps).
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
            foil_detect=foil_detect,
            foil_pierce_um=foil_pierce_um,
            foil_pierce_speed_sps=foil_pierce_speed_sps,
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
            pre_dispense_cb: object = None,
            skip_preamble: bool = False,
            skip_home_z: bool = False,
    ) -> dict | bool:
        '''Move to a location and firmware cart-dispense.

        Sequence: move_to_location -> (Z to cartridge_z) ->
        [pre_dispense_cb] -> cart_dispense (wait DONE) ->
        home Z.

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
            pre_dispense_cb: Optional callable invoked right
                before the pump command fires (after move + Z).
            skip_preamble: If True, skip move_to_location and
                the Z descent to cartridge_z. Use when the
                caller has already positioned the tip at the
                cartridge port (e.g. a previous cart_dispense
                that was issued with ``skip_home_z=True``).
            skip_home_z: If True, do not home the Z axis after
                the dispense completes. Use to chain multiple
                cart dispenses at the same port without the
                tip leaving the cartridge between legs.

        Returns:
            When stream is False: True on success.
            When stream is True: dict with 'ok' and
            '_pressure_samples' on success. False on failure.
        '''
        if not skip_preamble:
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
        else:
            LOG.info(
                'cart_dispense_at loc=%d: skip_preamble '
                '(assuming tip already at cartridge port)',
                loc_id,
            )

        if callable(pre_dispense_cb):
            pre_dispense_cb()

        dispense_time = (
            float(volume_ul) / max(vel_ul_s, 0.01)
        )
        long_timeout = max(
            timeout_s,
            dispense_time + float(sleep_s) + 60.0,
        )
        # When the caller is chaining another cart dispense at
        # this same port (skip_home_z=True), force the firmware
        # reasp-retract to 0.  Otherwise the tip ends 2 mm above
        # cart_z, and the next leg would dispense into air above
        # the port.
        effective_z_retract = 0 if skip_home_z else z_retract_mm
        r = self.send_command_wait_done(
            cmd={
                'cmd': 'cart_dispense',
                'volume': volume_ul,
                'vel': vel_ul_s,
                'reasp': reasp_ul,
                'sleep_s': sleep_s,
                'z_retract_mm': effective_z_retract,
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

        if not skip_home_z:
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
        else:
            LOG.info(
                'cart_dispense_at loc=%d: skip_home_z '
                '(holding Z at cartridge port for next leg)',
                loc_id,
            )
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
            pre_dispense_cb: object = None,
    ) -> dict | bool:
        '''Move to a location and firmware back-and-forth
        dispense for a fixed duration.

        Sequence: move_to_location -> (Z to cartridge_z) ->
        [pre_dispense_cb] -> cart_dispense_bf (wait DONE) ->
        home Z.

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
            pre_dispense_cb: Optional callable invoked right
                before the pump command fires (after move + Z).

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

        if callable(pre_dispense_cb):
            pre_dispense_cb()

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
