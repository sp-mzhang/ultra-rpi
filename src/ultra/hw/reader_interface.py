'''ultra.hw.reader_interface -- PProc reader MCU interface.

USB serial interface to the PProc optical reader MCU. Sends
ASCII commands over virtual UART and receives binary TLV
data streams. Ported and simplified from sway's
pprocinterface.py (no sway dependencies).

The PProc MCU uses a simple ASCII command protocol:
  - Commands: "id", "start <seconds>", "stop",
    "resume <chunk_id>", "config <id> [value]"
  - ACK format: "ACK_<cmd_id>" (7 bytes)
  - TLV stream: binary, starts after "start" command
'''
from __future__ import annotations

import logging
import re
import struct
import time
from typing import Optional

import serial
import serial.tools.list_ports

LOG = logging.getLogger(__name__)

DEFAULT_BAUD = 1_000_000
ACK_REGEX = re.compile(r'(?<=ACK_)\d{1,2}')
TLV_HEADER_SIZE = 4
TLV_TYPE_CHUNK = 4
TLV_TYPE_DATA_LOSS = 5

CMD_ID = 1
CMD_START = 2
CMD_STOP = 3
CMD_RESUME = 4
CMD_CONFIG_READ = 5
CMD_CONFIG_WRITE = 6


def find_pproc_port() -> str | None:
    '''Auto-detect PProc reader USB serial port.

    Opens each available serial port and sends an "id"
    command. The PProc responds with a 7-byte ACK
    ("ACK_01") followed by the board ID string
    ("siphox-pproc-X.Y.Z"). We read and discard the
    ACK, then check the board ID.

    Returns:
        Port device path (e.g. /dev/ttyACM0) or None.
    '''
    ports = serial.tools.list_ports.comports()
    LOG.info(
        'Scanning %d serial ports for PProc reader: %s',
        len(ports),
        [p.device for p in ports],
    )
    for p in ports:
        try:
            with serial.Serial(
                port=p.device,
                baudrate=DEFAULT_BAUD,
                timeout=0.5,
            ) as ser:
                ser.reset_input_buffer()
                ser.write(b'stop\r')
                time.sleep(0.1)
                ser.reset_input_buffer()
                ser.write(b'id\r')
                time.sleep(0.2)
                raw = ser.read(64)
                text = raw.decode(
                    'utf-8', errors='ignore',
                )
                if 'siphox-pproc' in text:
                    idx = text.index('siphox-pproc')
                    board_id = text[idx:].strip()
                    LOG.info(
                        'Found PProc on %s: %s',
                        p.device, board_id,
                    )
                    return p.device
                LOG.debug(
                    'Port %s not PProc (got %r)',
                    p.device,
                    text[:40],
                )
        except (
            serial.SerialException,
            OSError,
        ) as exc:
            LOG.debug(
                'Port %s probe failed: %s',
                p.device, exc,
            )
            continue
    return None


class ReaderInterface:
    '''USB serial interface to the PProc reader MCU.

    Provides methods for identification, configuration,
    and TLV data stream control.

    Attributes:
        _port: Serial port path.
        _baud: Baud rate.
        _ser: pyserial Serial instance or None.
        _board_id: Board identification string.
    '''

    def __init__(
            self,
            port: str = 'auto',
            baud: int = DEFAULT_BAUD,
    ) -> None:
        '''Initialize the reader interface.

        Args:
            port: Serial port path or "auto" for
                auto-detection.
            baud: Baud rate (default 1M).
        '''
        self._port = port
        self._baud = baud
        self._ser: Optional[serial.Serial] = None
        self._board_id: str = ''

    def connect(self) -> bool:
        '''Open the serial port and identify the reader.

        If port is "auto", attempts auto-detection.

        Returns:
            True if connected and identified.
        '''
        port = self._port
        if port == 'auto':
            port = find_pproc_port()
            if port is None:
                LOG.error(
                    'PProc reader not found '
                    '(auto-detect failed)',
                )
                return False
            self._port = port

        try:
            self._ser = serial.Serial(
                port=port,
                baudrate=self._baud,
                timeout=0.1,
                write_timeout=0,
            )
            self._ser.reset_input_buffer()
        except serial.SerialException as err:
            LOG.error(
                f'Reader serial open failed: {err}',
            )
            return False

        self.stop_stream()
        time.sleep(0.1)

        self._board_id = self.get_id()
        if not self._board_id:
            LOG.error('Reader did not respond to id')
            self.disconnect()
            return False

        LOG.info(
            f'Reader connected: {port} '
            f'id={self._board_id}',
        )
        return True

    def disconnect(self) -> None:
        '''Close the serial port.'''
        if self._ser:
            try:
                self.stop_stream()
            except Exception:
                pass
            self._ser.close()
            self._ser = None
            LOG.info('Reader disconnected')

    @property
    def board_id(self) -> str:
        '''Reader board identification string.'''
        return self._board_id

    def _send_cmd(self, cmd_str: str) -> None:
        '''Send an ASCII command to the reader.

        Args:
            cmd_str: Command string (without terminator).
        '''
        if not self._ser:
            return
        data = f'{cmd_str}\r'.encode('utf-8')
        self._ser.write(data)
        self._ser.flush()

    def _read_ack(
            self,
            expected_cmd: int,
            byte_count: int = 7,
    ) -> bool:
        '''Read and validate an ACK response.

        Args:
            expected_cmd: Expected command ID integer.
            byte_count: Bytes to read for ACK.

        Returns:
            True if ACK matches expected command.
        '''
        if not self._ser:
            return False
        raw = self._ser.read(byte_count)
        text = raw.decode('utf-8', errors='ignore')
        match = ACK_REGEX.search(text)
        if match:
            ack_id = int(match.group())
            return ack_id == expected_cmd
        return False

    def get_id(self) -> str:
        '''Query the reader board ID.

        The PProc responds with a 7-byte ACK ("ACK_01")
        followed by the board ID string. We read both,
        skip past the ACK, and extract the ID.

        Returns:
            Board ID string or empty on failure.
        '''
        if not self._ser:
            return ''
        self._ser.reset_input_buffer()
        self._send_cmd('id')
        time.sleep(0.2)
        raw = self._ser.read(64)
        text = raw.decode(
            'utf-8', errors='ignore',
        )
        if 'siphox-pproc' in text:
            idx = text.index('siphox-pproc')
            return text[idx:].strip()
        return text.strip()

    def start_stream(
            self, acq_seconds: int = 3,
    ) -> bool:
        '''Start the TLV data stream.

        Sends "start <seconds>" command. After ACK,
        binary TLV data will begin arriving on the
        serial port.

        Args:
            acq_seconds: Acquisition duration in seconds.

        Returns:
            True if ACK received.
        '''
        if not self._ser:
            return False
        self._ser.reset_input_buffer()
        self._send_cmd(f'start {acq_seconds}')
        time.sleep(0.05)
        return self._read_ack(CMD_START)

    def stop_stream(self) -> bool:
        '''Stop the TLV data stream.

        Returns:
            True if ACK received.
        '''
        if not self._ser:
            return False
        self._ser.reset_input_buffer()
        self._send_cmd('stop')
        time.sleep(0.05)
        return self._read_ack(
            CMD_STOP, byte_count=8500,
        )

    def resume_stream(
            self, chunk_id: int,
    ) -> bool:
        '''Resume stream from a specific chunk ID.

        Args:
            chunk_id: Chunk ID to resume from.

        Returns:
            True if ACK received.
        '''
        if not self._ser:
            return False
        self._ser.reset_input_buffer()
        self._send_cmd(f'resume {chunk_id}')
        time.sleep(0.05)
        return self._read_ack(CMD_RESUME)

    def read_bytes(
            self, max_bytes: int = 65536,
    ) -> bytes:
        '''Read available bytes from the serial port.

        Non-blocking -- returns whatever is available
        up to max_bytes.

        Args:
            max_bytes: Maximum bytes to read.

        Returns:
            Raw bytes from the serial port.
        '''
        if not self._ser:
            return b''
        n = min(self._ser.in_waiting, max_bytes)
        if n <= 0:
            return b''
        return self._ser.read(n)

    def read_config(
            self, param_id: int,
    ) -> str:
        '''Read a configuration parameter.

        Args:
            param_id: Parameter ID integer.

        Returns:
            Response string from the reader.
        '''
        if not self._ser:
            return ''
        self._ser.reset_input_buffer()
        self._send_cmd(f'config {param_id}\r\n')
        time.sleep(0.1)
        raw = self._ser.read(100)
        return raw.decode(
            'utf-8', errors='ignore',
        ).strip()

    def write_config(
            self,
            param_id: int,
            value: str | int | float,
    ) -> bool:
        '''Write a configuration parameter.

        Args:
            param_id: Parameter ID integer.
            value: Value to write.

        Returns:
            True if response contains "success".
        '''
        if not self._ser:
            return False
        self._ser.reset_input_buffer()
        self._send_cmd(
            f'config {param_id} {value}\r\n',
        )
        time.sleep(0.1)
        raw = self._ser.read(100)
        resp = raw.decode(
            'utf-8', errors='ignore',
        ).strip()
        return 'success' in resp.lower()
