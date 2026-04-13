'''ultra.services.nfc_provisioning.nfc_service

NFC Type 4 Tag card emulation service using ST25R3918 via I2C.

Ported from scripts/test_nfc_emulate.py (proven working on hardware).
Runs as a background thread, serving an NDEF URL for phone taps and
optionally accepting WiFi credentials via custom APDU files.
'''
from __future__ import annotations

import json
import logging
import threading
import time
from typing import (
    Callable,
    Optional,
)

try:
    import smbus2
    I2C_AVAILABLE = True
except ImportError:
    smbus2 = None  # type: ignore[assignment]
    I2C_AVAILABLE = False


LOG = logging.getLogger(__name__)

# ── I2C mode byte prefixes ───────────────────────────────────
_MODE_REG_WRITE = 0x00
_MODE_REG_READ = 0x40
_MODE_FIFO_LOAD = 0x80
_MODE_FIFO_READ = 0x9F
_MODE_PT_A_LOAD = 0xA0

# ── Register addresses (ST25R3916 family) ─────────────────────
_REG_IO_CONF1 = 0x00
_REG_IO_CONF2 = 0x01
_REG_OP_CONTROL = 0x02
_REG_MODE = 0x03
_REG_BIT_RATE = 0x04
_REG_ISO14443A_NFC = 0x05
_REG_PASSIVE_TARGET = 0x08
_REG_AUX = 0x0A
_REG_TIMER_EMV_CONTROL = 0x12
_REG_IRQ_MASK_MAIN = 0x16
_REG_IRQ_MASK_TIMER = 0x17
_REG_IRQ_MASK_ERROR = 0x18
_REG_IRQ_MASK_TARGET = 0x19
_REG_IRQ_MAIN = 0x1A
_REG_IRQ_TIMER_NFC = 0x1B
_REG_IRQ_ERROR_WUP = 0x1C
_REG_IRQ_TARGET = 0x1D
_REG_FIFO_STATUS1 = 0x1E
_REG_PT_STATUS = 0x21
_REG_NUM_TX_BYTES1 = 0x22
_REG_NUM_TX_BYTES2 = 0x23
_REG_ANT_TUNE_A = 0x26
_REG_ANT_TUNE_B = 0x27
_REG_FIELD_THRESHOLD_ACTV = 0x2A
_REG_FIELD_THRESHOLD_DEACT = 0x2B
_REG_AUX_DISPLAY = 0x31
_REG_OVERSHOOT_CONF1 = 0x32
_REG_OVERSHOOT_CONF2 = 0x33
_REG_UNDERSHOOT_CONF1 = 0x34
_REG_UNDERSHOOT_CONF2 = 0x35
_REG_IC_IDENTITY = 0x3F

# ── Direct commands ───────────────────────────────────────────
_CMD_SET_DEFAULT = 0xC1
_CMD_STOP = 0xC2
_CMD_TRANSMIT_WITH_CRC = 0xC4
_CMD_GOTO_SENSE = 0xCD
_CMD_ADJUST_REGULATORS = 0xD6
_CMD_CLEAR_FIFO = 0xDB
_CMD_SPACE_B_ACCESS = 0xFB

# ── IRQ bits ──────────────────────────────────────────────────
_IRQ_M_OSC = 0x80
_IRQ_M_RXE = 0x10
_IRQ_M_TXE = 0x08
_IRQ_T_DCT = 0x80
_IRQ_T_EON = 0x10
_IRQ_T_EOF = 0x08
_IRQ_TGT_WU_A = 0x01
_IRQ_TGT_WU_A_X = 0x02
_IRQ_TGT_RXE_PTA = 0x10

# ── OP_CONTROL bits ───────────────────────────────────────────
_OP_EN = 0x80
_OP_RX_EN = 0x40
_OP_EFD_AUTO = 0x03

# ── MODE register ─────────────────────────────────────────────
_MODE_TARG_NFCA = 0xC8

# ── Card emulation identity ──────────────────────────────────
_EMUL_NFCID = [0x08, 0xDE, 0xAD, 0xBE]
_EMUL_ATQA = [0x02, 0x00]
_EMUL_SAK = 0x20
_EMUL_ATS = [0x05, 0x78, 0x80, 0xA0, 0x00]

# ── NDEF application ─────────────────────────────────────────
_NDEF_AID = [0xD2, 0x76, 0x00, 0x00, 0x85, 0x01, 0x01]

_CC_FILE = bytes([
    0x00, 0x0F,
    0x20,
    0x00, 0x3B,
    0x00, 0x34,
    0x04, 0x06,
    0xE1, 0x04,
    0x00, 0x80,
    0x00,
    0xFF,
])

# File IDs for Type 4 Tag emulation
_FID_CC = 0xE103
_FID_NDEF = 0xE104
_FID_WIFI_CREDS = 0xE105
_FID_WIFI_STATUS = 0xE106
_FID_WIFI_NETWORKS = 0xE107

# WiFi status constants
STATUS_IDLE = 0
STATUS_CONNECTING = 1
STATUS_CONNECTED = 2
STATUS_FAILED = 3


def _build_ndef_url(device_sn: str) -> bytes:
    '''Build NDEF URI record: https://siphox.com/setup?sn={device_sn}

    Returns the full NDEF file contents (2-byte length + NDEF message).
    '''
    uri_body = f'siphox.com/setup?sn={device_sn}'.encode()
    payload = b'\x04' + uri_body  # 0x04 = https:// prefix
    ndef_msg = bytes([
        0xD1, 0x01, len(payload), 0x55,
    ]) + payload
    return len(ndef_msg).to_bytes(2, 'big') + ndef_msg


class NFCService:
    '''ST25R3918-based NFC card emulation service over I2C.

    Runs a card emulation loop in a background thread. Phones can
    tap to read the NDEF URL. During WiFi provisioning, also accepts
    WiFi credentials via UPDATE BINARY to file 0xE105.
    '''

    def __init__(
            self,
            device_sn: str,
            on_wifi_credentials: Callable[[str, str], None] | None = None,
            on_tap: Callable[[], None] | None = None,
            i2c_bus: int = 2,
            i2c_addr: int = 0x50,
    ) -> None:
        '''Create an NFC card emulation service.

        Args:
            device_sn: Serial number embedded in the NDEF URL.
            on_wifi_credentials: Called with (ssid, password) when the
                phone app writes WiFi credentials via UPDATE BINARY.
            on_tap: Called (no arguments) each time a phone connects
                to the NFC tag, before any APDU exchange. Runs on the
                NFC background thread -- must be non-blocking.
            i2c_bus: Linux I2C bus number.
            i2c_addr: I2C address of the ST25R3918.
        '''
        self._device_sn = device_sn
        self._on_wifi_credentials = on_wifi_credentials
        self._on_tap = on_tap
        self._i2c_bus = i2c_bus
        self._i2c_addr = i2c_addr
        self._bus: Optional[smbus2.SMBus] = None  # type: ignore[union-attr]
        self._running = False
        self._wifi_status = STATUS_IDLE
        self._wifi_networks: list[str] = []
        self._thread: Optional[threading.Thread] = None
        self._ndef_file = _build_ndef_url(device_sn)
        self._provisioning_mode = on_wifi_credentials is not None

    # ── I2C transport ─────────────────────────────────────────

    def _read_reg(self, reg: int) -> int:
        assert self._bus is not None
        mode_byte = _MODE_REG_READ | (reg & 0x3F)
        w = smbus2.i2c_msg.write(self._i2c_addr, [mode_byte])
        r = smbus2.i2c_msg.read(self._i2c_addr, 1)
        self._bus.i2c_rdwr(w, r)
        return list(r)[0]

    def _write_reg(self, reg: int, val: int) -> None:
        assert self._bus is not None
        mode_byte = _MODE_REG_WRITE | (reg & 0x3F)
        self._bus.write_byte_data(
            self._i2c_addr, mode_byte, val & 0xFF,
        )

    def _direct_cmd(self, cmd: int) -> None:
        assert self._bus is not None
        self._bus.write_byte(self._i2c_addr, cmd)

    def _read_fifo(self, n: int) -> list[int]:
        assert self._bus is not None
        w = smbus2.i2c_msg.write(self._i2c_addr, [_MODE_FIFO_READ])
        r = smbus2.i2c_msg.read(self._i2c_addr, n)
        self._bus.i2c_rdwr(w, r)
        return list(r)

    def _write_fifo(self, data: list[int] | bytes) -> None:
        assert self._bus is not None
        w = smbus2.i2c_msg.write(
            self._i2c_addr, [_MODE_FIFO_LOAD] + list(data),
        )
        self._bus.i2c_rdwr(w)

    def _clear_all_irqs(self) -> None:
        self._read_reg(_REG_IRQ_MAIN)
        self._read_reg(_REG_IRQ_TIMER_NFC)
        self._read_reg(_REG_IRQ_ERROR_WUP)
        self._read_reg(_REG_IRQ_TARGET)

    def _wait_irq(
            self,
            mask: int,
            reg: int = _REG_IRQ_MAIN,
            timeout_ms: int = 50,
    ) -> int:
        deadline = time.monotonic() + timeout_ms / 1000.0
        acc = 0
        while time.monotonic() < deadline:
            val = self._read_reg(reg)
            acc |= val
            if acc & mask:
                return acc
            time.sleep(0.0003)
        return acc

    def _write_ptmem_a(
            self,
            nfcid: list[int],
            atqa: list[int],
            sak: int,
    ) -> None:
        ptmem = list(nfcid) + [0x00] * (10 - len(nfcid))
        ptmem += list(atqa)
        ptmem += [sak, sak, sak]
        assert self._bus is not None
        w = smbus2.i2c_msg.write(
            self._i2c_addr, [_MODE_PT_A_LOAD] + ptmem,
        )
        self._bus.i2c_rdwr(w)

    def _target_tx(self, data: list[int]) -> None:
        self._direct_cmd(_CMD_CLEAR_FIFO)
        self._clear_all_irqs()
        nbits = len(data) * 8
        self._write_reg(_REG_NUM_TX_BYTES2, nbits & 0xFF)
        self._write_reg(_REG_NUM_TX_BYTES1, (nbits >> 8) & 0xFF)
        self._write_fifo(data)
        self._direct_cmd(_CMD_TRANSMIT_WITH_CRC)
        deadline = time.monotonic() + 0.05
        while time.monotonic() < deadline:
            if self._read_reg(_REG_IRQ_MAIN) & _IRQ_M_TXE:
                break
            time.sleep(0.0002)

    def _target_rx(self, timeout_ms: int = 3000) -> list[int] | None:
        self._clear_all_irqs()
        deadline = time.monotonic() + timeout_ms / 1000.0
        while time.monotonic() < deadline:
            m = self._read_reg(_REG_IRQ_MAIN)
            if m & _IRQ_M_RXE:
                n = self._read_reg(_REG_FIFO_STATUS1)
                return self._read_fifo(n) if n > 0 else None
            time.sleep(0.0005)
        return None

    # ── Chip initialization ───────────────────────────────────

    def _init_chip(self) -> None:
        self._direct_cmd(_CMD_SET_DEFAULT)
        time.sleep(0.02)

        self._write_reg(_REG_OP_CONTROL, _OP_EN | _OP_EFD_AUTO)
        time.sleep(0.01)
        self._clear_all_irqs()
        self._wait_irq(_IRQ_M_OSC, _REG_IRQ_MAIN, timeout_ms=200)
        LOG.debug('NFC oscillator stable')

        self._clear_all_irqs()
        self._direct_cmd(_CMD_ADJUST_REGULATORS)
        self._wait_irq(
            _IRQ_T_DCT, _REG_IRQ_TIMER_NFC, timeout_ms=100,
        )
        time.sleep(0.005)

        # 3.3V supply
        io2 = self._read_reg(_REG_IO_CONF2)
        self._write_reg(_REG_IO_CONF2, io2 | 0x80)

        # Field thresholds
        self._write_reg(_REG_FIELD_THRESHOLD_ACTV, 0x13)
        self._write_reg(_REG_FIELD_THRESHOLD_DEACT, 0x02)

        # Disable overshoot/undershoot protection
        self._write_reg(_REG_OVERSHOOT_CONF1, 0x00)
        self._write_reg(_REG_OVERSHOOT_CONF2, 0x00)
        self._write_reg(_REG_UNDERSHOOT_CONF1, 0x00)
        self._write_reg(_REG_UNDERSHOOT_CONF2, 0x00)

        # Disable AWS regulator (Space B)
        self._direct_cmd(_CMD_SPACE_B_ACCESS)
        aux_b = self._read_reg(0x28)
        self._write_reg(0x28, aux_b & ~0x04)
        self._direct_cmd(_CMD_SPACE_B_ACCESS)

        # Clear parity/NFCIP1
        self._write_reg(_REG_ISO14443A_NFC, 0x00)

        # Timer config
        tmr = self._read_reg(_REG_TIMER_EMV_CONTROL)
        tmr = (tmr & ~0x60) | 0x10
        self._write_reg(_REG_TIMER_EMV_CONTROL, tmr)

        # NFCID length = 4 bytes
        aux = self._read_reg(_REG_AUX)
        self._write_reg(_REG_AUX, aux & ~0x30)

        # Load PT Memory
        self._write_ptmem_a(_EMUL_NFCID, _EMUL_ATQA, _EMUL_SAK)

        # Enable NFC-A auto-response
        self._write_reg(_REG_PASSIVE_TARGET, 0x0C)

        # Enable all interrupts
        self._write_reg(_REG_IRQ_MASK_MAIN, 0x00)
        self._write_reg(_REG_IRQ_MASK_TIMER, 0x00)
        self._write_reg(_REG_IRQ_MASK_ERROR, 0x00)
        self._write_reg(_REG_IRQ_MASK_TARGET, 0x00)

        LOG.info('ST25R3918 card emulation initialized (I2C)')

    # ── APDU processing ───────────────────────────────────────

    def _process_apdu(
            self,
            apdu: list[int],
            selected_file: str | None,
    ) -> tuple[list[int], str | None]:
        if len(apdu) < 4:
            return ([0x6F, 0x00], selected_file)

        ins, p1, p2 = apdu[1], apdu[2], apdu[3]
        lc = apdu[4] if len(apdu) > 4 else 0
        data = apdu[5:5 + lc]

        # SELECT
        if ins == 0xA4:
            if p1 == 0x04 and data == _NDEF_AID:
                LOG.debug('APDU: SELECT NDEF App')
                return ([0x90, 0x00], selected_file)
            if p1 == 0x00 and len(data) >= 2:
                fid = (data[0] << 8) | data[1]
                if fid == _FID_CC:
                    LOG.debug('APDU: SELECT CC')
                    return ([0x90, 0x00], 'cc')
                if fid == _FID_NDEF:
                    LOG.debug('APDU: SELECT NDEF')
                    return ([0x90, 0x00], 'ndef')
                if fid == _FID_WIFI_STATUS:
                    LOG.debug('APDU: SELECT WiFi Status')
                    return ([0x90, 0x00], 'wifi_status')
                if fid == _FID_WIFI_NETWORKS:
                    LOG.debug('APDU: SELECT WiFi Networks')
                    return ([0x90, 0x00], 'wifi_networks')
                if fid == _FID_WIFI_CREDS:
                    LOG.debug('APDU: SELECT WiFi Creds')
                    return ([0x90, 0x00], 'wifi_creds')
            return ([0x6A, 0x82], selected_file)

        # READ BINARY
        if ins == 0xB0:
            offset = (p1 << 8) | p2
            le = apdu[4] if len(apdu) > 4 else 0
            src = self._get_file_contents(selected_file)
            if src is not None:
                chunk = list(src[offset:offset + le])
                LOG.debug(
                    f'APDU: READ {selected_file} '
                    f'[{offset}:{offset+le}] -> {len(chunk)}B',
                )
                return (chunk + [0x90, 0x00], selected_file)
            return ([0x6A, 0x82], selected_file)

        # UPDATE BINARY (WiFi credentials)
        if ins == 0xD6:
            if selected_file == 'wifi_creds' and lc > 0:
                self._handle_wifi_write(bytes(data))
                return ([0x90, 0x00], selected_file)
            return ([0x69, 0x82], selected_file)

        return ([0x6D, 0x00], selected_file)

    def _get_file_contents(
            self, selected_file: str | None,
    ) -> bytes | None:
        if selected_file == 'cc':
            return _CC_FILE
        if selected_file == 'ndef':
            return self._ndef_file
        if selected_file == 'wifi_status':
            return bytes([self._wifi_status])
        if selected_file == 'wifi_networks':
            return json.dumps(self._wifi_networks).encode()[:255]
        return None

    def _handle_wifi_write(self, data: bytes) -> None:
        try:
            obj = json.loads(data.decode('utf-8'))
            ssid = obj.get('s') or obj.get('ssid', '')
            password = obj.get('p') or obj.get('password', '')
            if ssid and self._on_wifi_credentials:
                LOG.info('WiFi credentials received via NFC')
                self._on_wifi_credentials(ssid, password)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            LOG.warning(f'Invalid WiFi credential payload: {e}')

    # ── Card emulation loop ───────────────────────────────────

    def _emulation_loop(self) -> None:
        while self._running:
            try:
                self._emulate_one_session()
            except Exception:
                if self._running:
                    LOG.exception('NFC emulation error')
                    time.sleep(0.5)

    def _emulate_one_session(self) -> None:
        # Prepare listen mode
        self._write_reg(
            _REG_OP_CONTROL, _OP_EN | _OP_RX_EN | _OP_EFD_AUTO,
        )
        self._direct_cmd(_CMD_STOP)
        self._clear_all_irqs()
        self._direct_cmd(_CMD_GOTO_SENSE)
        self._write_reg(_REG_MODE, _MODE_TARG_NFCA)

        # Check if field already present
        aux_disp = self._read_reg(_REG_AUX_DISPLAY)
        efd = (aux_disp >> 6) & 1

        if not efd:
            # Wait for external field or selection
            while self._running:
                irq_t = self._read_reg(_REG_IRQ_TIMER_NFC)
                irq_tgt = self._read_reg(_REG_IRQ_TARGET)
                if irq_t & _IRQ_T_EON:
                    break
                if irq_tgt & (_IRQ_TGT_WU_A | _IRQ_TGT_WU_A_X):
                    break
                time.sleep(0.005)

            if not self._running:
                return

            LOG.debug('NFC field detected')
            self._clear_all_irqs()
            self._direct_cmd(_CMD_GOTO_SENSE)
            self._write_reg(_REG_MODE, _MODE_TARG_NFCA)

        # Wait for NFC-A selection (hardware anticollision)
        selected = False
        t0 = time.monotonic()
        while self._running and time.monotonic() - t0 < 10:
            irq_tgt = self._read_reg(_REG_IRQ_TARGET)
            if irq_tgt & (
                _IRQ_TGT_WU_A | _IRQ_TGT_WU_A_X |
                _IRQ_TGT_RXE_PTA
            ):
                pt = self._read_reg(_REG_PT_STATUS) & 0x0F
                if irq_tgt & (
                    _IRQ_TGT_WU_A | _IRQ_TGT_WU_A_X
                ) or pt >= 0x05:
                    selected = True
                    break
            if self._read_reg(_REG_IRQ_TIMER_NFC) & _IRQ_T_EOF:
                LOG.debug('NFC field lost')
                return

        if not selected:
            return

        LOG.debug('Phone connected via NFC')
        if self._on_tap is not None:
            try:
                self._on_tap()
            except Exception as err:
                LOG.warning(f'on_tap callback error: {err}')

        # Handle RATS -> ATS
        rx = self._target_rx(timeout_ms=2000)
        if rx is None:
            return

        if len(rx) >= 2 and rx[0] == 0xE0:
            self._target_tx(_EMUL_ATS)
        elif rx[0] & 0xF0 == 0xD0:
            self._target_tx([rx[0]])
        else:
            return

        # ISO-DEP APDU exchange
        selected_file: str | None = None
        while self._running:
            rx = self._target_rx(timeout_ms=5000)
            if rx is None or len(rx) < 2:
                break

            pcb = rx[0]

            # S-block (DESELECT)
            if (pcb & 0xC7) == 0xC2:
                LOG.debug('Phone disconnected')
                self._target_tx([pcb])
                break

            # R-block (ACK/NAK)
            if (pcb & 0xE0) == 0xA0:
                self._target_tx([pcb & 0x03 | 0xA2])
                continue

            # I-block (APDU)
            if (pcb & 0xE2) == 0x02:
                apdu = rx[1:]
                rapdu, selected_file = self._process_apdu(
                    apdu, selected_file,
                )
                resp_pcb = (pcb & 0x03) | 0x02
                self._target_tx([resp_pcb] + rapdu)
                continue

        LOG.debug('NFC session ended, listening again')

    # ── Public API ────────────────────────────────────────────

    def start(self) -> None:
        '''Start NFC emulation (blocking).'''
        if not I2C_AVAILABLE:
            LOG.error('smbus2 not available, cannot start NFC')
            return
        self._open_bus()
        self._init_chip()
        self._running = True
        self._emulation_loop()

    def start_async(self) -> None:
        '''Start NFC emulation in background thread.'''
        if not I2C_AVAILABLE:
            LOG.error('smbus2 not available, cannot start NFC')
            return
        self._open_bus()
        self._init_chip()
        self._running = True
        self._thread = threading.Thread(
            target=self._emulation_loop,
            name='NFCEmulationThread',
            daemon=True,
        )
        self._thread.start()
        LOG.info('NFC emulation started (I2C async)')

    def stop(self) -> None:
        '''Stop NFC emulation and release I2C bus.'''
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self._bus:
            try:
                self._write_reg(_REG_OP_CONTROL, 0x00)
            except Exception:
                pass
            try:
                self._bus.close()
            except Exception:
                pass
            self._bus = None
        LOG.info('NFC emulation stopped')

    def set_wifi_status(self, status: int) -> None:
        '''Set WiFi status readable via file 0xE106.'''
        self._wifi_status = status

    def set_wifi_networks(self, networks: list[str]) -> None:
        '''Set available WiFi networks readable via file 0xE107.'''
        self._wifi_networks = networks

    def update_device_sn(self, device_sn: str) -> None:
        '''Update the NDEF URL with a new serial number.'''
        self._device_sn = device_sn
        self._ndef_file = _build_ndef_url(device_sn)

    def set_provisioning_mode(
            self,
            enabled: bool,
            on_wifi_credentials: Callable[[str, str], None] | None = None,
    ) -> None:
        '''Enable/disable WiFi provisioning mode.'''
        self._provisioning_mode = enabled
        if on_wifi_credentials is not None:
            self._on_wifi_credentials = on_wifi_credentials

    def _open_bus(self) -> None:
        if self._bus is not None:
            return
        self._bus = smbus2.SMBus(self._i2c_bus)
        # Verify chip identity
        ic_id = self._read_reg(_REG_IC_IDENTITY)
        ic_type = (ic_id >> 3) & 0x1F
        if ic_type != 0x05:
            self._bus.close()
            self._bus = None
            raise RuntimeError(
                f'Wrong NFC chip (IC=0x{ic_id:02X}, '
                f'type={ic_type}, expected 5)',
            )
        LOG.info(f'ST25R3918 detected (IC=0x{ic_id:02X})')
