#!/usr/bin/env python3
'''Binary frame protocol for RPi to STM32 communication.

Implements the SOH-framed binary protocol matching the firmware
usart_frame.h/c. Frame format:

  [SOH][CMD_LSB][CMD_MSB][LEN][DATA...][CRC_LSB][CRC_MSB]

All multi-byte fields are little-endian.
CRC-16/CCITT-FALSE over Command + Length + Data.
'''

import struct

# =============================================================================
# Frame constants
# =============================================================================

SOH = 0x01
FRAME_OVERHEAD = 6
MAX_DATA_LEN = 255

# =============================================================================
# Command IDs (mirrors usart_frame.h)
# =============================================================================

# System (0x80xx)
CMD_PING            = 0x8001
CMD_GET_STATUS      = 0x8002
CMD_GET_VERSION     = 0x8003
CMD_GET_FLAGS       = 0x8004
CMD_GET_POSITION    = 0x8005
CMD_GET_SENSORS     = 0x8006
CMD_RESET           = 0x8007
CMD_SET_LOG_LEVEL   = 0x8008
CMD_ENTER_BOOTLOADER = 0x8009

# State control (0x81xx)
CMD_SET_STATE       = 0x8101
CMD_SET_CTRL_MODE   = 0x8102
CMD_START_PROTOCOL  = 0x8103
CMD_ABORT           = 0x8104
CMD_PAUSE           = 0x8105
CMD_RESUME          = 0x8106

# Motion (0x82xx)
CMD_HOME_ALL        = 0x8201
CMD_HOME_GANTRY     = 0x8202
CMD_HOME_X_AXIS     = 0x8203
CMD_HOME_Z_AXIS     = 0x8204
CMD_HOME_Y_AXIS     = 0x8208
CMD_GET_GANTRY_STATUS = 0x8205
CMD_Z_TEST_GPIO     = 0x8206
CMD_MOVE_GANTRY     = 0x8207
CMD_MOVE_Z_AXIS     = 0x8209
CMD_MOVE_TO_WELL        = 0x820A
CMD_TH_POWER_EN         = 0x820B
CMD_MOVE_TO_LOCATION    = 0x820C
CMD_SET_LOC_CENTRE      = 0x820D
CMD_SET_LOC_OFFSET      = 0x820E
CMD_TIP_SWAP            = 0x820F
CMD_LID_MOVE            = 0x8210
CMD_READ_Z_DRV          = 0x8211
CMD_GET_MOTOR_STATUS    = 0x8212
CMD_SET_MOTOR_TELEM     = 0x8213

# Lift stepper (0x8Bxx)
CMD_LIFT_HOME       = 0x8B01
CMD_LIFT_STOP       = 0x8B02
CMD_LIFT_MOVE       = 0x8B03
CMD_LIFT_STATUS     = 0x8B04
CMD_LIFT_MOVE_TOP   = 0x8B05

# Pump basic (0x83xx)
CMD_PUMP_ASPIRATE   = 0x8301
CMD_PUMP_DISPENSE   = 0x8302
CMD_PUMP_PRIME      = 0x8303
CMD_PUMP_BLOWOUT    = 0x8304

# Pump advanced (0x84xx)
CMD_PUMP_INIT       = 0x8401
CMD_PUMP_SET_RES    = 0x8402
CMD_PUMP_SET_GAIN   = 0x8403
CMD_PUMP_MOVE_ABS   = 0x8404
CMD_PUMP_EN_STREAM  = 0x8405
CMD_PUMP_WAIT_IDLE  = 0x8406
CMD_PUMP_GET_STATUS = 0x8407
CMD_PUMP_RAW        = 0x8408
CMD_PUMP_TEST_MOVE  = 0x8409
CMD_PUMP_LLD_START  = 0x840A
CMD_PUMP_LLD_STOP   = 0x840B
CMD_PUMP_PISTON_RST = 0x840C
CMD_PUMP_STREAM_TST = 0x840D

# Centrifuge (0x85xx)
CMD_CFUGE_START      = 0x8501
CMD_CFUGE_STOP       = 0x8502
CMD_CFUGE_ROCK       = 0x8503
CMD_CFUGE_STATUS     = 0x8504
CMD_CFUGE_MOVE_ANGLE = 0x8505
CMD_CFUGE_HOME       = 0x8506
CMD_CFUGE_BLDC_CMD   = 0x8507
CMD_CFUGE_POWER      = 0x8508
CMD_CFUGE_UNLOCK     = 0x8509
CMD_CFUGE_LOCK       = 0x850A
CMD_CFUGE_REVERSE    = 0x850B
CMD_CFUGE_GOTO_SERUM   = 0x850C
CMD_CFUGE_GOTO_PIPETTE = 0x850D
CMD_CFUGE_GOTO_BLISTER = 0x850E

# BLDC driver command IDs (for use with CMD_CFUGE_BLDC_CMD)
BLDC_GET_STATE         = 0x0001
BLDC_GET_ENCODER_ALIGN = 0x0002
BLDC_GET_VBUS          = 0x0003
BLDC_GET_TEMP          = 0x0004
BLDC_GET_ERROR         = 0x0005
BLDC_CLEAR_ERROR       = 0x0006
BLDC_GET_RPM           = 0x0007
BLDC_GET_ANGLE         = 0x0008
BLDC_GET_TARGET_RPM    = 0x0009
BLDC_SET_TARGET_RPM    = 0x000A
BLDC_GET_ALIGN_ANGLE   = 0x000B
BLDC_SET_ALIGN_ANGLE   = 0x000C
BLDC_GET_AUTO_ALIGN    = 0x000D
BLDC_SET_AUTO_ALIGN    = 0x000E
BLDC_START_MOTOR       = 0x0010
BLDC_STOP_MOTOR        = 0x0011
BLDC_START_ALIGNMENT   = 0x0012
BLDC_ENCODER_ALIGN     = 0x0013
BLDC_GET_POS_HOLD      = 0x0014
BLDC_SET_POS_HOLD      = 0x0015

# V1.01/V1.02 position-hold tuning commands
BLDC_GET_POS_DEV_THRESH    = 0x0016  # uint32, 0.01 deg
BLDC_SET_POS_DEV_THRESH    = 0x0017
BLDC_GET_POS_CTRL_MAX_TIME = 0x0018  # uint32, ms
BLDC_SET_POS_CTRL_MAX_TIME = 0x0019
BLDC_GET_SOFT_CURR_LIMIT   = 0x001A  # uint16, 0.1 A
BLDC_SET_SOFT_CURR_LIMIT   = 0x001B
BLDC_GET_STOP_POS_THRESH   = 0x001C  # uint32, 0.01 deg
BLDC_SET_STOP_POS_THRESH   = 0x001D
BLDC_GET_STOP_DETECT_TIME  = 0x001E  # uint32, ms
BLDC_SET_STOP_DETECT_TIME  = 0x001F

BLDC_GET_MAX_CURRENT       = 0x0050  # uint16, 0.1 A (V1.03)
BLDC_SET_MAX_CURRENT       = 0x0051

BLDC_NOTIFY_ERROR      = 0x2040  # async unsolicited error from controller

BLDC_GET_SPEED_PID     = 0x0020
BLDC_SET_SPEED_PID     = 0x0021
BLDC_GET_POS_PID       = 0x0022
BLDC_SET_POS_PID       = 0x0023
BLDC_GET_TRIGGER_INFO  = 0x0030
BLDC_SET_TRIGGER_EN    = 0x0031
BLDC_SET_TRIGGER_POL   = 0x0032
BLDC_SET_TRIGGER_WIDTH = 0x0033
BLDC_SET_TRIGGER_POS   = 0x0034

# Door (0x86xx)
CMD_DOOR_OPEN       = 0x8601
CMD_DOOR_CLOSE      = 0x8602
CMD_DOOR_STATUS     = 0x8603

# Tip (0x87xx)
# TIP_PICKUP (0x8701) and TIP_EJECT (0x8702) removed — collided with
# FAN_SET_DUTY / FAN_GET_STATUS.  Use gantry_tip_swap (0x820F) instead.

# Configuration (0x88xx)
CMD_SET_SPEED       = 0x8801
CMD_SET_ACCEL       = 0x8802
CMD_CALIBRATE       = 0x8803

# Well geometry IDs (must match well_id_t in well_geometry.h)
WELL_ID_SMALL = 0    # Ultra small well (~452 µL, area ≈5–30 mm²)
WELL_ID_LARGE = 1    # Ultra large well (~262 µL, area ≈3–52 mm²)
WELL_ID_AUTO  = 0xFF # Firmware auto-detects from last gantry location
# Small = narrower/taller, Large = wider/shorter

# Gantry Z axis physical bottom limit (µsteps, must match GANTRY_Z_MIN_POS
# in gantry_hw.h).  Use this as z_bottom in LLD/smart_aspirate so timing
# calculations reflect the actual travel distance the hardware can achieve.
GANTRY_Z_MIN_POS = -16404

GANTRY_XY_USTEPS_PER_MM = 3200.0 / (3.14159265358979 * 14.32)  # ~71.14
Z_USTEPS_PER_MM = 400.0 / 0.6096       # ~656.17 µsteps/mm (pipette Z)
LIFT_USTEPS_PER_MM = 16.0 / 0.0254     # ~629.92 µsteps/mm (lift)

# Liquid handling (0x8Axx)
CMD_LLD_PERFORM     = 0x8A01
CMD_LLF_START       = 0x8A02
CMD_LLF_STOP        = 0x8A03
CMD_SMART_ASPIRATE  = 0x8A04
CMD_WELL_DISPENSE   = 0x8A06
CMD_CART_DISPENSE_BF = 0x8A07
CMD_CART_DISPENSE   = 0x8A08
CMD_TIP_MIX         = 0x8A09

# LED / UI board (0x8Cxx)
CMD_LED_SET_PIXEL   = 0x8C01
CMD_LED_SET_BUTTON  = 0x8C02
CMD_LED_SET_PIXEL_OFF = 0x8C04
CMD_LED_SET_ALL_OFF = 0x8C05
CMD_LED_SET_PATTERN = 0x8C06
CMD_LED_CAM_SET     = 0x8C07
CMD_GET_PRESS_DATA  = 0x8A05

# Air heater (0x8Dxx)
CMD_AIR_HEATER_SET_DUTY   = 0x8D01
CMD_AIR_HEATER_SET_EN     = 0x8D02
CMD_AIR_HEATER_SET_FAN    = 0x8D03
CMD_AIR_HEATER_GET_STATUS = 0x8D04
CMD_AIR_HEATER_SET_CTRL   = 0x8D05
RSP_AIR_HEATER_STATUS     = 0x9D04

# System fans (0x87xx)
CMD_FAN_SET_DUTY          = 0x8701
CMD_FAN_GET_STATUS        = 0x8702
RSP_FAN_STATUS            = 0x9702

# Accelerometer (0x8Exx)
CMD_ACCEL_GET_STATUS      = 0x8E01
RSP_ACCEL_STATUS          = 0x9E01
CMD_ACCEL_STREAM_START    = 0x8E03
CMD_ACCEL_STREAM_STOP     = 0x8E04
CMD_ACCEL_RESET           = 0x8E05
# Unsolicited stream push — firmware emits ~25 batches/s once
# ACCEL_STREAM_START is acknowledged. Wire id sits in the 0x9Exx
# response range (not 0xA0xx/0xB0xx), so is_async_msg() doesn't
# catch it — stm32_interface RX loops must check RSP_ACCEL_STREAM
# explicitly before the generic async path.
RSP_ACCEL_STREAM          = 0x9E03

# Temperature sensors — EXT_NTC1/2, INT_NTC (0x91xx)
# Response wire ID = 0x9101 + 0x1000 = 0xA101 (not treated as async).
CMD_TEMP_GET_STATUS       = 0x9101
RSP_TEMP_STATUS           = 0xA101

# Flowcell heater (0x8Fxx)
CMD_FC_HEATER_SET_DUTY    = 0x8F01
CMD_FC_HEATER_SET_EN      = 0x8F02
CMD_FC_HEATER_GET_STATUS  = 0x8F03
CMD_FC_HEATER_SET_CTRL    = 0x8F04
RSP_FC_HEATER_STATUS      = 0x9F03

# Specific response IDs for custom result frames
RSP_LLD_PERFORM      = 0x9A01
RSP_SMART_ASPIRATE   = 0x9A04
RSP_WELL_DISPENSE    = 0x9A06
RSP_CART_DISPENSE_BF = 0x9A07
RSP_CART_DISPENSE    = 0x9A08

# Generic error response
RSP_ERROR           = 0x9FFF

# Async messages (0xA0xx)
MSG_STATUS          = 0xA001
MSG_EVENT           = 0xA002
MSG_TELEMETRY       = 0xA003
MSG_PRESSURE        = 0xA004
MSG_LOG             = 0xA005
MSG_ERROR           = 0xA006
MSG_PUMP_DONE       = 0xA007
MSG_GANTRY_DONE     = 0xA008
MSG_LIFT_DONE       = 0xA009
MSG_MOTOR_TELEMETRY = 0xA00A

# Pump transfer flags
PUMP_FLAG_STREAMING = 0x01
PUMP_FLAG_WAIT      = 0x02
PUMP_FLAG_VALIDATE  = 0x04
PUMP_FLAG_INTEGRATE = 0x08

# Command name to ID mapping for high-level API
CMD_NAME_TO_ID = {
    'ping':                 CMD_PING,
    'get_status':           CMD_GET_STATUS,
    'get_version':          CMD_GET_VERSION,
    'get_flags':            CMD_GET_FLAGS,
    'get_position':         CMD_GET_POSITION,
    'get_sensors':          CMD_GET_SENSORS,
    'reset':                CMD_RESET,
    'set_log_level':        CMD_SET_LOG_LEVEL,
    'enter_bootloader':     CMD_ENTER_BOOTLOADER,
    'set_state':            CMD_SET_STATE,
    'set_control_mode':     CMD_SET_CTRL_MODE,
    'start_protocol':       CMD_START_PROTOCOL,
    'abort':                CMD_ABORT,
    'pause':                CMD_PAUSE,
    'resume':               CMD_RESUME,
    'home_all':             CMD_HOME_ALL,
    'home_gantry':          CMD_HOME_GANTRY,
    'home_x_axis':          CMD_HOME_X_AXIS,
    'home_y_axis':          CMD_HOME_Y_AXIS,
    'home_z_axis':          CMD_HOME_Z_AXIS,
    'get_gantry_status':    CMD_GET_GANTRY_STATUS,
    'z_axis_test_gpio':     CMD_Z_TEST_GPIO,
    'move_gantry':          CMD_MOVE_GANTRY,
    'move_z_axis':          CMD_MOVE_Z_AXIS,
    'move_to_well':         CMD_MOVE_TO_WELL,
    'th_power_en':          CMD_TH_POWER_EN,
    'move_to_location':     CMD_MOVE_TO_LOCATION,
    'set_loc_centre':       CMD_SET_LOC_CENTRE,
    'set_loc_offset':       CMD_SET_LOC_OFFSET,
    'gantry_tip_swap':      CMD_TIP_SWAP,
    'lid_move':             CMD_LID_MOVE,
    'read_z_drv':           CMD_READ_Z_DRV,
    'get_motor_status':     CMD_GET_MOTOR_STATUS,
    'set_motor_telem':      CMD_SET_MOTOR_TELEM,
    'lift_home':            CMD_LIFT_HOME,
    'lift_stop':            CMD_LIFT_STOP,
    'lift_move':            CMD_LIFT_MOVE,
    'lift_status':          CMD_LIFT_STATUS,
    'lift_move_top':        CMD_LIFT_MOVE_TOP,
    'pump_aspirate':        CMD_PUMP_ASPIRATE,
    'pump_dispense':        CMD_PUMP_DISPENSE,
    'pump_prime':           CMD_PUMP_PRIME,
    'pump_blowout':         CMD_PUMP_BLOWOUT,
    'pump_init':            CMD_PUMP_INIT,
    'pump_set_resolution':  CMD_PUMP_SET_RES,
    'pump_set_pressure_gain': CMD_PUMP_SET_GAIN,
    'pump_move_absolute':   CMD_PUMP_MOVE_ABS,
    'pump_enable_streaming': CMD_PUMP_EN_STREAM,
    'pump_wait_idle':       CMD_PUMP_WAIT_IDLE,
    'pump_get_status':      CMD_PUMP_GET_STATUS,
    'pump_raw':             CMD_PUMP_RAW,
    'pump_test_move':       CMD_PUMP_TEST_MOVE,
    'pump_lld_start':       CMD_PUMP_LLD_START,
    'pump_lld_stop':        CMD_PUMP_LLD_STOP,
    'pump_piston_reset':    CMD_PUMP_PISTON_RST,
    'pump_stream_test':     CMD_PUMP_STREAM_TST,
    'centrifuge_start':     CMD_CFUGE_START,
    'centrifuge_stop':      CMD_CFUGE_STOP,
    'centrifuge_rock':      CMD_CFUGE_ROCK,
    'centrifuge_status':    CMD_CFUGE_STATUS,
    'centrifuge_move_angle': CMD_CFUGE_MOVE_ANGLE,
    'centrifuge_home':      CMD_CFUGE_HOME,
    'centrifuge_bldc_cmd':  CMD_CFUGE_BLDC_CMD,
    'centrifuge_power':     CMD_CFUGE_POWER,
    'centrifuge_unlock':    CMD_CFUGE_UNLOCK,
    'centrifuge_lock':      CMD_CFUGE_LOCK,
    'centrifuge_reverse':   CMD_CFUGE_REVERSE,
    'centrifuge_goto_serum':   CMD_CFUGE_GOTO_SERUM,
    'centrifuge_goto_pipette': CMD_CFUGE_GOTO_PIPETTE,
    'centrifuge_goto_blister': CMD_CFUGE_GOTO_BLISTER,
    'door_open':            CMD_DOOR_OPEN,
    'door_close':           CMD_DOOR_CLOSE,
    'door_status':          CMD_DOOR_STATUS,
    'set_speed':            CMD_SET_SPEED,
    'set_acceleration':     CMD_SET_ACCEL,
    'calibrate':            CMD_CALIBRATE,
    'lld_perform':          CMD_LLD_PERFORM,
    'llf_start':            CMD_LLF_START,
    'llf_stop':             CMD_LLF_STOP,
    'smart_aspirate':       CMD_SMART_ASPIRATE,
    'well_dispense':        CMD_WELL_DISPENSE,
    'cart_dispense_bf':     CMD_CART_DISPENSE_BF,
    'cart_dispense':        CMD_CART_DISPENSE,
    'tip_mix':              CMD_TIP_MIX,
    'get_pressure_data':    CMD_GET_PRESS_DATA,
    'fc_heater_set_duty':   CMD_FC_HEATER_SET_DUTY,
    'fc_heater_set_en':     CMD_FC_HEATER_SET_EN,
    'fc_heater_get_status': CMD_FC_HEATER_GET_STATUS,
    'fc_heater_set_ctrl':   CMD_FC_HEATER_SET_CTRL,
    'led_set_pixel':        CMD_LED_SET_PIXEL,
    'led_set_button':       CMD_LED_SET_BUTTON,
    'led_set_pixel_off':    CMD_LED_SET_PIXEL_OFF,
    'led_set_all_off':      CMD_LED_SET_ALL_OFF,
    'led_set_pattern':      CMD_LED_SET_PATTERN,
    'cam_led_set':          CMD_LED_CAM_SET,
    'air_heater_set_duty':  CMD_AIR_HEATER_SET_DUTY,
    'air_heater_set_en':    CMD_AIR_HEATER_SET_EN,
    'air_heater_set_fan':   CMD_AIR_HEATER_SET_FAN,
    'air_heater_get_status': CMD_AIR_HEATER_GET_STATUS,
    'air_heater_set_ctrl':  CMD_AIR_HEATER_SET_CTRL,
    'fan_set_duty':         CMD_FAN_SET_DUTY,
    'fan_get_status':       CMD_FAN_GET_STATUS,
    'accel_get_status':     CMD_ACCEL_GET_STATUS,
    'accel_stream_start':   CMD_ACCEL_STREAM_START,
    'accel_stream_stop':    CMD_ACCEL_STREAM_STOP,
    'accel_reset':          CMD_ACCEL_RESET,
    'temp_get_status':      CMD_TEMP_GET_STATUS,
    'fw_update_start':     CMD_FW_UPDATE_START,
    'fw_write_block':      CMD_FW_WRITE_BLOCK,
}


def cmd_to_rsp(cmd_id: int) -> int:
    '''Convert command ID (0x8xyz) to response ID (0x9xyz).'''
    return cmd_id + 0x1000


def is_async_msg(cmd_id: int) -> bool:
    '''Check if a command ID is an async message.

    Firmware defines async IDs as 0xA0xx, but Proto_SendResponse
    adds 0x1000, so they arrive on the wire as 0xB0xx.
    '''
    return (cmd_id & 0xFF00) in (0xA000, 0xB000)


# =============================================================================
# CRC-16/CCITT-FALSE
# =============================================================================

def crc16_ccitt(data: bytes) -> int:
    '''Calculate CRC-16/CCITT-FALSE.

    Polynomial 0x1021, init 0xFFFF, no reflect, no final XOR.
    Matches the firmware usart_frame_crc16() implementation.

    Args:
        data: Bytes to calculate CRC over.

    Returns:
        16-bit CRC value.
    '''
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


# =============================================================================
# Frame build / parse
# =============================================================================

def build_frame(
        command: int,
        data: bytes = b'',
) -> bytes:
    '''Build a binary frame.

    Args:
        command: 16-bit command ID.
        data: Payload bytes (0-255 bytes).

    Returns:
        Complete frame bytes including SOH and CRC.

    Raises:
        ValueError: If data exceeds 255 bytes.
    '''
    if len(data) > MAX_DATA_LEN:
        raise ValueError(
            f'Data length {len(data)} exceeds max {MAX_DATA_LEN}'
        )

    cmd_bytes = struct.pack('<H', command)
    length_byte = struct.pack('B', len(data))

    crc_input = cmd_bytes + length_byte + data
    crc = crc16_ccitt(crc_input)
    crc_bytes = struct.pack('<H', crc)

    return (
        bytes([SOH])
        + cmd_bytes
        + length_byte
        + data
        + crc_bytes
    )


class FrameParser:
    '''Byte-by-byte frame parser state machine.

    Feed bytes via feed(). When a complete CRC-valid frame
    arrives, feed() returns a (command, data) tuple.
    '''

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        '''Reset parser to wait-for-SOH state.'''
        self._state = 0
        self._command = 0
        self._length = 0
        self._data = bytearray()
        self._crc_received = 0

    def feed(self, byte: int) -> tuple[int, bytes] | None:
        '''Feed one byte into the parser.

        Args:
            byte: Received byte (0-255).

        Returns:
            (command, data) tuple when a valid frame is
            complete, or None if more bytes needed or frame
            was invalid.
        '''
        if self._state == 0:  # WAIT_SOH
            if byte == SOH:
                self._command = 0
                self._length = 0
                self._data = bytearray()
                self._crc_received = 0
                self._state = 1
        elif self._state == 1:  # CMD_LSB
            self._command = byte
            self._state = 2
        elif self._state == 2:  # CMD_MSB
            self._command |= (byte << 8)
            self._state = 3
        elif self._state == 3:  # LENGTH
            self._length = byte
            if self._length > 0:
                self._state = 4
            else:
                self._state = 5
        elif self._state == 4:  # READ_DATA
            self._data.append(byte)
            if len(self._data) >= self._length:
                self._state = 5
        elif self._state == 5:  # CRC_LSB
            self._crc_received = byte
            self._state = 6
        elif self._state == 6:  # CRC_MSB
            self._crc_received |= (byte << 8)
            self._state = 0

            crc_input = (
                struct.pack('<H', self._command)
                + bytes([self._length])
                + bytes(self._data)
            )
            crc_calc = crc16_ccitt(crc_input)

            if crc_calc == self._crc_received:
                return (self._command, bytes(self._data))
            return None

        return None


# =============================================================================
# Payload pack helpers (command payloads)
# =============================================================================

def pack_seq(seq: int) -> bytes:
    '''Pack a sequence-only payload (4 bytes).'''
    return struct.pack('<I', seq)


_INT32_MAX = 0x7FFFFFFF  # "skip this axis" sentinel (INT32_MAX)


def pack_move_gantry(
        seq: int,
        x_mm: float | None = None,
        y_mm: float | None = None,
        z_mm: float | None = None,
        speed: float = 0.0,
        z_speed: float = 0.0,
) -> bytes:
    '''Pack CMD_MOVE_GANTRY payload.

    All positions in mm -> um on wire. Speeds in mm/s -> 0.1 mm/s.
    Firmware converts um -> usteps internally for each axis.

    Args:
        seq    : sequence number.
        x_mm   : absolute X in mm; None = skip axis.
        y_mm   : absolute Y in mm; None = skip axis.
        z_mm   : absolute Z in mm; None = skip axis.
        speed  : XY cruise speed in mm/s; 0 = firmware default.
        z_speed: Z  cruise speed in mm/s; 0 = firmware default.
                 Firmware still clamps Z at GANTRY_Z_MAX_SPS
                 (~18 mm/s).

    Returns:
        Packed bytes: struct.pack('<IiiiHH', ...) = 20 bytes.
    '''
    def _to_um(mm):
        if mm is None:
            return _INT32_MAX
        return int(round(mm * 1000.0))

    speed_01mms   = int(round(speed   * 10.0))
    z_speed_01mms = int(round(z_speed * 10.0))
    return struct.pack('<IiiiHH', seq,
                       _to_um(x_mm), _to_um(y_mm), _to_um(z_mm),
                       speed_01mms, z_speed_01mms)


def pack_lift_move(
        seq: int,
        target_mm: float,
        speed: float = 0.0,
) -> bytes:
    '''Pack CMD_LIFT_MOVE (0x8B03) payload.

    Position in mm → µm on wire; speed in mm/s → 0.1 mm/s.
    Firmware converts µm → µsteps internally.

    Args:
        seq      : Sequence number.
        target_mm: Absolute position in mm from home.
        speed    : Cruise speed in mm/s; 0 = firmware default.

    Returns:
        Packed bytes: struct.pack('<IiH', ...).
    '''
    target_um = int(round(target_mm * 1000.0))
    speed_01mms = int(round(speed * 10.0))
    return struct.pack('<IiH', seq, target_um, speed_01mms)


def pack_move_z_axis(
        seq: int,
        position_mm: float,
        speed: float = 0.0,
        acceleration: int = 0,
) -> bytes:
    '''Pack CMD_MOVE_Z_AXIS payload.

    Position in mm → µm on wire; speed in mm/s → 0.1 mm/s.
    Firmware converts µm → µsteps internally.

    Args:
        seq         : sequence number.
        position_mm : absolute Z position in mm.
        speed       : cruise speed in mm/s (0 = firmware default).
        acceleration: reserved (0 = firmware default).
    '''
    position_um = int(round(position_mm * 1000.0))
    speed_01mms = int(round(speed * 10.0))
    return struct.pack('<IiHH', seq, position_um, speed_01mms,
                       acceleration)


def pack_move_to_well(
        seq: int,
        well_id: int,
        z_offset: int = 0,
) -> bytes:
    '''Pack CMD_MOVE_TO_WELL payload.'''
    return struct.pack('<IBh', seq, well_id, z_offset)


def pack_th_power_en(seq: int, enable: bool) -> bytes:
    '''Pack CMD_TH_POWER_EN payload.

    Args:
        seq   : sequence number.
        enable: True to power on Toolhead 24V, False to power off.

    Returns:
        Packed bytes: seq(4) + enable(1) = 5 bytes.
    '''
    return struct.pack('<IB', seq, 1 if enable else 0)


def pack_move_to_location(
        seq: int,
        location_id: int,
        speed_01mms: int = 0,
) -> bytes:
    '''Pack CMD_MOVE_TO_LOCATION payload.

    Firmware looks up the cartridge location by ID, homes Z if needed,
    then moves X and Y only.

    Args:
        seq         : sequence number.
        location_id : location_id_t enum value (0 .. LOC_COUNT-1).
        speed_01mms : XY cruise speed in 0.1 mm/s (0 = firmware default).

    Returns:
        Packed bytes: seq(4) + location_id(1) + speed_01mms(2) = 7 bytes.
    '''
    return struct.pack('<IBH', seq, location_id, speed_01mms)


def pack_set_loc_centre(
        seq: int,
        x_um: int,
        y_um: int,
        z_um: int,
) -> bytes:
    '''Pack CMD_SET_LOC_CENTRE payload.

    Updates the cartridge reference centre that all 58 named locations
    are measured relative to.

    Args:
        seq : sequence number.
        x_um: gantry X of cartridge centre (µm).
        y_um: gantry Y of cartridge centre (µm).
        z_um: gantry Z of cartridge centre (µm).

    Returns:
        Packed bytes: seq(4) + x_um(4) + y_um(4) + z_um(4) = 16 bytes.
    '''
    return struct.pack('<Iiii', seq, x_um, y_um, z_um)


def pack_set_loc_offset(
        seq: int,
        dx_um: int,
        dy_um: int,
        dz_um: int,
) -> bytes:
    '''Pack CMD_SET_LOC_OFFSET payload.

    Updates the global calibration trim applied to every named location.

    Args:
        seq  : sequence number.
        dx_um: X calibration trim (µm).
        dy_um: Y calibration trim (µm).
        dz_um: Z calibration trim (µm).

    Returns:
        Packed bytes: seq(4) + dx_um(4) + dy_um(4) + dz_um(4) = 16 bytes.
    '''
    return struct.pack('<Iiii', seq, dx_um, dy_um, dz_um)


def pack_tip_swap(
        seq: int,
        from_id: int,
        to_id: int,
        x_eject_um: int = 0,
        pick_depth_um: int = 0,
        retract_um: int = 0,
        xy_speed_01mms: int = 0,
        z_speed_01mms: int = 0,
) -> bytes:
    '''Pack CMD_TIP_SWAP payload.

    Instructs the firmware to return the current tip and/or pick up a
    new one using gantry motions. from_id=0 skips the return sequence;
    to_id=0 skips the pickup sequence.

    Optional motion overrides. 0 means use firmware default for that
    parameter:
      x_eject_um    : +X seat offset from slot centre (um); 0 = 10 mm def.
      pick_depth_um : Signed absolute gantry Z target (um from home,
                      negative); 0 = firmware default (-19 mm).
      retract_um    : Z upward move after pickup (um).
      xy_speed_01mms: XY cruise speed in 0.1 mm/s; 0 = firmware default
                      (s_tip_xy_speed_01mms, typically 25 mm/s).
      z_speed_01mms : Z  cruise speed in 0.1 mm/s; 0 = firmware default
                      (s_tip_z_speed_01mms, typically 6 mm/s).

    Args:
        seq           : sequence number.
        from_id       : tip slot to strip tip into (1-8); 0 = skip return.
        to_id         : tip slot to pick up from   (1-8); 0 = skip pickup.
        x_eject_um    : optional +X offset (um); 0 = firmware default (10 mm).
        pick_depth_um : signed absolute gantry Z (um, negative); 0 = def.
        retract_um    : optional Z retract (um); 0 = default.
        xy_speed_01mms: XY cruise speed (0.1 mm/s); 0 = firmware default.
        z_speed_01mms : Z  cruise speed (0.1 mm/s); 0 = firmware default.

    Returns:
        6 bytes (short form) when all optional params are at defaults;
        else 22 bytes extended form.
    '''
    _defaults = (
        x_eject_um == 0
        and pick_depth_um == 0
        and retract_um == 0
        and xy_speed_01mms == 0
        and z_speed_01mms == 0
    )
    if _defaults:
        return struct.pack('<IBB', seq, from_id, to_id)
    return struct.pack(
        '<IBBHiiiH',
        seq, from_id, to_id,
        xy_speed_01mms,
        x_eject_um, pick_depth_um, retract_um,
        z_speed_01mms,
    )


def pack_lid_move(
        seq: int,
        open: bool,
        z_engage_um: int = 0,
        xy_speed_01mms: int = 0,
        z_speed_01mms: int = 0,
        x_open_extra_um: int = 0,
) -> bytes:
    '''Pack CMD_LID_MOVE payload.

    Instructs the firmware to open or close the cartridge lid using a
    notch-pivot gantry sequence.

    Optional motion overrides. 0 means use firmware default for that
    parameter:
      z_engage_um    : Signed absolute gantry Z target (um, negative);
                       0 = firmware default (-4500 um / -4.5 mm).
      xy_speed_01mms : XY cruise speed in 0.1 mm/s; 0 = firmware default
                       (s_lid_xy_speed_01mms, typically 25 mm/s).
      z_speed_01mms  : Z  cruise speed in 0.1 mm/s; 0 = firmware default
                       (s_lid_z_speed_01mms, typically 6 mm/s).
      x_open_extra_um: Extra X travel away from home on open (um);
                       0 = firmware default (10 mm).

    Args:
        seq             : sequence number.
        open            : True = open lid, False = close lid.
        z_engage_um     : signed absolute gantry Z (um, negative); 0 = def.
        xy_speed_01mms  : XY cruise speed (0.1 mm/s); 0 = firmware default.
        z_speed_01mms   : Z  cruise speed (0.1 mm/s); 0 = firmware default.
        x_open_extra_um : extra X travel on open (um); 0 = firmware default.

    Returns:
        5 bytes (short form) when all optional params are at defaults;
        17 bytes extended-v2 form when x_open_extra_um is set;
        else 13 bytes extended form.
    '''
    _defaults = (
        z_engage_um == 0
        and xy_speed_01mms == 0
        and z_speed_01mms == 0
        and x_open_extra_um == 0
    )
    if _defaults:
        return struct.pack('<IB', seq, int(open))
    if x_open_extra_um != 0:
        return struct.pack(
            '<IBHHii',
            seq, int(open),
            xy_speed_01mms, z_speed_01mms,
            z_engage_um, x_open_extra_um,
        )
    return struct.pack(
        '<IBHHi',
        seq, int(open),
        xy_speed_01mms, z_speed_01mms,
        z_engage_um,
    )


def pack_pump_transfer(
        seq: int,
        volume_ul: int,
        speed_ul_s: float,
        cutoff_ul_s: float = 0.0,
        streaming: bool = False,
        wait: bool = True,
        validate: bool = False,
        integrate: bool = False,
) -> bytes:
    '''Pack CMD_PUMP_ASPIRATE or CMD_PUMP_DISPENSE payload.'''
    flags = 0
    if streaming:
        flags |= PUMP_FLAG_STREAMING
    if wait:
        flags |= PUMP_FLAG_WAIT
    if validate:
        flags |= PUMP_FLAG_VALIDATE
    if integrate:
        flags |= PUMP_FLAG_INTEGRATE
    return struct.pack(
        '<IIff B',
        seq, volume_ul, float(speed_ul_s), float(cutoff_ul_s), flags,
    )


def pack_pump_move_abs(
        seq: int,
        position: int,
        wait: bool = True,
) -> bytes:
    '''Pack CMD_PUMP_MOVE_ABS payload.'''
    flags = 0x01 if wait else 0x00
    return struct.pack('<IIB', seq, position, flags)


def pack_pump_streaming(
        seq: int,
        mode: int,
) -> bytes:
    '''Pack CMD_PUMP_EN_STREAM payload.'''
    return struct.pack('<IB', seq, mode)


def pack_pump_wait_idle(
        seq: int,
        timeout_ms: int = 30000,
) -> bytes:
    '''Pack CMD_PUMP_WAIT_IDLE payload.'''
    return struct.pack('<II', seq, timeout_ms)


def pack_pump_raw(
        seq: int,
        command: str,
        query: bool = False,
        timeout_ms: int = 5000,
) -> bytes:
    '''Pack CMD_PUMP_RAW payload.'''
    cmd_bytes = command.encode('ascii')
    if len(cmd_bytes) > 63:
        cmd_bytes = cmd_bytes[:63]
    flags = 0x01 if query else 0x00
    hdr = struct.pack('<BIB', flags, timeout_ms, len(cmd_bytes))
    return struct.pack('<I', seq) + hdr + cmd_bytes


def pack_pump_lld_start(
        seq: int,
        threshold: int = 50,
        wait_ms: int = 500,
        dispense_s: int = 3,
        save_samples: bool = False,
) -> bytes:
    '''Pack CMD_PUMP_LLD_START payload.'''
    flags = 0x01 if save_samples else 0x00
    return struct.pack(
        '<IBHBB', seq, threshold, wait_ms, dispense_s, flags,
    )


def pack_pump_stream_test(
        seq: int,
        duration_ms: int = 1000,
        steps: int = 1000,
        dispense: bool = False,
) -> bytes:
    '''Pack CMD_PUMP_STREAM_TEST payload.'''
    flags = 0x01 if dispense else 0x00
    return struct.pack('<IHIB', seq, duration_ms, steps, flags)


def pack_pump_resolution(seq: int, mode: int) -> bytes:
    '''Pack CMD_PUMP_SET_RES payload.'''
    return struct.pack('<IB', seq, mode)


def pack_pump_gain(seq: int, gain: int) -> bytes:
    '''Pack CMD_PUMP_SET_GAIN payload.'''
    return struct.pack('<IB', seq, gain)


def pack_led_set_pixel(
        seq: int,
        idx: int,
        r: int, g: int, b: int, w: int,
) -> bytes:
    '''Pack CMD_LED_SET_PIXEL payload.

    Args:
        seq: Sequence number.
        idx: LED index 0-4, or 0xFF for all LEDs.
        r, g, b, w: RGBW color values (0-255 each).
    '''
    return struct.pack('<IBBBBB', seq, idx, r, g, b, w)


def pack_led_set_button(seq: int, on: bool) -> bytes:
    '''Pack CMD_LED_SET_BUTTON payload.

    Args:
        seq: Sequence number.
        on: True = button LED on, False = off.
    '''
    return struct.pack('<IB', seq, 1 if on else 0)


def pack_led_set_pixel_off(seq: int, idx: int) -> bytes:
    '''Pack CMD_LED_SET_PIXEL_OFF payload.

    Args:
        seq: Sequence number.
        idx: LED index 0-4, or 0xFF for all LEDs.
    '''
    return struct.pack('<IB', seq, idx)


def pack_led_set_all_off(seq: int) -> bytes:
    '''Pack CMD_LED_SET_ALL_OFF payload.

    Args:
        seq: Sequence number.
    '''
    return struct.pack('<I', seq)


def pack_led_set_pattern(
    seq: int,
    pattern: int,
    stage: int = 0,
) -> bytes:
    '''Pack CMD_LED_SET_PATTERN payload.

    Args:
        seq:     Sequence number (uint32).
        pattern: Pattern ID — 0=none, 1=waiting, 2=ready,
                 3=error, 4=progress, 5=scanning.
        stage:   Progress stage 1-5. Ignored for all patterns
                 other than progress (4). Pass 0 otherwise.
    '''
    return struct.pack('<IBB', seq, pattern, stage)


def pack_cam_led_set(seq: int, on: bool) -> bytes:
    '''Pack CMD_LED_CAM_SET payload (0x8C07).

    Holds the toolhead camera illumination LED (PC12) steadily
    on (on=True) or off (on=False). While on, the centrifuge
    revolution strobe is suppressed in firmware -- do not send
    on=True during a centrifuge spin.

    Args:
        seq: Sequence number (uint32).
        on:  True = steady on, False = release override (off).
    '''
    return struct.pack('<IB', seq, 1 if on else 0)


# =====================================================================
# Air heater helpers (0x8Dxx)
# =====================================================================

def pack_air_heater_set_duty(seq: int, pct: int) -> bytes:
    '''Pack CMD_AIR_HEATER_SET_DUTY payload. pct = 0-100.'''
    return struct.pack('<IB', seq, pct)


def pack_air_heater_set_en(seq: int, enable: bool) -> bytes:
    '''Pack CMD_AIR_HEATER_SET_EN payload. enable = True/False.'''
    return struct.pack('<IB', seq, 1 if enable else 0)


def pack_air_heater_set_fan(seq: int, pct: int) -> bytes:
    '''Pack CMD_AIR_HEATER_SET_FAN payload. pct = 0-100.'''
    return struct.pack('<IB', seq, pct)


def pack_air_heater_get_status(seq: int) -> bytes:
    '''Pack CMD_AIR_HEATER_GET_STATUS payload.'''
    return struct.pack('<I', seq)


def pack_air_heater_set_ctrl(seq: int, setpoint_c: float,
                             hysteresis_c: float, heater_duty: int,
                             fan_duty: int, enable: bool) -> bytes:
    '''Pack CMD_AIR_HEATER_SET_CTRL payload (11 bytes).'''
    return struct.pack('<IhhBBB', seq,
                       int(setpoint_c * 10),
                       int(hysteresis_c * 10),
                       heater_duty, fan_duty,
                       1 if enable else 0)


def unpack_air_heater_status(data: bytes) -> dict:
    '''Unpack AIR_HEATER_GET_STATUS response (0x9D04).

    Extended proto_rsp_air_heater_status_t (21 bytes):
        seq(4) + error(1) + prim_x10(2) + sec_x10(2) +
        heater_duty(1) + fan_duty(1) + heater_en(1) + otp(1) +
        ctrl_enabled(1) + ctrl_setpoint_x10(2) + ctrl_hyst_x10(2) +
        ctrl_heater_duty(1) + ctrl_fan_duty(1) + ctrl_heating(1)
    '''
    if len(data) < 21:
        return {'seq': 0, 'error': 0xFF}
    (seq, error, prim_x10, sec_x10, h_duty, f_duty, h_en, otp,
     ctrl_en, ctrl_sp_x10, ctrl_hyst_x10, ctrl_h_duty, ctrl_f_duty,
     ctrl_heating) = struct.unpack_from('<IBhhBBBBBhhBBB', data)
    return {
        'seq': seq,
        'error': error,
        'prim_temp_c': prim_x10 / 10.0,
        'sec_temp_c': sec_x10 / 10.0,
        'heater_duty': h_duty,
        'fan_duty': f_duty,
        'heater_en': bool(h_en),
        'otp': bool(otp),
        'ctrl_enabled': bool(ctrl_en),
        'ctrl_setpoint_c': ctrl_sp_x10 / 10.0,
        'ctrl_hysteresis_c': ctrl_hyst_x10 / 10.0,
        'ctrl_heater_duty': ctrl_h_duty,
        'ctrl_fan_duty': ctrl_f_duty,
        'ctrl_heating': bool(ctrl_heating),
    }


def pack_fan_set_duty(seq: int, pct: int) -> bytes:
    '''Pack CMD_FAN_SET_DUTY payload (5 bytes: seq + duty).'''
    return struct.pack('<IB', seq, pct)


def pack_fan_get_status(seq: int) -> bytes:
    '''Pack CMD_FAN_GET_STATUS payload (seq only, 4 bytes).'''
    return struct.pack('<I', seq)


def unpack_fan_status(data: bytes) -> dict:
    '''Unpack FAN_GET_STATUS response (0x9702).

    proto_rsp_fan_status_t (8 bytes):
        seq(4) + error(1) + duty(1) + rpm(2)
    '''
    if len(data) < 8:
        return {'seq': 0, 'error': 0xFF}
    seq, error, duty, rpm = struct.unpack_from('<IBBH', data)
    return {
        'seq': seq,
        'error': error,
        'duty': duty,
        'rpm': rpm,
    }


def pack_accel_get_status(seq: int) -> bytes:
    '''Pack CMD_ACCEL_GET_STATUS payload (seq only, 4 bytes).'''
    return struct.pack('<I', seq)


def unpack_accel_status(data: bytes) -> dict:
    '''Unpack ACCEL_GET_STATUS response (0x9E01).

    proto_rsp_accel_status_t (18 bytes):
        seq(4) + error(1) + x_mg(4) + y_mg(4) + z_mg(4) + initialized(1)
    '''
    if len(data) < 18:
        return {'seq': 0, 'error': 0xFF}
    seq, error, x_mg, y_mg, z_mg, init = struct.unpack_from('<IBiiiB', data)
    return {
        'seq': seq,
        'error': error,
        'x_g': x_mg / 1000.0,
        'y_g': y_mg / 1000.0,
        'z_g': z_mg / 1000.0,
        'initialized': bool(init),
    }


def pack_accel_stream_start(seq: int) -> bytes:
    return struct.pack('<I', seq)


def pack_accel_stream_stop(seq: int) -> bytes:
    return struct.pack('<I', seq)


def pack_accel_reset(seq: int) -> bytes:
    return struct.pack('<I', seq)


def unpack_accel_stream(data: bytes) -> dict:
    '''Unpack an accel stream push frame (0x9E03).

    Wire layout (must match accel_service.c stream_send_batch):
      seq(2)       — ISR sequence counter, increments per FIFO IRQ
      tick_ms(4)   — HAL_GetTick() at the moment of the IRQ
      count(1)     — samples in this batch (typ. 16)
      buf_used(1)  — occupied slots in cyclic buffer (0..7)
      samples(count * 6)  — int16 x, y, z each (raw LSBs)
    '''
    if len(data) < 8:
        return {'seq': 0, 'tick_ms': 0, 'count': 0,
                'buf_used': 0, 'samples': []}
    seq      = int.from_bytes(data[0:2], 'little')
    tick_ms  = int.from_bytes(data[2:6], 'little')
    count    = data[6]
    buf_used = data[7]
    samples  = []
    offset = 8
    for _ in range(count):
        if offset + 6 > len(data):
            break
        x, y, z = struct.unpack_from('<hhh', data, offset)
        samples.append((x, y, z))
        offset += 6
    return {
        'seq': seq, 'tick_ms': tick_ms,
        'count': count, 'buf_used': buf_used,
        'samples': samples,
    }


def pack_temp_get_status(seq: int) -> bytes:
    '''Pack CMD_TEMP_GET_STATUS payload (seq only, 4 bytes).'''
    return struct.pack('<I', seq)


def unpack_temp_status(data: bytes) -> dict:
    '''Unpack TEMP_GET_STATUS response (0xA101).

    proto_rsp_temp_status_t (21 bytes):
        seq(4) + error(1) +
        ext1_x10(2) + ext2_x10(2) + int_x10(2) +
        prim_air_x10(2) + sec_air_x10(2) + flowcell_x10(2) +
        flowcell_otp(1) + air_otp(1) +
        flowcell_heater_en(1) + air_heater_en(1)

    Temperatures are in 0.1 degC units (signed int16).
    Backward-compatible: returns minimal dict if payload < 21 bytes.
    '''
    if len(data) < 11:
        return {'seq': 0, 'error': 0xFF}
    seq, error, ext1_x10, ext2_x10, int_x10 = struct.unpack_from(
        '<IBhhh', data)
    result = {
        'seq': seq,
        'error': error,
        'ext1_temp_c': ext1_x10 / 10.0,
        'ext2_temp_c': ext2_x10 / 10.0,
        'int_temp_c':  int_x10  / 10.0,
    }
    if len(data) >= 21:
        (
            prim_air_x10, sec_air_x10, flowcell_x10,
            fc_otp, air_otp, fc_en, air_en,
        ) = struct.unpack_from('<hhhBBBB', data, 11)
        result.update({
            'prim_air_temp_c': prim_air_x10 / 10.0,
            'sec_air_temp_c': sec_air_x10 / 10.0,
            'flowcell_temp_c': flowcell_x10 / 10.0,
            'flowcell_otp': bool(fc_otp),
            'air_otp': bool(air_otp),
            'flowcell_heater_en': bool(fc_en),
            'air_heater_en': bool(air_en),
        })
    return result


def pack_set_state(seq: int, state: int) -> bytes:
    '''Pack CMD_SET_STATE payload.'''
    return struct.pack('<IB', seq, state)


def pack_set_control_mode(seq: int, mode: int) -> bytes:
    '''Pack CMD_SET_CONTROL_MODE payload.'''
    return struct.pack('<IB', seq, mode)


def pack_start_protocol(
        seq: int,
        protocol_id: int = 0,
) -> bytes:
    '''Pack CMD_START_PROTOCOL payload.'''
    return struct.pack('<IB', seq, protocol_id)


def pack_centrifuge_start(
        seq: int,
        rpm: int,
        duration_s: int,
) -> bytes:
    '''Pack CMD_CENTRIFUGE_START payload.'''
    return struct.pack('<IHH', seq, rpm, duration_s)


def pack_centrifuge_move_angle(
        seq: int,
        angle_001deg: int,
        move_rpm: int = 500,
) -> bytes:
    '''Pack CMD_CENTRIFUGE_MOVE_ANGLE payload.

    Args:
        seq: Sequence number.
        angle_001deg: Target angle in 0.01 degree units
            (0-36000 maps to 0-360 degrees).
        move_rpm: RPM for the brief spin that moves the
            motor toward the target (default 500).
    '''
    return struct.pack('<IiH', seq, angle_001deg, move_rpm)


def pack_centrifuge_bldc_cmd(
        seq: int,
        bldc_cmd: int,
        data: bytes = b'',
) -> bytes:
    '''Pack CMD_CENTRIFUGE_BLDC_CMD payload.

    Generic pass-through for any BLDC driver command.

    Args:
        seq: Sequence number.
        bldc_cmd: BLDC driver command ID (0x0001-0x0034).
        data: Command-specific data (up to 8 bytes).
    '''
    padded = data[:8].ljust(8, b'\x00')
    return struct.pack(
        '<IHB8s', seq, bldc_cmd, len(data), padded,
    )


def pack_bldc_pos_pid(
        seq: int,
        p_gain: int,
        p_shift: int,
        i_gain: int,
        i_shift: int,
) -> bytes:
    '''Pack centrifuge_bldc_cmd payload for SET_POS_PID (0x0023).

    Builds the inner BLDC data bytes (8 bytes, little-endian):
        int16  p_gain, uint16 p_shift, int16 i_gain, uint16 i_shift
    then wraps in pack_centrifuge_bldc_cmd.

    Args:
        seq    : Sequence number.
        p_gain : Proportional gain (signed 16-bit).
        p_shift: Proportional gain right-shift (unsigned 16-bit).
        i_gain : Integral gain (signed 16-bit).
        i_shift: Integral gain right-shift (unsigned 16-bit).

    Returns:
        Packed centrifuge_bldc_cmd payload bytes.
    '''
    inner = struct.pack(
        '<hHhH', p_gain, p_shift, i_gain, i_shift,
    )
    return pack_centrifuge_bldc_cmd(seq, BLDC_SET_POS_PID, inner)


def pack_bldc_get_pos_pid(seq: int) -> bytes:
    '''Pack centrifuge_bldc_cmd payload for GET_POS_PID (0x0022).

    Args:
        seq: Sequence number.

    Returns:
        Packed centrifuge_bldc_cmd payload bytes.
    '''
    return pack_centrifuge_bldc_cmd(seq, BLDC_GET_POS_PID, b'')


def unpack_bldc_pos_pid(raw_data_hex: str) -> dict:
    '''Unpack the 8-byte data field from a GET_POS_PID response.

    Args:
        raw_data_hex: Hex string from unpack_rsp_centrifuge_bldc
            result["data"] (e.g. "0100040001000400").

    Returns:
        Dict with p_gain, p_shift, i_gain, i_shift, p_eff, i_eff.
        p_eff = p_gain / (1 << p_shift) (effective float gain).
    '''
    raw = bytes.fromhex(raw_data_hex) if raw_data_hex else b''
    if len(raw) < 8:
        return {}
    p_gain, p_shift, i_gain, i_shift = struct.unpack_from(
        '<hHhH', raw,
    )
    p_eff = p_gain / (1 << p_shift) if p_shift < 32 else 0.0
    i_eff = i_gain / (1 << i_shift) if i_shift < 32 else 0.0
    return {
        'p_gain': p_gain, 'p_shift': p_shift, 'p_eff': p_eff,
        'i_gain': i_gain, 'i_shift': i_shift, 'i_eff': i_eff,
    }


def pack_bldc_set_pos_hold_thresh(
        seq: int,
        dev_thresh_001deg: int,
        ctrl_max_ms: int,
        stop_pos_thresh_001deg: int,
        stop_detect_ms: int,
) -> list:
    '''Build list of four centrifuge_bldc_cmd payloads for V1.02
    position-hold thresholds (0x0017, 0x0019, 0x001D, 0x001F).

    Each threshold is a uint32 little-endian. Returns a list of four
    packed byte strings; the caller should send them in order.

    Args:
        seq                   : Base sequence number (incremented
            per frame by caller).
        dev_thresh_001deg     : Min deviation to engage hold
            (0.01 deg units).
        ctrl_max_ms           : Max position-control runtime (ms).
        stop_pos_thresh_001deg: Window to decide motor stopped
            (0.01 deg units).
        stop_detect_ms        : Time inside window → stopped (ms).

    Returns:
        List of four packed payload bytes.
    '''
    frames = []
    for cmd, val in (
        (BLDC_SET_POS_DEV_THRESH,    dev_thresh_001deg),
        (BLDC_SET_POS_CTRL_MAX_TIME, ctrl_max_ms),
        (BLDC_SET_STOP_POS_THRESH,   stop_pos_thresh_001deg),
        (BLDC_SET_STOP_DETECT_TIME,  stop_detect_ms),
    ):
        frames.append(
            pack_centrifuge_bldc_cmd(
                seq, cmd, struct.pack('<I', val),
            )
        )
        seq += 1
    return frames


def pack_bldc_get_pos_hold_thresh(seq: int) -> list:
    '''Build list of four centrifuge_bldc_cmd payloads for reading
    the V1.02 position-hold thresholds (0x0016, 0x0018, 0x001C,
    0x001E).

    Args:
        seq: Base sequence number (incremented per frame by caller).

    Returns:
        List of four packed payload bytes.
    '''
    cmds = [
        BLDC_GET_POS_DEV_THRESH,
        BLDC_GET_POS_CTRL_MAX_TIME,
        BLDC_GET_STOP_POS_THRESH,
        BLDC_GET_STOP_DETECT_TIME,
    ]
    return [
        pack_centrifuge_bldc_cmd(seq + i, c, b'')
        for i, c in enumerate(cmds)
    ]


def pack_bldc_set_soft_curr_limit(seq: int, limit_01a: int) -> bytes:
    '''Pack centrifuge_bldc_cmd payload for SET_SOFT_CURR_LIMIT (0x001B).

    Args:
        seq      : Sequence number.
        limit_01a: Over-current threshold in units of 0.1 A
                   (e.g. 200 = 20.0 A). uint16.

    Returns:
        Packed centrifuge_bldc_cmd payload bytes.
    '''
    inner = struct.pack('<H', limit_01a)
    return pack_centrifuge_bldc_cmd(seq, BLDC_SET_SOFT_CURR_LIMIT, inner)


def pack_bldc_get_soft_curr_limit(seq: int) -> bytes:
    '''Pack centrifuge_bldc_cmd payload for GET_SOFT_CURR_LIMIT (0x001A).

    Args:
        seq: Sequence number.

    Returns:
        Packed centrifuge_bldc_cmd payload bytes.
    '''
    return pack_centrifuge_bldc_cmd(seq, BLDC_GET_SOFT_CURR_LIMIT, b'')


def unpack_bldc_soft_curr_limit(raw_data_hex: str) -> dict:
    '''Unpack the 2-byte data field from a GET_SOFT_CURR_LIMIT response.

    Args:
        raw_data_hex: Hex string from unpack_rsp_centrifuge_bldc
            result["data"] (e.g. "c800").

    Returns:
        Dict with limit_01a (uint16) and limit_a (float, amps).
    '''
    raw = bytes.fromhex(raw_data_hex) if raw_data_hex else b''
    if len(raw) < 2:
        return {}
    limit_01a = struct.unpack_from('<H', raw)[0]
    return {
        'limit_01a': limit_01a,
        'limit_a': limit_01a / 10.0,
    }


def pack_bldc_set_max_current(seq: int, max_01a: int) -> bytes:
    '''Pack centrifuge_bldc_cmd payload for SET_MAX_CURRENT (0x0051, V1.03).

    Args:
        seq     : Sequence number.
        max_01a : Max output current in units of 0.1 A
                  (e.g. 100 = 10.0 A). uint16.

    Returns:
        Packed centrifuge_bldc_cmd payload bytes.
    '''
    inner = struct.pack('<H', max_01a)
    return pack_centrifuge_bldc_cmd(seq, BLDC_SET_MAX_CURRENT, inner)


def pack_bldc_get_max_current(seq: int) -> bytes:
    '''Pack centrifuge_bldc_cmd payload for GET_MAX_CURRENT (0x0050, V1.03).

    Args:
        seq: Sequence number.

    Returns:
        Packed centrifuge_bldc_cmd payload bytes.
    '''
    return pack_centrifuge_bldc_cmd(seq, BLDC_GET_MAX_CURRENT, b'')


def unpack_bldc_max_current(raw_data_hex: str) -> dict:
    '''Unpack the 2-byte data field from a GET_MAX_CURRENT response.

    Args:
        raw_data_hex: Hex string from unpack_rsp_centrifuge_bldc
            result["data"] (e.g. "6400").

    Returns:
        Dict with max_01a (uint16) and max_a (float, amps).
    '''
    raw = bytes.fromhex(raw_data_hex) if raw_data_hex else b''
    if len(raw) < 2:
        return {}
    max_01a = struct.unpack_from('<H', raw)[0]
    return {
        'max_01a': max_01a,
        'max_a': max_01a / 10.0,
    }


def pack_centrifuge_power(
        seq: int,
        enable: bool,
) -> bytes:
    '''Pack CMD_CENTRIFUGE_POWER payload.

    Enables or disables the 24V centrifuge motor supply via CENT_POW_EN
    (PA5 on the Ultra carrier STM32H735ZGT6).

    Args:
        seq: Sequence number.
        enable: True to switch 24V supply ON, False to switch it OFF.
    '''
    return struct.pack('<IB', seq, 1 if enable else 0)


def pack_centrifuge_goto(
        seq: int,
        angle_open_initial_deg: int = 290,
        move_rpm: int = 1,
) -> bytes:
    '''Pack CMD_CFUGE_GOTO_SERUM / PIPETTE / BLISTER payload.
    Wire: seq(4) + angle_open_initial_deg(2) + move_rpm(2) = 8 bytes.
    Firmware derives: pipette=init-90, serum=init-180, blister=init-270.
    '''
    return struct.pack(
        '<IHH',
        seq,
        angle_open_initial_deg & 0xFFFF,
        move_rpm & 0xFFFF,
    )


def pack_centrifuge_sequence(
        seq: int,
        angle_open_initial_deg: int = 290,
        angle_open_end_deg: int = 0,
        angle_lock_initial_deg: int = 0,
        angle_lock_end_deg: int = 0,
        angle_extra_deg: int = 15,
        lift_high_01mm: int = 380,
        lift_mid_01mm: int = 280,
        move_rpm: int = 1,
) -> bytes:
    '''Pack CMD_CENTRIFUGE_UNLOCK/LOCK/REVERSE shared payload.

    Wire format (little-endian, 19 bytes total):
        uint32  seq
        uint16  angle_open_initial_deg  (e.g. 290)
        uint16  angle_open_end_deg      (0 = auto: open_init - 30)
        uint16  angle_lock_initial_deg  (0 = auto: open_init - 210)
        uint16  angle_lock_end_deg      (0 = auto: open_init - 180)
        uint8   angle_extra_deg         (e.g. 15)
        uint16  lift_high_01mm          (e.g. 380 = 38.0 mm)
        uint16  lift_mid_01mm           (e.g. 280 = 28.0 mm)
        uint16  move_rpm                (e.g. 1)

    When open_end / lock_initial / lock_end are 0, firmware derives them
    from angle_open_initial_deg.

    Returns:
        Packed payload bytes (19 bytes).
    '''
    return struct.pack(
        '<IHHHHBHHH',
        seq,
        angle_open_initial_deg & 0xFFFF,
        angle_open_end_deg     & 0xFFFF,
        angle_lock_initial_deg & 0xFFFF,
        angle_lock_end_deg     & 0xFFFF,
        angle_extra_deg        & 0xFF,
        lift_high_01mm         & 0xFFFF,
        lift_mid_01mm          & 0xFFFF,
        move_rpm               & 0xFFFF,
    )


def unpack_rsp_centrifuge_sequence(data: bytes) -> dict:
    '''Unpack RSP_CENTRIFUGE_UNLOCK/LOCK/REVERSE response payload.

    Wire format: uint32 seq, uint8 error (5 bytes total).

    Returns:
        dict with keys: seq, error, ok (True when error == 0).
    '''
    if len(data) < 5:
        return {'ok': False, 'error': 0xFF, 'msg': 'response too short'}
    seq, error = struct.unpack_from('<IB', data, 0)
    return {'seq': seq, 'error': error, 'ok': error == 0}


def pack_set_value(seq: int, value: int) -> bytes:
    '''Pack CMD_SET_SPEED or CMD_SET_ACCEL payload.'''
    return struct.pack('<Ii', seq, value)


def pack_lld_perform(
        seq: int,
        threshold: int = 10,
        z_start: int = 0,
        z_bottom: int = GANTRY_Z_MIN_POS,
        timeout_ms: int = 20000,
        z_speed_sps: int = 0,
) -> bytes:
    '''Pack CMD_LLD_PERFORM payload.

    Wire layout matches proto_cmd_lld_perform_t (19 bytes):
        seq(4) threshold(1) z_start(4) z_bottom(4) timeout_ms(4) z_speed_sps(2)

    Args:
        seq: Sequence number.
        threshold: LLD pressure-rise threshold (firmware units).
        z_start: Z position to begin descent (µsteps, typically 0 or negative).
        z_bottom: Hard bottom limit (µsteps, negative = downward).
                  Default is GANTRY_Z_MIN_POS (−16404 µsteps ≈ −25.0 mm),
                  the actual hardware travel limit.
        timeout_ms: Detection timeout in milliseconds.
        z_speed_sps: Z descent speed in steps/s (0 = firmware default).
    '''
    return struct.pack(
        '<IBiiIH',
        seq, threshold, z_start, z_bottom, timeout_ms, z_speed_sps,
    )


def pack_llf_start(
        seq: int,
        well_id: int = WELL_ID_AUTO,
        z_speed_sps: int = 0,
) -> bytes:
    '''Pack CMD_LLF_START payload.

    Wire layout matches proto_cmd_llf_start_t (7 bytes):
        seq(4) well_id(1) z_speed_sps(2)

    Args:
        seq: Sequence number.
        well_id: Index into firmware k_well_geometry[] lookup table.
        z_speed_sps: Z-follow speed in steps/s (0 = firmware default).
    '''
    return struct.pack('<IBH', seq, well_id, z_speed_sps)


LIQUID_FLAG_STREAM      = 0x01
LIQUID_FLAG_FOIL_DETECT = 0x02

def pack_smart_aspirate(
        seq: int,
        volume_ul: int,
        pump_speed_ul_s: float = 100.0,
        lld_threshold: int = 20,
        z_entry: int = 0,
        z_bottom: int = GANTRY_Z_MIN_POS,
        z_speed_sps: int = 0,
        well_id: int = WELL_ID_AUTO,
        air_slug_ul: int = 0,
        stream: bool = False,
        foil_detect: bool = True,
        foil_pierce_um: int = 0,
        foil_pierce_speed_sps: int = 0,
) -> bytes:
    '''Pack CMD_SMART_ASPIRATE payload.

    Base wire layout matches proto_cmd_smart_aspirate_t (29 bytes):
        seq(4) volume_ul(4) pump_speed_ul_s(4f) lld_threshold(1)
        z_entry(4) z_bottom(4) z_speed_sps(2) well_id(1) air_slug_ul(4)
        flags(1)

    Extended wire layout (35 bytes) appends:
        foil_pierce_um(4i) foil_pierce_speed_sps(2)

    This packer always emits the 35-byte extended form.  When the
    foil_pierce_* fields are 0 the firmware falls back to its own
    constants (FOIL_PIERCE_MM / FOIL_PIERCE_SPEED_SPS) so older
    callers that don't set them behave identically to before.  Older
    firmware that only accepts the 29-byte base form will reject the
    frame; deploy firmware and RPi together.

    Args:
        seq: Sequence number.
        volume_ul: Volume to aspirate in µL.
        pump_speed_ul_s: Pump aspiration speed in µL/s (float).
        lld_threshold: LLD pressure-rise threshold (firmware units).
        z_entry: Z position to start LLD descent (µsteps).
        z_bottom: Hard bottom limit (µsteps, negative = downward).
                  Default is GANTRY_Z_MIN_POS (−16404 µsteps ≈ −25.0 mm),
                  the actual hardware travel limit.
        z_speed_sps: Z descent/follow speed in steps/s
            (0 = firmware default).
        well_id: Index into firmware k_well_geometry[]
            lookup table.
        air_slug_ul: Air slug volume (µL) to aspirate
            before LLD; 0 = skip.
        stream: Enable real-time pressure streaming
            (MSG_PRESSURE 0xA004).
        foil_detect: When True, firmware assumes foil is
            intact and always punctures + re-detects liquid.
            Default True (safe for unaccessed wells).
        foil_pierce_um: Puncture stroke depth in µm (relative descent
            below foil-contact Z).  0 = firmware default
            (FOIL_PIERCE_MM, typically 2.0 mm).
        foil_pierce_speed_sps: Puncture Z speed in steps/s.
            0 = firmware default (FOIL_PIERCE_SPEED_SPS,
            typically 2000 sps).
    '''
    flags = 0
    if stream:
        flags |= LIQUID_FLAG_STREAM
    if foil_detect:
        flags |= LIQUID_FLAG_FOIL_DETECT
    return struct.pack(
        '<IIfBiiHBIBiH',
        seq, volume_ul, float(pump_speed_ul_s), lld_threshold,
        z_entry, z_bottom, z_speed_sps, well_id, air_slug_ul, flags,
        int(foil_pierce_um), int(foil_pierce_speed_sps),
    )


# Well dispense flag bits
WELL_DISP_FLAG_BLOWOUT = 0x01


def pack_well_dispense(
        seq: int,
        z_depth_mm: int = 0,
        volume_ul: int = 0,
        speed_ul_s: float = 100.0,
        z_retract_mm: int = 5,
        blowout: bool = True,
) -> bytes:
    '''Pack CMD_WELL_DISPENSE payload.

    Wire layout matches proto_cmd_well_dispense_t (17 bytes):
        seq(4) z_depth_mm(2) volume_ul(4) speed_ul_s(4f)
        z_retract_mm(2) flags(1)

    Args:
        seq: Sequence number.
        z_depth_mm: Dispense depth in mm (positive down); 0 = max depth.
        volume_ul: Dispense volume in µL.
        speed_ul_s: Dispense speed in µL/s (float, default 100).
        z_retract_mm: Z retract before piston reset in mm (default 5).
        blowout: If True, retract Z and piston reset after dispense.
    '''
    flags = WELL_DISP_FLAG_BLOWOUT if blowout else 0
    return struct.pack(
        '<IHIfHB',
        seq, z_depth_mm, volume_ul, float(speed_ul_s),
        z_retract_mm, flags,
    )


def pack_cart_dispense_bf(
        seq: int,
        duration_s: int = 170,
        vel_ul_s: float = 1.0,
        for_vol_ul: int = 60,
        back_vol_ul: int = 30,
        reasp_ul: int = 12,
        sleep_s: int = 30,
        z_retract_mm: int = 2,
        stream: bool = False,
) -> bytes:
    '''Pack CMD_CART_DISPENSE_BF payload.

    Wire layout matches proto_cmd_cart_dispense_bf_t (22 bytes):
        seq(4) duration_s(2) _reserved(2) vel_ul_s(4f) for_vol_ul(2)
        back_vol_ul(2) reasp_ul(2) sleep_s(2) z_retract_mm(2) flags(1)

    Caller handles XY and Z positioning before this command.

    Args:
        seq: Sequence number.
        duration_s: Total B&F duration in seconds (default 170).
        vel_ul_s: Dispense/aspirate speed in µL/s (float, default 1).
        for_vol_ul: Forward dispense per cycle in µL (default 60).
        back_vol_ul: Backward aspirate per cycle in µL (default 30).
        reasp_ul: Re-aspirate volume after dwell; 0 = skip (default 12).
        sleep_s: Dwell after dispense in seconds (default 30).
        z_retract_mm: Z retract for reasp in mm (default 2).
        stream: Enable real-time pressure streaming (MSG_PRESSURE 0xA004).
    '''
    flags = LIQUID_FLAG_STREAM if stream else 0
    return struct.pack(
        '<IHHfHHHHHB',
        seq, duration_s, 0, float(vel_ul_s),
        for_vol_ul, back_vol_ul, reasp_ul, sleep_s,
        z_retract_mm, flags,
    )


def pack_cart_dispense(
        seq: int,
        volume_ul: int,
        vel_ul_s: float = 1.0,
        reasp_ul: int = 12,
        sleep_s: int = 0,
        z_retract_mm: int = 2,
        stream: bool = False,
) -> bytes:
    '''Pack CMD_CART_DISPENSE payload.

    Wire layout matches proto_cmd_cart_dispense_t (18 bytes):
        seq(4) volume_ul(4) vel_ul_s(4f) reasp_ul(2)
        sleep_s(2) z_retract_mm(2) flags(1)

    Caller handles XY and Z positioning before this command.

    Args:
        seq: Sequence number.
        volume_ul: Volume to dispense in µL.
        vel_ul_s: Dispense speed in µL/s (float, default 1).
        reasp_ul: Re-aspirate volume after dwell; 0 = skip (default 12).
        sleep_s: Dwell after dispense in seconds (default 0).
        z_retract_mm: Z retract for reasp in mm (default 2).
        stream: Enable real-time pressure streaming (MSG_PRESSURE 0xA004).
    '''
    flags = LIQUID_FLAG_STREAM if stream else 0
    return struct.pack(
        '<IIfHHHB',
        seq, volume_ul, float(vel_ul_s),
        reasp_ul, sleep_s, z_retract_mm, flags,
    )


def pack_tip_mix(
        seq: int,
        mix_vol_ul: int = 150,
        speed_ul_s: float = 100.0,
        cycles: int = 4,
        pull_vol_ul: int = 0,
) -> bytes:
    '''Pack CMD_TIP_MIX payload.

    Wire layout matches proto_cmd_tip_mix_t (15 bytes):
        seq(4) mix_vol_ul(2) speed_ul_s(4f) cycles(1) pull_vol_ul(2)

    Caller handles gantry Z positioning before this command.

    Args:
        seq: Sequence number.
        mix_vol_ul: Volume per aspirate/dispense cycle in µL (default 150).
        speed_ul_s: Pump speed in µL/s (float, default 100).
        cycles: Number of asp+disp cycles (default 4).
        pull_vol_ul: Final aspirate volume to retain in tip; 0 = skip.
    '''
    return struct.pack(
        '<IHfBH',
        seq, mix_vol_ul, float(speed_ul_s), cycles, pull_vol_ul,
    )


def pack_fc_heater_set_duty(seq: int, pct: int) -> bytes:
    '''Pack CMD_FC_HEATER_SET_DUTY payload. pct = 0-100.'''
    return struct.pack('<IB', seq, pct)


def pack_fc_heater_set_en(seq: int, enable: bool) -> bytes:
    '''Pack CMD_FC_HEATER_SET_EN payload.'''
    return struct.pack('<IB', seq, 1 if enable else 0)


def pack_fc_heater_get_status(seq: int) -> bytes:
    '''Pack CMD_FC_HEATER_GET_STATUS payload (seq only).'''
    return struct.pack('<I', seq)


def pack_fc_heater_set_ctrl(
        seq: int,
        setpoint_x10: int,
        kp_x1000: int,
        ki_x1000: int,
        kd_x1000: int,
        enable: bool,
) -> bytes:
    '''Pack CMD_FC_HEATER_SET_CTRL payload (13 bytes).

    Wire layout: seq(4) + setpoint_x10(2) + kp_x1000(2) +
                 ki_x1000(2) + kd_x1000(2) + enable(1).
    '''
    return struct.pack(
        '<IHHHHB', seq,
        setpoint_x10, kp_x1000, ki_x1000, kd_x1000,
        1 if enable else 0,
    )


def unpack_fc_heater_status(data: bytes) -> dict:
    '''Unpack FC_HEATER_GET_STATUS response (0x9F03).

    proto_rsp_fc_heater_status_t (20 bytes packed):
        seq(4) + error(1) + temp_x100(2) + heater_duty(1) +
        heater_en(1) + otp(1) + ctrl_enabled(1) +
        ctrl_setpoint_x10(2) + ctrl_kp_x1000(2) +
        ctrl_ki_x1000(2) + ctrl_kd_x1000(2) + ctrl_heating(1)
    '''
    if len(data) < 20:
        return {'seq': 0, 'error': 0xFF}
    (seq, error, temp_x100, h_duty, h_en, otp,
     ctrl_en, ctrl_sp_x10, ctrl_kp, ctrl_ki, ctrl_kd,
     ctrl_heating) = struct.unpack_from(
        '<IBhBBBBHHHHB', data,
    )
    return {
        'seq': seq,
        'error': error,
        'temp_c': temp_x100 / 100.0,
        'heater_duty': h_duty,
        'heater_en': bool(h_en),
        'otp': bool(otp),
        'ctrl_enabled': bool(ctrl_en),
        'ctrl_setpoint_c': ctrl_sp_x10 / 10.0,
        'ctrl_kp': ctrl_kp / 1000.0,
        'ctrl_ki': ctrl_ki / 1000.0,
        'ctrl_kd': ctrl_kd / 1000.0,
        'ctrl_heating': bool(ctrl_heating),
    }


# =============================================================================
# Payload unpack helpers (response payloads)
# =============================================================================

def unpack_rsp_common(data: bytes) -> dict:
    '''Unpack common response header (seq + error).'''
    if len(data) < 5:
        return {'seq': 0, 'error': 0xFF}
    seq, error = struct.unpack_from('<IB', data)
    return {'seq': seq, 'error': error}


def unpack_rsp_ping(data: bytes) -> dict:
    '''Unpack RSP_PING payload.'''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, ts = struct.unpack_from('<IBI', data)
    return {'seq': seq, 'error': error, 'timestamp_ms': ts}


def unpack_rsp_version(data: bytes) -> dict:
    '''Unpack RSP_GET_VERSION payload.'''
    if len(data) < 24:
        return unpack_rsp_common(data)
    seq, error, major, minor, patch = struct.unpack_from(
        '<IBBBB', data,
    )
    build_raw = data[8:24]
    build = build_raw.split(b'\x00', 1)[0].decode(
        'ascii', errors='replace',
    )
    return {
        'seq': seq, 'error': error,
        'major': major, 'minor': minor,
        'patch': patch, 'build': build,
    }


def unpack_rsp_flags(data: bytes) -> dict:
    '''Unpack RSP_GET_FLAGS payload.'''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, flags = struct.unpack_from('<IBI', data)
    return {'seq': seq, 'error': error, 'flags': flags}


def unpack_rsp_position(data: bytes) -> dict:
    '''Unpack RSP_GET_POSITION (0x9005) payload.

    Payload: seq(4) + error(1) + x(4) + y(4) + z(4) = 17 bytes.
    Returns gantry X, gantry Y, gantry Z (all in µsteps).
    Lift position is not in this response; use LIFT_STATUS (0x8B04).
    Optional z_axis (4 bytes) at offset 17 if firmware extends format.
    '''
    if len(data) < 17:
        return unpack_rsp_common(data)
    seq, error, x, y, z = struct.unpack_from('<IBiii', data)
    z_axis = 0
    if len(data) >= 21:
        z_axis = struct.unpack_from('<i', data, 17)[0]
    return {
        'seq': seq, 'error': error,
        'x': x, 'y': y, 'z': z, 'z_axis': z_axis,
    }


def unpack_rsp_gantry_status(data: bytes) -> dict:
    '''Unpack RSP_GET_GANTRY_STATUS (0x9205) payload.

    Payload: seq(4)+error(1)+x_homed(1)+y_homed(1)+z_homed(1)+
             x(4)+y(4)+z(4)+sensors(1) = 21 bytes.

    sensor_bits:
      bit 0 — y_home  (YSW1, beam blocked = at home)
      bit 1 — y_front (YSW2, beam blocked = at front limit)
      bit 2 — z_home  (Z home sensor triggered)
      bit 3 — x_mid   (XMID_HOME, P6 on X-PCBA PCAL6408A)
      bit 4 — x_end   (XEND_HOME, P5 on X-PCBA PCAL6408A)
    '''
    if len(data) < 20:
        return unpack_rsp_common(data)
    seq, error, x_homed, y_homed, z_homed, x, y, z = (
        struct.unpack_from('<IBBBBiii', data)
    )
    sensors = 0
    if len(data) >= 21:
        sensors = struct.unpack_from('<B', data, 20)[0]
    return {
        'seq': seq, 'error': error,
        'x_homed': bool(x_homed), 'y_homed': bool(y_homed),
        'z_homed': bool(z_homed),
        'x': x, 'y': y, 'z': z,
        'y_home':  bool(sensors & 0x01),
        'y_front': bool(sensors & 0x02),
        'z_home':  bool(sensors & 0x04),
        'x_mid':   bool(sensors & 0x08),
        'x_end':   bool(sensors & 0x10),
    }


def unpack_rsp_read_z_drv(data: bytes) -> dict:
    '''Unpack RSP_READ_Z_DRV (0x9211) payload.
    Wire: seq(4) + error(1) + uart_ok(1) + drv_status(4) = 10 bytes.'''
    if len(data) < 10:
        return unpack_rsp_common(data)
    seq, error, uart_ok, drv_status = struct.unpack_from('<IBBI', data)
    return {
        'seq': seq, 'error': error,
        'uart_ok': bool(uart_ok),
        'drv_status': drv_status,
    }


def pack_set_motor_telem(seq: int, enable: bool) -> bytes:
    '''Pack CMD_SET_MOTOR_TELEM (0x8213) payload.
    Wire: seq(4) + enable(1) = 5 bytes.'''
    return struct.pack('<IB', seq, int(enable))


def _unpack_axis_status(data: bytes, offset: int) -> dict:
    '''Unpack a single axis status block (4 bytes).'''
    cs, stst, pwm, faults = struct.unpack_from(
        '<BBBB', data, offset,
    )
    return {
        'cs_actual': cs,
        'stst': bool(stst),
        'pwm_scale_sum': pwm,
        'faults': {
            'otpw': bool(faults & 0x01),
            'ot':   bool(faults & 0x02),
            's2ga': bool(faults & 0x04),
            's2gb': bool(faults & 0x08),
            'ola':  bool(faults & 0x10),
            'olb':  bool(faults & 0x20),
        },
    }


def unpack_rsp_motor_status(data: bytes) -> dict:
    '''Unpack RSP_GET_MOTOR_STATUS (0x9212) payload.
    Wire: seq(4) + error(1) + x(4) + y(4) + z(4) = 17 bytes.'''
    if len(data) < 17:
        return unpack_rsp_common(data)
    seq, error = struct.unpack_from('<IB', data)
    return {
        'seq': seq,
        'error': error,
        'x': _unpack_axis_status(data, 5),
        'y': _unpack_axis_status(data, 9),
        'z': _unpack_axis_status(data, 13),
    }


def unpack_msg_motor_telemetry(data: bytes) -> dict:
    '''Unpack MSG_MOTOR_TELEMETRY (0xA00A) async payload.
    Wire: elapsed_ms(4) + x_cs(1) + x_pwm(1) + y_cs(1) + y_pwm(1)
          + z_cs(1) + z_pwm(1) = 10 bytes.'''
    if len(data) < 10:
        return {'raw': data.hex()}
    elapsed, = struct.unpack_from('<I', data)
    x_cs, x_pwm = struct.unpack_from('<BB', data, 4)
    y_cs, y_pwm = struct.unpack_from('<BB', data, 6)
    z_cs, z_pwm = struct.unpack_from('<BB', data, 8)
    return {
        'elapsed_ms': elapsed,
        'x': {'cs_actual': x_cs, 'pwm_scale_sum': x_pwm},
        'y': {'cs_actual': y_cs, 'pwm_scale_sum': y_pwm},
        'z': {'cs_actual': z_cs, 'pwm_scale_sum': z_pwm},
    }


def unpack_rsp_sensors(data: bytes) -> dict:
    '''Unpack RSP_GET_SENSORS payload.'''
    if len(data) < 11:
        return unpack_rsp_common(data)
    seq, error, p, t, h = struct.unpack_from('<IBhhH', data)
    return {
        'seq': seq, 'error': error,
        'pressure_raw': p, 'temp_c_x10': t,
        'humidity_pct_x10': h,
    }


def unpack_rsp_pump_data(data: bytes) -> dict:
    '''Unpack response with pump integral data.'''
    if len(data) < 15:
        return unpack_rsp_common(data)
    seq, error, batch, rt, rate = struct.unpack_from(
        '<IBiiH', data,
    )
    return {
        'seq': seq, 'error': error,
        'batch_integral': batch / 100.0,
        'rt_integral': rt / 100.0,
        'pump_rate_hz': rate / 10.0,
    }


def unpack_rsp_lld_result(data: bytes) -> dict:
    '''Unpack RSP_LLD_PERFORM payload.

    Wire layout matches proto_rsp_lld_result_t (20 bytes):
        seq(4) error(1) detected(1) z_position(4) time_ms(4)
        pressure_delta(2) sample_count(4)
    '''
    if len(data) < 20:
        return unpack_rsp_common(data)
    (
        seq, error, detected, z_pos, time_ms, p_delta, sample_count,
    ) = struct.unpack_from('<IBBiIhI', data)
    return {
        'seq': seq,
        'error': error,
        'detected': bool(detected),
        'z_position': z_pos,
        'time_ms': time_ms,
        'pressure_delta': p_delta,
        'sample_count': sample_count,
        'status': 'OK' if error == 0 else 'ERROR',
    }


def unpack_rsp_smart_aspirate(data: bytes) -> dict:
    '''Unpack RSP_SMART_ASPIRATE payload.

    Wire layout matches proto_rsp_smart_aspirate_t (17 bytes):
        seq(4) error(1) lld_z(4) final_z(4) sample_count(4)
    '''
    if len(data) < 17:
        return unpack_rsp_common(data)
    (
        seq, error, lld_z, final_z, sample_count,
    ) = struct.unpack_from('<IBiiI', data)
    return {
        'seq': seq,
        'error': error,
        'lld_z': lld_z,
        'final_z': final_z,
        'sample_count': sample_count,
        'status': 'OK' if error == 0 else 'ERROR',
    }


def unpack_rsp_well_dispense(data: bytes) -> dict:
    '''Unpack RSP_WELL_DISPENSE payload (9 bytes).'''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, final_z = struct.unpack_from('<IBi', data)
    return {
        'seq': seq,
        'error': error,
        'final_z': final_z,
        'status': 'OK' if error == 0 else 'ERROR',
    }


def unpack_rsp_cart_dispense_bf(data: bytes) -> dict:
    '''Unpack RSP_CART_DISPENSE_BF payload (9 bytes).'''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, final_z = struct.unpack_from('<IBi', data)
    return {
        'seq': seq,
        'error': error,
        'final_z': final_z,
        'status': 'OK' if error == 0 else 'ERROR',
    }


def unpack_rsp_cart_dispense(data: bytes) -> dict:
    '''Unpack RSP_CART_DISPENSE payload (9 bytes).'''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, final_z = struct.unpack_from('<IBi', data)
    return {
        'seq': seq,
        'error': error,
        'final_z': final_z,
        'status': 'OK' if error == 0 else 'ERROR',
    }


def unpack_rsp_tip_mix(data: bytes) -> dict:
    '''Unpack RSP_TIP_MIX payload (5 bytes: seq + error).'''
    return unpack_rsp_common(data)


def unpack_rsp_centrifuge_status(data: bytes) -> dict:
    '''Unpack RSP_CENTRIFUGE_STATUS payload.'''
    if len(data) < 21:
        return unpack_rsp_common(data)
    (
        seq, error, driver_online, state,
        rpm, angle, vbus, temp, eflags,
    ) = struct.unpack_from('<IBBBiiHhH', data)
    return {
        'seq': seq,
        'error': error,
        'driver_online': bool(driver_online),
        'state': state,
        'rpm': rpm,
        'angle_001deg': angle,
        'vbus_01v': vbus,
        'temp_001c': temp,
        'error_flags': f'0x{eflags:04X}',
    }


def unpack_rsp_centrifuge_angle(data: bytes) -> dict:
    '''Unpack RSP_CENTRIFUGE_MOVE_ANGLE payload.'''
    if len(data) < 13:
        return unpack_rsp_common(data)
    seq, error, target, actual = struct.unpack_from(
        '<IBii', data,
    )
    return {
        'seq': seq,
        'error': error,
        'target_001deg': target,
        'actual_001deg': actual,
        'target_deg': target / 100.0,
        'actual_deg': actual / 100.0,
    }


def unpack_rsp_centrifuge_home(data: bytes) -> dict:
    '''Unpack RSP_CENTRIFUGE_HOME payload.'''
    if len(data) < 11:
        return unpack_rsp_common(data)
    seq, error, astate, offset, success = struct.unpack_from(
        '<IBBiB', data,
    )
    return {
        'seq': seq,
        'error': error,
        'align_state': astate,
        'offset': offset,
        'success': bool(success),
    }


def unpack_rsp_centrifuge_bldc(data: bytes) -> dict:
    '''Unpack RSP_CENTRIFUGE_BLDC_CMD payload.'''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, bcmd, ok, dlen = struct.unpack_from(
        '<IBHBB', data,
    )
    d = data[9:9 + dlen] if dlen > 0 else b''
    return {
        'seq': seq,
        'error': error,
        'bldc_cmd': f'0x{bcmd:04X}',
        'ok': bool(ok),
        'data': d.hex() if d else '',
    }


def unpack_rsp_lift_home(data: bytes) -> dict:
    '''Unpack RSP_LIFT_HOME (0x9B01) payload.

    Matches proto_rsp_lift_home_t:
        seq(4) + error(1) + success(1) + position_steps(4).

    Args:
        data: Raw response payload bytes.

    Returns:
        Dict with seq, error, success, position_steps.
    '''
    if len(data) < 10:
        return unpack_rsp_common(data)
    seq, error, success, pos = struct.unpack_from(
        '<IBBi', data,
    )
    return {
        'seq': seq,
        'error': error,
        'success': bool(success),
        'position_steps': pos,
    }


def unpack_rsp_lift_move(data: bytes) -> dict:
    '''Unpack RSP_LIFT_MOVE (0x9B03) payload.

    Matches proto_rsp_lift_move_t:
        seq(4) + error(1) + position_steps(4).

    Args:
        data: Raw response payload bytes.

    Returns:
        Dict with seq, error, position_steps.
    '''
    if len(data) < 9:
        return unpack_rsp_common(data)
    seq, error, pos = struct.unpack_from('<IBi', data)
    return {
        'seq': seq,
        'error': error,
        'position_steps': pos,
    }


def unpack_rsp_lift_status(data: bytes) -> dict:
    '''Unpack RSP_LIFT_STATUS (0x9B04) payload.

    Matches proto_rsp_lift_status_t:
        seq(4) + error(1) + is_homed(1) +
        position_steps(4) + at_home(1) + at_top(1) +
        current_pct(1).

    Args:
        data: Raw response payload bytes.

    Returns:
        Dict with seq, error, is_homed, position_steps,
        at_home, at_top, current_pct.
    '''
    if len(data) < 12:
        return unpack_rsp_common(data)
    seq, error, is_homed, pos, at_home, at_top = (
        struct.unpack_from('<IBBiBB', data)
    )
    current_pct = struct.unpack_from('<B', data, 12)[0] if len(data) >= 13 else 0
    return {
        'seq': seq,
        'error': error,
        'is_homed': bool(is_homed),
        'position_steps': pos,
        'at_home': bool(at_home),
        'at_top': bool(at_top),
        'current_pct': current_pct,
    }


def unpack_rsp_door(data: bytes) -> dict:
    '''Unpack door response (0x9601 / 0x9602 / 0x9603) payload.

    Matches proto_rsp_door_t:
        seq(4) + error(1) + is_open(1) + is_closed(1) + is_moving(1)
        = 8 bytes.

    Args:
        data: Raw response payload bytes.

    Returns:
        Dict with seq, error, is_open, is_closed, is_moving.
    '''
    if len(data) < 8:
        return unpack_rsp_common(data)
    seq, error, is_open, is_closed, is_moving = struct.unpack_from(
        '<IBBBB', data,
    )
    return {
        'seq': seq,
        'error': error,
        'is_open': bool(is_open),
        'is_closed': bool(is_closed),
        'is_moving': bool(is_moving),
    }


def unpack_msg_status(data: bytes) -> dict:
    '''Unpack MSG_STATUS async payload.

    Base size is 36 bytes.  When the z_axis_pos field is present
    (40 bytes) it is decoded; otherwise defaults to 0.
    '''
    if len(data) < 36:
        return {}
    offset = 0
    ts = struct.unpack_from('<I', data, offset)[0]; offset += 4
    main_st = data[offset]; offset += 1
    sub_st = data[offset]; offset += 1
    progress = data[offset]; offset += 1
    flags_raw = struct.unpack_from('<I', data, offset)[0]
    offset += 4
    gx = struct.unpack_from('<i', data, offset)[0]; offset += 4
    gy = struct.unpack_from('<i', data, offset)[0]; offset += 4
    gz = struct.unpack_from('<i', data, offset)[0]; offset += 4
    motion = data[offset]; offset += 1
    pressure = struct.unpack_from('<h', data, offset)[0]
    offset += 2
    pump_pos = struct.unpack_from('<H', data, offset)[0]
    offset += 2
    temp = struct.unpack_from('<h', data, offset)[0]; offset += 2
    rpm = struct.unpack_from('<H', data, offset)[0]; offset += 2
    tip_f = data[offset]; offset += 1
    door_f = data[offset]; offset += 1
    last_err = data[offset]; offset += 1
    err_cnt = data[offset]; offset += 1

    z_axis_pos = 0
    if len(data) >= 40:
        z_axis_pos = struct.unpack_from(
            '<i', data, offset
        )[0]

    return {
        'timestamp_ms': ts,
        'main_state': main_st,
        'sub_state': sub_st,
        'progress': progress,
        'flags_raw': flags_raw,
        'gantry_x': gx, 'gantry_y': gy, 'lift_z': gz,
        'z_axis_pos': z_axis_pos,
        'gantry_moving': bool(motion & 0x01),
        'lift_moving': bool(motion & 0x02),
        'gantry_homed': bool(motion & 0x04),
        'lift_homed': bool(motion & 0x08),
        'z_axis_homed': bool(motion & 0x10),
        'pressure_raw': pressure,
        'pump_position': pump_pos,
        'temp_c_x10': temp,
        'centrifuge_rpm': rpm,
        'tip_attached': bool(tip_f & 0x01),
        'tip_well_id': (tip_f >> 1) & 0x7F,
        'door_open': bool(door_f & 0x01),
        'door_closed': bool(door_f & 0x02),
        'drawer_locked': bool(door_f & 0x04),
        'last_error': last_err,
        'error_count': err_cnt,
    }


def unpack_msg_pressure(data: bytes) -> dict:
    '''Unpack MSG_PRESSURE async payload (8 bytes).

    The firmware packs timestamp in 100 µs units (pump clock).
    We convert to true milliseconds here so downstream code can
    divide by 1000 to get seconds.
    '''
    if len(data) < 8:
        return {}
    ts_100us, pressure, pos = struct.unpack_from('<IhH', data)
    return {
        'timestamp_ms': ts_100us / 10.0,
        'pressure_raw': pressure,
        'pump_position': pos,
    }


def unpack_msg_event(data: bytes) -> dict:
    '''Unpack MSG_EVENT async payload.'''
    if len(data) < 5:
        return {}
    ts = struct.unpack_from('<I', data)[0]
    event_type = data[4]
    detail = ''
    if len(data) > 5:
        detail = data[5:].split(b'\x00', 1)[0].decode(
            'ascii', errors='replace',
        )
    return {
        'timestamp_ms': ts,
        'event_type': event_type,
        'detail': detail,
    }


def unpack_msg_pump_done(data: bytes) -> dict:
    '''Unpack MSG_PUMP_DONE (0xA007) async payload.

    Wire layout (proto_msg_pump_done_t):
      cmd_id (2B LE) + seq (4B LE) + error (1B)
    '''
    if len(data) < 7:
        return {}
    cmd_id, seq, error = struct.unpack_from('<HIB', data)
    return {
        'cmd_id': cmd_id,
        'seq':    seq,
        'error':  error,
    }


def unpack_msg_gantry_done(data: bytes) -> dict:
    '''Unpack MSG_GANTRY_DONE (0xA008) async payload.

    Wire layout (proto_msg_gantry_done_t):
      cmd_id (2B LE) + seq (4B LE) + error (1B)
    '''
    if len(data) < 7:
        return {}
    cmd_id, seq, error = struct.unpack_from('<HIB', data)
    return {
        'cmd_id': cmd_id,
        'seq':    seq,
        'error':  error,
    }


def unpack_msg_lift_done(data: bytes) -> dict:
    '''Unpack MSG_LIFT_DONE (0xA009) async payload.

    Wire layout (proto_msg_lift_done_t):
      cmd_id (2B LE) + seq (4B LE) + error (1B) + position_steps (4B LE)
    '''
    if len(data) < 11:
        return {}
    cmd_id, seq, error, pos = struct.unpack_from('<HIBi', data)
    return {
        'cmd_id': cmd_id,
        'seq':    seq,
        'error':  error,
        'position_steps': pos,
    }


def unpack_msg_error(data: bytes) -> dict:
    '''Unpack MSG_ERROR async payload.'''
    if len(data) < 5:
        return {}
    ts = struct.unpack_from('<I', data)[0]
    error_code = data[4]
    msg = ''
    if len(data) > 5:
        msg = data[5:].split(b'\x00', 1)[0].decode(
            'ascii', errors='replace',
        )
    return {
        'timestamp_ms': ts,
        'error_code': error_code,
        'message': msg,
    }

# OTA firmware update (0x90xx)
CMD_FW_UPDATE_START   = 0x9001
CMD_FW_WRITE_BLOCK    = 0x9002

def pack_fw_update_start(seq: int) -> bytes:
    return struct.pack('<I', seq)

def pack_fw_write_block(seq: int, offset: int, data: bytes) -> bytes:
    return struct.pack('<II', seq, offset) + data
