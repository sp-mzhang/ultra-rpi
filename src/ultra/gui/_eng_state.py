'''Shared mutable state for the engineering STM32 interface.

Allows api_stm32, api_fc_sequence, api_firmware, and api_protocol
to share the engineering STM32 handle and camera singleton without
circular imports.
'''
from __future__ import annotations

eng_stm32: dict = {'iface': None}
camera = {'instance': None}


def get_eng_stm32():
    '''Return the engineering STM32 interface or None.'''
    return eng_stm32['iface']
