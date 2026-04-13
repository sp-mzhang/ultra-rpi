'''ultra.services.nfc_provisioning

NFC-based WiFi provisioning for Raspberry Pi devices.
Uses ST25R3918 card emulation over I2C to exchange WiFi
credentials with a mobile app.
'''
from ultra.services.nfc_provisioning.nfc_service import (
    I2C_AVAILABLE,
    NFCService,
)
from ultra.services.nfc_provisioning.provisioner import (
    NFCWiFiProvisioner,
)

__all__ = [
    'I2C_AVAILABLE',
    'NFCService',
    'NFCWiFiProvisioner',
]
