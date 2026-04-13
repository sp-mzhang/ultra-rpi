'''ultra.services.iot_provisioning.credential_manager

Secure storage for AWS IoT device credentials.
Handles saving, loading, and validation of provisioned certificates.
'''
from __future__ import annotations

import json
import logging
import os
import os.path as op
import shutil
from datetime import (
    datetime,
    timezone,
)
from typing import Any

from ultra.services.iot_provisioning import config

LOG = logging.getLogger(__name__)


class CredentialManager:
    '''Manages AWS IoT device credentials.

    Handles secure storage and retrieval of device certificates
    provisioned via AWS IoT Fleet Provisioning.

    Storage location: ~/.siphox/iot_credentials/
        device.cert.pem    - Device certificate (PEM)
        device.private.key - Private key (PEM)
        root-CA.crt        - Amazon Root CA
        metadata.json      - Provisioning metadata

    Attributes:
        credentials_dir: Path to directory holding credential files.
    '''

    def __init__(
            self,
            credentials_dir: str | None = None,
    ) -> None:
        '''Initialize credential manager.

        Args:
            credentials_dir: Override default credentials directory.
        '''
        self.credentials_dir = (
            credentials_dir or config.get_credentials_dir()
        )
        self._cert_path = op.join(
            self.credentials_dir,
            config.DEVICE_CERT_FILENAME,
        )
        self._key_path = op.join(
            self.credentials_dir,
            config.DEVICE_KEY_FILENAME,
        )
        self._ca_path = op.join(
            self.credentials_dir,
            config.CA_CERT_FILENAME,
        )
        self._metadata_path = op.join(
            self.credentials_dir,
            config.METADATA_FILENAME,
        )
        LOG.debug(
            f'CredentialManager initialized: '
            f'{self.credentials_dir}',
        )

    @property
    def cert_path(self) -> str:
        '''Path to device certificate.'''
        return self._cert_path

    @property
    def key_path(self) -> str:
        '''Path to device private key.'''
        return self._key_path

    @property
    def ca_path(self) -> str:
        '''Path to CA certificate.'''
        return self._ca_path

    def has_credentials(self) -> bool:
        '''Check if credential files exist.

        Returns:
            True if cert, key, and CA files are present.
        '''
        required = [
            self._cert_path,
            self._key_path,
            self._ca_path,
        ]
        return all(op.exists(f) for f in required)

    def has_valid_credentials(self) -> bool:
        '''Check if credentials exist and are valid.

        Returns:
            True if credential files exist and metadata is present
            with a valid thing_name.
        '''
        if not self.has_credentials():
            LOG.debug('Credentials not found')
            return False

        try:
            metadata = self.load_metadata()
            if not metadata:
                LOG.warning('Metadata file missing or invalid')
                return False
            if not metadata.get('thing_name'):
                LOG.warning('Metadata missing thing_name')
                return False
        except Exception as err:
            LOG.warning(f'Error reading metadata: {err}')
            return False

        LOG.debug('Credentials valid')
        return True

    def save_credentials(
            self,
            certificate_pem: str,
            private_key: str,
            thing_name: str,
            certificate_id: str,
            device_sn: str,
            station_id: int,
    ) -> bool:
        '''Save provisioned credentials securely.

        Args:
            certificate_pem: Device certificate in PEM format.
            private_key: Device private key in PEM format.
            thing_name: AWS IoT thing name.
            certificate_id: AWS IoT certificate ID.
            device_sn: Device serial number.
            station_id: Station ID.

        Returns:
            True if saved successfully.
        '''
        try:
            os.makedirs(
                self.credentials_dir,
                mode=config.DIR_PERMISSIONS,
                exist_ok=True,
            )
            os.chmod(self.credentials_dir, config.DIR_PERMISSIONS)

            self._write_secure_file(
                self._cert_path,
                certificate_pem,
            )
            LOG.info(f'Saved device certificate: {self._cert_path}')

            self._write_secure_file(self._key_path, private_key)
            LOG.info(f'Saved device private key: {self._key_path}')

            ca_source = config.get_ca_cert_path()
            if op.exists(ca_source):
                shutil.copy2(ca_source, self._ca_path)
                os.chmod(self._ca_path, config.FILE_PERMISSIONS)
                LOG.info(
                    f'Copied CA certificate: {self._ca_path}',
                )
            else:
                LOG.warning(
                    f'CA certificate not found at {ca_source}',
                )

            metadata: dict[str, Any] = {
                'thing_name': thing_name,
                'certificate_id': certificate_id,
                'device_sn': device_sn,
                'station_id': station_id,
                'provisioned_at': (
                    datetime.now(timezone.utc).isoformat()
                ),
                'iot_endpoint': config.get_iot_endpoint(),
                'aws_region': config.get_iot_region(),
            }
            self._write_secure_file(
                self._metadata_path,
                json.dumps(metadata, indent=2),
            )
            LOG.info(f'Saved metadata: {self._metadata_path}')
            return True

        except Exception as err:
            LOG.error(f'Failed to save credentials: {err}')
            return False

    def load_metadata(self) -> dict[str, Any] | None:
        '''Load provisioning metadata.

        Returns:
            Metadata dict or None if not found or invalid.
        '''
        if not op.exists(self._metadata_path):
            return None
        try:
            with open(self._metadata_path, 'r') as f:
                return json.load(f)
        except Exception as err:
            LOG.error(f'Failed to load metadata: {err}')
            return None

    def get_thing_name(self) -> str | None:
        '''Get thing name from metadata.

        Returns:
            Thing name string or None if not provisioned.
        '''
        metadata = self.load_metadata()
        return metadata.get('thing_name') if metadata else None

    def delete_credentials(self) -> bool:
        '''Delete all stored credentials.

        Returns:
            True if deleted successfully.
        '''
        try:
            if op.exists(self.credentials_dir):
                shutil.rmtree(self.credentials_dir)
                LOG.info(
                    f'Deleted credentials directory: '
                    f'{self.credentials_dir}',
                )
            return True
        except Exception as err:
            LOG.error(f'Failed to delete credentials: {err}')
            return False

    def _write_secure_file(self, path: str, content: str) -> None:
        '''Write content to file atomically with secure permissions.

        Args:
            path: Target file path.
            content: String content to write.
        '''
        temp_path = path + '.tmp'
        try:
            with open(temp_path, 'w') as f:
                f.write(content)
            os.chmod(temp_path, config.FILE_PERMISSIONS)
            os.rename(temp_path, path)
        finally:
            if op.exists(temp_path):
                os.unlink(temp_path)


def is_provisioned(
        credentials_dir: str | None = None,
) -> bool:
    '''Check if device has valid AWS IoT credentials.

    Args:
        credentials_dir: Override default credentials directory.

    Returns:
        True if valid credentials exist.
    '''
    manager = CredentialManager(credentials_dir=credentials_dir)
    return manager.has_valid_credentials()
