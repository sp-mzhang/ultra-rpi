'''ultra.services.iot_provisioning.provisioner

Main AWS IoT provisioning orchestrator.
Coordinates IoT client and credential manager for device provisioning.
'''
from __future__ import annotations

import os.path as op
import threading
import time
import logging

from ultra.services.iot_provisioning import config
from ultra.services.iot_provisioning.credential_manager import CredentialManager
from ultra.services.iot_provisioning.iot_client import (
    AWSIOT_AVAILABLE,
    IoTClient,
)

LOG = logging.getLogger(__name__)


class IoTProvisioner:
    '''AWS IoT provisioning orchestrator.

    Coordinates IoT client and credential manager to provision a
    device via AWS IoT fleet provisioning.

    Attributes:
        device_sn: Device serial number.
        station_id: Station ID.
        credential_manager: Manages stored device credentials.
    '''

    def __init__(
            self,
            device_sn: str = 'unknown',
            station_id: int = -1,
            test_mode: bool = False,
    ):
        '''Initialize IoT provisioner.

        Args:
            device_sn: Device serial number.
            station_id: Station ID.
            test_mode: If True, skip actual provisioning (log only).
        '''
        self.device_sn = device_sn
        self.station_id = station_id
        self.test_mode = test_mode

        self.credential_manager = CredentialManager()
        self._running = False
        self._provisioned = False
        self._background_thread: threading.Thread | None = None

        LOG.info(
            f'IoTProvisioner initialized: '
            f'device_sn={device_sn}, '
            f'station_id={station_id}, '
            f'test_mode={test_mode}',
        )

    def is_provisioned(self) -> bool:
        '''Check if device has valid credentials.

        Returns:
            True if valid credentials exist.
        '''
        return self.credential_manager.has_valid_credentials()

    def run(self) -> bool:
        '''Run provisioning process.

        Blocks until provisioning succeeds or all retries fail.

        Returns:
            True if successfully provisioned.
        '''
        if self.is_provisioned():
            thing_name = self.credential_manager.get_thing_name()
            LOG.info(f'Device already provisioned as: {thing_name}')
            return True

        if not self._check_prerequisites():
            LOG.error('Prerequisites not met -- cannot provision')
            return False

        LOG.info('Starting AWS IoT provisioning...')
        self._running = True
        self._provisioned = False

        for attempt in range(config.MAX_PROVISIONING_RETRIES):
            if not self._running:
                break
            LOG.info(
                f'Provisioning attempt '
                f'{attempt + 1}/{config.MAX_PROVISIONING_RETRIES}',
            )
            success = self._do_provisioning()
            if success:
                self._provisioned = True
                LOG.info('Provisioning successful!')
                return True

            if attempt < config.MAX_PROVISIONING_RETRIES - 1:
                wait_time = (
                    config.RETRY_BACKOFF_BASE_S * (2 ** attempt)
                )
                LOG.info(f'Retrying in {wait_time}s...')
                time.sleep(wait_time)

        LOG.warning('Provisioning failed after all retries')
        self._running = False
        return False

    def start_background_retry(self) -> None:
        '''Start background thread to retry provisioning periodically.

        Use when initial provisioning fails but device should
        continue in offline mode and retry when connectivity returns.
        '''
        if (
                self._background_thread and
                self._background_thread.is_alive()
        ):
            LOG.debug('Background retry already running')
            return

        self._running = True
        self._background_thread = threading.Thread(
            target=self._background_retry_loop,
            name='IoTProvisioningRetryThread',
            daemon=True,
        )
        self._background_thread.start()
        LOG.info('Started background provisioning retry thread')

    def stop(self) -> None:
        '''Stop provisioning and any background retry.'''
        LOG.info('Stopping IoT provisioner...')
        self._running = False

    def _check_prerequisites(self) -> bool:
        '''Check if prerequisites for provisioning are met.

        Returns:
            True if all prerequisites are satisfied.
        '''
        if not AWSIOT_AVAILABLE:
            LOG.error('AWS IoT SDK not available')
            return False

        claim_cert = config.get_claim_cert_path()
        claim_key = config.get_claim_key_path()

        if not op.exists(claim_cert):
            LOG.error(f'Claim certificate not found: {claim_cert}')
            return False

        if not op.exists(claim_key):
            LOG.error(f'Claim private key not found: {claim_key}')
            return False

        LOG.debug('Prerequisites met')
        return True

    def _do_provisioning(self) -> bool:
        '''Execute a single provisioning attempt.

        Returns:
            True if provisioning and credential save succeeded.
        '''
        if self.test_mode:
            LOG.info('=== TEST MODE: Not actually provisioning ===')
            LOG.info(f'Would provision device: {self.device_sn}')
            return True

        client = IoTClient(device_sn=self.device_sn)
        try:
            if not client.connect_with_claim():
                LOG.error(
                    'Failed to connect with claim certificate',
                )
                return False

            result = client.provision_device(
                device_sn=self.device_sn,
                station_id=self.station_id,
            )

            if not result.success:
                LOG.error(
                    f'Provisioning failed: {result.error_message}',
                )
                return False

            success = self.credential_manager.save_credentials(
                certificate_pem=result.certificate_pem or '',
                private_key=result.private_key or '',
                thing_name=result.thing_name or '',
                certificate_id=result.certificate_id or '',
                device_sn=self.device_sn,
                station_id=self.station_id,
            )

            if not success:
                LOG.error('Failed to save credentials')
                return False

            self._post_provisioning_api_calls()
            return True

        except Exception as err:
            LOG.error(f'Provisioning error: {err}')
            return False
        finally:
            client.disconnect()

    def _post_provisioning_api_calls(self) -> None:
        '''Make post-provisioning API calls (health check, register).

        Non-fatal -- if these fail provisioning is still successful.
        '''
        LOG.info('Verifying API connectivity via MQTT...')
        try:
            client = IoTClient(
                credential_manager=self.credential_manager,
                device_sn=self.device_sn,
            )
            if not client.connect_with_device_cert():
                LOG.warning(
                    'MQTT API connection failed (non-fatal)',
                )
                return
            try:
                try:
                    health = client.health_check()
                    LOG.info(f'Health check passed: {health}')
                except Exception as err:
                    LOG.warning(
                        f'Health check failed (non-fatal): {err}',
                    )
                try:
                    reg = client.register_device(
                        serial_number=self.device_sn,
                        fleet_certificate_name=(
                            'ultrathink-fleet-cert'
                        ),
                    )
                    LOG.info(f'Device registered: {reg}')
                except Exception as err:
                    LOG.warning(
                        f'Device registration failed '
                        f'(non-fatal): {err}',
                    )
            finally:
                client.disconnect()
        except Exception as err:
            LOG.warning(
                f'Post-provisioning API call failed '
                f'(non-fatal): {err}',
            )

    def _background_retry_loop(self) -> None:
        '''Background thread that retries provisioning periodically.'''
        LOG.info('Background retry loop started')

        while self._running and not self._provisioned:
            if self.is_provisioned():
                self._provisioned = True
                LOG.info(
                    'Credentials found -- background retry complete',
                )
                break

            LOG.debug(
                f'Waiting {config.BACKGROUND_RETRY_INTERVAL_S}s '
                f'before retry...',
            )
            for _ in range(int(config.BACKGROUND_RETRY_INTERVAL_S)):
                if not self._running or self._provisioned:
                    break
                time.sleep(1)

            if not self._running or self._provisioned:
                break

            LOG.info(
                'Background retry: attempting provisioning...',
            )
            if self._do_provisioning():
                self._provisioned = True
                LOG.info(
                    'Background retry: provisioning successful!',
                )
                break

        LOG.info('Background retry loop stopped')


def check_and_provision(
        device_sn: str = 'unknown',
        station_id: int = -1,
        start_background_retry: bool = True,
) -> bool:
    '''Check credentials and provision if needed.

    Convenience function for use in startup_state_machine.

    Args:
        device_sn: Device serial number.
        station_id: Station ID.
        start_background_retry: If initial provisioning fails, start
            background retry thread.

    Returns:
        True if credentials are valid (existing or newly provisioned).
    '''
    provisioner = IoTProvisioner(
        device_sn=device_sn,
        station_id=station_id,
    )

    if provisioner.is_provisioned():
        thing_name = provisioner.credential_manager.get_thing_name()
        LOG.info(f'Already provisioned as: {thing_name}')
        return True

    success = provisioner.run()

    if not success and start_background_retry:
        LOG.warning(
            'Initial provisioning failed -- '
            'starting background retry',
        )
        provisioner.start_background_retry()

    return success
