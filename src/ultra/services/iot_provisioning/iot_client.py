'''ultra.services.iot_provisioning.iot_client

AWS IoT client for fleet provisioning and MQTT API calls.
Handles MQTT5 connections with both claim certificates (provisioning)
and device certificates (API calls).
'''
from __future__ import annotations

import json
import logging
import threading
import uuid
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import (
    datetime,
    timezone,
)
from typing import Any

from ultra.services.iot_provisioning import config
from ultra.services.iot_provisioning.credential_manager import CredentialManager

LOG = logging.getLogger(__name__)

try:
    from awscrt import (
        mqtt5,
        mqtt_request_response,
    )
    from awsiot import (
        iotidentity,
        mqtt5_client_builder,
    )
    AWSIOT_AVAILABLE = True
except ImportError as err:
    LOG.warning(f'AWS IoT SDK not available: {err}')
    AWSIOT_AVAILABLE = False


@dataclass
class ProvisioningResult:
    '''Result of fleet provisioning operation.

    Attributes:
        success: True if provisioning succeeded.
        certificate_pem: Device certificate PEM string.
        private_key: Device private key PEM string.
        thing_name: AWS IoT thing name.
        certificate_id: AWS IoT certificate ID.
        error_message: Error description if not success.
    '''
    success: bool
    certificate_pem: str | None = None
    private_key: str | None = None
    thing_name: str | None = None
    certificate_id: str | None = None
    error_message: str | None = None


class IoTClient:
    '''AWS IoT client for fleet provisioning and MQTT API calls.

    Supports two connection modes:
    1. Claim certificate (fleet provisioning): connect_with_claim()
    2. Device certificate (API calls): connect_with_device_cert()

    Attributes:
        endpoint: AWS IoT Core MQTT endpoint.
        client_id: MQTT client ID.
    '''

    def __init__(
            self,
            endpoint: str | None = None,
            client_id: str | None = None,
            credential_manager: CredentialManager | None = None,
            response_timeout_s: float | None = None,
            device_sn: str = 'unknown',
    ):
        '''Initialize IoT client.

        Args:
            endpoint: AWS IoT endpoint (uses config default if None).
            client_id: MQTT client ID (random if None).
            credential_manager: Credential manager for device cert
                operations.
            response_timeout_s: Timeout waiting for MQTT API response.
            device_sn: Device serial number used as identifier.
        '''
        if not AWSIOT_AVAILABLE:
            raise RuntimeError(
                'AWS IoT SDK not available. '
                'Install with: uv pip install awsiotsdk',
            )

        self.endpoint = endpoint or config.get_iot_endpoint()
        self._device_sn = device_sn
        self.client_id = client_id or (
            f'{device_sn}-{uuid.uuid4().hex[:8]}'
        )

        self._mqtt_client = None
        self._identity_client = None
        self._connection_success: Future | None = None
        self._stopped: Future | None = None

        self._cred_manager = credential_manager
        self._response_timeout_s = (
            response_timeout_s or
            config.get_mqtt_api_response_timeout()
        )
        self._topic_prefix = config.get_mqtt_api_topic_prefix()
        self._pending_requests: dict[str, Future] = {}
        self._pending_lock = threading.Lock()
        self._command_callback: Any = None

        LOG.info(
            f'IoTClient initialized: '
            f'endpoint={self.endpoint}, '
            f'client_id={self.client_id}',
        )

    @property
    def thing_name(self) -> str | None:
        '''Get the IoT thing name from credentials.'''
        if self._cred_manager:
            return self._cred_manager.get_thing_name()
        return None

    def _connect(
            self,
            cert_path: str,
            key_path: str,
            ca_path: str,
            client_id_prefix: str,
            timeout_s: float,
    ) -> bool:
        '''Connect to AWS IoT Core with mTLS via MQTT5.

        Args:
            cert_path: Path to client certificate.
            key_path: Path to client private key.
            ca_path: Path to CA certificate.
            client_id_prefix: Prefix for MQTT client ID.
            timeout_s: Connection timeout in seconds.

        Returns:
            True if connected successfully.
        '''
        if self._mqtt_client:
            self.disconnect()

        LOG.info('Connecting to AWS IoT...')
        LOG.debug(f'  Endpoint: {self.endpoint}')
        LOG.debug(f'  Cert: {cert_path}')
        LOG.debug(f'  CA: {ca_path}')

        self._connection_success = Future()
        self._stopped = Future()
        self.client_id = f'{client_id_prefix}-{uuid.uuid4().hex[:8]}'

        connect_options = mqtt5.ConnectPacket(
            client_id=self.client_id,
        )

        try:
            self._mqtt_client = mqtt5_client_builder.mtls_from_path(
                endpoint=self.endpoint,
                port=8883,
                connect_options=connect_options,
                cert_filepath=cert_path,
                pri_key_filepath=key_path,
                ca_filepath=ca_path,
                clean_session=True,
                keep_alive_secs=config.MQTT_KEEP_ALIVE_S,
                on_publish_received=self._on_publish_received,
                on_lifecycle_connection_success=(
                    self._on_connection_success
                ),
                on_lifecycle_connection_failure=(
                    self._on_connection_failure
                ),
                on_lifecycle_disconnection=self._on_disconnection,
                on_lifecycle_stopped=self._on_stopped,
            )
        except Exception as err:
            LOG.error(f'Failed to create MQTT client: {err}')
            return False

        self._mqtt_client.start()  # type: ignore[attr-defined]

        try:
            self._connection_success.result(timeout=timeout_s)
            LOG.info('Connected to AWS IoT')
            return True
        except FutureTimeoutError:
            LOG.error(f'Connection timeout after {timeout_s}s')
            self._mqtt_client.stop()  # type: ignore[attr-defined]
            return False
        except Exception as err:
            LOG.error(f'Connection failed: {err}')
            self._mqtt_client.stop()  # type: ignore[attr-defined]
            return False

    def connect_with_claim(
            self,
            claim_cert_path: str | None = None,
            claim_key_path: str | None = None,
            ca_cert_path: str | None = None,
            timeout_s: float = config.MQTT_CONNECTION_TIMEOUT_S,
    ) -> bool:
        '''Connect using claim certificates for fleet provisioning.

        Args:
            claim_cert_path: Path to claim certificate.
            claim_key_path: Path to claim private key.
            ca_cert_path: Path to CA certificate.
            timeout_s: Connection timeout in seconds.

        Returns:
            True if connected successfully.
        '''
        claim_cert_path = (
            claim_cert_path or config.get_claim_cert_path()
        )
        claim_key_path = (
            claim_key_path or config.get_claim_key_path()
        )
        ca_cert_path = ca_cert_path or config.get_ca_cert_path()

        if not self._connect(
            cert_path=claim_cert_path,
            key_path=claim_key_path,
            ca_path=ca_cert_path,
            client_id_prefix=self._device_sn,
            timeout_s=timeout_s,
        ):
            return False

        rr_options = mqtt_request_response.ClientOptions(
            max_request_response_subscriptions=2,
            max_streaming_subscriptions=0,
            operation_timeout_in_seconds=int(timeout_s),
        )
        self._identity_client = iotidentity.IotIdentityClientV2(
            self._mqtt_client,
            rr_options,
        )
        return True

    def connect_with_device_cert(
            self,
            credential_manager: CredentialManager | None = None,
            timeout_s: float = config.MQTT_CONNECTION_TIMEOUT_S,
    ) -> bool:
        '''Connect using provisioned device certificates for API calls.

        Args:
            credential_manager: Credential manager (uses
                self._cred_manager if None).
            timeout_s: Connection timeout in seconds.

        Returns:
            True if connected successfully.
        '''
        cred_mgr = credential_manager or self._cred_manager
        if not cred_mgr:
            LOG.error('No credential manager provided')
            return False
        self._cred_manager = cred_mgr

        thing_name = cred_mgr.get_thing_name()
        if not thing_name:
            LOG.error('Device not provisioned -- no thing name found')
            return False

        if not cred_mgr.has_valid_credentials():
            LOG.error('Device credentials not valid')
            return False

        LOG.info(f'Connecting as {thing_name}...')
        return self._connect(
            cert_path=cred_mgr.cert_path,
            key_path=cred_mgr.key_path,
            ca_path=cred_mgr.ca_path,
            client_id_prefix=thing_name,
            timeout_s=timeout_s,
        )

    def disconnect(self, timeout_s: float = 10.0) -> None:
        '''Disconnect from AWS IoT.

        Args:
            timeout_s: Timeout for graceful disconnection.
        '''
        if self._mqtt_client:
            LOG.info('Disconnecting from AWS IoT...')
            self._mqtt_client.stop()
            try:
                if self._stopped:
                    self._stopped.result(timeout=timeout_s)
            except Exception:
                pass
            LOG.info('Disconnected')

        self._mqtt_client = None
        self._identity_client = None

        with self._pending_lock:
            for future in self._pending_requests.values():
                if not future.done():
                    future.set_exception(
                        RuntimeError(
                            'Client disconnected before response',
                        ),
                    )
            self._pending_requests.clear()

    def provision_device(
            self,
            template_name: str | None = None,
            device_sn: str = 'unknown',
            station_id: int = -1,
            timeout_s: float = config.MQTT_CONNECTION_TIMEOUT_S,
    ) -> ProvisioningResult:
        '''Run fleet provisioning via CreateKeysAndCertificate
        and RegisterThing.

        Requires prior connect_with_claim() call.

        Args:
            template_name: Provisioning template name.
            device_sn: Device serial number.
            station_id: Station ID.
            timeout_s: Operation timeout in seconds.

        Returns:
            ProvisioningResult with credentials or error.
        '''
        if not self._identity_client:
            return ProvisioningResult(
                success=False,
                error_message=(
                    'Not connected. '
                    'Call connect_with_claim() first.'
                ),
            )

        template_name = (
            template_name or config.get_provisioning_template()
        )
        LOG.info('Starting fleet provisioning...')
        LOG.debug(f'  Template: {template_name}')
        LOG.debug(f'  Device SN: {device_sn}')
        LOG.debug(f'  Station ID: {station_id}')

        try:
            LOG.info('Creating keys and certificate...')
            create_request = (
                iotidentity.CreateKeysAndCertificateRequest()
            )
            create_response = (
                self._identity_client
                .create_keys_and_certificate(create_request)
                .result(timeout=timeout_s)
            )
            LOG.info('Certificate created successfully')
            LOG.debug(
                f'  Certificate ID: '
                f'{create_response.certificate_id}',
            )

            LOG.info('Registering thing with template...')
            register_request = iotidentity.RegisterThingRequest(
                template_name=template_name,
                certificate_ownership_token=(
                    create_response.certificate_ownership_token
                ),
                parameters={
                    'SerialNumber': device_sn,
                },
            )
            register_response = (
                self._identity_client
                .register_thing(register_request)
                .result(timeout=timeout_s)
            )
            LOG.info(
                f'Thing registered: '
                f'{register_response.thing_name}',
            )

            return ProvisioningResult(
                success=True,
                certificate_pem=create_response.certificate_pem,
                private_key=create_response.private_key,
                thing_name=register_response.thing_name,
                certificate_id=create_response.certificate_id,
            )

        except FutureTimeoutError:
            error_msg = f'Provisioning timeout after {timeout_s}s'
            LOG.error(error_msg)
            return ProvisioningResult(
                success=False,
                error_message=error_msg,
            )
        except Exception as err:
            error_msg = f'Provisioning failed: {err}'
            LOG.error(error_msg)
            return ProvisioningResult(
                success=False,
                error_message=error_msg,
            )

    def health_check(self) -> dict[str, Any]:
        '''Call health endpoint via MQTT.

        Returns:
            Health check response from cloud.
        '''
        return self._request('health')

    def register_device(
            self,
            serial_number: str,
            fleet_certificate_name: str = 'ultrathink-fleet-cert',
            hw_status: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        '''Register device with cloud and signal it is ready.

        This is the canonical "device ready" notification, matching
        the pattern from ultra-companion-linux (mateusz/dev).
        Publishes to device/{thing_name}/register_device and awaits
        a cloud response.

        Args:
            serial_number: Device serial number.
            fleet_certificate_name: Name of the fleet certificate.
            hw_status: Optional hardware/network status to include
                (ip_address, wifi_ssid, station_id, etc).

        Returns:
            Registration response dict from cloud.
        '''
        activation_date = datetime.now(timezone.utc).strftime(
            '%Y-%m-%dT%H:%M:%S',
        )
        payload: dict[str, Any] = {
            'serial_number': serial_number,
            'activation_date': activation_date,
            'fleet_certificate_name': fleet_certificate_name,
            'status': 'active',
        }
        if hw_status:
            payload.update(hw_status)
        return self._request('register_device', payload)

    def subscribe_to_commands(
            self,
            callback: Any,
            timeout_s: float | None = None,
    ) -> bool:
        '''Subscribe to cloud command topic and invoke callback.

        The cloud publishes JSON to:
            device/{thing_name}/commands

        Each message must contain a ``command`` key, e.g.:
            {"command": "start_experiment", "chip_id": "..."}

        The callback receives the parsed payload dict.

        Args:
            callback: Called with parsed payload dict on each message.
            timeout_s: Subscribe timeout in seconds.

        Returns:
            True if subscribed successfully.
        '''
        if not self._mqtt_client:
            LOG.warning(
                'Not connected, cannot subscribe to commands',
            )
            return False

        timeout_s = timeout_s or self._response_timeout_s
        prefix = self._topic_prefix
        thing = self.thing_name or 'unknown'
        topic = f'{prefix}/{thing}/commands'
        LOG.info(f'Subscribing to commands topic: {topic}')

        self._command_callback = callback

        try:
            sub_future = self._mqtt_client.subscribe(  # type: ignore[attr-defined]
                subscribe_packet=mqtt5.SubscribePacket(
                    subscriptions=[
                        mqtt5.Subscription(
                            topic_filter=topic,
                            qos=mqtt5.QoS.AT_LEAST_ONCE,
                        ),
                    ],
                ),
            )
            sub_future.result(timeout=timeout_s)
            LOG.info(f'Subscribed to commands topic: {topic}')
            return True
        except Exception as err:
            LOG.error(
                f'Failed to subscribe to commands topic: {err}',
            )
            return False

    def notify_device_ready(
            self,
            hw_status: dict[str, Any] | None = None,
            timeout_s: float | None = None,
    ) -> bool:
        '''Publish device_ready event to device/{thing_name}/event.

        Signals the cloud that the device is online and ready for
        use.  Routes through the IoT rule on device/+/event to
        the report_event Lambda.  hw_status fields (ip, ssid, etc.)
        are included in the payload for diagnostics but are not
        used by the cloud event handler.

        Args:
            hw_status: Optional hardware/network status dict
                (device_sn, station_id, ip_address, wifi_ssid).
            timeout_s: Publish timeout in seconds.

        Returns:
            True if published successfully.
        '''
        return self.publish_event(
            event_type='device_ready',
            extra=hw_status,
            timeout_s=timeout_s,
        )

    def publish_event(
            self,
            event_type: str,
            cartridge_id: str | None = None,
            state: str | None = None,
            extra: dict[str, Any] | None = None,
            timeout_s: float | None = None,
    ) -> bool:
        '''Publish a device lifecycle event (fire-and-forget).

        Publishes to device/{thing_name}/event which is the single
        canonical topic routed by the cloud IoT rule to the
        report_event Lambda.

        Common event_type values (from cloud device_events.py):
            device_ready, drawer_open, drawer_closed,
            self_check_started, self_check_complete,
            self_check_failed, cartridge_inserted,
            cartridge_removed, start_assay_protocol
            (-> test_started), end_assay_protocol (requires
            state="succeed" or state="failed").

        Args:
            event_type: Canonical event type string accepted
                by the cloud report_event handler. Aliases
                start_assay_protocol and end_assay_protocol
                are mapped to test_started / test_completed
                / test_failed server-side.
            cartridge_id: Optional cartridge identifier
                included in the cloud state record.
            state: Required for completion event types
                end_assay_protocol and
                end_validation_protocol. Must be
                ``succeed`` or ``failed``.
            extra: Optional additional fields merged into
                the payload (e.g. hw_status fields).
            timeout_s: Publish timeout in seconds.

        Returns:
            True if published successfully.
        '''
        if not self._mqtt_client:
            LOG.warning(
                'Not connected, cannot publish event',
            )
            return False

        timeout_s = timeout_s or self._response_timeout_s
        prefix = self._topic_prefix
        thing = self.thing_name or 'unknown'
        topic = f'{prefix}/{thing}/event'

        pub_dict: dict[str, Any] = {
            'event_type': event_type,
            'timestamp': datetime.now(timezone.utc).strftime(
                '%Y-%m-%dT%H:%M:%S',
            ),
        }
        if cartridge_id:
            pub_dict['cartridge_id'] = cartridge_id
        if state:
            pub_dict['state'] = state
        if extra:
            pub_dict.update(extra)
        pub_payload = json.dumps(pub_dict)

        LOG.info(
            f'Publishing event_type={event_type!r} to {topic}',
        )
        LOG.debug(f'  Payload: {pub_payload}')

        try:
            pub_future = self._mqtt_client.publish(
                mqtt5.PublishPacket(
                    topic=topic,
                    payload=pub_payload,
                    qos=mqtt5.QoS.AT_LEAST_ONCE,
                ),
            )
            pub_future.result(timeout=timeout_s)
            LOG.info('event published successfully')
            return True
        except Exception as err:
            LOG.error(f'Failed to publish event: {err}')
            return False

    def _get_topic(self, action: str) -> str:
        '''Build request topic.

        Args:
            action: API action name.

        Returns:
            Full MQTT topic string.
        '''
        return f'{self._topic_prefix}/{self.thing_name}/{action}'

    def _get_response_topic(self, action: str) -> str:
        '''Build response topic.

        Args:
            action: API action name.

        Returns:
            Full MQTT response topic string.
        '''
        return (
            f'{self._topic_prefix}'
            f'/{self.thing_name}/{action}/response'
        )

    def _subscribe_response_topic(
            self,
            action: str,
            timeout_s: float | None = None,
    ) -> bool:
        '''Subscribe to the response topic for an action.

        Args:
            action: API action name.
            timeout_s: Subscribe timeout.

        Returns:
            True if subscribed successfully.
        '''
        timeout_s = timeout_s or self._response_timeout_s
        topic = self._get_response_topic(action)
        LOG.debug(f'Subscribing to {topic}')
        try:
            sub_future = self._mqtt_client.subscribe(  # type: ignore[attr-defined]
                subscribe_packet=mqtt5.SubscribePacket(
                    subscriptions=[
                        mqtt5.Subscription(
                            topic_filter=topic,
                            qos=mqtt5.QoS.AT_LEAST_ONCE,
                        ),
                    ],
                ),
            )
            suback = sub_future.result(timeout=timeout_s)
            LOG.debug(f'Subscribed to {topic}: {suback.reason_codes}')
            return True
        except Exception as err:
            LOG.error(f'Failed to subscribe to {topic}: {err}')
            return False

    def _unsubscribe_response_topic(self, action: str) -> None:
        '''Unsubscribe from the response topic.

        Args:
            action: API action name.
        '''
        topic = self._get_response_topic(action)
        try:
            self._mqtt_client.unsubscribe(  # type: ignore[attr-defined]
                unsubscribe_packet=mqtt5.UnsubscribePacket(
                    topic_filters=[topic],
                ),
            )
        except Exception as err:
            LOG.warning(
                f'Failed to unsubscribe from {topic}: {err}',
            )

    def _request(
            self,
            action: str,
            payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        '''Make MQTT request/response API call.

        Args:
            action: API action name.
            payload: Request payload (merged with request_id etc).

        Returns:
            Response dict from cloud.

        Raises:
            RuntimeError: If not connected or subscribe fails.
            TimeoutError: If response not received within timeout.
        '''
        if not self._mqtt_client:
            raise RuntimeError(
                'Not connected. '
                'Call connect_with_device_cert() first.',
            )

        if not self._subscribe_response_topic(action):
            raise RuntimeError(
                f'Failed to subscribe to {action} response topic',
            )

        request_id = uuid.uuid4().hex
        request_payload = {
            'request_id': request_id,
            'timestamp': datetime.now(timezone.utc).strftime(
                '%Y-%m-%dT%H:%M:%S',
            ),
            'thing_name': self.thing_name,
        }
        if payload:
            request_payload.update(payload)

        response_future: Future = Future()
        with self._pending_lock:
            self._pending_requests[request_id] = response_future

        request_topic = self._get_topic(action)
        request_json = json.dumps(request_payload)
        LOG.info(
            f'MQTT API request: {action} '
            f'(request_id={request_id})',
        )

        try:
            pub_future = self._mqtt_client.publish(
                mqtt5.PublishPacket(
                    topic=request_topic,
                    payload=request_json,
                    qos=mqtt5.QoS.AT_LEAST_ONCE,
                ),
            )
            pub_future.result(timeout=self._response_timeout_s)
        except Exception as err:
            with self._pending_lock:
                self._pending_requests.pop(request_id, None)
            self._unsubscribe_response_topic(action)
            raise RuntimeError(
                f'Failed to publish {action} request: {err}',
            )

        try:
            response = response_future.result(
                timeout=self._response_timeout_s,
            )
            LOG.info(
                f'MQTT API response: {action} -> '
                f'{response.get("status", "unknown")}',
            )
            return response
        except FutureTimeoutError:
            LOG.error(
                f'MQTT API timeout: no response for {action} '
                f'after {self._response_timeout_s}s',
            )
            raise TimeoutError(
                f'No response for {action} within '
                f'{self._response_timeout_s}s',
            )
        finally:
            with self._pending_lock:
                self._pending_requests.pop(request_id, None)
            self._unsubscribe_response_topic(action)

    def _on_publish_received(
            self,
            publish_received_data: Any,
    ) -> None:
        '''Callback for incoming MQTT messages.

        Dispatches response to pending Future by request_id.
        '''
        publish_packet = publish_received_data.publish_packet
        topic = publish_packet.topic
        payload_bytes = publish_packet.payload
        LOG.debug(f'MQTT message received on {topic}')

        try:
            payload_str = (
                payload_bytes.decode('utf-8')
                if isinstance(payload_bytes, (bytes, bytearray))
                else str(payload_bytes)
            )
            response = json.loads(payload_str)
        except (json.JSONDecodeError, UnicodeDecodeError) as err:
            LOG.warning(f'Failed to parse MQTT response: {err}')
            return

        # Dispatch command messages to command callback
        thing = self.thing_name or 'unknown'
        commands_topic = (
            f'{self._topic_prefix}/{thing}/commands'
        )
        if topic == commands_topic:
            if self._command_callback is not None:
                try:
                    self._command_callback(response)
                except Exception as err:
                    LOG.warning(
                        f'Command callback error: {err}',
                    )
            else:
                LOG.warning(
                    f'Command received but no callback: '
                    f'{response}',
                )
            return

        # Dispatch request/response messages by request_id
        request_id = response.get('request_id')
        if not request_id:
            LOG.warning(
                f'MQTT response missing request_id on {topic}',
            )
            return

        with self._pending_lock:
            future = self._pending_requests.get(request_id)

        if future and not future.done():
            future.set_result(response)
        else:
            LOG.warning(
                f'No pending request for '
                f'request_id={request_id} (topic={topic})',
            )

    def _on_connection_success(self, event: Any) -> None:
        '''Lifecycle: connection succeeded.'''
        LOG.debug('MQTT connection success')
        if (
                self._connection_success and
                not self._connection_success.done()
        ):
            self._connection_success.set_result(True)

    def _on_connection_failure(self, event: Any) -> None:
        '''Lifecycle: connection failed.'''
        LOG.debug('MQTT connection failure')
        if (
                self._connection_success and
                not self._connection_success.done()
        ):
            self._connection_success.set_exception(
                Exception('Connection failed'),
            )

    def _on_disconnection(self, event: Any) -> None:
        '''Lifecycle: disconnected.'''
        reason = (
            event.disconnect_packet.reason_code
            if event.disconnect_packet else 'None'
        )
        LOG.debug(f'MQTT disconnected: {reason}')

    def _on_stopped(self, event: Any) -> None:
        '''Lifecycle: client stopped.'''
        LOG.debug('MQTT client stopped')
        if self._stopped and not self._stopped.done():
            self._stopped.set_result(True)


def create_device_client() -> IoTClient | None:
    '''Create IoTClient for API calls if device is provisioned.

    Creates client but does NOT connect -- caller must call
    connect_with_device_cert().

    Returns:
        IoTClient instance or None if not provisioned.
    '''
    cred_manager = CredentialManager()
    if not cred_manager.has_valid_credentials():
        LOG.warning(
            'Cannot create device client -- device not provisioned',
        )
        return None

    metadata = cred_manager.load_metadata()
    device_sn = (
        metadata.get('device_sn', 'unknown') if metadata else 'unknown'
    )

    try:
        return IoTClient(
            credential_manager=cred_manager,
            device_sn=device_sn,
        )
    except RuntimeError as err:
        LOG.warning(f'Cannot create device client: {err}')
        return None
