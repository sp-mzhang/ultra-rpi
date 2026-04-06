'''ultra.services.iot_client -- AWS IoT MQTT client.

Publishes protocol status snapshots and discrete events
to AWS IoT Core via MQTT5. Subscribes to the event bus
and uploads at step transitions, pause/resume, and
protocol completion.

Topic structure:
  ultra/{device_sn}/status   -- ProtocolSnapshot JSON
  ultra/{device_sn}/events   -- discrete events
  ultra/{device_sn}/results  -- final analysis results
'''
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)


class IoTClient:
    '''AWS IoT MQTT client for Ultra RPi.

    Handles MQTT connection, publishing, and event bus
    subscription for automatic status uploads.

    Attributes:
        _device_sn: Device serial number.
        _endpoint: AWS IoT endpoint URL.
        _cert_dir: Directory containing IoT certificates.
        _connected: Whether MQTT is connected.
    '''

    def __init__(
            self,
            config: dict[str, Any],
            event_bus: EventBus,
    ) -> None:
        '''Initialize the IoT client.

        Args:
            config: IoT section of application config.
            event_bus: Application event bus.
        '''
        self._endpoint = config.get('endpoint', '')
        self._cert_dir = config.get(
            'cert_dir', '/etc/ultra/certs/',
        )
        self._device_sn = config.get(
            'device_sn', 'unknown',
        )
        self._event_bus = event_bus
        self._connected = False
        self._mqtt_client: Any = None
        self._last_upload_time: float = 0.0

        self._subscribe_events()

    def _subscribe_events(self) -> None:
        '''Subscribe to event bus for auto-upload.'''
        upload_events = [
            'step_changed', 'protocol_paused',
            'protocol_resumed', 'protocol_done',
            'protocol_error',
        ]
        for event_name in upload_events:
            self._event_bus.on(
                event_name, self._on_protocol_event,
            )

    async def _on_protocol_event(
            self, data: dict,
    ) -> None:
        '''Handle protocol events for cloud upload.

        Throttles uploads to at most once per 5 seconds
        for step_changed events.

        Args:
            data: Event data dict.
        '''
        now = time.time()
        if (
            now - self._last_upload_time < 5.0
            and 'step' in data
        ):
            return

        self._last_upload_time = now
        self.publish_status(data)

    def connect(self) -> bool:
        '''Connect to AWS IoT Core.

        Returns:
            True if connection succeeded.
        '''
        if not self._endpoint:
            LOG.warning(
                'IoT endpoint not configured '
                '-- skipping connection',
            )
            return False

        cert_path = os.path.join(
            self._cert_dir, 'device.pem.crt',
        )
        key_path = os.path.join(
            self._cert_dir, 'device.pem.key',
        )
        ca_path = os.path.join(
            self._cert_dir, 'AmazonRootCA1.pem',
        )

        if not all(
            os.path.isfile(p)
            for p in (cert_path, key_path, ca_path)
        ):
            LOG.warning(
                'IoT certificates not found in '
                f'{self._cert_dir}',
            )
            return False

        try:
            import ssl
            import paho.mqtt.client as mqtt

            self._mqtt_client = mqtt.Client(
                client_id=self._device_sn,
                protocol=mqtt.MQTTv5,
            )
            ssl_ctx = ssl.create_default_context(
                cafile=ca_path,
            )
            ssl_ctx.load_cert_chain(
                certfile=cert_path,
                keyfile=key_path,
            )
            self._mqtt_client.tls_set_context(ssl_ctx)
            self._mqtt_client.connect(
                self._endpoint, port=8883,
            )
            self._mqtt_client.loop_start()
            self._connected = True
            LOG.info(
                f'IoT connected to {self._endpoint}',
            )
            return True

        except ImportError:
            LOG.warning(
                'paho-mqtt not installed '
                '-- IoT disabled',
            )
            return False
        except Exception as err:
            LOG.error(
                f'IoT connection failed: {err}',
            )
            return False

    def disconnect(self) -> None:
        '''Disconnect from AWS IoT.'''
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception as err:
                LOG.warning(
                    f'IoT disconnect error: {err}',
                )
            self._mqtt_client = None
        self._connected = False
        LOG.info('IoT disconnected')

    def publish_status(
            self, snapshot: dict,
    ) -> None:
        '''Publish a protocol status snapshot.

        Args:
            snapshot: Status dict (ProtocolSnapshot.to_dict()
                or event data).
        '''
        self._publish(
            f'ultra/{self._device_sn}/status',
            snapshot,
        )

    def publish_event(
            self,
            event_type: str,
            **kwargs: Any,
    ) -> None:
        '''Publish a discrete event.

        Args:
            event_type: Event type string.
            **kwargs: Additional event data.
        '''
        payload = {
            'event_type': event_type,
            'timestamp': time.time(),
            **kwargs,
        }
        self._publish(
            f'ultra/{self._device_sn}/events',
            payload,
        )

    def publish_results(
            self, results: dict,
    ) -> None:
        '''Publish final analysis results.

        Args:
            results: Analysis results dict.
        '''
        self._publish(
            f'ultra/{self._device_sn}/results',
            results,
        )

    def _publish(
            self,
            topic: str,
            payload: dict,
    ) -> None:
        '''Publish a JSON payload to an MQTT topic.

        Args:
            topic: MQTT topic string.
            payload: Dict to serialize as JSON.
        '''
        if not self._connected or not self._mqtt_client:
            LOG.debug(
                f'IoT not connected -- '
                f'skipping publish to {topic}',
            )
            return
        try:
            msg = json.dumps(payload)
            self._mqtt_client.publish(
                topic, msg, qos=1,
            )
            LOG.debug(
                f'IoT published to {topic} '
                f'({len(msg)} bytes)',
            )
        except Exception as err:
            LOG.warning(
                f'IoT publish failed: {err}',
            )
