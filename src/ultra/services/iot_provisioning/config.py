'''ultra.services.iot_provisioning.config

Configuration constants for AWS IoT provisioning.
All values can be overridden via environment variables.
'''
import os
import os.path as op

# AWS IoT Core
IOT_ENDPOINT_DEFAULT = (
    'av6fq1kv2yjq0-ats.iot.us-east-1.amazonaws.com'
)
IOT_REGION_DEFAULT = 'us-east-1'

# Fleet provisioning template
PROVISIONING_TEMPLATE_DEFAULT = 'ultrathink-fleet-template'

# Claim certificate paths (pre-deployed to /etc/siphox/)
CLAIM_CERT_DIR = '/etc/ultra/certs'
CLAIM_CERT_PATH_DEFAULT = op.join(
    CLAIM_CERT_DIR, 'claim.cert.pem',
)
CLAIM_KEY_PATH_DEFAULT = op.join(
    CLAIM_CERT_DIR, 'claim.private.key',
)
CA_CERT_PATH_DEFAULT = op.join(
    CLAIM_CERT_DIR, 'root-CA.crt',
)

# Device credentials directory (written after provisioning)
CREDENTIALS_DIR_DEFAULT = '/etc/ultra/certs/device'
DEVICE_CERT_FILENAME = 'device.cert.pem'
DEVICE_KEY_FILENAME = 'device.private.key'
METADATA_FILENAME = 'metadata.json'
CA_CERT_FILENAME = 'root-CA.crt'

# Provisioning retry settings
MAX_PROVISIONING_RETRIES = 3
RETRY_BACKOFF_BASE_S = 5.0   # 5s, 10s, 20s exponential backoff
BACKGROUND_RETRY_INTERVAL_S = 300.0   # 5 minutes

# MQTT settings
MQTT_CONNECTION_TIMEOUT_S = 30
MQTT_KEEP_ALIVE_S = 30

# File permissions
DIR_PERMISSIONS = 0o700   # rwx------
FILE_PERMISSIONS = 0o600  # rw-------

# MQTT API (device-to-cloud pub/sub after provisioning)
MQTT_API_TOPIC_PREFIX_DEFAULT = 'device'
MQTT_API_RESPONSE_TIMEOUT_S = 10.0


def get_iot_endpoint() -> str:
    '''Get AWS IoT endpoint from env or default.'''
    return os.environ.get(
        'SIPHOX_IOT_ENDPOINT',
        IOT_ENDPOINT_DEFAULT,
    )


def get_iot_region() -> str:
    '''Get AWS IoT region from env or default.'''
    return os.environ.get(
        'SIPHOX_IOT_REGION',
        IOT_REGION_DEFAULT,
    )


def get_provisioning_template() -> str:
    '''Get provisioning template name from env or default.'''
    return os.environ.get(
        'SIPHOX_IOT_TEMPLATE',
        PROVISIONING_TEMPLATE_DEFAULT,
    )


def get_credentials_dir() -> str:
    '''Get credentials directory from env or default.'''
    path = os.environ.get(
        'SIPHOX_IOT_CREDENTIALS_DIR',
        CREDENTIALS_DIR_DEFAULT,
    )
    return op.expanduser(path)


def get_claim_cert_path() -> str:
    '''Get claim certificate path from env or default.'''
    return os.environ.get(
        'SIPHOX_IOT_CLAIM_CERT',
        CLAIM_CERT_PATH_DEFAULT,
    )


def get_claim_key_path() -> str:
    '''Get claim private key path from env or default.'''
    return os.environ.get(
        'SIPHOX_IOT_CLAIM_KEY',
        CLAIM_KEY_PATH_DEFAULT,
    )


def get_ca_cert_path() -> str:
    '''Get CA certificate path from env or default.'''
    return os.environ.get(
        'SIPHOX_IOT_CA_CERT',
        CA_CERT_PATH_DEFAULT,
    )


def get_mqtt_api_topic_prefix() -> str:
    '''Get MQTT API topic prefix from env or default.'''
    return os.environ.get(
        'SIPHOX_MQTT_API_TOPIC_PREFIX',
        MQTT_API_TOPIC_PREFIX_DEFAULT,
    )


def get_mqtt_api_response_timeout() -> float:
    '''Get MQTT API response timeout in seconds.'''
    val = os.environ.get(
        'SIPHOX_MQTT_API_RESPONSE_TIMEOUT_S', '',
    )
    return float(val) if val else MQTT_API_RESPONSE_TIMEOUT_S
