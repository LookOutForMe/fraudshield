"""
Configuration for Fraud Detection System
All credentials and settings from environment variables.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv() 

BASE_DIR = Path(__file__).resolve().parent
CERTS_DIR = BASE_DIR / 'certs'
DATA_DIR = BASE_DIR / 'data'
MODELS_DIR = BASE_DIR / 'models'

CERTS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)
(DATA_DIR / 'raw').mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)

ca_cert = CERTS_DIR / 'ca.pem'
service_cert = CERTS_DIR / 'service.cert'
service_key = CERTS_DIR / 'service.key'

KAFKA_CONFIG = {
    'bootstrap_servers': 'farud-detection-ariyanshaw143-b9ee.k.aivencloud.com:17043',
    'security_protocol': 'SSL',
    'ssl_cafile': str(ca_cert),
    'ssl_certfile': str(service_cert),
    'ssl_keyfile': str(service_key),
    'ssl_check_hostname': True,
    'producer_config': {
        'acks': 'all',
        'retries': 3,
        'max_in_flight_requests_per_connection': 1,
        'compression_type': None,
        'linger_ms': 5,
        'batch_size': 32768,
    },
    'consumer_config': {
        'group_id': 'fraud-detection-group',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'max_poll_records': 500,
        'session_timeout_ms': 30000,
    }
}

KAFKA_TOPICS = {
    'transactions': 'fraud-transactions',
    'alerts': 'fraud-alerts',
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Missing SUPABASE_URL or SUPABASE_KEY in environment. "
        "Set them in .env (local) or as GitHub Secrets."
    )

MODEL_PATH = str(MODELS_DIR / 'fraud_model.pkl')

MODEL_FEATURES = [
    'amount', 'amount_log', 'amount_sqrt',
    'balance_orig_diff', 'balance_dest_diff',
    'balance_orig_ratio', 'balance_dest_ratio',
    'orig_balance_error', 'dest_balance_error',
    'hour', 'day', 'is_weekend', 'is_night',
    'type_encoded', 'is_high_amount',
    'is_zero_balance_orig', 'is_zero_balance_dest',
    'amount_to_balance_ratio', 'exact_balance_transfer',
    'amount_vs_avg', 'amount_vs_std', 'txn_frequency',
    'isFlaggedFraud'
]

STREAMING_CONFIG = {
    'max_messages': int(os.getenv('MAX_MESSAGES', '1000')),
    'delay': float(os.getenv('DELAY', '0.1')),
}