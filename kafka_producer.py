"""
Stream transactions to Aiven Kafka.
"""
import json
import time
import os
import sys
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from tqdm import tqdm
from config import KAFKA_CONFIG, KAFKA_TOPICS, STREAMING_CONFIG

class TransactionProducer:
    def __init__(self):
        print("🔧 Initializing Kafka Producer...")
        producer_config = {
            'bootstrap_servers': KAFKA_CONFIG['bootstrap_servers'],
            'security_protocol': KAFKA_CONFIG['security_protocol'],
            'ssl_cafile': KAFKA_CONFIG['ssl_cafile'],
            'ssl_certfile': KAFKA_CONFIG['ssl_certfile'],
            'ssl_keyfile': KAFKA_CONFIG['ssl_keyfile'],
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda v: str(v).encode('utf-8') if v else None,
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 1,
            'compression_type': None,
            'linger_ms': 5,
            'batch_size': 32768,
        }
        self.producer = KafkaProducer(**producer_config)
        self.topic = KAFKA_TOPICS['transactions']
        self.messages_sent = 0
        self.messages_failed = 0
        print(f"✅ Connected to: {KAFKA_CONFIG['bootstrap_servers']}")
        print(f"✅ Topic: {self.topic}")

    def send_transaction(self, row_data, idx):
        try:
            transaction = {
                'transaction_id': f"TXN_{idx}_{int(time.time()*1000)}",
                'timestamp': datetime.now().isoformat(),
                'step': int(row_data.get('step', 0)),
                'type': str(row_data.get('type', 'UNKNOWN')),
                'amount': float(row_data.get('amount', 0)),
                'nameOrig': str(row_data.get('nameOrig', '')),
                'oldbalanceOrg': float(row_data.get('oldbalanceOrg', 0)),
                'newbalanceOrig': float(row_data.get('newbalanceOrig', 0)),
                'nameDest': str(row_data.get('nameDest', '')),
                'oldbalanceDest': float(row_data.get('oldbalanceDest', 0)),
                'newbalanceDest': float(row_data.get('newbalanceDest', 0)),
                'isFraud': int(row_data.get('isFraud', 0)),
                'isFlaggedFraud': int(row_data.get('isFlaggedFraud', 0))
            }
            future = self.producer.send(
                topic=self.topic,
                key=str(idx),
                value=transaction
            )
            future.get(timeout=10)
            self.messages_sent += 1
        except Exception as e:
            self.messages_failed += 1
            if self.messages_failed % 100 == 0:
                print(f"\n❌ Error on transaction {idx}: {e}")

    def stream_file(self, filepath, max_messages, delay):
        print("\n" + "=" * 60)
        print("🚀 STARTING TRANSACTION STREAM")
        print("=" * 60)
        df = pd.read_csv(filepath)
        if max_messages:
            df = df.head(max_messages)
        print(f"📊 Total transactions to send: {len(df):,}")
        print(f"🚨 Fraud cases in data: {df['isFraud'].sum():,}")
        print(f"⏱️  Delay per message: {delay}s")
        start_time = time.time()
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Streaming"):
            self.send_transaction(row, idx)
            time.sleep(delay)
        self.producer.flush()
        elapsed = time.time() - start_time
        total = self.messages_sent + self.messages_failed
        print(f"\n{'=' * 60}")
        print(f"📊 STREAMING COMPLETE")
        print(f"{'=' * 60}")
        print(f"✅ Sent: {self.messages_sent:,}")
        print(f"❌ Failed: {self.messages_failed:,}")
        print(f"⏱️  Time: {elapsed:.1f}s")
        print(f"📈 Rate: {total/elapsed:.1f} msg/s")

    def close(self):
        self.producer.close()
        print("👋 Producer closed")


def download_from_gdrive(file_id, destination_path):

    try:
        import gdown
        url = f"https://drive.google.com/uc?id={file_id}"
        print(f"📥 Downloading from Google Drive (ID: {file_id}) ...")
        gdown.download(url, destination_path, quiet=False)
        if not os.path.exists(destination_path) or os.path.getsize(destination_path) == 0:
            raise Exception("Downloaded file is empty or missing")
        print(f"✅ Downloaded to {destination_path} ({os.path.getsize(destination_path)/1024/1024:.1f} MB)")
    except Exception as e:
        print(f"gdown failed ({e}), trying direct requests with confirmation...")
        import requests
        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        session = requests.Session()
        response = session.get(url, stream=True)
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm={value}"
                response = session.get(url, stream=True)
                break
        total_size = int(response.headers.get('content-length', 0))
        with open(destination_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc='Downloading') as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        print(f"✅ Downloaded via requests.")


if __name__ == "__main__":
    max_msgs = int(os.getenv('MAX_MESSAGES', STREAMING_CONFIG['max_messages']))
    delay = float(os.getenv('DELAY', STREAMING_CONFIG['delay']))
    gdrive_file_id = os.getenv('GDRIVE_FILE_ID')
    csv_url = os.getenv('CSV_URL')

    # Determine CSV source
    if gdrive_file_id:
        print("📂 Google Drive mode: file ID detected")
        csv_path = 'data/raw/paysim_data.csv' 
        download_from_gdrive(gdrive_file_id, csv_path)
    elif csv_url:
        print(f"📂 URL mode: {csv_url}")
        csv_path = csv_url
    else:
        csv_path = 'data/raw/paysim_sample.csv'
        print(f"📂 Local fallback: {csv_path}")

    if not os.path.exists(csv_path) and not csv_url:
        print(f"❌ File not found: {csv_path}")
        print("Set GDRIVE_FILE_ID or CSV_URL environment variable, or place a CSV in data/raw/")
        sys.exit(1)

    producer = TransactionProducer()
    try:
        producer.stream_file(filepath=csv_path, max_messages=max_msgs, delay=delay)
    except KeyboardInterrupt:
        print("\n⚠️  Stopped by user")
    finally:
        producer.close()