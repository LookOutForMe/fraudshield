"""
Production Fraud Detector – Streaming version with full feature parity.
"""
import json
import time
import os
import pandas as pd
import numpy as np
import joblib
from collections import deque, defaultdict
from datetime import datetime
from kafka import KafkaConsumer
from supabase import create_client
from config import KAFKA_CONFIG, KAFKA_TOPICS, MODEL_PATH, SUPABASE_URL, SUPABASE_KEY


class StreamingFeatureExtractor:
    """
    Incremental feature engineering for streaming transactions.
    """
    def __init__(self, global_median_amount, global_amount_quantile_95):
        self.global_median_amount = global_median_amount
        self.global_amount_quantile_95 = global_amount_quantile_95
        self.orig_history = defaultdict(lambda: {
            'txn_list': deque(),
            'destinations': set(),
            'sum': 0.0, 'sum_sq': 0.0, 'count': 0,
            'last_step': None
        })
        self.dest_history = defaultdict(lambda: {
            'txn_list': deque(),
            'sum': 0.0, 'sum_sq': 0.0, 'count': 0,
            'last_step': None,
            'originators': set()
        })

    def _update_window(self, dq, step, amount, window_hours=24):
        dq.append((step, amount))
        while dq and dq[0][0] <= step - window_hours:
            dq.popleft()

    def _window_features(self, dq, step, prefix, window_hours_list):
        feats = {}
        for w in window_hours_list:
            cnt = sum(1 for s, a in dq if s > step - w)
            total = sum(a for s, a in dq if s > step - w)
            feats[f'{prefix}_cnt_{w}h'] = cnt
            feats[f'{prefix}_sum_{w}h'] = total
        return feats

    def extract(self, transaction):
        txn = {
            'step': int(transaction.get('step', 0)),
            'type': str(transaction.get('type', '')),
            'amount': float(transaction.get('amount', 0)),
            'nameOrig': str(transaction.get('nameOrig', '')),
            'nameDest': str(transaction.get('nameDest', '')),
            'oldbalanceOrg': float(transaction.get('oldbalanceOrg', 0)),
            'newbalanceOrig': float(transaction.get('newbalanceOrig', 0)),
            'oldbalanceDest': float(transaction.get('oldbalanceDest', 0)),
            'newbalanceDest': float(transaction.get('newbalanceDest', 0)),
            'isFraud': int(transaction.get('isFraud', 0)),
            'isFlaggedFraud': int(transaction.get('isFlaggedFraud', 0))
        }
        step = txn['step']
        amt = txn['amount']
        nameOrig = txn['nameOrig']
        nameDest = txn['nameDest']

        orig = self.orig_history[nameOrig]
        self._update_window(orig['txn_list'], step, amt)
        count_before = orig['count']
        sum_before = orig['sum']
        sum_sq_before = orig['sum_sq']
        # expanding stats BEFORE adding current
        avg_before = sum_before / count_before if count_before > 0 else self.global_median_amount
        std_before = np.sqrt(max(0, (sum_sq_before / count_before - avg_before**2))) if count_before > 0 else 0.0
        last_step_before = orig['last_step']
        time_since_last = step - last_step_before if last_step_before is not None else 999
        velocity_before = count_before / (step + 1) if step > 0 else 0.0
        out_degree_before = len(orig['destinations'])
        orig['destinations'].add(nameDest)
        orig['count'] += 1
        orig['sum'] += amt
        orig['sum_sq'] += amt * amt
        orig['last_step'] = step

        dest = self.dest_history[nameDest]
        self._update_window(dest['txn_list'], step, amt)
        count_dest_before = dest['count']
        sum_dest_before = dest['sum']
        sum_sq_dest_before = dest['sum_sq']
        avg_dest_before = sum_dest_before / count_dest_before if count_dest_before > 0 else self.global_median_amount
        std_dest_before = np.sqrt(max(0, (sum_sq_dest_before / count_dest_before - avg_dest_before**2))) if count_dest_before > 0 else 0.0
        time_since_last_dest = step - dest['last_step'] if dest['last_step'] is not None else 999
        velocity_dest_before = count_dest_before / (step + 1) if step > 0 else 0.0
        unique_originators_dest_before = len(dest['originators'])
        dest['originators'].add(nameOrig)
        dest['count'] += 1
        dest['sum'] += amt
        dest['sum_sq'] += amt * amt
        dest['last_step'] = step

        feats = {}
        feats['hour'] = step % 24
        feats['day_of_week'] = (step // 24) % 7
        feats['is_night'] = 1 if (feats['hour'] >= 22 or feats['hour'] <= 6) else 0
        feats['is_weekend'] = 1 if feats['day_of_week'] in [5, 6] else 0
        feats['is_business_hours'] = 1 if (9 <= feats['hour'] <= 17) else 0

        for t in ['CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER']:
            feats[f'type_{t}'] = 1 if txn['type'] == t else 0

        feats['amount'] = amt
        feats['amount_log'] = np.log1p(max(amt, 0))
        feats['amount_sqrt'] = np.sqrt(max(amt, 0))

        feats['has_zero_balance_before'] = 1 if txn['oldbalanceOrg'] == 0 else 0
        feats['has_zero_balance_dest_before'] = 1 if txn['oldbalanceDest'] == 0 else 0
        feats['amount_to_oldbalance_ratio'] = amt / (txn['oldbalanceOrg'] + 1)
        feats['is_amount_exceeds_balance'] = 1 if amt > txn['oldbalanceOrg'] else 0

        feats['is_full_drain'] = 1 if amt == txn['oldbalanceOrg'] else 0
        feats['remaining_ratio'] = txn['newbalanceOrig'] / (txn['oldbalanceOrg'] + 1)
        feats['dest_growth_ratio'] = (txn['newbalanceDest'] - txn['oldbalanceDest']) / (amt + 1)
        feats['origin_balance_error'] = abs((txn['oldbalanceOrg'] - amt) - txn['newbalanceOrig'])
        feats['dest_balance_error'] = abs((txn['oldbalanceDest'] + amt) - txn['newbalanceDest'])
        feats['is_origin_zero_after'] = 1 if txn['newbalanceOrig'] == 0 else 0
        feats['is_dest_empty_shell'] = 1 if (txn['oldbalanceDest'] == 0 and txn['newbalanceDest'] == 0) else 0
        feats['dest_is_merchant'] = 1 if txn['nameDest'].startswith('M') else 0

        feats['orig_txn_count'] = count_before
        feats['orig_avg_amount'] = avg_before
        feats['orig_std_amount'] = std_before
        feats['orig_time_since_last'] = time_since_last
        feats['orig_velocity'] = velocity_before

        orig_win_feats = self._window_features(orig['txn_list'], step, 'orig', [1, 6, 24])
        feats.update(orig_win_feats)

        feats['dest_txn_count'] = count_dest_before
        feats['dest_avg_amount'] = avg_dest_before
        feats['dest_std_amount'] = std_dest_before
        feats['dest_time_since_last'] = time_since_last_dest
        feats['dest_velocity'] = velocity_dest_before

        dest_win_feats = self._window_features(dest['txn_list'], step, 'dest', [1, 6, 24])
        feats.update(dest_win_feats)

        feats['amount_vs_avg_ratio'] = amt / (avg_before + 1)
        feats['amount_zscore'] = (amt - avg_before) / (std_before + 1) if std_before > 0 else 0.0

        feats['dest_unique_originators_sofar'] = unique_originators_dest_before
        feats['dest_txn_frequency_sofar'] = count_dest_before
        feats['orig_unique_dest_sofar'] = out_degree_before

        feats['amount_vs_dest_avg'] = amt / (avg_dest_before + 1)
        feats['is_new_recipient'] = 1 if out_degree_before == 0 else 0

        feats['is_round_amount'] = 1 if amt % 1000 == 0 else 0
        feats['is_high_amount'] = 1 if amt > self.global_amount_quantile_95 else 0

        return feats


class FraudDetector:
    def __init__(self):
        print("=" * 60)
        print("INITIALIZING FRAUD DETECTOR (Feature‑Parity Stream)")
        print("=" * 60)
        self.model = None
        self.feature_names = []
        self.use_ml = False
        self.stream_extractor = None
        self.production_threshold = 0.5
        self.global_median_amount = 100000.0
        self.global_amount_quantile_95 = 500000.0
        used_placeholder_globals = True

        try:
            model_data = joblib.load(MODEL_PATH)
            if isinstance(model_data, dict):
                self.model = model_data.get('model')
                self.feature_names = model_data.get('feature_names', [])
                roc_auc = model_data.get('roc_auc', 'N/A')
                self.production_threshold = model_data.get('production_threshold', 0.5)
                if 'global_median_amount' in model_data and 'global_amount_quantile_95' in model_data:
                    self.global_median_amount = model_data['global_median_amount']
                    self.global_amount_quantile_95 = model_data['global_amount_quantile_95']
                    used_placeholder_globals = False
            else:
                self.model = model_data
                self.feature_names = []
            if self.model is not None:
                self.use_ml = True
                print(f"Model loaded. ROC‑AUC: {roc_auc}. Features: {len(self.feature_names)}. "
                      f"Production threshold: {self.production_threshold}")
                if used_placeholder_globals:
                    print("WARNING: model file has no saved global_median_amount/global_amount_quantile_95")
                else:
                    print(f"Loaded training globals: median_amount={self.global_median_amount:.2f}, "
                          f"amount_quantile_95={self.global_amount_quantile_95:.2f}")
                sample_feats = set(self._sample_feature_keys())
                missing_in_extractor = set(self.feature_names) - sample_feats
                if missing_in_extractor:
                    print(f"WARNING: model expects {len(missing_in_extractor)} feature(s) this extractor "
                          f"does not produce (will be filled with 0.0): {sorted(missing_in_extractor)}")
            else:
                print("Model object is None")
        except Exception as e:
            print(f"Model load failed: {e}. Using rule‑based detection.")

        if self.use_ml and self.feature_names:
            self.stream_extractor = StreamingFeatureExtractor(
                self.global_median_amount, self.global_amount_quantile_95
            )
            print("Streaming feature extractor initialised.")
        else:
            self.stream_extractor = None

        print("\nConnecting to Supabase...")
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            _ = self.supabase.table('fraud_alerts').select('id').limit(1).execute()
            print("Supabase connected!")
        except Exception as e:
            print(f"Supabase error: {e}")
            self.supabase = None

        print("\nConnecting to Kafka...")
        consumer_config = {
            'bootstrap_servers': KAFKA_CONFIG['bootstrap_servers'],
            'security_protocol': KAFKA_CONFIG['security_protocol'],
            'ssl_cafile': KAFKA_CONFIG['ssl_cafile'],
            'ssl_certfile': KAFKA_CONFIG['ssl_certfile'],
            'ssl_keyfile': KAFKA_CONFIG['ssl_keyfile'],
            'value_deserializer': lambda v: json.loads(v.decode('utf-8')),
            'key_deserializer': lambda v: v.decode('utf-8') if v else None,
        }
        consumer_config.update(KAFKA_CONFIG['consumer_config'])
        self.consumer = KafkaConsumer(KAFKA_TOPICS['transactions'], **consumer_config)
        print(f"Listening on: {KAFKA_TOPICS['transactions']}")

        self.stats = {
            'processed': 0, 'frauds': 0, 'stored': 0,
            'errors': 0, 'start_time': datetime.now()
        }
        self.max_messages = int(os.getenv('MAX_MESSAGES', '0'))

    def _sample_feature_keys(self):
        dummy_extractor = StreamingFeatureExtractor(
            self.global_median_amount, self.global_amount_quantile_95
        )
        dummy_txn = {
            'step': 1, 'type': 'PAYMENT', 'amount': 100.0,
            'nameOrig': '__sanity_check_orig__', 'nameDest': '__sanity_check_dest__',
            'oldbalanceOrg': 1000.0, 'newbalanceOrig': 900.0,
            'oldbalanceDest': 0.0, 'newbalanceDest': 100.0,
            'isFraud': 0, 'isFlaggedFraud': 0
        }
        return dummy_extractor.extract(dummy_txn).keys()

    def predict(self, transaction):
        if self.use_ml and self.stream_extractor and self.feature_names:
            try:
                feats = self.stream_extractor.extract(transaction)
                vector = np.array([feats.get(name, 0.0) for name in self.feature_names], dtype=np.float64)
                X = vector.reshape(1, -1)
                pred_proba = self.model.predict_proba(X)[0][1]
                pred = pred_proba >= self.production_threshold
                return bool(pred), float(pred_proba)
            except Exception:
                pass

        # Rule‑based fallback
        amount = float(transaction.get('amount', 0))
        oldbalanceOrg = float(transaction.get('oldbalanceOrg', 0))
        newbalanceOrig = float(transaction.get('newbalanceOrig', 0))
        oldbalanceDest = float(transaction.get('oldbalanceDest', 0))
        newbalanceDest = float(transaction.get('newbalanceDest', 0))
        txn_type = str(transaction.get('type', ''))
        risk_score = 0
        if txn_type in ['TRANSFER', 'CASH_OUT']:
            risk_score += 20
        if oldbalanceDest == 0:
            risk_score += 15
        if newbalanceOrig == 0:
            risk_score += 20
        drain_ratio = amount / (oldbalanceOrg + 1)
        if drain_ratio > 0.9:
            risk_score += 20
        origin_err = abs((oldbalanceOrg - amount) - newbalanceOrig)
        dest_err = abs((oldbalanceDest + amount) - newbalanceDest)
        if origin_err > 1e6:
            risk_score += 15
        if dest_err > 1e6:
            risk_score += 10
        if amount == oldbalanceOrg and amount > 0:
            risk_score += 20
        if newbalanceOrig == 0 and oldbalanceOrg > 0:
            risk_score += 15
        if oldbalanceDest == 0 and newbalanceDest == 0 and amount > 0:
            risk_score += 15
        if int(transaction.get('isFlaggedFraud', 0)):
            risk_score += 50
        probability = min(risk_score / 100.0, 0.99)
        is_fraud = risk_score >= 50
        return is_fraud, probability

    def store_alert(self, transaction, is_fraud, probability):
        if self.supabase is None:
            return False
        try:
            alert_data = {
                'transaction_id': str(transaction.get('transaction_id', 'UNKNOWN')),
                'timestamp': transaction.get('timestamp', datetime.now().isoformat()),
                'transaction_type': str(transaction.get('type', 'UNKNOWN')),
                'amount': float(transaction.get('amount', 0)),
                'origin_account': str(transaction.get('nameOrig', '')),
                'destination_account': str(transaction.get('nameDest', '')),
                'fraud_probability': float(probability),
                'predicted_fraud': bool(is_fraud),
                'actual_fraud': bool(transaction.get('isFraud', 0)),
                'is_flagged': bool(transaction.get('isFlaggedFraud', 0)),
                'processed_at': datetime.now().isoformat()
            }
            result = self.supabase.table('fraud_alerts').insert(alert_data).execute()
            if result.data:
                self.stats['stored'] += 1
                return True
            return False
        except Exception:
            return False

    def process_message(self, message):
        try:
            transaction = message.value
            self.stats['processed'] += 1
            is_fraud, probability = self.predict(transaction)
            if probability > 0.3:
                self.stats['frauds'] += 1
                stored = self.store_alert(transaction, is_fraud, probability)
                status = "STORED" if stored else "FAILED"
                print(f"FRAUD [{status}] ID:{transaction.get('transaction_id')} "
                      f"Amount:${float(transaction.get('amount',0)):,.0f} Prob:{probability:.1%}")
            if self.stats['processed'] % 100 == 0:
                elapsed = max((datetime.now() - self.stats['start_time']).seconds, 1)
                print(f"Processed:{self.stats['processed']:,} Frauds:{self.stats['frauds']:,} "
                      f"Stored:{self.stats['stored']:,}")
        except Exception:
            self.stats['errors'] += 1

    def run(self):
        print(f"\nStarted: {datetime.now().strftime('%H:%M:%S')}")
        print(f"Detection: {'ML (streaming features)' if self.use_ml else 'Rule‑Based (enhanced)'}")
        print(f"Will stop after {self.max_messages} messages (0 = unlimited)")
        try:
            for message in self.consumer:
                self.process_message(message)
                if self.max_messages > 0 and self.stats['processed'] >= self.max_messages:
                    break
        except KeyboardInterrupt:
            print("\nStopped by user")
        finally:
            elapsed = max((datetime.now() - self.stats['start_time']).seconds, 1)
            print(f"\nDone! Processed:{self.stats['processed']:,} Frauds:{self.stats['frauds']:,} "
                  f"Stored:{self.stats['stored']:,} Rate:{self.stats['processed']/elapsed:.1f}/s")
            self.consumer.close()


if __name__ == "__main__":
    detector = FraudDetector()
    detector.run()