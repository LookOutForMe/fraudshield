<div align="center">

# 🛡️ FraudShield

**Real-time financial fraud detection powered by Kafka, XGBoost, and Supabase**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Aiven-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-ML%20Engine-EE4C2C?style=flat-square)](https://xgboost.readthedocs.io)
[![Supabase](https://img.shields.io/badge/Supabase-Postgres-3ECF8E?style=flat-square&logo=supabase&logoColor=white)](https://supabase.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?style=flat-square&logo=githubactions&logoColor=white)](https://github.com/features/actions)

</div>

---

## 📌 What is FraudShield?

FraudShield is an **end-to-end, production-grade fraud detection pipeline** that ingests financial transactions in real time, runs them through a streaming XGBoost classifier with 40+ engineered features, and surfaces alerts in a live Streamlit operations console — all automated via GitHub Actions CI/CD.

The system is built around three pillars:
- **Stream ingestion** — A Kafka producer on Aiven streams synthetic transaction data (PaySim-style) at configurable throughput
- **Real-time inference** — A Kafka consumer does incremental feature engineering on the fly and scores each transaction with a trained XGBoost model
- **Live observability** — A Streamlit dashboard backed by Supabase Postgres shows alert ledgers, risk distributions, and KPIs in real time

> No batch jobs. No offline ETL. Every transaction goes from wire to decision in under a second.

---

## 🏗️ Architecture

![FraudShield Architecture](https://github.com/user-attachments/assets/55fc149c-941c-4cf9-b13d-6b1932742a5e)

<details>
<summary>Text diagram (fallback)</summary>

```
┌─────────────────────────────────────────────────────────────────────┐
│                        GITHUB ACTIONS CI/CD                         │
│         producer.yml (manual)    consumer.yml (on: detect)          │
└───────────────────┬─────────────────────────┬───────────────────────┘
                    │                         │
          ┌─────────▼──────────┐   ┌──────────▼──────────┐
          │  kafka_producer.py │   │  spark_consumer.py  │
          │  (Transaction feed)│   │  (Fraud Detector)   │
          └─────────┬──────────┘   └──────────┬──────────┘
                    │                         │
          ┌─────────▼──────────────────────────▼──────────┐
          │          Aiven Kafka (SSL/TLS)                 │
          │       Topic: transactions-stream               │
          └─────────────────────────┬──────────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │   StreamingFeatureExtractor       │
                    │   • 40+ engineered features       │
                    │   • Sliding window aggregations   │
                    │   • Balance anomaly detection     │
                    └───────────────┬──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │   XGBoost Classifier              │
                    │   • Loaded from models/           │
                    │   • Adaptive threshold (F1-opt)   │
                    │   • Falls back to rule engine     │
                    └───────────────┬──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │   Supabase (Postgres)             │
                    │   Table: fraud_alerts             │
                    └───────────────┬──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │   Streamlit Dashboard (app.py)   │
                    │   FraudShield Operations Console │
                    └──────────────────────────────────┘
```

</details>

---

## ⚙️ Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| **Message Broker** | Aiven Kafka (SSL/TLS) | Transaction streaming backbone |
| **ML Engine** | XGBoost (via `joblib`) | Fraud scoring & probability output |
| **Feature Engineering** | Custom `StreamingFeatureExtractor` | Incremental, stateful feature computation |
| **Database** | Supabase (Postgres) | Alert persistence & real-time queries |
| **Dashboard** | Streamlit + Plotly | Ops console with live auto-refresh |
| **CI/CD** | GitHub Actions | Automated producer & consumer deployment |
| **Config** | `python-dotenv` + GitHub Secrets | Secure credential management |

---

## 🧠 ML Pipeline Deep Dive

### Feature Engineering (40+ features)

The `StreamingFeatureExtractor` computes features **incrementally per transaction** without any batch lookback — making it viable for true real-time inference.

**Amount-based features**
- `amount`, `amount_log`, `amount_sqrt`
- `amount_to_oldbalance_ratio` — fraction of origin balance being moved
- `is_amount_exceeds_balance` — binary flag for overdraft-style behavior
- `is_round_amount` — round amounts (multiples of 1000) are a fraud signal
- `is_high_amount` — above 95th percentile of training distribution

**Behavioral / velocity features**
- Sliding window counts and sums over 1h, 6h, 24h windows for both origin and destination accounts
- `txn_frequency` — transactions per unit time for the origin account
- `is_new_recipient` — first-ever transaction to this destination
- `time_since_last`, `velocity_before` — temporal spacing signals

**Balance anomaly features**
- `origin_balance_error` — accounts for transactions where `old - amount ≠ new` (a known fraud pattern)
- `is_full_drain` — exact drain of origin account balance
- `is_origin_zero_after`, `has_zero_balance_before`
- `is_dest_empty_shell` — destination has zero balance before and after

**Transaction type features**
- One-hot encoded: `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER`
- `type_encoded` — label-encoded transaction type
- `dest_is_merchant` — destination account prefix 'M' indicates merchant

**Temporal features**
- `hour`, `day_of_week`, `is_night` (22:00–06:00)

### Model Details

- **Algorithm**: XGBoost Gradient Boosted Trees
- **Serialization**: `joblib` dictionary format storing `model`, `feature_names`, `production_threshold`, `roc_auc`, `global_median_amount`, `global_amount_quantile_95`
- **Threshold**: F1-optimal threshold stored at training time, loaded at inference time
- **Fallback**: Rule-based engine (balance drain, empty shell, zero-balance patterns) kicks in if model file is absent

### Rule-based Fallback Logic

When the ML model isn't available, `spark_consumer.py` scores transactions using hand-crafted heuristics derived from domain knowledge:
- Full account drain (`amount == oldbalanceOrg`)
- Destination is an empty shell (0 balance before and after)
- Zero balance origin before transfer
- Drain ratio > 90% of balance

---

## 📁 Project Structure

```
fraudshield/
├── app.py                          # Streamlit dashboard (FraudShield Operations Console)
├── config.py                       # Kafka config, Supabase credentials, model path, features
├── kafka_producer.py               # Streams CSV transactions → Aiven Kafka topic
├── spark_consumer.py               # Consumes Kafka, runs inference, writes to Supabase
├── requirements.txt                # All Python dependencies
├── .env                            # Local env vars (gitignored)
├── .gitignore                      # Excludes certs/, models/, .env, secrets
├── models/
│   └── fraud_model.pkl             # Serialized XGBoost model (gitignored, injected by CI)
├── certs/
│   ├── ca.pem                      # Aiven CA certificate (gitignored)
│   ├── service.cert                # Client certificate (gitignored)
│   └── service.key                 # Client private key (gitignored)
├── data/
│   └── raw/                        # Raw transaction CSVs (gitignored)
└── .github/
    └── workflows/
        ├── producer.yml            # GitHub Actions: stream transactions (manual trigger)
        └── consumer.yml           # GitHub Actions: run fraud detector (on: detect event)
```

---

## 🔄 GitHub Actions CI/CD

FraudShield has a **two-workflow CI/CD setup** designed for headless, secrets-driven operation:

### `producer.yml` — Transaction Stream
- **Trigger**: `workflow_dispatch` (manual)
- Installs Python 3.10 dependencies
- Writes Kafka SSL certs from GitHub Secrets (`KAFKA_CA_CERT`, `KAFKA_SERVICE_CERT`, `KAFKA_SERVICE_KEY`)
- Streams transactions via `kafka_producer.py`

### `consumer.yml` — Fraud Detector
- **Trigger**: `workflow_dispatch` + `on: detect`
- Installs Python 3.10 dependencies
- Reconstructs Kafka certs from secrets
- Decodes and writes the trained model from `MODEL_BASE64` secret (base64-encoded `.pkl`)
- Injects `SUPABASE_URL` + `SUPABASE_KEY` as environment variables
- Runs `spark_consumer.py` to consume, score, and persist alerts

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_KEY` | Supabase anon/service key |
| `KAFKA_CA_CERT` | Aiven CA certificate (PEM, base64 or raw) |
| `KAFKA_SERVICE_CERT` | Aiven client certificate |
| `KAFKA_SERVICE_KEY` | Aiven client private key |
| `MODEL_BASE64` | Base64-encoded `fraud_model.pkl` |

---

## 📊 Dashboard Features

The Streamlit app (`app.py`) is a **Fraud Operations Console** with a dark, monospace aesthetic built on JetBrains Mono + Inter. It connects to Supabase and auto-refreshes at a configurable cadence (5s / 10s / 30s / 1 min).

**Sidebar controls**
- Time range filter: Last 1h / 6h / 24h / 7d
- Risk threshold slider (0.0–1.0) — filters what counts as an alert in the ledger
- Max records: 10–500
- Auto-refresh cadence + force-refresh button

**KPI Ledger (top bar)**
- Total Alerts, Amount At Risk, Detection Rate, Critical Risk (≥0.8 probability), Fraud Caught + amount saved

**Charts**
- Alert Volume Timeline — area chart bucketed by time
- Amount At Risk Timeline — exposure over time
- Fraud By Transaction Type — donut chart breakdown
- Risk Score Distribution — histogram of fraud probabilities

**Alert Ledger (live table)**
- All alerts tab + Critical tab (≥0.8 probability) with color-coded risk rows
- Columns: `transaction_id`, `processed_at`, `transaction_type`, `amount`, `origin_account`, `fraud_probability`, `actual_fraud`

---

## 🚀 Local Setup

### Prerequisites
- Python 3.10+
- An [Aiven](https://aiven.io) Kafka service with SSL credentials
- A [Supabase](https://supabase.com) project with a `fraud_alerts` table
- A trained `fraud_model.pkl` (or the system will use rule-based fallback)

### 1. Clone and install

```bash
git clone https://github.com/LookOutForMe/fraudshield.git
cd fraudshield
pip install -r requirements.txt
```

### 2. Configure environment

```bash
# .env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key
```

```toml
# .streamlit/secrets.toml
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-anon-key"
```

Place your Aiven SSL certificates in `certs/`:
```
certs/
├── ca.pem
├── service.cert
└── service.key
```

Update `config.py` with your Kafka bootstrap server address.

### 3. Create Supabase table

```sql
create table fraud_alerts (
  id uuid primary key default gen_random_uuid(),
  transaction_id text,
  processed_at timestamptz default now(),
  transaction_type text,
  amount numeric,
  origin_account text,
  destination_account text,
  fraud_probability float,
  predicted_fraud boolean,
  actual_fraud boolean
);
```

### 4. Run the pipeline

```bash
# Terminal 1 — start the consumer (fraud detector)
python spark_consumer.py

# Terminal 2 — start streaming transactions
python kafka_producer.py

# Terminal 3 — launch the dashboard
streamlit run app.py
```

---

## 📦 Dependencies

```
kafka-python
pandas
numpy
joblib
supabase
python-dotenv
plotly>=5.0.0
streamlit>=1.25.0
xgboost
scikit-learn
```

---

## 🔐 Security Notes

- All Kafka connections use mutual TLS (SSL) — certs are never committed to the repo
- Supabase credentials are injected via `.streamlit/secrets.toml` locally and GitHub Secrets in CI
- The trained model is base64-encoded and injected at runtime in CI, keeping the binary out of version control
- `.gitignore` excludes `certs/`, `models/`, `.env`, and `data/raw/`

---

## 🙋 Author

**Ariyan Shaw** — [@LookOutForMe](https://github.com/LookOutForMe)  
📧 ariyanshaw123@gmail.com

---

<div align="center">

Built with ☕ and an unhealthy obsession with sub-second latency.

</div>
