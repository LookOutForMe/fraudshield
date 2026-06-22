# fraudshield
☁️ Cloud‑native fraud detection pipeline – Kafka → XGBoost → Supabase → Streamlit.


- **Producer** – streams transactions from a CSV into Kafka (batch mode, scheduled or manual).
- **Consumer** – a streaming feature extractor identical to the training pipeline; scores transactions with a pre‑trained XGBoost model and writes alerts to Supabase.
- **Dashboard** – a beautifully styled, real‑time operations console displaying fraud alerts, trends, and risk distributions.

All components are decoupled; the producer and consumer run as GitHub Actions workflows (completely free for public repos), and the dashboard is hosted on Streamlit Community Cloud.

## 🧱 Tech Stack

| Component | Technology | Free tier |
|-----------|------------|-----------|
| Message queue | Aiven Kafka (SSL) | Trial/free plan |
| Database | Supabase (PostgreSQL) | 500 MB |
| ML model | XGBoost (scikit‑learn compatible) | – |
| Producer/Consumer | Python (kafka‑python) on GitHub Actions | 2 000 min/month |
| Dashboard | Streamlit (Community Cloud) | Unlimited public apps |
| Data storage | Google Drive (raw CSV) | 15 GB free |

## 🚀 Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/your-username/fraud-shield.git
cd fraud-shield
