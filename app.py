"""
FraudShield — Fraud Operations Console
A real-time fraud detection command console built on Supabase + Streamlit.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from supabase import create_client
import time

# ──────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FraudShield — Fraud Operations Console",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────
# DESIGN TOKENS
# ──────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

:root {
    --ink: #0B0E14;
    --panel: #141A24;
    --panel-2: #1B2330;
    --line: #242C3B;
    --signal: #5EEAD4;
    --signal-dim: rgba(94, 234, 212, 0.12);
    --alert: #FB7185;
    --alert-dim: rgba(251, 113, 133, 0.14);
    --amber: #FBBF66;
    --amber-dim: rgba(251, 191, 102, 0.12);
    --text: #EAF0F7;
    --text-mute: #8B96A8;
    --text-faint: #4F596B;
}

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: var(--ink); }

.mono, .stMetric div[data-testid="stMetricValue"], .stMetric div[data-testid="stMetricDelta"] {
    font-family: 'JetBrains Mono', monospace !important;
    font-variant-numeric: tabular-nums;
}

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1400px; }

/* ── Masthead ─────────────────────────────────────────────────────── */
.console-mast {
    display: flex; align-items: baseline; justify-content: space-between;
    border-bottom: 1px solid var(--line); padding-bottom: 18px; margin-bottom: 4px;
}
.console-title {
    font-size: 1.7rem; font-weight: 800; color: var(--text);
    letter-spacing: -0.02em; display: flex; align-items: center; gap: 10px;
}
.console-title .mark { color: var(--signal); font-size: 1.5rem; }
.console-sub {
    font-family: 'JetBrains Mono', monospace; font-size: 0.72rem;
    color: var(--text-faint); letter-spacing: 0.08em; text-transform: uppercase;
    margin-top: 2px;
}
.console-clock {
    font-family: 'JetBrains Mono', monospace; font-size: 0.85rem;
    color: var(--text-mute); text-align: right;
}
.console-clock .live-dot {
    display: inline-block; width: 7px; height: 7px; border-radius: 50%;
    background: var(--signal); margin-right: 7px;
    box-shadow: 0 0 0 0 rgba(94,234,212,0.6);
    animation: livepulse 2s infinite;
}
@keyframes livepulse {
    0% { box-shadow: 0 0 0 0 rgba(94,234,212,0.55); }
    70% { box-shadow: 0 0 0 8px rgba(94,234,212,0); }
    100% { box-shadow: 0 0 0 0 rgba(94,234,212,0); }
}

/* ── KPI ledger ───────────────────────────────────────────────────── */
.ledger {
    display: grid; grid-template-columns: repeat(5, 1fr);
    background: var(--panel); border: 1px solid var(--line); border-radius: 14px;
    overflow: hidden; margin: 18px 0 22px 0;
}
.ledger-cell {
    padding: 18px 20px; border-right: 1px solid var(--line);
    transition: background 0.15s ease;
}
.ledger-cell:last-child { border-right: none; }
.ledger-cell:hover { background: var(--panel-2); }
.ledger-label {
    font-size: 0.68rem; color: var(--text-faint); text-transform: uppercase;
    letter-spacing: 0.09em; font-weight: 600; margin-bottom: 8px;
}
.ledger-value {
    font-family: 'JetBrains Mono', monospace; font-size: 1.75rem; font-weight: 700;
    color: var(--text); font-variant-numeric: tabular-nums; line-height: 1;
}
.ledger-value.alert-tone { color: var(--alert); }
.ledger-value.signal-tone { color: var(--signal); }
.ledger-delta {
    font-family: 'JetBrains Mono', monospace; font-size: 0.72rem;
    color: var(--text-mute); margin-top: 7px;
}

/* ── Section headers ──────────────────────────────────────────────── */
.section-eyebrow {
    font-family: 'JetBrains Mono', monospace; font-size: 0.7rem;
    color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.1em;
    margin-bottom: 2px;
}
.section-title {
    font-size: 1.05rem; font-weight: 700; color: var(--text); margin-bottom: 14px;
}

/* ── Panels ────────────────────────────────────────────────────────── */
.panel {
    background: var(--panel); border: 1px solid var(--line); border-radius: 14px;
    padding: 20px 22px; margin-bottom: 18px;
}

/* ── Critical alert cards ─────────────────────────────────────────── */
.threat-row {
    display: flex; align-items: center; gap: 14px;
    background: var(--alert-dim); border-left: 3px solid var(--alert);
    border-radius: 8px; padding: 13px 16px; margin-bottom: 8px;
    font-family: 'JetBrains Mono', monospace; font-size: 0.82rem; color: var(--text);
}
.threat-row .sev { color: var(--alert); font-weight: 700; min-width: 64px; }
.threat-row .meta { color: var(--text-mute); }
.threat-row .amt { color: var(--text); font-weight: 600; margin-left: auto; }

/* ── Status pills (sidebar) ───────────────────────────────────────── */
.status-pill {
    display: flex; align-items: center; gap: 8px; padding: 9px 12px;
    background: var(--panel-2); border: 1px solid var(--line); border-radius: 8px;
    font-family: 'JetBrains Mono', monospace; font-size: 0.74rem; color: var(--text-mute);
    margin-bottom: 6px;
}
.status-pill .dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
.status-pill .dot.ok { background: var(--signal); box-shadow: 0 0 6px var(--signal); }
.status-pill .dot.down { background: var(--alert); box-shadow: 0 0 6px var(--alert); }
.status-pill .dot.warn { background: var(--amber); box-shadow: 0 0 6px var(--amber); }

/* ── Streamlit widget restyling ───────────────────────────────────── */
section[data-testid="stSidebar"] { background: var(--panel); border-right: 1px solid var(--line); }
div[data-testid="stMetric"] {
    background: transparent; border: none; padding: 0; box-shadow: none;
}
.stTabs [data-baseweb="tab-list"] { gap: 4px; background: var(--panel-2); padding: 4px; border-radius: 10px; }
.stTabs [data-baseweb="tab"] {
    border-radius: 7px; color: var(--text-mute); font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem; padding: 7px 16px;
}
.stTabs [aria-selected="true"] { background: var(--panel); color: var(--signal) !important; }

.empty-state {
    text-align: center; padding: 48px 20px; color: var(--text-faint);
    font-family: 'JetBrains Mono', monospace; font-size: 0.82rem;
}
.empty-state .glyph { font-size: 1.8rem; color: var(--text-faint); margin-bottom: 10px; }

.footer-strip {
    margin-top: 28px; padding-top: 16px; border-top: 1px solid var(--line);
    display: flex; justify-content: space-between; align-items: center;
    font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; color: var(--text-faint);
}

::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: var(--ink); }
::-webkit-scrollbar-thumb { background: var(--line); border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

PLOTLY_TEMPLATE = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="JetBrains Mono, monospace", color="#8B96A8", size=11),
    margin=dict(l=0, r=10, t=10, b=0),
    xaxis=dict(gridcolor="#1B2330", zerolinecolor="#242C3B", showgrid=True),
    yaxis=dict(gridcolor="#1B2330", zerolinecolor="#242C3B", showgrid=True),
    hoverlabel=dict(bgcolor="#1B2330", font_family="JetBrains Mono, monospace", bordercolor="#242C3B"),
)

# ──────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ──────────────────────────────────────────────────────────────────────────
EMPTY_STATS = {
    'total_alerts': 0, 'total_amount': 0, 'avg_probability': 0, 'unique_accounts': 0,
    'actual_frauds': 0, 'alerts_last_hour': 0, 'high_risk_count': 0, 'max_probability': 0,
    'false_positives': 0, 'detection_rate': 0, 'avg_amount': 0, 'total_saved': 0,
    'medium_risk_count': 0, 'low_risk_count': 0,
}


@st.cache_resource(ttl=300)
def init_supabase():
    try:
        url = st.secrets['SUPABASE_URL']
        key = st.secrets['SUPABASE_KEY']
        return create_client(url, key), None
    except KeyError:
        return None, "Missing SUPABASE_URL / SUPABASE_KEY in .streamlit/secrets.toml"
    except Exception as e:
        return None, f"Connection failed: {e}"


def _coerce_bool_col(df, col):
    if col not in df.columns:
        return pd.Series([False] * len(df))
    s = df[col]
    if s.dtype == bool:
        return s
    return s.astype(str).str.lower().isin(['true', '1', 't', 'yes'])


@st.cache_data(ttl=10)
def fetch_statistics(hours=24):
    supabase, err = init_supabase()
    if not supabase:
        return dict(EMPTY_STATS), err
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        response = supabase.table('fraud_alerts').select('*').gte('processed_at', since).execute()
        if not response.data:
            return dict(EMPTY_STATS), None

        df = pd.DataFrame(response.data)
        actual_fraud = _coerce_bool_col(df, 'actual_fraud')
        predicted_fraud = _coerce_bool_col(df, 'predicted_fraud')

        total_alerts = len(df)
        actual_frauds = int(actual_fraud.sum())
        predicted_frauds = int(predicted_fraud.sum())
        total_amount = float(df['amount'].sum()) if 'amount' in df else 0.0
        false_positives = max(predicted_frauds - actual_frauds, 0)

        stats = dict(EMPTY_STATS)
        stats.update({
            'total_alerts': total_alerts,
            'total_amount': total_amount,
            'avg_probability': float(df['fraud_probability'].mean()) if 'fraud_probability' in df else 0,
            'unique_accounts': int(df['origin_account'].nunique()) if 'origin_account' in df else 0,
            'actual_frauds': actual_frauds,
            'max_probability': float(df['fraud_probability'].max()) if 'fraud_probability' in df else 0,
            'false_positives': false_positives,
            'detection_rate': (actual_frauds / max(predicted_frauds, 1)) * 100,
            'avg_amount': total_amount / max(total_alerts, 1),
            'total_saved': float(df.loc[actual_fraud, 'amount'].sum()) if 'amount' in df else 0.0,
        })
        if 'fraud_probability' in df:
            p = df['fraud_probability'].astype(float)
            stats['high_risk_count'] = int((p > 0.8).sum())
            stats['medium_risk_count'] = int(((p > 0.5) & (p <= 0.8)).sum())
            stats['low_risk_count'] = int((p <= 0.5).sum())
        if 'processed_at' in df:
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            ts = pd.to_datetime(df['processed_at'], utc=True, errors='coerce')
            stats['alerts_last_hour'] = int((ts > one_hour_ago).sum())
        return stats, None
    except Exception as e:
        return dict(EMPTY_STATS), f"Stats query failed: {e}"


@st.cache_data(ttl=10)
def fetch_recent_alerts(limit=100, min_probability=0.5):
    supabase, err = init_supabase()
    if not supabase:
        return pd.DataFrame(), err
    try:
        response = (supabase.table('fraud_alerts').select('*')
                    .gte('fraud_probability', min_probability)
                    .order('processed_at', desc=True).limit(limit).execute())
        if not response.data:
            return pd.DataFrame(), None
        df = pd.DataFrame(response.data)
        if 'processed_at' in df:
            df['processed_at'] = pd.to_datetime(df['processed_at'], utc=True, errors='coerce')
        if 'amount' in df:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        if 'fraud_probability' in df:
            df['fraud_probability'] = pd.to_numeric(df['fraud_probability'], errors='coerce').fillna(0)
        if 'actual_fraud' in df:
            df['actual_fraud'] = _coerce_bool_col(df, 'actual_fraud')
        return df, None
    except Exception as e:
        return pd.DataFrame(), f"Alert query failed: {e}"


@st.cache_data(ttl=10)
def fetch_timeline_data(hours=24):
    supabase, err = init_supabase()
    if not supabase:
        return pd.DataFrame(), err
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        response = (supabase.table('fraud_alerts')
                    .select('processed_at, amount, fraud_probability, transaction_type')
                    .gte('processed_at', since).order('processed_at').execute())
        if not response.data:
            return pd.DataFrame(), None
        df = pd.DataFrame(response.data)
        df['processed_at'] = pd.to_datetime(df['processed_at'], utc=True, errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        df['fraud_probability'] = pd.to_numeric(df['fraud_probability'], errors='coerce').fillna(0)
        # FIX: use lowercase aliases required by pandas >= 2.2 / 3.x
        if hours <= 1:
            df['time_bucket'] = df['processed_at'].dt.floor('5min')
        elif hours <= 24:
            df['time_bucket'] = df['processed_at'].dt.floor('h')
        else:
            df['time_bucket'] = df['processed_at'].dt.floor('D')
        return df, None
    except Exception as e:
        return pd.DataFrame(), f"Timeline query failed: {e}"


@st.cache_data(ttl=30)
def fetch_transaction_types():
    supabase, err = init_supabase()
    if not supabase:
        return pd.DataFrame(), err
    try:
        response = supabase.table('fraud_alerts').select('transaction_type, amount, fraud_probability').execute()
        if not response.data:
            return pd.DataFrame(), None
        df = pd.DataFrame(response.data)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        df['fraud_probability'] = pd.to_numeric(df['fraud_probability'], errors='coerce').fillna(0)
        type_stats = df.groupby('transaction_type').agg(
            count=('amount', 'count'), total_amount=('amount', 'sum'),
            avg_amount=('amount', 'mean'), avg_probability=('fraud_probability', 'mean'),
        ).round(2)
        return type_stats.reset_index(), None
    except Exception as e:
        return pd.DataFrame(), f"Transaction-type query failed: {e}"


@st.cache_data(ttl=10)
def fetch_pulse(minutes=60):
    supabase, err = init_supabase()
    if not supabase:
        return pd.DataFrame(), err
    try:
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        response = (supabase.table('fraud_alerts').select('processed_at')
                    .gte('processed_at', since).execute())
        if not response.data:
            return pd.DataFrame(), None
        df = pd.DataFrame(response.data)
        df['processed_at'] = pd.to_datetime(df['processed_at'], utc=True, errors='coerce')
        df['bucket'] = df['processed_at'].dt.floor('1min')
        counts = df.groupby('bucket').size().reset_index(name='count')
        full_range = pd.date_range(
            end=datetime.now(timezone.utc).replace(second=0, microsecond=0),
            periods=minutes, freq='1min', tz='UTC'
        )
        counts = counts.set_index('bucket').reindex(full_range, fill_value=0).reset_index()
        counts.columns = ['bucket', 'count']
        return counts, None
    except Exception as e:
        return pd.DataFrame(), f"Pulse query failed: {e}"


# ──────────────────────────────────────────────────────────────────────────
# FORMATTERS
# ──────────────────────────────────────────────────────────────────────────
def format_currency(amount):
    if amount is None or amount == 0:
        return "$0"
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount >= 1_000_000_000:
        return f"{sign}${amount/1_000_000_000:.2f}B"
    if amount >= 1_000_000:
        return f"{sign}${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"{sign}${amount/1_000:.1f}K"
    return f"{sign}${amount:,.0f}"


def format_probability(prob):
    return f"{(prob or 0)*100:.1f}%"


def ledger_cell(label, value, delta=None, tone=""):
    tone_class = f" {tone}" if tone else ""
    delta_html = f'<div class="ledger-delta">{delta}</div>' if delta else ''
    return f"""<div class="ledger-cell">
        <div class="ledger-label">{label}</div>
        <div class="ledger-value{tone_class}">{value}</div>
        {delta_html}
    </div>"""


# ──────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────────────
supabase_client, conn_err = init_supabase()

with st.sidebar:
    st.markdown("""
        <div style="padding: 6px 2px 18px 2px;">
            <div style="font-size:1.5rem; font-weight:800; color:#EAF0F7; display:flex; align-items:center; gap:8px;">
                <span style="color:#5EEAD4;">◈</span> FraudShield
            </div>
            <div style="font-family:'JetBrains Mono',monospace; font-size:0.7rem; color:#4F596B; letter-spacing:0.08em; margin-top:2px;">
                OPERATIONS CONSOLE
            </div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-eyebrow">System</div>', unsafe_allow_html=True)
    db_ok = supabase_client is not None
    st.markdown(f"""
        <div class="status-pill"><span class="dot {'ok' if db_ok else 'down'}"></span>Database {'connected' if db_ok else 'unreachable'}</div>
        <div class="status-pill"><span class="dot ok"></span>Model serving</div>
        <div class="status-pill"><span class="dot ok"></span>Stream ingest</div>
    """, unsafe_allow_html=True)
    if conn_err:
        st.error(conn_err, icon="⚠️")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-eyebrow">Filters</div>', unsafe_allow_html=True)
    time_range = st.selectbox(
        "Time horizon", ["Last 15 minutes", "Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days"],
        index=3, label_visibility="visible"
    )
    time_map = {"Last 15 minutes": 0.25, "Last 1 hour": 1, "Last 6 hours": 6, "Last 24 hours": 24, "Last 7 days": 168}
    selected_hours = time_map[time_range]
    min_probability = st.slider("Risk threshold", 0.0, 1.0, 0.5, 0.05)
    max_alerts = st.slider("Max records", 10, 500, 100, 10)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-eyebrow">Refresh</div>', unsafe_allow_html=True)
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_interval = 10
    if auto_refresh:
        refresh_choice = st.selectbox("Cadence", ["5 seconds", "10 seconds", "30 seconds", "1 minute"], index=1)
        refresh_interval = {"5 seconds": 5, "10 seconds": 10, "30 seconds": 30, "1 minute": 60}[refresh_choice]
    if st.button("Force refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("About this console"):
        st.markdown(
            "**FraudShield v3**\n\n"
            "Real-time fraud surfacing on top of a streaming XGBoost classifier, "
            "fed by Kafka and persisted to Supabase.\n\n"
            "Risk threshold filters what counts as an *alert* in the ledger below — "
            "it does not change what the model flags upstream.\n\n"
            "---\n"
            "**Built by** Ariyan Shaw\n\n"
            "📧 [ariyanshaw123@gmail.com](mailto:ariyanshaw123@gmail.com)"
        )

# ──────────────────────────────────────────────────────────────────────────
# MASTHEAD
# ──────────────────────────────────────────────────────────────────────────
now_str = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
mast_l, mast_r = st.columns([3, 1])
with mast_l:
    st.markdown(f"""
        <div class="console-title"><span class="mark">◈</span>FraudShield</div>
        <div class="console-sub">REAL-TIME FRAUD OPERATIONS — {time_range.upper()}</div>
    """, unsafe_allow_html=True)
with mast_r:
    st.markdown(f"""
        <div class="console-clock"><span class="live-dot"></span>{now_str}</div>
    """, unsafe_allow_html=True)
st.markdown('<div style="border-bottom:1px solid #242C3B; margin: 14px 0 0 0;"></div>', unsafe_allow_html=True)

if not supabase_client:
    st.error(
        f"**Cannot reach the database.** {conn_err}\n\n"
        "Add `SUPABASE_URL` and `SUPABASE_KEY` to `.streamlit/secrets.toml` and reload.",
        icon="🛑"
    )
    st.stop()

# ──────────────────────────────────────────────────────────────────────────
# PULSE STRIP
# FIX: config= does not belong in update_layout() — it's a Plotly.js
# renderer option, not a layout property. Pass it only to st.plotly_chart().
# Also override xaxis/yaxis inside the spread dict to avoid the
# "multiple values for keyword argument" TypeError from the previous version.
# ──────────────────────────────────────────────────────────────────────────
pulse_df, pulse_err = fetch_pulse(60)
if not pulse_df.empty:
    fig_pulse = go.Figure()
    fig_pulse.add_trace(go.Scatter(
        x=pulse_df['bucket'], y=pulse_df['count'], mode='lines',
        line=dict(color='#5EEAD4', width=1.6, shape='spline', smoothing=0.6),
        fill='tozeroy', fillcolor='rgba(94,234,212,0.08)', hovertemplate='%{y} alerts<extra></extra>',
    ))
    fig_pulse.update_layout(**{
        **PLOTLY_TEMPLATE,
        'margin': dict(l=0, r=0, t=4, b=0),
        'height': 64,
        'showlegend': False,
        'xaxis': dict(visible=False),   # overrides PLOTLY_TEMPLATE's xaxis
        'yaxis': dict(visible=False),   # overrides PLOTLY_TEMPLATE's yaxis
    })
    st.plotly_chart(fig_pulse, use_container_width=True, config={'displayModeBar': False})
else:
    st.markdown(
        '<div style="height:64px; border:1px dashed #242C3B; border-radius:10px; '
        'display:flex; align-items:center; justify-content:center; color:#4F596B; '
        'font-family:\'JetBrains Mono\',monospace; font-size:0.75rem;">'
        'NO PULSE DATA — STREAM QUIET IN LAST 60 MIN</div>', unsafe_allow_html=True
    )

# ──────────────────────────────────────────────────────────────────────────
# KPI LEDGER
# ──────────────────────────────────────────────────────────────────────────
stats, stats_err = fetch_statistics(selected_hours)
if stats_err:
    st.warning(stats_err, icon="⚠️")

ledger_html = '<div class="ledger">'
ledger_html += ledger_cell("Total Alerts", f"{stats['total_alerts']:,}", f"+{stats['alerts_last_hour']} last hour")
ledger_html += ledger_cell("Amount At Risk", format_currency(stats['total_amount']), f"avg {format_currency(stats['avg_amount'])}/alert")
ledger_html += ledger_cell("Detection Rate", f"{stats['detection_rate']:.1f}%", f"{stats['false_positives']} false positives",
                            tone="signal-tone" if stats['detection_rate'] >= 70 else "")
ledger_html += ledger_cell("Critical Risk", f"{stats['high_risk_count']:,}", f"{stats['medium_risk_count']} medium",
                            tone="alert-tone" if stats['high_risk_count'] > 0 else "")
ledger_html += ledger_cell("Fraud Caught", f"{stats['actual_frauds']:,}", f"saved {format_currency(stats['total_saved'])}",
                            tone="signal-tone")
ledger_html += '</div>'
st.markdown(ledger_html, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────
# TIMELINE CHARTS
# ──────────────────────────────────────────────────────────────────────────
timeline_df, timeline_err = fetch_timeline_data(selected_hours)
if timeline_err:
    st.warning(timeline_err, icon="⚠️")

ch1, ch2 = st.columns(2)

with ch1:
    st.markdown('<div class="section-eyebrow">Volume</div><div class="section-title">Alert Timeline</div>', unsafe_allow_html=True)
    if not timeline_df.empty:
        agg = timeline_df.groupby('time_bucket').agg(
            alerts=('processed_at', 'count'), total_amount=('amount', 'sum'),
        ).reset_index()
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=agg['time_bucket'], y=agg['alerts'],
            marker=dict(color='#FB7185', opacity=0.85, line=dict(width=0)),
            hovertemplate='%{y} alerts<extra></extra>',
        ))
        fig.update_layout(**PLOTLY_TEMPLATE, height=320, bargap=0.3)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.markdown('<div class="empty-state"><div class="glyph">∿</div>No alerts in this window yet.</div>', unsafe_allow_html=True)

with ch2:
    st.markdown('<div class="section-eyebrow">Exposure</div><div class="section-title">Amount At Risk</div>', unsafe_allow_html=True)
    if not timeline_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=agg['time_bucket'], y=agg['total_amount'], mode='lines',
            line=dict(color='#5EEAD4', width=2.2, shape='spline', smoothing=0.4),
            fill='tozeroy', fillcolor='rgba(94,234,212,0.08)',
            hovertemplate='$%{y:,.0f}<extra></extra>',
        ))
        fig.update_layout(**PLOTLY_TEMPLATE, height=320)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.markdown('<div class="empty-state"><div class="glyph">∿</div>No exposure data in this window yet.</div>', unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────
# TYPE BREAKDOWN + RISK DISTRIBUTION
# ──────────────────────────────────────────────────────────────────────────
ch3, ch4 = st.columns(2)

with ch3:
    st.markdown('<div class="section-eyebrow">Composition</div><div class="section-title">Fraud By Transaction Type</div>', unsafe_allow_html=True)
    type_df, type_err = fetch_transaction_types()
    if type_err:
        st.warning(type_err, icon="⚠️")
    if not type_df.empty:
        palette = ['#5EEAD4', '#FB7185', '#FBBF66', '#7C9BFF', '#C792EA', '#4F596B']
        fig = go.Figure(data=[go.Pie(
            labels=type_df['transaction_type'], values=type_df['count'], hole=0.62,
            marker=dict(colors=palette, line=dict(color='#0B0E14', width=2)),
            textfont=dict(family='JetBrains Mono, monospace', size=11, color='#EAF0F7'),
            hovertemplate='%{label}: %{value} (%{percent})<extra></extra>',
        )])
        fig.update_layout(**PLOTLY_TEMPLATE, height=320, showlegend=True,
                           legend=dict(orientation='v', font=dict(size=10), x=1.05, y=0.5))
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.markdown('<div class="empty-state"><div class="glyph">◷</div>No transaction-type data yet.</div>', unsafe_allow_html=True)

with ch4:
    st.markdown('<div class="section-eyebrow">Distribution</div><div class="section-title">Risk Score Spread</div>', unsafe_allow_html=True)
    alerts_for_hist, hist_err = fetch_recent_alerts(limit=500, min_probability=0.0)
    if hist_err:
        st.warning(hist_err, icon="⚠️")
    if not alerts_for_hist.empty and 'fraud_probability' in alerts_for_hist.columns:
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=alerts_for_hist['fraud_probability'], nbinsx=25,
            marker=dict(color='#5EEAD4', opacity=0.75, line=dict(width=0)),
        ))
        fig.add_vline(x=0.5, line_dash="dot", line_color="#FBBF66", annotation_text="medium",
                      annotation_font=dict(family='JetBrains Mono, monospace', size=10, color='#FBBF66'))
        fig.add_vline(x=0.8, line_dash="dot", line_color="#FB7185", annotation_text="high",
                      annotation_font=dict(family='JetBrains Mono, monospace', size=10, color='#FB7185'))
        fig.update_layout(**PLOTLY_TEMPLATE, height=320, bargap=0.05)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.markdown('<div class="empty-state"><div class="glyph">◷</div>No risk-score data yet.</div>', unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────
# ALERT LEDGER
# ──────────────────────────────────────────────────────────────────────────
st.markdown('<div class="section-eyebrow">Live Feed</div><div class="section-title">Alert Ledger</div>', unsafe_allow_html=True)
alerts_df, alerts_err = fetch_recent_alerts(limit=max_alerts, min_probability=min_probability)
if alerts_err:
    st.warning(alerts_err, icon="⚠️")

if not alerts_df.empty:
    tab1, tab2 = st.tabs(["All Alerts", f"Critical ({int((alerts_df.get('fraud_probability', pd.Series(dtype=float)) >= 0.8).sum())})"])

    with tab1:
        display_cols = ['transaction_id', 'processed_at', 'transaction_type', 'amount',
                         'origin_account', 'fraud_probability', 'actual_fraud']
        available = [c for c in display_cols if c in alerts_df.columns]
        display_df = alerts_df[available].copy()

        raw_prob = alerts_df['fraud_probability'].values if 'fraud_probability' in alerts_df else np.zeros(len(alerts_df))

        if 'amount' in display_df:
            display_df['amount'] = display_df['amount'].apply(format_currency)
        if 'fraud_probability' in display_df:
            display_df['fraud_probability'] = display_df['fraud_probability'].apply(format_probability)
        if 'processed_at' in display_df:
            display_df['processed_at'] = display_df['processed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        if 'actual_fraud' in display_df:
            display_df['actual_fraud'] = display_df['actual_fraud'].map({True: 'Confirmed', False: 'Flagged'})
        display_df.columns = [c.replace('_', ' ').title() for c in display_df.columns]

        # Vectorized row styling — no per-row try/except string parsing
        def style_by_risk(_df):
            styles = pd.DataFrame('', index=_df.index, columns=_df.columns)
            high = raw_prob >= 0.8
            med = (raw_prob >= 0.6) & (raw_prob < 0.8)
            styles.loc[high, :] = 'background-color: rgba(251,113,133,0.12); font-weight: 600; color: #FECDD6;'
            styles.loc[med, :] = 'background-color: rgba(251,191,102,0.10); color: #FDE3BB;'
            return styles

        st.dataframe(
            display_df.style.apply(style_by_risk, axis=None),
            use_container_width=True, height=460, hide_index=True,
        )

    with tab2:
        if 'fraud_probability' in alerts_df.columns:
            critical = alerts_df[alerts_df['fraud_probability'] >= 0.8]
            if not critical.empty:
                st.markdown(
                    f'<div style="color:#FB7185; font-family:\'JetBrains Mono\',monospace; '
                    f'font-size:0.85rem; margin-bottom:12px;">⬤ {len(critical)} critical threat(s) at or above 80% confidence</div>',
                    unsafe_allow_html=True
                )
                for _, alert in critical.head(8).iterrows():
                    ts = alert.get('processed_at', None)
                    ts_str = ts.strftime('%H:%M:%S') if pd.notna(ts) else 'N/A'
                    st.markdown(f"""
                        <div class="threat-row">
                            <span class="sev">{format_probability(alert.get('fraud_probability', 0))}</span>
                            <span class="meta">{alert.get('transaction_id', 'N/A')} · {alert.get('transaction_type', 'N/A')} · {alert.get('origin_account', 'N/A')} · {ts_str}</span>
                            <span class="amt">{format_currency(alert.get('amount', 0))}</span>
                        </div>
                    """, unsafe_allow_html=True)
            else:
                st.markdown(
                    '<div class="empty-state"><div class="glyph">✓</div>No critical threats at the current risk threshold.</div>',
                    unsafe_allow_html=True
                )
else:
    st.markdown("""
        <div class="empty-state">
            <div class="glyph">◷</div>
            No alerts at this risk threshold yet.<br>
            <span style="color:#4F596B; font-size:0.72rem;">
                Run the pipeline: <code>python src/kafka_producer.py</code> and <code>python src/fraud_detector.py</code>
            </span>
        </div>
    """, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────
# FOOTER
# ──────────────────────────────────────────────────────────────────────────
st.markdown(f"""
    <div class="footer-strip">
        <span>FraudShield Operations Console · Kafka → XGBoost → Supabase</span>
        <span style="color: var(--text-faint);">
            Built by <span style="color: var(--signal);">Ariyan Shaw</span>
            · <a href="mailto:ariyanshaw123@gmail.com" style="color: var(--text-mute); text-decoration: none;">ariyanshaw123@gmail.com</a>
        </span>
        <span>Last updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</span>
    </div>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────
# AUTO-REFRESH — non-blocking
# Uses a 1-second polling tick tracked via session_state so the UI stays
# responsive while still honouring the user's chosen refresh cadence.
# ──────────────────────────────────────────────────────────────────────────
if auto_refresh:
    if 'last_refresh_ts' not in st.session_state:
        st.session_state.last_refresh_ts = time.time()
    elapsed = time.time() - st.session_state.last_refresh_ts
    remaining = max(refresh_interval - elapsed, 0)
    if remaining <= 0:
        st.session_state.last_refresh_ts = time.time()
        st.cache_data.clear()
        st.rerun()
    else:
        time.sleep(min(1.0, remaining))
        st.rerun()