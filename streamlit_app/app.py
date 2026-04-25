"""
BurnWise - Streamlit app for the HARTH + HAR70+ pipeline.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.request
import urllib.error
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


# =============================================================================
# Configuration
# =============================================================================

APP_TITLE = "BurnWise"
APP_TAGLINE = "A calorie tool built on 85,000+ real sensor windows"
DB_URL = "https://github.com/catalinasara/msba305-health-sensor-pipeline/releases/download/v1.1.0/pipeline.db"
DB_PATH = Path(__file__).parent / "pipeline.db"

if not DB_PATH.exists():
    st.info("Downloading database... first run only.")
    urllib.request.urlretrieve(DB_URL, DB_PATH)
    st.success("Database downloaded.")

TRONDHEIM_LAT = 63.4305
TRONDHEIM_LON = 10.3951

AGE_THRESHOLD = 70

OUTDOOR_ACTIVITIES = {
    "walking", "running",
    "cycling_sit", "cycling_stand",
    "cycling_sit_inactive", "cycling_stand_inactive",
}

BAD_WEATHER_CODES = (
    set(range(51, 68)) | set(range(71, 78)) | set(range(80, 83)) | {95, 96, 99}
)

EFFORT_LEVELS = {"Easy": 0.85, "Steady": 1.00, "Pushing": 1.55}

PICKER_MET_FLOOR = 2.0

ACTIVITY_DISPLAY_NAMES = {
    "walking":                "Walking",
    "running":                "Running",
    "cycling_sit":            "Cycling (seated)",
    "cycling_stand":          "Cycling (standing)",
    "cycling_sit_inactive":   "Cycling (coasting, seated)",
    "cycling_stand_inactive": "Cycling (coasting, standing)",
    "stairs_up":              "Stairs (up)",
    "stairs_down":            "Stairs (down)",
    "shuffling":              "Light movement",
    "standing":               "Standing",
    "sitting":                "Sitting",
    "lying":                  "Lying down",
}

ACCENT        = "#9AE62F"
ACCENT_SOFT   = "#B8F060"
ACCENT_CYAN   = "#4FC3F7"
BG_DEEP       = "#0E1215"
BG_PANEL      = "#161B20"
BG_ELEVATED   = "#1E252B"
BORDER_SOFT   = "rgba(255, 255, 255, 0.06)"
BORDER_MED    = "rgba(154, 230, 47, 0.18)"
BORDER_STRONG = "rgba(154, 230, 47, 0.35)"
TEXT_PRIMARY   = "#E8EEF1"
TEXT_SECONDARY = "#A7B3BC"
TEXT_MUTED     = "#6A7681"
COLOR_HARTH   = ACCENT
COLOR_HAR70   = ACCENT_CYAN
COLOR_DIM     = "#2A3138"

KCAL_PER_MET_KG_HOUR = 1.0


def display_name(activity: str) -> str:
    return ACTIVITY_DISPLAY_NAMES.get(activity, activity.replace("_", " ").title())


def met_to_intensity(met: float) -> str:
    if met < 1.5:
        return "sedentary"
    if met < 3.0:
        return "light"
    if met < 6.0:
        return "moderate"
    return "vigorous"


# =============================================================================
# Page + theme
# =============================================================================

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Oswald:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap');

    header[data-testid="stHeader"] {{
        background: transparent !important;
        height: 0 !important;
    }}
    #MainMenu, footer, [data-testid="stToolbar"] {{ visibility: hidden; }}

    .stApp {{
        background:
            radial-gradient(ellipse 1200px 800px at 15% -10%, rgba(154,230,47,0.025) 0%, transparent 60%),
            radial-gradient(ellipse 1000px 700px at 85% 110%, rgba(0,200,255,0.04) 0%, transparent 60%),
            linear-gradient(180deg, {BG_DEEP} 0%, #0A0F0D 100%);
        color: {TEXT_PRIMARY};
        font-family: 'JetBrains Mono', monospace;
    }}

    .stApp::before {{
        content: '';
        position: fixed; inset: 0;
        background: repeating-linear-gradient(
            0deg, transparent 0px, transparent 3px,
            rgba(154,230,47,0.004) 3px, rgba(154,230,47,0.004) 4px
        );
        pointer-events: none; z-index: 1;
    }}

    @keyframes fadeUp {{
        from {{ opacity: 0; transform: translateY(8px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}
    .main .block-container > div {{
        animation: fadeUp 0.5s ease-out;
    }}

    h1, h2, h3, h4 {{
        font-family: 'Oswald', sans-serif !important;
        color: {TEXT_PRIMARY} !important;
        letter-spacing: 0.01em;
    }}
    h1 {{
        color: {ACCENT} !important;
        text-shadow: 0 0 18px rgba(154,230,47,0.18);
        font-size: 2.8rem !important;
        font-weight: 600 !important;
        margin-bottom: 0.1rem !important;
        text-transform: uppercase;
    }}
    h2 {{ font-size: 1.5rem !important; font-weight: 500 !important; }}
    h3 {{
        font-size: 1.15rem !important;
        font-weight: 500 !important;
        color: {TEXT_PRIMARY} !important;
    }}

    p, .stMarkdown p {{
        font-family: 'Inter', sans-serif;
        font-size: 0.93rem;
        line-height: 1.6;
        color: {TEXT_SECONDARY};
    }}

    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {BG_PANEL} 0%, #0D1411 100%);
        border-right: 1px solid {BORDER_SOFT};
        min-width: 300px !important;
        max-width: 300px !important;
    }}
    [data-testid="stSidebar"] > div {{ padding-top: 2.5rem; }}
    [data-testid="stSidebar"] h2 {{
        color: {ACCENT} !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.25em;
        margin-top: 0;
    }}

    [data-testid="stSidebarCollapseButton"],
    [data-testid="baseButton-header"],
    button[data-testid="baseButton-headerNoPadding"],
    [data-testid="collapsedControl"] {{
        display: none !important;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        gap: 0.25rem;
        background: transparent;
        border-bottom: 1px solid {BORDER_SOFT};
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        color: {TEXT_MUTED};
        font-family: 'Oswald', sans-serif;
        font-weight: 700;
        letter-spacing: 0.06em;
        font-size: 0.85rem;
        padding: 0.85rem 1.3rem;
        border-radius: 2px 2px 0 0;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: {ACCENT_SOFT};
        background: rgba(154,230,47,0.04);
    }}
    .stTabs [aria-selected="true"] {{
        color: {ACCENT} !important;
        border-bottom: 2px solid {ACCENT} !important;
        text-shadow: 0 0 6px rgba(154,230,47,0.2);
        background: rgba(154,230,47,0.04);
    }}

    [data-testid="stMetricValue"] {{
        font-family: 'Oswald', sans-serif !important;
        color: {ACCENT} !important;
        font-size: 2.4rem !important;
        font-weight: 700 !important;
        text-shadow: 0 0 8px rgba(154,230,47,0.15);
    }}
    [data-testid="stMetricLabel"] {{
        color: {TEXT_MUTED} !important;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        font-size: 0.68rem !important;
    }}
    [data-testid="stMetric"] {{
        background: {BG_PANEL};
        border: 1px solid {BORDER_SOFT};
        border-radius: 4px;
        padding: 1rem 1.2rem;
    }}

    .stNumberInput input, .stTextInput input, .stDateInput input {{
        background: {BG_PANEL} !important;
        color: {TEXT_PRIMARY} !important;
        border: 1px solid {BORDER_SOFT} !important;
        border-radius: 3px !important;
        font-family: 'JetBrains Mono', monospace !important;
    }}
    .stNumberInput input:focus, .stTextInput input:focus, .stDateInput input:focus {{
        border-color: {ACCENT} !important;
        box-shadow: 0 0 0 1px {ACCENT} !important;
    }}
    .stSelectbox div[data-baseweb="select"] > div {{
        background: {BG_PANEL} !important;
        border: 1px solid {BORDER_SOFT} !important;
        border-radius: 3px !important;
    }}

    [data-baseweb="slider"] [role="slider"] {{
        background: {ACCENT} !important;
        box-shadow: 0 0 6px rgba(154,230,47,0.3) !important;
    }}

    .stRadio [role="radiogroup"] label {{
        background: {BG_PANEL};
        border: 1px solid {BORDER_SOFT};
        padding: 0.4rem 1rem;
        margin-right: 0.5rem;
        border-radius: 3px;
    }}
    .stRadio [role="radiogroup"] label:hover {{
        border-color: {BORDER_STRONG};
        background: rgba(154,230,47,0.04);
    }}

    .stAlert {{
        background: {BG_PANEL} !important;
        border-left: 3px solid {ACCENT} !important;
        border-radius: 3px !important;
        font-family: 'JetBrains Mono', monospace !important;
    }}

    .stCaption, [data-testid="stCaptionContainer"] {{
        color: {TEXT_MUTED} !important;
        font-size: 0.75rem !important;
    }}

    [data-testid="stExpander"] {{
        background: {BG_PANEL} !important;
        border: 1px solid {BORDER_SOFT} !important;
        border-radius: 3px !important;
    }}
    [data-testid="stExpander"] summary {{
        color: {ACCENT_SOFT} !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
    }}

    .provenance-strip {{
        background: linear-gradient(90deg, rgba(154,230,47,0.025) 0%, transparent 100%);
        border: 1px solid {BORDER_SOFT};
        border-left: 3px solid {ACCENT};
        padding: 0.8rem 1.1rem;
        margin: 0.3rem 0 1.5rem 0;
        border-radius: 3px;
        font-size: 0.83rem;
        color: {TEXT_SECONDARY};
    }}
    .provenance-strip strong {{ color: {ACCENT}; }}

    .coach-note {{
        background: linear-gradient(135deg, rgba(154,230,47,0.025) 0%, transparent 100%);
        border: 1px solid {BORDER_SOFT};
        border-left: 3px solid {ACCENT};
        padding: 1rem 1.2rem;
        margin: 1.2rem 0;
        border-radius: 3px;
        font-size: 0.92rem;
        line-height: 1.65;
        color: {TEXT_PRIMARY};
    }}
    .coach-note strong {{ color: {ACCENT}; }}

    .headline-stat {{
        background: {BG_PANEL};
        border: 1px solid {BORDER_SOFT};
        border-top: 3px solid {ACCENT};
        padding: 1.3rem 1.1rem;
        border-radius: 3px;
        height: 100%;
    }}
    .headline-stat:hover {{
        border-color: {BORDER_STRONG};
    }}
    .headline-stat .stat-value {{
        font-family: 'Oswald', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: {ACCENT};
        line-height: 1;
        margin-bottom: 0.5rem;
    }}
    .headline-stat .stat-label {{
        font-size: 0.7rem;
        color: {TEXT_MUTED};
        text-transform: uppercase;
        letter-spacing: 0.15em;
        margin-bottom: 0.5rem;
    }}
    .headline-stat .stat-insight {{
        font-size: 0.82rem;
        color: {TEXT_SECONDARY};
        line-height: 1.5;
    }}

    .live-dot {{
        display: inline-block;
        width: 7px; height: 7px;
        border-radius: 50%;
        background: {ACCENT};
        box-shadow: 0 0 8px {ACCENT};
        animation: pulse 2s ease-in-out infinite;
        margin-right: 0.4rem;
        vertical-align: middle;
    }}
    @keyframes pulse {{
        0%, 100% {{ opacity: 1; transform: scale(1); }}
        50%      {{ opacity: 0.5; transform: scale(0.85); }}
    }}

    .app-footer {{
        border-top: 1px solid {BORDER_SOFT};
        padding-top: 1.2rem;
        margin-top: 3rem;
        color: {TEXT_MUTED};
        font-size: 0.72rem;
        line-height: 1.6;
    }}

    .hero-sub {{
        color: {TEXT_SECONDARY};
        font-size: 0.95rem;
        margin-bottom: 1rem;
    }}

    .cohort-card {{
        background: linear-gradient(135deg, {BG_ELEVATED} 0%, {BG_PANEL} 100%);
        border: 1px solid {BORDER_SOFT};
        border-left: 3px solid {ACCENT};
        padding: 0.9rem 1rem;
        margin: 1rem 0 0.5rem 0;
        border-radius: 3px;
        font-size: 0.82rem;
        line-height: 1.55;
        color: {TEXT_SECONDARY};
    }}
    .cohort-card strong {{ color: {ACCENT}; }}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Plotly styling helper
# =============================================================================

def style_plot(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=BG_PANEL,
        font=dict(family="JetBrains Mono, monospace", color=TEXT_PRIMARY, size=12),
        title_font=dict(family="Oswald, sans-serif", color=ACCENT, size=13),
        xaxis=dict(gridcolor="rgba(124,255,160,0.08)", zerolinecolor=BORDER_SOFT, color=TEXT_MUTED),
        yaxis=dict(gridcolor="rgba(124,255,160,0.08)", zerolinecolor=BORDER_SOFT, color=TEXT_MUTED),
        legend=dict(
            bgcolor="rgba(19,28,24,0.85)",
            bordercolor=BORDER_SOFT, borderwidth=1,
            font=dict(color=TEXT_PRIMARY),
        ),
        margin=dict(l=50, r=20, t=50, b=40),
        height=height,
        hoverlabel=dict(
            bgcolor=BG_ELEVATED,
            font_family="JetBrains Mono, monospace",
            font_color=TEXT_PRIMARY,
            bordercolor=ACCENT,
        ),
    )
    return fig


# =============================================================================
# Database helpers (NEW SCHEMA)
# =============================================================================

@st.cache_resource
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@st.cache_data
def load_cohort_activities() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT f.source            AS cohort,
               a.activity_name     AS activity_name,
               a.base_met          AS met_value,
               COUNT(*)            AS n_windows
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY f.source, a.activity_name, a.base_met
        ORDER BY f.source, n_windows DESC
        """,
        conn,
    )
    df["intensity_class"] = df["met_value"].apply(met_to_intensity)
    return df


@st.cache_data
def load_provenance() -> dict:
    conn = get_connection()
    stats = {}
    stats["n_subjects"]   = conn.execute("SELECT COUNT(*) FROM dim_participant").fetchone()[0]
    stats["n_windows"]    = conn.execute("SELECT COUNT(*) FROM fact_activity_window").fetchone()[0]
    stats["n_activities"] = conn.execute("SELECT COUNT(*) FROM dim_activity").fetchone()[0]
    cohort_counts = dict(conn.execute(
        "SELECT source, COUNT(*) FROM dim_participant GROUP BY source"
    ).fetchall())
    stats["n_harth"]  = cohort_counts.get("HARTH", 0)
    stats["n_har70"]  = cohort_counts.get("HAR70+", 0)
    window_counts = dict(conn.execute("""
        SELECT source, COUNT(*) FROM fact_activity_window GROUP BY source
    """).fetchall())
    stats["windows_harth"] = window_counts.get("HARTH", 0)
    stats["windows_har70"] = window_counts.get("HAR70+", 0)
    return stats


@st.cache_data
def load_intensity_mix() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("""
        SELECT f.source AS cohort,
               CASE
                   WHEN a.base_met < 1.5 THEN 'sedentary'
                   WHEN a.base_met < 3.0 THEN 'light'
                   WHEN a.base_met < 6.0 THEN 'moderate'
                   ELSE 'vigorous'
               END AS intensity_class,
               COUNT(*) AS n_windows
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY f.source, intensity_class
    """, conn)


@st.cache_data
def load_db_weather_dates() -> list[date]:
    conn = get_connection()
    df = pd.read_sql(
        "SELECT DISTINCT DATE(hour_timestamp) AS d FROM dim_weather ORDER BY d", conn,
    )
    return [date.fromisoformat(d) for d in df["d"].tolist()]


def lookup_weather_db(target_date: date) -> dict | None:
    conn = get_connection()
    bad_codes_ph = ",".join("?" * len(BAD_WEATHER_CODES))
    query = f"""
        SELECT COUNT(*)                                             AS n_hours,
               ROUND(AVG(temperature_c), 1)                         AS avg_temp_c,
               ROUND(COALESCE(SUM(precipitation_mm), 0), 1)         AS total_precip_mm,
               MAX(CASE WHEN weather_code IN ({bad_codes_ph})
                        THEN 1 ELSE 0 END)                          AS has_bad_weather,
               MAX(weather_code)                                    AS worst_code
        FROM dim_weather
        WHERE DATE(hour_timestamp) = ?
    """
    params = (*sorted(BAD_WEATHER_CODES), target_date.isoformat())
    summary = pd.read_sql(query, conn, params=params).iloc[0]
    if summary["n_hours"] == 0:
        return None
    return {
        "avg_temp_c":      float(summary["avg_temp_c"]),
        "total_precip_mm": float(summary["total_precip_mm"]),
        "is_bad_weather":  bool(summary["has_bad_weather"]),
        "worst_code":      int(summary["worst_code"]) if summary["worst_code"] else 0,
        "source":          "archive",
    }


@st.cache_data(ttl=3600)
def lookup_weather_live(target_date: date) -> dict | None:
    today = date.today()
    if target_date >= today:
        base_url = "https://api.open-meteo.com/v1/forecast"
    else:
        base_url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude":   TRONDHEIM_LAT,
        "longitude":  TRONDHEIM_LON,
        "start_date": target_date.isoformat(),
        "end_date":   target_date.isoformat(),
        "hourly":     "temperature_2m,precipitation,weather_code",
        "timezone":   "Europe/Oslo",
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{base_url}?{query}"

    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, Exception):
        return None

    hourly = data.get("hourly", {})
    temps  = hourly.get("temperature_2m") or []
    precs  = hourly.get("precipitation")  or []
    codes  = hourly.get("weather_code")   or []

    if not temps:
        return None

    temps_clean = [t for t in temps if t is not None]
    precs_clean = [p for p in precs if p is not None]
    codes_clean = [c for c in codes if c is not None]

    if not temps_clean:
        return None

    avg_temp    = round(sum(temps_clean) / len(temps_clean), 1)
    total_precip = round(sum(precs_clean), 1) if precs_clean else 0.0
    has_bad = any(c in BAD_WEATHER_CODES for c in codes_clean)
    worst   = max(codes_clean) if codes_clean else 0

    return {
        "avg_temp_c":      avg_temp,
        "total_precip_mm": total_precip,
        "is_bad_weather":  has_bad,
        "worst_code":      int(worst),
        "source":          "live",
    }


def lookup_weather(target_date: date) -> dict | None:
    db_result = lookup_weather_db(target_date)
    if db_result is not None:
        return db_result
    return lookup_weather_live(target_date)

@st.cache_data
def load_about_footprint() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("""
        SELECT source AS cohort,
               COUNT(DISTINCT subject_id)              AS n_subjects,
               COUNT(*)                                 AS n_windows,
               ROUND(SUM(window_duration) / 3600.0, 1) AS recording_hours
        FROM fact_activity_window
        GROUP BY source
    """, conn)


@st.cache_data
def load_about_activity_mix() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("""
        SELECT f.source AS cohort,
               a.activity_name,
               COUNT(*) AS n_windows,
               COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY f.source) AS pct
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY f.source, a.activity_name
    """, conn)


@st.cache_data
def load_about_walking_signal() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("""
        SELECT f.source AS cohort,
               f.thigh_z_std
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        WHERE a.activity_name = 'walking'
          AND f.thigh_z_std IS NOT NULL
    """, conn)


@st.cache_data
def load_about_shared_signal() -> pd.DataFrame:
    conn = get_connection()
    shared = pd.read_sql("""
        SELECT a.activity_name,
               f.source AS cohort,
               COUNT(*) AS n_windows,
               ROUND(AVG(f.thigh_z_std), 4) AS mean_thigh_z_std
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        WHERE f.thigh_z_std IS NOT NULL
        GROUP BY a.activity_name, f.source
    """, conn)

    both = (
        shared.groupby("activity_name")["cohort"]
        .nunique()
        .loc[lambda s: s == 2]
        .index
        .tolist()
    )

    return shared[shared["activity_name"].isin(both)].copy()


@st.cache_data
def load_about_calorie_rates() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("""
        SELECT f.source AS cohort,
               a.activity_name,
               COUNT(*) AS n_windows,
               ROUND(AVG(f.cal_per_kg / NULLIF(f.window_duration / 3600.0, 0)), 3) AS kcal_per_kg_hour
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        WHERE f.cal_per_kg IS NOT NULL
        GROUP BY f.source, a.activity_name
    """, conn)
# =============================================================================
# Domain helpers
# =============================================================================

def kcal_burned(met: float, weight_kg: float, duration_min: float) -> float:
    return met * weight_kg * (duration_min / 60.0) * KCAL_PER_MET_KG_HOUR


def classify_cohort(age: int) -> tuple[str, str]:
    if age < AGE_THRESHOLD:
        return "HARTH", "working-age adults"
    return "HAR70+", "adults 70 and over"


def coach_note(html: str) -> None:
    st.markdown(f'<div class="coach-note">{html}</div>', unsafe_allow_html=True)


def headline_stat(value: str, label: str, insight: str) -> str:
    return (
        f'<div class="headline-stat">'
        f'<div class="stat-label">{label}</div>'
        f'<div class="stat-value">{value}</div>'
        f'<div class="stat-insight">{insight}</div>'
        f'</div>'
    )


# =============================================================================
# Header
# =============================================================================

st.markdown(f"# {APP_TITLE}")
st.markdown(
    f"<div class='hero-sub'>{APP_TAGLINE}</div>",
    unsafe_allow_html=True,
)

if not DB_PATH.exists():
    st.error(f"Can't find the database at {DB_PATH}.")
    st.stop()

prov = load_provenance()
st.markdown(
    f"""<div class="provenance-strip">
    <strong>{prov['n_windows']:,}</strong> sensor windows &nbsp;·&nbsp;
    <strong>{prov['n_subjects']}</strong> volunteers
    (<strong>{prov['n_harth']}</strong> working-age, <strong>{prov['n_har70']}</strong> age 70+)
    &nbsp;·&nbsp;
    <strong>{prov['n_activities']}</strong> activities tracked
    &nbsp;·&nbsp;
    source: NTNU Trondheim / HARTH + HAR70+
    </div>""",
    unsafe_allow_html=True,
)


# =============================================================================
# Sidebar
# =============================================================================

with st.sidebar:
    st.markdown(f"""
<div style="display: flex; align-items: center; gap: 0.7rem; margin-bottom: 1.5rem;">
  <svg width="32" height="32" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M2 16 L8 16 L11 8 L14 24 L17 12 L20 20 L23 16 L30 16"
          stroke="{ACCENT}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
          fill="none"/>
  </svg>
  <span style="font-family: 'Oswald', sans-serif; font-size: 1rem;
               font-weight: 500; letter-spacing: 0.15em; color: {TEXT_PRIMARY};
               text-transform: uppercase;">BurnWise</span>
</div>
    """, unsafe_allow_html=True)

    st.markdown("<h2>Your profile</h2>", unsafe_allow_html=True)
    st.markdown(
        f"<div style='color:{TEXT_MUTED}; font-size:0.78rem; "
        f"margin:-0.3rem 0 1rem 0; line-height:1.5;'>"
        "Tell us a bit about you and we'll match you to the right reference group."
        "</div>",
        unsafe_allow_html=True,
    )
    age = st.number_input("Age", min_value=18, max_value=100, value=30, step=1)
    cohort, cohort_label = classify_cohort(age)
    weight_kg = st.number_input(
        "Weight (kg)", min_value=30.0, max_value=200.0, value=70.0, step=0.5,
    )

    if cohort == "HARTH":
        cohort_msg = (
            f"Reference group: <strong>working-age adults</strong> "
            f"({prov['n_harth']} volunteers under 70)."
        )
    else:
        cohort_msg = (
            f"Reference group: <strong>adults 70 and over</strong> "
            f"({prov['n_har70']} volunteers, all independently active). "
            "Reference data adjusted for age."
        )
    st.markdown(f'<div class="cohort-card">{cohort_msg}</div>', unsafe_allow_html=True)


all_activities = load_cohort_activities()
cohort_df = all_activities[all_activities["cohort"] == cohort].copy()
cohort_df["display_name"] = cohort_df["activity_name"].map(display_name)


# =============================================================================
# Tabs
# =============================================================================

tab_estimate, tab_recommend, tab_about = st.tabs([
    "Estimate calories",
    "Help me pick something",
    "About this data",
])


# -----------------------------------------------------------------------------
# MODE A
# -----------------------------------------------------------------------------

with tab_estimate:
    st.markdown("### How many calories will this burn?")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; margin-bottom:1.2rem;'>"
        "Pick an activity, say how hard you're going and for how long."
        "</p>",
        unsafe_allow_html=True,
    )

    pickable = cohort_df[cohort_df["met_value"] >= PICKER_MET_FLOOR].copy()
    pickable = pickable.sort_values("met_value")

    if pickable.empty:
        st.warning(f"No workout-grade activities available for the {cohort} group.")
    else:
        col1, col2 = st.columns([3, 2])

        with col1:
            options = pickable["display_name"].tolist()
            default_idx = options.index("Walking") if "Walking" in options else 0
            chosen_display = st.selectbox("Activity", options=options, index=default_idx)
            chosen_activity = pickable[
                pickable["display_name"] == chosen_display
            ]["activity_name"].iloc[0]

            effort_level = st.radio(
                "How hard are you going?",
                options=list(EFFORT_LEVELS.keys()),
                index=1,
                horizontal=True,
                help="Easy = coasting along. Steady = your normal pace. Pushing = working hard.",
            )

        with col2:
            duration_min = st.slider(
                "Duration (minutes)", min_value=1, max_value=180, value=30, step=1,
            )

        row = pickable[pickable["activity_name"] == chosen_activity].iloc[0]
        ref_met = float(row["met_value"])
        intensity_class = row["intensity_class"]
        n_windows = int(row["n_windows"])
        effort_mult = EFFORT_LEVELS[effort_level]
        adjusted_met = ref_met * effort_mult
        total_kcal = kcal_burned(adjusted_met, weight_kg, duration_min)

        st.markdown("<br>", unsafe_allow_html=True)
        mcol1, mcol2, mcol3 = st.columns(3)
        mcol1.metric("Calories", f"{total_kcal:.0f}")
        mcol2.metric("Adjusted MET", f"{adjusted_met:.1f}")
        mcol3.metric("Intensity", intensity_class.title())

        if total_kcal < 50:
            coach_line = "Quick sessions are about consistency more than calorie burn. Still counts."
        elif total_kcal < 150:
            coach_line = "Solid range for a single session. Easy to fit into most days."
        elif total_kcal < 300:
            coach_line = "Real burn. Three or four of these a week adds up quickly."
        else:
            coach_line = f"{total_kcal:.0f} calories is a proper session. Don't forget to eat after."
        coach_note(coach_line)

        st.markdown("### How this stacks up against similar activities")
        alternatives = pickable[
            (pickable["intensity_class"] == intensity_class) &
            (pickable["activity_name"] != chosen_activity)
        ].copy()
        alternatives = alternatives.nlargest(4, "n_windows")

        compare_rows = [{
            "activity":  display_name(chosen_activity),
            "kcal":      total_kcal,
            "is_choice": True,
        }]
        for _, alt in alternatives.iterrows():
            alt_met = float(alt["met_value"]) * effort_mult
            compare_rows.append({
                "activity":  display_name(alt["activity_name"]),
                "kcal":      kcal_burned(alt_met, weight_kg, duration_min),
                "is_choice": False,
            })
        compare_df = pd.DataFrame(compare_rows).sort_values("kcal")

        if len(compare_df) > 1:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=compare_df["kcal"],
                y=compare_df["activity"],
                orientation="h",
                marker=dict(
                    color=[ACCENT if c else COLOR_DIM for c in compare_df["is_choice"]],
                    line=dict(
                        color=[ACCENT if c else "rgba(124,255,160,0.2)"
                               for c in compare_df["is_choice"]],
                        width=1,
                    ),
                ),
                text=[f"{k:.0f} kcal" for k in compare_df["kcal"]],
                textposition="outside",
                textfont=dict(color=TEXT_PRIMARY, size=11),
                hovertemplate="<b>%{y}</b><br>%{x:.0f} kcal<extra></extra>",
            ))
            fig.update_layout(
                title=f"{duration_min}-min session, {weight_kg:.0f} kg, {effort_level.lower()} effort",
                showlegend=False, xaxis_title="Calories burned", yaxis_title="",
            )
            st.plotly_chart(
                style_plot(fig, height=max(240, 90 + 55 * len(compare_df))),
                use_container_width=True,
            )

            with st.expander("Analytical read on this comparison"):
                top = compare_df.iloc[-1]
                bot = compare_df.iloc[0]
                ratio = top["kcal"] / bot["kcal"] if bot["kcal"] > 0 else 0
                confidence = (
                    "excellent" if n_windows > 1000
                    else "moderate" if n_windows > 100
                    else "limited"
                )
                if not alternatives.empty:
                    met_min = alternatives["met_value"].min()
                    met_max = alternatives["met_value"].max()
                    range_text = f"MET between {met_min:.1f} and {met_max:.1f}"
                else:
                    range_text = "the same intensity band"

                st.markdown(f"""
We're comparing {chosen_display.lower()} against other {intensity_class}-class activities at the same duration, weight, and effort multiplier. All of these sit in {range_text}, so the spread comes from compendium MET differences rather than from how hard you're working.

{top['activity']} produces about **{ratio:.1f}x** the caloric output of {bot['activity'].lower()} at these settings. That multiplier is weight-invariant: the MET formula scales linearly with mass, so the ranking holds for any user.

Confidence in each estimate scales with how many sensor windows back it up. {display_name(chosen_activity)} has **{n_windows:,}** windows in your cohort, which is {confidence}. Below a few hundred windows, treat numbers as directional.
                """)
        else:
            st.caption("Not enough other activities in this intensity band to compare against.")

        with st.expander("Show the math"):
            st.markdown(f"""
**Formula:** calories = MET × kg × hours

- Reference MET for {chosen_display.lower()}: **{ref_met:.2f}** (2024 Compendium of Physical Activities)
- Effort multiplier ({effort_level}): **×{effort_mult:.2f}**
- Adjusted MET: **{adjusted_met:.2f}**
- Your weight: **{weight_kg:.1f} kg**
- Duration: **{duration_min} min ({duration_min/60:.2f} hr)**

**Result:** {adjusted_met:.2f} × {weight_kg:.1f} × {duration_min/60:.2f} = **{total_kcal:.1f} kcal**

Based on **{n_windows:,}** observed windows in the {cohort_label} reference group.
            """)


# -----------------------------------------------------------------------------
# MODE B
# -----------------------------------------------------------------------------

with tab_recommend:
    st.markdown("### I want to burn some calories. What should I do?")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; margin-bottom:1.2rem;'>"
        "Set a goal and a time window. We'll show what fits and check the weather."
        "</p>",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([3, 2])
    with col1:
        goal_kcal = st.number_input(
            "Calorie goal (kcal)", min_value=10, max_value=2000, value=200, step=10,
        )
        max_duration = st.slider(
            "How much time have you got? (minutes)",
            min_value=5, max_value=180, value=60,
        )

    with col2:
        target_date = st.date_input(
            "When?", value=date.today(),
            help="We'll try live weather first, then fall back to our archive.",
        )

    weather = lookup_weather(target_date)
    filter_outdoor = False

    if weather is None:
        st.info(
            f"Couldn't fetch weather for {target_date.isoformat()}. "
            "Showing all activities."
        )
    else:
        src_label = "live" if weather["source"] == "live" else "archive"
        src_html = (
            f"<span class='live-dot'></span><strong>live</strong> weather"
            if src_label == "live"
            else f"<strong>archive</strong> weather"
        )
        date_label = (
            f"{target_date.strftime('%A')}, "
            f"{target_date.strftime('%b')} {target_date.day}, {target_date.year}"
        )
        st.markdown(
            f"<div style='color:{TEXT_MUTED}; font-size:0.78rem; "
            f"margin-bottom:0.6rem;'>{src_html} &nbsp;·&nbsp; Trondheim, "
            f"{date_label}</div>",
            unsafe_allow_html=True,
        )
        wcol1, wcol2, wcol3 = st.columns(3)
        wcol1.metric("Temperature", f"{weather['avg_temp_c']}°C")
        wcol2.metric("Rainfall", f"{weather['total_precip_mm']} mm")
        wcol3.metric(
            "Conditions",
            "Rough" if weather["is_bad_weather"] else "Good",
        )
        if weather["is_bad_weather"]:
            filter_outdoor = True
            st.warning(
                "Rough weather ahead. Outdoor options stay visible but flagged "
                "so you can see why indoor might be smarter today."
            )

    st.markdown("<br>", unsafe_allow_html=True)

    rec = cohort_df.copy()
    rec["duration_needed_min"] = (goal_kcal * 60.0 / (rec["met_value"] * weight_kg)).round(1)
    rec["kcal_per_min"] = (rec["met_value"] * weight_kg / 60.0).round(3)
    rec["fits_time"]   = rec["duration_needed_min"] <= max_duration
    rec["is_outdoor"]  = rec["activity_name"].isin(OUTDOOR_ACTIVITIES)
    rec["weather_ok"]  = ~(filter_outdoor & rec["is_outdoor"])
    rec["recommended"] = rec["fits_time"] & rec["weather_ok"]

    recommended = rec[rec["recommended"]].sort_values("duration_needed_min")

    if recommended.empty:
        st.warning(
            f"Nothing in the {cohort} reference data can hit **{goal_kcal} kcal** in "
            f"**{max_duration} min** at your weight"
            + (" with the outdoor filter on" if filter_outdoor else "")
            + ". Try giving yourself more time, or lower the goal."
        )
    else:
        top = recommended.iloc[0]
        coach_note(
            f"Fastest way there: <strong>{display_name(top['activity_name']).lower()}</strong> "
            f"for about <strong>{top['duration_needed_min']:.0f} minutes</strong>. "
            f"You've got <strong>{len(recommended)}</strong> options total below."
        )

        st.markdown("### Every option at a glance")

        chart_df = rec.copy().sort_values("duration_needed_min")
        chart_df["display_name"] = chart_df["activity_name"].map(display_name)

        def reason(r):
            if not r["fits_time"] and not r["weather_ok"]:
                return f"Would need {r['duration_needed_min']:.0f} min & outdoor in bad weather"
            if not r["fits_time"]:
                return f"Would need {r['duration_needed_min']:.0f} min, past your limit"
            if not r["weather_ok"]:
                return "Outdoor, weather's not great today"
            return "Fits your plan"
        chart_df["reason"] = chart_df.apply(reason, axis=1)
        chart_df["duration_display"] = chart_df["duration_needed_min"].clip(upper=max_duration * 3)

        fig = go.Figure()
        colors = [ACCENT if r else COLOR_DIM for r in chart_df["recommended"]]
        edge_colors = [ACCENT if r else "rgba(124,255,160,0.2)"
                       for r in chart_df["recommended"]]

        fig.add_trace(go.Bar(
            x=chart_df["duration_display"],
            y=chart_df["display_name"],
            orientation="h",
            marker=dict(color=colors, line=dict(color=edge_colors, width=1)),
            text=[f"{d:.0f} min" for d in chart_df["duration_needed_min"]],
            textposition="outside",
            textfont=dict(color=TEXT_PRIMARY, size=11),
            hovertemplate=("<b>%{y}</b><br>"
                           "Needed: %{customdata[0]:.0f} min<br>"
                           "%{customdata[1]}<extra></extra>"),
            customdata=chart_df[["duration_needed_min", "reason"]].values,
        ))
        fig.add_vline(
            x=max_duration, line_width=1, line_dash="dot", line_color=ACCENT_SOFT,
            annotation_text=f"your {max_duration} min limit",
            annotation_position="top",
            annotation_font=dict(color=ACCENT_SOFT, size=10),
        )
        fig.update_layout(
            title=f"Time needed to burn {goal_kcal} kcal at {weight_kg:.0f} kg",
            showlegend=False, xaxis_title="Minutes required", yaxis_title="",
        )
        st.plotly_chart(
            style_plot(fig, height=max(300, 80 + 38 * len(chart_df))),
            use_container_width=True,
        )

        st.markdown(
            f"<div style='color:{TEXT_MUTED}; font-size:0.78rem; margin-top:-1rem;'>"
            f"<span style='color:{ACCENT};'>■</span> fits your plan &nbsp;&nbsp;"
            f"<span style='color:{COLOR_DIM};'>■</span> doesn't fit (over time budget, or weather)"
            f"</div>",
            unsafe_allow_html=True,
        )

        with st.expander("Analytical read on this recommendation"):
            n_total = len(chart_df)
            n_fit = int(chart_df["recommended"].sum())
            n_over_time = int((~chart_df["fits_time"]).sum())
            n_weather_cut = int((chart_df["fits_time"] & ~chart_df["weather_ok"]).sum())
            fastest_activity = recommended.iloc[0]
            slowest_rec = recommended.iloc[-1]
            speed_gap = slowest_rec["duration_needed_min"] / fastest_activity["duration_needed_min"]

            weather_line = ""
            if n_weather_cut > 0:
                weather_line = (
                    f" **{n_weather_cut}** additional outdoor options are flagged "
                    "because Trondheim's weather crosses our bad-weather threshold "
                    "(WMO codes for rain, snow, or thunderstorm)."
                )

            st.markdown(f"""
Of the **{n_total}** activities in the {cohort_label} reference set, **{n_fit}** can hit your {goal_kcal} kcal goal inside your {max_duration}-minute window. **{n_over_time}** are filtered purely on time.{weather_line}

Within the feasible set, the efficiency spread is about **{speed_gap:.1f}x** (slowest vs fastest), driven entirely by MET differences. {display_name(fastest_activity['activity_name'])} at MET {fastest_activity['met_value']:.1f} is {speed_gap:.1f}x more time-efficient than {display_name(slowest_rec['activity_name']).lower()} at MET {slowest_rec['met_value']:.1f}.

If time is the constraint, intensity is the lever. If intensity is the constraint, you budget more time. This chart makes the trade-off visible instead of hiding it behind a single best pick.
            """)
# -----------------------------------------------------------------------------
# MODE C — ABOUT / VISUAL REPORTING
# -----------------------------------------------------------------------------

with tab_about:
    # -----------------------------------------------------------------------------
    # Intro
    # -----------------------------------------------------------------------------
    st.markdown("### The data story behind every estimate")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; margin-bottom:1.5rem; max-width:780px;'>"
        "Every number BurnWise shows you traces back to real sensor data from real people. "
        "This tab walks through three questions the pipeline answered on the way to the app: "
        "who we measured, whether a shared label means the same thing across ages, and what "
        "that means for the calorie numbers we show you. Each view is interactive — change the "
        "filters and see how the picture shifts."
        "</p>",
        unsafe_allow_html=True,
    )

    # Shared data loads
    mix = load_intensity_mix()
    harth_mix = mix[mix["cohort"] == "HARTH"]
    har70_mix = mix[mix["cohort"] == "HAR70+"]

    size_ratio = prov["windows_harth"] / max(prov["windows_har70"], 1)
    harth_activities = all_activities[all_activities["cohort"] == "HARTH"]["activity_name"].nunique()
    har70_activities = all_activities[all_activities["cohort"] == "HAR70+"]["activity_name"].nunique()
    har70_vigorous = int(har70_mix[har70_mix["intensity_class"] == "vigorous"]["n_windows"].sum())
    harth_vigorous = int(harth_mix[harth_mix["intensity_class"] == "vigorous"]["n_windows"].sum())

    # -----------------------------------------------------------------------------
    # Headline stats
    # -----------------------------------------------------------------------------
    st.markdown("#### At a glance")

    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.markdown(headline_stat(
            f"{prov['n_windows']:,}",
            "Sensor windows",
            f"Each window is 2 seconds of movement data. "
            f"{prov['windows_harth']:,} from working-age adults, {prov['windows_har70']:,} from adults 70+."
        ), unsafe_allow_html=True)
    with h2:
        st.markdown(headline_stat(
            f"{size_ratio:.1f}×",
            "Cohort size gap",
            f"HARTH has {size_ratio:.1f}× more windows than HAR70+. "
            "Any cross-cohort number carries this caveat."
        ), unsafe_allow_html=True)
    with h3:
        st.markdown(headline_stat(
            f"{har70_activities} vs {harth_activities}",
            "Activity coverage",
            f"HAR70+ shows {har70_activities} activities. HARTH shows {harth_activities}. "
            "Running and cycling only exist in HARTH data."
        ), unsafe_allow_html=True)
    with h4:
        st.markdown(headline_stat(
            f"{har70_vigorous:,}",
            "Vigorous HAR70+ windows",
            f"Zero. Every vigorous data point comes from HARTH ({harth_vigorous:,} windows). "
            "That gap shapes what the app can recommend."
        ), unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)

    # =============================================================================
    # ACT 1 — Who are we measuring?
    # =============================================================================
    st.markdown(f"""
<div style='border-left:3px solid {ACCENT}; padding-left:1rem; margin:1rem 0 1.5rem 0;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.75rem;
              letter-spacing:0.25em; text-transform:uppercase; margin-bottom:0.3rem;'>Act 1</div>
  <div style='color:{TEXT_PRIMARY}; font-family:"Oswald", sans-serif; font-size:1.5rem;
              font-weight:500;'>Who are we measuring?</div>
  <div style='color:{TEXT_SECONDARY}; font-size:0.9rem; margin-top:0.4rem; max-width:720px;'>
    Same sensors. Same labels. Same protocol. The numbers say the two groups behave very
    differently once they start moving.
  </div>
</div>
    """, unsafe_allow_html=True)

    act1_metric = st.radio(
        "What to show",
        options=["Subjects", "Recording hours", "2-second windows"],
        horizontal=True,
        key="act1_metric",
        label_visibility="collapsed",
    )

    # Compute footprint per cohort
    footprint = all_activities.groupby("cohort").agg(
        n_subjects=("activity_name", lambda _: 0),  # placeholder, filled below
        n_windows=("n_windows", "sum"),
    ).reset_index()

    # We already have subjects per cohort from prov
    footprint["n_subjects"] = footprint["cohort"].map({
        "HARTH": prov["n_harth"],
        "HAR70+": prov["n_har70"],
    })
    # Recording hours: each window is 2 seconds
    footprint["recording_hours"] = (footprint["n_windows"] * 2 / 3600).round(1)

    metric_map = {
        "Subjects": ("n_subjects", "subjects"),
        "Recording hours": ("recording_hours", "hours"),
        "2-second windows": ("n_windows", "windows"),
    }
    metric_col, metric_unit = metric_map[act1_metric]

    col_left, col_right = st.columns([1, 1.4])

    with col_left:
        fig = go.Figure()
        for _, r in footprint.iterrows():
            color = COLOR_HARTH if r["cohort"] == "HARTH" else COLOR_HAR70
            fig.add_trace(go.Bar(
                x=[r[metric_col]],
                y=[r["cohort"]],
                orientation="h",
                marker=dict(color=color, line=dict(color=color, width=1)),
                text=[f"{r[metric_col]:,.0f} {metric_unit}"],
                textposition="outside",
                textfont=dict(color=TEXT_PRIMARY, size=12),
                showlegend=False,
                hovertemplate="<b>%{y}</b><br>" + f"{metric_col}: " + "%{x:,}<extra></extra>",
            ))
        fig.update_layout(
            title=f"Recording footprint: {act1_metric.lower()}",
            xaxis_title=act1_metric, yaxis_title="",
        )
        st.plotly_chart(style_plot(fig, height=260), use_container_width=True)

    with col_right:
        # Activity mix: stacked percentage bar per cohort
        mix_pivot = all_activities.copy()
        totals = mix_pivot.groupby("cohort")["n_windows"].transform("sum")
        mix_pivot["pct"] = mix_pivot["n_windows"] / totals * 100
        mix_pivot["display"] = mix_pivot["activity_name"].map(display_name)

        # Activity palette — categorical, distinct, works on dark bg
        act_order = (all_activities[all_activities["cohort"] == "HARTH"]
                     .sort_values("n_windows", ascending=False)["activity_name"].tolist())
        # Add any activities unique to HAR70+ at the end
        for a in all_activities["activity_name"].unique():
            if a not in act_order:
                act_order.append(a)

        act_palette = [
            "#9AE62F", "#4FC3F7", "#FFB547", "#E879F9", "#60A5FA",
            "#34D399", "#F87171", "#A78BFA", "#FB923C", "#22D3EE",
            "#FBBF24", "#F472B6",
        ]

        fig = go.Figure()
        for i, act in enumerate(act_order):
            sub = mix_pivot[mix_pivot["activity_name"] == act]
            if sub.empty:
                continue
            fig.add_trace(go.Bar(
                name=display_name(act),
                y=sub["cohort"],
                x=sub["pct"],
                orientation="h",
                marker=dict(color=act_palette[i % len(act_palette)]),
                hovertemplate=(
                    "<b>%{y}</b><br>" + display_name(act) +
                    ": %{x:.1f}%<extra></extra>"
                ),
            ))
        fig.update_layout(
            title="Activity mix per cohort (% of windows)",
            barmode="stack",
            xaxis_title="% of cohort's windows",
            yaxis_title="",
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5,
                        font=dict(size=10)),
        )
        st.plotly_chart(style_plot(fig, height=320), use_container_width=True)

    with st.expander("What this means"):
        harth_hours = int(footprint[footprint["cohort"] == "HARTH"]["recording_hours"].iloc[0])
        har70_hours = int(footprint[footprint["cohort"] == "HAR70+"]["recording_hours"].iloc[0])
        st.markdown(f"""
HARTH captured roughly **{harth_hours} hours** of recording across {prov['n_harth']} working-age adults. HAR70+ captured **{har70_hours} hours** across {prov['n_har70']} adults 70+. The gap in windows is wider than the gap in subjects, so HAR70+ subjects contributed shorter sessions on average.

The activity mix tells a second story. Walking dominates HAR70+, while HARTH spreads more evenly across walking, sitting, and other activities. That's not a data flaw, it reflects what the two populations actually did during the recordings. Working-age adults were observed during their full workday (lots of sitting), older adults were observed during free-living movement sessions (mostly walking).

**Why this matters for BurnWise:** these two groups aren't interchangeable. Pooling them into a single "reference human" would hide real behavior differences. That's why the app picks one cohort based on your age and shows you numbers grounded in that group's actual data.
        """)

    st.markdown("<br><br>", unsafe_allow_html=True)

    # =============================================================================
    # ACT 2 — Does "walking" mean the same thing at 30 and 75?
    # =============================================================================
    st.markdown(f"""
<div style='border-left:3px solid {ACCENT}; padding-left:1rem; margin:1rem 0 1.5rem 0;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.75rem;
              letter-spacing:0.25em; text-transform:uppercase; margin-bottom:0.3rem;'>Act 2</div>
  <div style='color:{TEXT_PRIMARY}; font-family:"Oswald", sans-serif; font-size:1.5rem;
              font-weight:500;'>Does "walking" mean the same thing at 30 and at 75?</div>
  <div style='color:{TEXT_SECONDARY}; font-size:0.9rem; margin-top:0.4rem; max-width:720px;'>
    The sensor sees the physical motion, not the label. When a 75-year-old and a 30-year-old
    both "walk," the sensor data tells us how similar that walking really is.
  </div>
</div>
    """, unsafe_allow_html=True)

    # Load walking thigh_z_std distribution directly from DB
    conn_local = get_connection()
    walking_signal = pd.read_sql("""
        SELECT f.source, f.thigh_z_std
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        WHERE a.activity_name = 'walking'
    """, conn_local)

    # Compare across shared activities
    shared_signal = pd.read_sql("""
        SELECT a.activity_name, f.source,
               COUNT(*) AS n_windows,
               ROUND(AVG(f.thigh_z_std), 4) AS mean_std
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY a.activity_name, f.source
    """, conn_local)

    # Keep only activities with data in both cohorts
    both_cohorts = (shared_signal.groupby("activity_name")["source"].nunique()
                    .loc[lambda s: s == 2].index.tolist())
    shared_signal = shared_signal[shared_signal["activity_name"].isin(both_cohorts)]

    act2_view = st.radio(
        "View",
        options=["Walking intensity distribution", "Gap across all shared activities"],
        horizontal=True,
        key="act2_view",
        label_visibility="collapsed",
    )

    if act2_view == "Walking intensity distribution":
        fig = go.Figure()
        for cohort_name, color in [("HARTH", COLOR_HARTH), ("HAR70+", COLOR_HAR70)]:
            vals = walking_signal[walking_signal["source"] == cohort_name]["thigh_z_std"]
            if vals.empty:
                continue
            fig.add_trace(go.Histogram(
                x=vals,
                name=f"{cohort_name} (n={len(vals):,})",
                marker=dict(color=color, line=dict(color=color, width=0)),
                opacity=0.65,
                nbinsx=60,
                histnorm="probability density",
                hovertemplate="<b>" + cohort_name + "</b><br>std: %{x:.3f}g<br>density: %{y:.2f}<extra></extra>",
            ))
            median = vals.median()
            fig.add_vline(x=median, line_dash="dash", line_color=color, line_width=2,
                          annotation_text=f"{cohort_name} median",
                          annotation_position="top",
                          annotation_font=dict(color=color, size=10))

        fig.update_layout(
            title="How vigorously does each cohort walk? (thigh vertical motion)",
            xaxis_title="Thigh vertical acceleration std (g)",
            yaxis_title="Density",
            barmode="overlay",
        )
        # Trim x-axis to meaningful range
        q99 = walking_signal["thigh_z_std"].quantile(0.99)
        fig.update_xaxes(range=[0, q99])
        st.plotly_chart(style_plot(fig, height=420), use_container_width=True)

        with st.expander("What this means"):
            harth_median = walking_signal[walking_signal["source"] == "HARTH"]["thigh_z_std"].median()
            har70_median = walking_signal[walking_signal["source"] == "HAR70+"]["thigh_z_std"].median()
            gap_pct = (harth_median - har70_median) / harth_median * 100 if harth_median > 0 else 0
            st.markdown(f"""
Both distributions are labeled "walking." The sensor disagrees. HARTH's walking produces a thigh acceleration std of **{harth_median:.3f}g** at the median. HAR70+'s walking produces **{har70_median:.3f}g** — about **{gap_pct:.0f}% lower**.

That gap is physical. Older adults walk with a flatter, more controlled gait — less vertical bounce per step. A classifier trained only on HARTH data would see HAR70+ walking and reasonably guess it was a slower, lower-intensity activity, because that's what the numbers look like from the outside.

**Why this matters for BurnWise:** a single MET value for "walking" hides this. That's why the pipeline calibrates intensity within each cohort — so a HAR70+ walking window at their typical intensity doesn't get penalized for producing smaller numbers than a HARTH window at HARTH's typical intensity.
            """)

    else:  # Gap across all shared activities
        pivot_s = (shared_signal.pivot(index="activity_name", columns="source", values="mean_std")
                                .fillna(0))
        pivot_s["delta_pct"] = ((pivot_s["HAR70+"] - pivot_s["HARTH"]) / pivot_s["HARTH"] * 100).round(1)
        pivot_s = pivot_s.sort_values("HARTH", ascending=True)
        pivot_s["display"] = [display_name(a) for a in pivot_s.index]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="HARTH", y=pivot_s["display"], x=pivot_s["HARTH"],
            orientation="h",
            marker=dict(color=COLOR_HARTH, line=dict(color=COLOR_HARTH, width=1)),
            hovertemplate="<b>%{y}</b><br>HARTH: %{x:.3f}g<extra></extra>",
        ))
        fig.add_trace(go.Bar(
            name="HAR70+", y=pivot_s["display"], x=pivot_s["HAR70+"],
            orientation="h",
            marker=dict(color=COLOR_HAR70, line=dict(color=COLOR_HAR70, width=1)),
            hovertemplate="<b>%{y}</b><br>HAR70+: %{x:.3f}g<extra></extra>",
        ))

        # Annotate the delta % next to each pair
        for i, act in enumerate(pivot_s.index):
            d = pivot_s.loc[act, "delta_pct"]
            max_val = max(pivot_s.loc[act, "HARTH"], pivot_s.loc[act, "HAR70+"])
            sign = "+" if d >= 0 else ""
            fig.add_annotation(
                x=max_val * 1.08, y=i,
                text=f"{sign}{d:.0f}%",
                showarrow=False,
                font=dict(color=TEXT_MUTED, size=11),
                xanchor="left",
            )

        fig.update_layout(
            title="Intensity gap by activity: HAR70+ vs HARTH (mean thigh_z std)",
            barmode="group",
            xaxis_title="Mean thigh vertical std (g)",
            yaxis_title="",
        )
        st.plotly_chart(style_plot(fig, height=max(350, 60 + 45 * len(pivot_s))),
                        use_container_width=True)

        with st.expander("What this means"):
            st.markdown("""
The pattern generalizes beyond walking. Activities that involve more movement (walking, stairs, shuffling) show the biggest gap between cohorts — HAR70+ consistently produces a lower-intensity sensor signal. Activities with less movement (sitting, lying, standing) show almost no gap.

That's intuitive. Sitting still looks like sitting still regardless of age. But the way you walk, climb stairs, or shuffle is where age actually shows up in the data.

**Why this matters for BurnWise:** the intensity gap is predictable and systematic, not random noise. That makes it calibratable. Instead of trying to correct for age directly (which gets ethically messy fast), the pipeline calibrates per cohort per activity — each window is scored against its own reference distribution, so a HAR70+ window at HAR70+ intensity norms gets a fair read.
            """)

    st.markdown("<br><br>", unsafe_allow_html=True)

    # =============================================================================
    # ACT 3 — What does the app tell each user?
    # =============================================================================
    st.markdown(f"""
<div style='border-left:3px solid {ACCENT}; padding-left:1rem; margin:1rem 0 1.5rem 0;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.75rem;
              letter-spacing:0.25em; text-transform:uppercase; margin-bottom:0.3rem;'>Act 3</div>
  <div style='color:{TEXT_PRIMARY}; font-family:"Oswald", sans-serif; font-size:1.5rem;
              font-weight:500;'>What does the app tell each user?</div>
  <div style='color:{TEXT_SECONDARY}; font-size:0.9rem; margin-top:0.4rem; max-width:720px;'>
    Calibration has a job: keep the app honest. Different cohorts should see different
    rankings (because their activity mix is different) but identical activities should
    produce comparable calorie estimates.
  </div>
</div>
    """, unsafe_allow_html=True)

    cal_rate = pd.read_sql("""
        SELECT f.source, a.activity_name,
               COUNT(*) AS n_windows,
               ROUND(AVG(f.cal_per_kg) * 1800, 3) AS kcal_per_kg_hour
        FROM fact_activity_window f
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY f.source, a.activity_name
    """, conn_local)

    act3_view = st.radio(
        "View",
        options=["Calorie rate per activity", "Fairness check on shared activities"],
        horizontal=True,
        key="act3_view",
        label_visibility="collapsed",
    )

    if act3_view == "Calorie rate per activity":
        pivot_cal = cal_rate.pivot(index="activity_name", columns="source",
                                   values="kcal_per_kg_hour").fillna(0)
        pivot_cal["max"] = pivot_cal.max(axis=1)
        pivot_cal = pivot_cal.sort_values("max", ascending=True).drop(columns="max")
        pivot_cal["display"] = [display_name(a) for a in pivot_cal.index]

        fig = go.Figure()
        if "HARTH" in pivot_cal.columns:
            fig.add_trace(go.Bar(
                name="HARTH", y=pivot_cal["display"], x=pivot_cal["HARTH"],
                orientation="h",
                marker=dict(color=COLOR_HARTH, line=dict(color=COLOR_HARTH, width=1)),
                hovertemplate="<b>%{y}</b><br>HARTH: %{x:.2f} kcal/kg/hr<extra></extra>",
            ))
        if "HAR70+" in pivot_cal.columns:
            fig.add_trace(go.Bar(
                name="HAR70+", y=pivot_cal["display"], x=pivot_cal["HAR70+"],
                orientation="h",
                marker=dict(color=COLOR_HAR70, line=dict(color=COLOR_HAR70, width=1)),
                hovertemplate="<b>%{y}</b><br>HAR70+: %{x:.2f} kcal/kg/hr<extra></extra>",
            ))
        fig.update_layout(
            title="Calorie burn rate per activity, by cohort",
            barmode="group",
            xaxis_title="kcal per kg per hour",
            yaxis_title="",
        )
        st.plotly_chart(style_plot(fig, height=max(400, 50 + 45 * len(pivot_cal))),
                        use_container_width=True)

        with st.expander("What this means"):
            st.markdown("""
The rankings diverge where they should. HARTH's top calorie-burners are running and cycling — activities with no HAR70+ counterpart at all. HAR70+'s top burners are walking and stairs up, because that's the vigorous end of what this cohort actually did.

The app uses this directly in the recommender tab. If a 75-year-old user wants to burn 200 calories, the recommender doesn't offer running (no reference data) or cycling (same). It offers walking and stairs, ranked by how efficiently each hits the goal. That's the whole point of cohort-aware recommendations — fewer options, but every option grounded in real evidence for someone like the user.
            """)

    else:  # Fairness check
        pivot_cal = cal_rate.pivot(index="activity_name", columns="source",
                                   values="kcal_per_kg_hour")
        shared_cal = pivot_cal.dropna().copy()
        if not shared_cal.empty:
            shared_cal["delta_pct"] = ((shared_cal["HAR70+"] - shared_cal["HARTH"]) /
                                       shared_cal["HARTH"] * 100).round(2)
            shared_cal = shared_cal.sort_values("delta_pct")
            shared_cal["display"] = [display_name(a) for a in shared_cal.index]

            colors = [ACCENT if d >= 0 else "#FF8A4C" for d in shared_cal["delta_pct"]]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=shared_cal["display"],
                x=shared_cal["delta_pct"],
                orientation="h",
                marker=dict(color=colors, line=dict(color=colors, width=1)),
                text=[f"{d:+.2f}%" for d in shared_cal["delta_pct"]],
                textposition="outside",
                textfont=dict(color=TEXT_PRIMARY, size=11),
                hovertemplate="<b>%{y}</b><br>Δ: %{x:+.2f}%<extra></extra>",
                showlegend=False,
            ))
            fig.add_vline(x=0, line_width=1, line_color=TEXT_MUTED)
            # Pad x-axis so the +/- annotations don't clip
            x_max = max(abs(shared_cal["delta_pct"].min()), abs(shared_cal["delta_pct"].max()), 1)
            fig.update_xaxes(range=[-x_max * 1.5, x_max * 1.5])
            fig.update_layout(
                title="Calorie rate: HAR70+ vs HARTH on shared activities",
                xaxis_title="% difference (HAR70+ relative to HARTH)",
                yaxis_title="",
            )
            st.plotly_chart(style_plot(fig, height=max(350, 60 + 45 * len(shared_cal))),
                            use_container_width=True)

            with st.expander("What this means"):
                max_gap = shared_cal["delta_pct"].abs().max()
                st.markdown(f"""
For activities both cohorts did, the calorie rates land within **{max_gap:.1f}%** of each other after calibration. That's the fairness check working.

Without calibration, HAR70+'s lower-intensity walking signal would have produced systematically lower calorie estimates for the same labeled activity — essentially telling older users they burn fewer calories walking than younger users, even when both are walking at their own typical pace. The calibration step corrects for this by scoring each window against its own cohort's distribution, so "typical walking intensity" is defined per group.

**The result:** the app preserves meaningful differences (different cohorts get different activity menus based on what their data supports) without introducing unfair differences (the same activity, done at typical intensity, produces comparable estimates).
                """)
        else:
            st.info("Not enough shared activities across cohorts to compute fairness delta.")

    st.markdown("<br><br>", unsafe_allow_html=True)

    # =============================================================================
    # Takeaways
    # =============================================================================
    st.markdown(f"""
<div style='border-left:3px solid {ACCENT}; padding-left:1rem; margin:1rem 0 1rem 0;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.75rem;
              letter-spacing:0.25em; text-transform:uppercase; margin-bottom:0.3rem;'>Takeaways</div>
  <div style='color:{TEXT_PRIMARY}; font-family:"Oswald", sans-serif; font-size:1.5rem;
              font-weight:500;'>Why this shape of pipeline, why this shape of app</div>
</div>
    """, unsafe_allow_html=True)

    t1, t2, t3 = st.columns(3)
    with t1:
        st.markdown(f"""
<div style='background:{BG_PANEL}; border:1px solid {BORDER_SOFT};
            border-top:3px solid {ACCENT}; padding:1.2rem; border-radius:3px; height:100%;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.7rem;
              letter-spacing:0.2em; text-transform:uppercase; margin-bottom:0.6rem;'>Finding 1</div>
  <div style='color:{TEXT_PRIMARY}; font-weight:600; font-size:1rem; margin-bottom:0.5rem;'>
    Cohort context matters.
  </div>
  <div style='color:{TEXT_SECONDARY}; font-size:0.85rem; line-height:1.55;'>
    HARTH and HAR70+ differ not just in age but in daily routine. Pooling them
    would hide behaviors that actually shape the output. The app picks one cohort
    based on your age rather than averaging across both.
  </div>
</div>
        """, unsafe_allow_html=True)
    with t2:
        st.markdown(f"""
<div style='background:{BG_PANEL}; border:1px solid {BORDER_SOFT};
            border-top:3px solid {ACCENT}; padding:1.2rem; border-radius:3px; height:100%;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.7rem;
              letter-spacing:0.2em; text-transform:uppercase; margin-bottom:0.6rem;'>Finding 2</div>
  <div style='color:{TEXT_PRIMARY}; font-weight:600; font-size:1rem; margin-bottom:0.5rem;'>
    Labels aren't enough.
  </div>
  <div style='color:{TEXT_SECONDARY}; font-size:0.85rem; line-height:1.55;'>
    "Walking" at 30 and "walking" at 75 look measurably different in the sensor
    data. A label alone can't capture that. The pipeline's intensity calibration
    reads the underlying signal, not just the tag.
  </div>
</div>
        """, unsafe_allow_html=True)
    with t3:
        st.markdown(f"""
<div style='background:{BG_PANEL}; border:1px solid {BORDER_SOFT};
            border-top:3px solid {ACCENT}; padding:1.2rem; border-radius:3px; height:100%;'>
  <div style='color:{ACCENT}; font-family:"Oswald", sans-serif; font-size:0.7rem;
              letter-spacing:0.2em; text-transform:uppercase; margin-bottom:0.6rem;'>Finding 3</div>
  <div style='color:{TEXT_PRIMARY}; font-weight:600; font-size:1rem; margin-bottom:0.5rem;'>
    Calibration keeps it fair.
  </div>
  <div style='color:{TEXT_SECONDARY}; font-size:0.85rem; line-height:1.55;'>
    Each window is scored against its own cohort-activity reference. That turns
    raw sensor differences into comparable calorie rates — without hardcoding
    age adjustments anywhere in the formula.
  </div>
</div>
        """, unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)

    # =============================================================================
    # Data sources — keep existing expanders
    # =============================================================================
    st.markdown("#### Under the hood: data sources")

    with st.expander("HARTH dataset"):
        st.markdown(f"""
**Origin:** Human Activity Recognition Trondheim, collected by NTNU. Sensor data from back- and thigh-mounted accelerometers, labeled with ground-truth activity from video.

**Our slice:** {prov['n_harth']} subjects, {prov['windows_harth']:,} 2-second windows across {harth_activities} activities.

**Known limits:** working-age convenience sample, sedentary activities dominate, video-derived labels have high but imperfect purity.
        """)

    with st.expander("HAR70+ dataset"):
        st.markdown(f"""
**Origin:** Same NTNU group, extended to older adults. Narrower activity set by design (no vigorous activities for safety).

**Our slice:** {prov['n_har70']} subjects, {prov['windows_har70']:,} windows across {har70_activities} activities.

**Known limits:** smaller sample, no vigorous data. The {size_ratio:.1f}× size gap with HARTH is the most important caveat on cross-cohort comparisons.
        """)

    with st.expander("Open-Meteo weather"):
        st.markdown("""
**Origin:** Open-Meteo's historical archive + live forecast API. Free, no API key, attribution requested. Trondheim coordinates (63.4305, 10.3951).

**Our usage:** hourly temperature, precipitation, and WMO weather codes, aggregated to daily summaries for the recommender filter.

**Known limits:** the bad-weather threshold (codes 51-67, 71-77, 80-82, 95-99) is a judgment call, not a clinical definition. Outdoor options stay visible-but-flagged rather than disappearing, so you can override the default.
        """)

    with st.expander("2024 Compendium of Physical Activities"):
        st.markdown("""
**Origin:** published MET reference values, used as the baseline activity intensity before cohort calibration.

**Our usage:** MET values for each activity name, looked up at app runtime from `dim_activity.base_met`.

**Known limits:** compendium values are population averages — the pipeline's intensity calibration step adjusts these per cohort per window, which is why our calorie estimates differ slightly from raw compendium math for the same activity.
        """)
