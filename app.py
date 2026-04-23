"""
Calorie Coach - Streamlit app for the HARTH + HAR70+ pipeline.

Three tabs:
  - Estimate calories (Mode A): calculator for a chosen activity + effort
  - Help me pick something (Mode B): recommender with live weather
  - About this data (Mode C): insights dashboard, satisfies project spec 4.6

Data sources wired in:
  - pipeline.db  (sensor data + historical weather, local SQLite)
  - Open-Meteo API  (live weather for dates not in the DB)

Schema targeted (confirmed against schema.sql):
  dim_subject(subject_id, cohort, age_group)
  dim_activity(activity_id, activity_name, met_value, intensity_class, compendium_code)
  dim_weather_hour(hour_key, temperature_c, humidity_pct, precipitation_mm,
                   wind_speed_kmh, weather_code, cloud_cover_pct)
  dim_date(date, year, month, day, day_of_week, is_weekend, date_is_real)
  fact_window(..., met_minutes)

Design decisions documented in /docs/decisions.md. Key ones here:
  1. Mode A picker hides MET<2 (postures) because nobody picks "sitting" as a
     workout. Mode B keeps them: honest floor-case for low goals + long time.
  2. Per-window intensity percentiles were originally planned (see app.py v1)
     but dropped: met_minutes was computed from compendium MET, so per-window
     MET is effectively constant and a percentile picker would be meaningless.
     Replaced with an effort multiplier on dim_activity.met_value.
  3. Weather is hybrid: DB first (historical dates in the pipeline range),
     Open-Meteo fallback for dates outside the DB. Timeouts short, caches
     aggressively.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


# =============================================================================
# Configuration
# =============================================================================

APP_TITLE = "Calorie Coach"
APP_TAGLINE = "A calorie tool built on 85,000+ real sensor windows"
DB_URL = "https://github.com/catalinasara/Data-Processing-Project/releases/download/v1.0.0/pipeline.db"

DB_PATH = Path(__file__).parent / "pipeline.db"

if not DB_PATH.exists():
    st.info("Downloading database... first run only.")
    urllib.request.urlretrieve(DB_URL, DB_PATH)
    st.success("Database downloaded.")

# NTNU Trondheim coordinates - matches where HARTH/HAR70+ data was collected.
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

EFFORT_LEVELS = {"Easy": 0.85, "Steady": 1.00, "Pushing": 1.20}

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

# Palette — professional sports-data aesthetic.
# Deep graphite base (not pure black, not green-black), sharp volt-green accent
# used sparingly, cool slate for contrast. Drops the "radioactive glow" feel while
# keeping the dark, data-product personality.
ACCENT        = "#9AE62F"   # volt green — saturated but not glowing
ACCENT_SOFT   = "#B8F060"   # hover states, soft emphasis
ACCENT_CYAN   = "#4FC3F7"   # secondary accent for two-cohort contrast
BG_DEEP       = "#0E1215"   # graphite, cool undertone
BG_PANEL      = "#161B20"   # panels, cards
BG_ELEVATED   = "#1E252B"   # hover, elevated surfaces
BORDER_SOFT   = "rgba(255, 255, 255, 0.06)"
BORDER_MED    = "rgba(154, 230, 47, 0.18)"
BORDER_STRONG = "rgba(154, 230, 47, 0.35)"
TEXT_PRIMARY   = "#E8EEF1"   # off-white, neutral
TEXT_SECONDARY = "#A7B3BC"   # slate
TEXT_MUTED     = "#6A7681"   # muted slate
COLOR_HARTH   = ACCENT
COLOR_HAR70   = ACCENT_CYAN
COLOR_DIM     = "#2A3138"

KCAL_PER_MET_KG_HOUR = 1.0


def display_name(activity: str) -> str:
    return ACTIVITY_DISPLAY_NAMES.get(activity, activity.replace("_", " ").title())


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

    /* ---------- hide Streamlit chrome ---------- */
    header[data-testid="stHeader"] {{
        background: transparent !important;
        height: 0 !important;
    }}
    #MainMenu, footer, [data-testid="stToolbar"] {{ visibility: hidden; }}

    /* ---------- base canvas ---------- */
    .stApp {{
        background:
            radial-gradient(ellipse 1200px 800px at 15% -10%, rgba(154,230,47,0.025) 0%, transparent 60%),
            radial-gradient(ellipse 1000px 700px at 85% 110%, rgba(0,200,255,0.04) 0%, transparent 60%),
            linear-gradient(180deg, {BG_DEEP} 0%, #0A0F0D 100%);
        color: {TEXT_PRIMARY};
        font-family: 'JetBrains Mono', monospace;
    }}

    /* subtle scanlines, very faint */
    .stApp::before {{
        content: '';
        position: fixed; inset: 0;
        background: repeating-linear-gradient(
            0deg, transparent 0px, transparent 3px,
            rgba(154,230,47,0.004) 3px, rgba(154,230,47,0.004) 4px
        );
        pointer-events: none; z-index: 1;
    }}

    /* ---------- entrance animation ---------- */
    @keyframes fadeUp {{
        from {{ opacity: 0; transform: translateY(8px); }}
        to   {{ opacity: 1; transform: translateY(0); }}
    }}
    .main .block-container > div {{
        animation: fadeUp 0.5s ease-out;
    }}

    /* ---------- typography ---------- */
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
        letter-spacing: 0.01em;
        text-transform: uppercase;
    }}
    h2 {{ font-size: 1.5rem !important; font-weight: 500 !important; letter-spacing: 0.02em; }}
    h3 {{
        font-size: 1.15rem !important;
        font-weight: 500 !important;
        color: {TEXT_PRIMARY} !important;
        letter-spacing: 0.02em;
    }}

    p, .stMarkdown p {{
        font-family: 'Inter', sans-serif;
        font-size: 0.93rem;
        line-height: 1.6;
        color: {TEXT_SECONDARY};
    }}

    /* ---------- sidebar ---------- */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {BG_PANEL} 0%, #0D1411 100%);
        border-right: 1px solid {BORDER_SOFT};
    }}
    [data-testid="stSidebar"] > div {{ padding-top: 2.5rem; }}
    [data-testid="stSidebar"] h2 {{
        color: {ACCENT} !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.25em;
        margin-top: 0;
    }}

    /* sidebar collapse/expand button — the selector Streamlit actually uses */
    [data-testid="baseButton-header"],
    button[data-testid="baseButton-headerNoPadding"],
    [data-testid="collapsedControl"] {{
        background: {BG_PANEL} !important;
        border: 1.5px solid {ACCENT} !important;
        border-radius: 4px !important;
        opacity: 1 !important;
        box-shadow: 0 0 0 1px rgba(154,230,47,0.1) !important;
        padding: 6px !important;
    }}
    [data-testid="baseButton-header"]:hover,
    button[data-testid="baseButton-headerNoPadding"]:hover,
    [data-testid="collapsedControl"]:hover {{
        background: {BG_ELEVATED} !important;
        box-shadow: 0 0 0 2px rgba(154,230,47,0.25) !important;
    }}
    /* the SVG icon inside the toggle — force visible accent color */
    [data-testid="baseButton-header"] svg,
    [data-testid="baseButton-headerNoPadding"] svg,
    [data-testid="collapsedControl"] svg {{
        color: {ACCENT} !important;
        fill: {ACCENT} !important;
    }}
    /* when the sidebar is collapsed, the floating arrow pins to top-left */
    [data-testid="collapsedControl"] {{
        top: 0.6rem !important;
        left: 0.6rem !important;
        position: fixed !important;
        z-index: 999 !important;
    }}

    /* ---------- tabs ---------- */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0.25rem;
        background: transparent;
        border-bottom: 1px solid {BORDER_SOFT};
        padding-bottom: 0;
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
        transition: all 0.2s ease;
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

    /* ---------- metrics ---------- */
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
        transition: border-color 0.2s;
    }}
    [data-testid="stMetric"]:hover {{
        border-color: {BORDER_STRONG};
    }}

    /* ---------- inputs ---------- */
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

    /* slider */
    [data-baseweb="slider"] [role="slider"] {{
        background: {ACCENT} !important;
        box-shadow: 0 0 6px rgba(154,230,47,0.3) !important;
    }}

    /* radio as pills */
    .stRadio [role="radiogroup"] label {{
        background: {BG_PANEL};
        border: 1px solid {BORDER_SOFT};
        padding: 0.4rem 1rem;
        margin-right: 0.5rem;
        border-radius: 3px;
        transition: all 0.2s;
    }}
    .stRadio [role="radiogroup"] label:hover {{
        border-color: {BORDER_STRONG};
        background: rgba(154,230,47,0.04);
    }}

    /* alerts */
    .stAlert {{
        background: {BG_PANEL} !important;
        border-left: 3px solid {ACCENT} !important;
        border-radius: 3px !important;
        font-family: 'JetBrains Mono', monospace !important;
    }}

    /* dataframe */
    [data-testid="stDataFrame"] {{
        border: 1px solid {BORDER_SOFT};
        border-radius: 3px;
    }}

    /* captions */
    .stCaption, [data-testid="stCaptionContainer"] {{
        color: {TEXT_MUTED} !important;
        font-size: 0.75rem !important;
    }}

    /* expander */
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

    /* ---------- custom components ---------- */
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
        transition: all 0.25s ease;
    }}
    .headline-stat:hover {{
        border-color: {BORDER_STRONG};
        transform: translateY(-2px);
    }}
    .headline-stat .stat-value {{
        font-family: 'Oswald', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: {ACCENT};
        text-shadow: 0 0 6px rgba(154,230,47,0.15);
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

    /* live-data indicator */
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

    /* footer */
    .app-footer {{
        border-top: 1px solid {BORDER_SOFT};
        padding-top: 1.2rem;
        margin-top: 3rem;
        color: {TEXT_MUTED};
        font-size: 0.72rem;
        line-height: 1.6;
    }}

    /* hero tagline */
    .hero-sub {{
        color: {TEXT_SECONDARY};
        font-size: 0.95rem;
        letter-spacing: 0.02em;
        margin-bottom: 1rem;
    }}

    /* cohort card in sidebar */
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
# Database helpers
# =============================================================================

@st.cache_resource
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@st.cache_data
def load_cohort_activities() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(
        """
        SELECT s.cohort            AS cohort,
               a.activity_name     AS activity_name,
               a.met_value         AS met_value,
               a.intensity_class   AS intensity_class,
               COUNT(*)            AS n_windows
        FROM fact_window  f
        JOIN dim_subject  s ON f.subject_id  = s.subject_id
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY s.cohort, a.activity_name, a.met_value, a.intensity_class
        ORDER BY s.cohort, n_windows DESC
        """,
        conn,
    )


@st.cache_data
def load_provenance() -> dict:
    conn = get_connection()
    stats = {}
    stats["n_subjects"]   = conn.execute("SELECT COUNT(*) FROM dim_subject").fetchone()[0]
    stats["n_windows"]    = conn.execute("SELECT COUNT(*) FROM fact_window").fetchone()[0]
    stats["n_activities"] = conn.execute("SELECT COUNT(*) FROM dim_activity").fetchone()[0]
    cohort_counts = dict(conn.execute(
        "SELECT cohort, COUNT(*) FROM dim_subject GROUP BY cohort"
    ).fetchall())
    stats["n_harth"]  = cohort_counts.get("HARTH", 0)
    stats["n_har70"]  = cohort_counts.get("HAR70+", 0)
    window_counts = dict(conn.execute("""
        SELECT s.cohort, COUNT(*) FROM fact_window f
        JOIN dim_subject s ON f.subject_id = s.subject_id
        GROUP BY s.cohort
    """).fetchall())
    stats["windows_harth"] = window_counts.get("HARTH", 0)
    stats["windows_har70"] = window_counts.get("HAR70+", 0)
    return stats


@st.cache_data
def load_intensity_mix() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("""
        SELECT s.cohort           AS cohort,
               a.intensity_class  AS intensity_class,
               COUNT(*)           AS n_windows
        FROM fact_window  f
        JOIN dim_subject  s ON f.subject_id  = s.subject_id
        JOIN dim_activity a ON f.activity_id = a.activity_id
        GROUP BY s.cohort, a.intensity_class
    """, conn)


@st.cache_data
def load_db_weather_dates() -> list[date]:
    conn = get_connection()
    df = pd.read_sql(
        "SELECT DISTINCT DATE(hour_key) AS d FROM dim_weather_hour ORDER BY d", conn,
    )
    return [date.fromisoformat(d) for d in df["d"].tolist()]


def lookup_weather_db(target_date: date) -> dict | None:
    """Historical weather from the pipeline DB."""
    conn = get_connection()
    bad_codes_ph = ",".join("?" * len(BAD_WEATHER_CODES))
    query = f"""
        SELECT COUNT(*)                                             AS n_hours,
               ROUND(AVG(temperature_c), 1)                         AS avg_temp_c,
               ROUND(COALESCE(SUM(precipitation_mm), 0), 1)         AS total_precip_mm,
               MAX(CASE WHEN weather_code IN ({bad_codes_ph})
                        THEN 1 ELSE 0 END)                          AS has_bad_weather,
               MAX(weather_code)                                    AS worst_code
        FROM dim_weather_hour
        WHERE DATE(hour_key) = ?
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


@st.cache_data(ttl=3600)  # cache live results for an hour
def lookup_weather_live(target_date: date) -> dict | None:
    """
    Query Open-Meteo for a given date in Trondheim.

    Uses the forecast endpoint for today/future and the archive endpoint for
    past dates. Free, no API key required. 3-second timeout with graceful
    fallback to None so the app stays responsive if the API is slow.

    Returns a dict in the same shape as lookup_weather_db.
    """
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
    """
    Hybrid: DB first for dates in the historical archive, Open-Meteo for others.

    This keeps the fast path fast (most dates users care about are "today") and
    preserves the frozen historical data for the dates the pipeline captured.
    """
    db_result = lookup_weather_db(target_date)
    if db_result is not None:
        return db_result
    return lookup_weather_live(target_date)


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
# Header + provenance
# =============================================================================

st.markdown(f"# {APP_TITLE}")
st.markdown(
    f"<div class='hero-sub'>{APP_TAGLINE}</div>",
    unsafe_allow_html=True,
)

if not DB_PATH.exists():
    st.error(f"Can't find the database at {DB_PATH}. Run the pipeline notebooks first.")
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
# Sidebar profile
# =============================================================================

with st.sidebar:
    # Small pulse-line mark — reads as "activity/signal" without being gym clipart.
    # Pure inline SVG so it scales crisp and recolors with the theme.
    st.markdown(f"""
<div style="display: flex; align-items: center; gap: 0.7rem; margin-bottom: 1.5rem;">
  <svg width="32" height="32" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M2 16 L8 16 L11 8 L14 24 L17 12 L20 20 L23 16 L30 16"
          stroke="{ACCENT}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
          fill="none"/>
  </svg>
  <span style="font-family: 'Oswald', sans-serif; font-size: 1rem;
               font-weight: 500; letter-spacing: 0.15em; color: {TEXT_PRIMARY};
               text-transform: uppercase;">Calorie Coach</span>
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
            "Same science, better-matched numbers."
        )
    st.markdown(f'<div class="cohort-card">{cohort_msg}</div>', unsafe_allow_html=True)


# Shared data
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
# MODE A - Estimate
# -----------------------------------------------------------------------------

with tab_estimate:
    st.markdown("### How many calories will this burn?")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; margin-bottom:1.2rem;'>"
        "Pick an activity, say how hard you're going and for how long. We'll handle the rest."
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
            coach_line = (
                "Quick sessions like this are about consistency more than calorie burn. "
                "Still counts, still worth doing."
            )
        elif total_kcal < 150:
            coach_line = (
                "Solid range for a single session. Easy to fit into most days without wiping you out."
            )
        elif total_kcal < 300:
            coach_line = (
                "Real burn. Three or four of these a week adds up quickly."
            )
        else:
            coach_line = (
                f"{total_kcal:.0f} calories is a proper session. Don't forget to eat after."
            )
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
                st.markdown(f"""
We're comparing {chosen_display.lower()} against other {intensity_class}-class activities at the same duration, weight, and effort multiplier. All of these sit in the same intensity band (MET between {alternatives['met_value'].min():.1f} and {alternatives['met_value'].max():.1f}), so the spread you see comes entirely from compendium MET differences, not from how hard you're working.

{top['activity']} produces about **{ratio:.1f}x** the caloric output of {bot['activity'].lower()} at these settings. That multiplier is weight-invariant, the MET formula scales linearly with mass, so this ranking holds for any user.

One caveat worth naming: confidence in each estimate scales with how many sensor windows back it up. {display_name(chosen_activity)} has **{n_windows:,}** windows in your cohort, which is {confidence}. Below a few hundred windows, treat numbers as directional rather than precise.
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

Based on **{n_windows:,}** observed windows of this activity in the {cohort_label} reference group.
            """)


# -----------------------------------------------------------------------------
# MODE B - Recommend
# -----------------------------------------------------------------------------

with tab_recommend:
    st.markdown("### I want to burn some calories. What should I do?")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; margin-bottom:1.2rem;'>"
        "Set a goal and a time window, we'll show what fits. We'll check the weather in Trondheim too."
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
            f"Couldn't fetch weather for {target_date.isoformat()} "
            "(API timeout or date outside our range). Showing all activities."
        )
    else:
        # Header line with live/archive indicator
        src_label = "live" if weather["source"] == "live" else "archive"
        src_html = (
            f"<span class='live-dot'></span><strong>live</strong> weather"
            if src_label == "live"
            else f"<strong>archive</strong> weather"
        )
        # Cross-platform date format: manual, avoiding %-d (Unix) / %#d (Windows).
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
                "Rough weather ahead. We'll still show outdoor options, just flagged "
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

            st.markdown(f"""
Of the **{n_total}** activities in the {cohort_label} reference set, **{n_fit}** can hit your {goal_kcal} kcal goal inside your {max_duration}-minute window.
**{n_over_time}** are filtered purely on time.
{f"**{n_weather_cut}** additional outdoor options are flagged because Trondheim's weather crosses our bad-weather threshold (WMO codes for rain, snow, or thunderstorm)." if n_weather_cut > 0 else ""}

Within the feasible set, the efficiency spread is about **{speed_gap:.1f}x** (slowest vs fastest), driven entirely by MET differences. {display_name(fastest_activity['activity_name'])} at MET {fastest_activity['met_value']:.1f} is {speed_gap:.1f}x more time-efficient than {display_name(slowest_rec['activity_name']).lower()} at MET {slowest_rec['met_value']:.1f}, which is exactly what the formula predicts (time scales as the inverse of MET).

**Takeaway:** if time is the constraint, intensity is the lever. If intensity is the constraint (injury, age, preference), you budget more time. This chart makes that trade-off visible instead of hiding it behind a single "best pick."
            """)


# -----------------------------------------------------------------------------
# MODE C - About this data (dashboard / insights)
# -----------------------------------------------------------------------------

with tab_about:
    st.markdown("### What's inside the pipeline")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; margin-bottom:1.5rem;'>"
        "A data-first view of what's behind every estimate. Where the numbers come from, "
        "where they're strong, and where they're thin."
        "</p>",
        unsafe_allow_html=True,
    )

    # ---- Headline findings ----
    st.markdown("#### Key findings")

    mix = load_intensity_mix()
    harth_mix = mix[mix["cohort"] == "HARTH"]
    har70_mix = mix[mix["cohort"] == "HAR70+"]

    size_ratio = prov["windows_harth"] / max(prov["windows_har70"], 1)
    harth_activities = all_activities[all_activities["cohort"] == "HARTH"]["activity_name"].nunique()
    har70_activities = all_activities[all_activities["cohort"] == "HAR70+"]["activity_name"].nunique()
    har70_vigorous = int(har70_mix[har70_mix["intensity_class"] == "vigorous"]["n_windows"].sum())
    harth_vigorous = int(harth_mix[harth_mix["intensity_class"] == "vigorous"]["n_windows"].sum())

    # Top 2 concentration
    def top2_share(cohort: str) -> tuple[int, list[str]]:
        sub = all_activities[all_activities["cohort"] == cohort].sort_values("n_windows", ascending=False)
        total = sub["n_windows"].sum()
        top2 = sub.head(2)
        return int(top2["n_windows"].sum() / total * 100), top2["activity_name"].tolist()
    harth_share, harth_top = top2_share("HARTH")
    har70_share, har70_top = top2_share("HAR70+")

    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.markdown(headline_stat(
            f"{size_ratio:.1f}×",
            "Cohort size gap",
            f"HARTH has {size_ratio:.1f}× more sensor windows than HAR70+. "
            f"Any comparison across cohorts carries this imbalance as a caveat."
        ), unsafe_allow_html=True)
    with h2:
        st.markdown(headline_stat(
            f"{har70_activities} vs {harth_activities}",
            "Activity coverage",
            f"HAR70+ has {har70_activities} activities observed. HARTH has {harth_activities}. "
            "The gap is real: older adults in this study don't run or cycle."
        ), unsafe_allow_html=True)
    with h3:
        st.markdown(headline_stat(
            f"{har70_vigorous:,}",
            "Vigorous-class HAR70+ windows",
            f"Zero. Every vigorous data point we have comes from HARTH ({harth_vigorous:,} windows). "
            "A structural bias in the dataset, not a pipeline bug."
        ), unsafe_allow_html=True)
    with h4:
        st.markdown(headline_stat(
            f"{harth_share}% / {har70_share}%",
            "Top-2 concentration",
            f"In both cohorts, just two activities ({display_name(harth_top[0]).lower()} and "
            f"{display_name(harth_top[1]).lower()}) account for most windows. Everything else is tail."
        ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Interactive cohort comparison ----
    st.markdown("#### Explore: cohort comparison")
    st.markdown(
        f"<p style='color:{TEXT_SECONDARY}; font-size:0.88rem;'>"
        "Toggle between views to see how HARTH and HAR70+ stack up. "
        "Click a legend item to isolate that cohort."
        "</p>",
        unsafe_allow_html=True,
    )

    view = st.radio(
        "View",
        options=["Sample size by activity", "Intensity mix", "MET profile"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if view == "Sample size by activity":
        # Grouped bar: activity × cohort
        pivot = all_activities.pivot_table(
            index="activity_name", columns="cohort",
            values="n_windows", fill_value=0,
        ).reset_index()
        pivot["total"] = pivot.get("HARTH", 0) + pivot.get("HAR70+", 0)
        pivot = pivot.sort_values("total", ascending=True)
        pivot["display"] = pivot["activity_name"].map(display_name)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="HARTH (under 70)", y=pivot["display"], x=pivot.get("HARTH", 0),
            orientation="h",
            marker=dict(color=COLOR_HARTH, line=dict(color=COLOR_HARTH, width=1)),
            hovertemplate="<b>%{y}</b><br>HARTH: %{x:,} windows<extra></extra>",
        ))
        fig.add_trace(go.Bar(
            name="HAR70+ (70 and over)", y=pivot["display"], x=pivot.get("HAR70+", 0),
            orientation="h",
            marker=dict(color=COLOR_HAR70, line=dict(color=COLOR_HAR70, width=1)),
            hovertemplate="<b>%{y}</b><br>HAR70+: %{x:,} windows<extra></extra>",
        ))
        fig.update_layout(
            title="Sensor windows per activity, by cohort",
            barmode="group", xaxis_title="Windows", yaxis_title="",
        )
        st.plotly_chart(style_plot(fig, height=500), use_container_width=True)

        with st.expander("Analytical read"):
            st.markdown("""
Two things jump out. First, sitting and walking dominate both cohorts, reflecting how people
actually spend their time, not how a gym programmer would want them to. Most ambient-activity
datasets look like this, and it's a meaningful constraint: we have strong confidence in walking
estimates and much weaker confidence for things like stairs (under 800 windows in HARTH, under 50 in HAR70+).

Second, the right-tail (vigorous activities) exists only in HARTH. There's no ethical or clean
way to fill that in for HAR70+ without either collecting new data or borrowing estimates from
the compendium, which would undermine our cohort-specific framing. We document this rather than
pretend the data is balanced.
            """)

    elif view == "Intensity mix":
        # Stacked percentage bars
        mix_pct = mix.copy()
        totals = mix_pct.groupby("cohort")["n_windows"].transform("sum")
        mix_pct["pct"] = mix_pct["n_windows"] / totals * 100

        intensity_order = ["sedentary", "light", "moderate", "vigorous"]
        intensity_colors = {
            "sedentary": "#2F3D33",
            "light":     "#4A8857",
            "moderate":  "#00C8FF",
            "vigorous":  ACCENT,
        }

        fig = go.Figure()
        for ic in intensity_order:
            sub = mix_pct[mix_pct["intensity_class"] == ic]
            if sub.empty:
                continue
            fig.add_trace(go.Bar(
                name=ic.title(),
                x=sub["cohort"], y=sub["pct"],
                marker=dict(color=intensity_colors[ic]),
                text=[f"{p:.0f}%" for p in sub["pct"]],
                textposition="inside",
                textfont=dict(color="white", size=12),
                hovertemplate=(
                    "<b>%{x}</b><br>" + ic.title() + ": %{y:.1f}% of windows<extra></extra>"
                ),
            ))
        fig.update_layout(
            title="Share of sensor windows by intensity class (%)",
            barmode="stack", xaxis_title="", yaxis_title="% of windows",
        )
        st.plotly_chart(style_plot(fig, height=420), use_container_width=True)

        with st.expander("Analytical read"):
            st.markdown(f"""
HARTH sits at {int(harth_mix[harth_mix['intensity_class']=='sedentary']['n_windows'].sum()/prov['windows_harth']*100)}% sedentary, which tracks with desk-based working-age life.
HAR70+ is actually *more* active on a percentage basis in the moderate band (mostly walking),
but has zero vigorous windows.

The planning implication: HAR70+-targeted recommendations in Mode B max out at the moderate band
by design, not by arbitrary filter. The data literally doesn't contain vigorous evidence for
older adults in this study. A recommender that showed running to a 75-year-old would be making
up numbers.
            """)

    else:  # MET profile
        avg_met = all_activities.copy()
        avg_met["display"] = avg_met["activity_name"].map(display_name)
        avg_met = avg_met.sort_values("met_value")

        fig = go.Figure()
        for cohort_label_plot, color in [("HARTH", COLOR_HARTH), ("HAR70+", COLOR_HAR70)]:
            sub = avg_met[avg_met["cohort"] == cohort_label_plot]
            fig.add_trace(go.Scatter(
                name=cohort_label_plot,
                x=sub["met_value"], y=sub["display"],
                mode="markers",
                marker=dict(
                    size=[min(40, max(8, n / 400)) for n in sub["n_windows"]],
                    color=color,
                    line=dict(color=color, width=1),
                    opacity=0.75,
                ),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + cohort_label_plot + "<br>"
                    "MET: %{x}<br>"
                    "Windows: %{customdata:,}<extra></extra>"
                ),
                customdata=sub["n_windows"],
            ))
        fig.update_layout(
            title="Activity MET values (bubble size = sample size)",
            xaxis_title="MET value", yaxis_title="",
        )
        st.plotly_chart(style_plot(fig, height=500), use_container_width=True)

        with st.expander("Analytical read"):
            st.markdown("""
Each bubble is an activity in one cohort. X-axis is the compendium MET value, bubble size is
how many sensor windows we have for it. The chart makes two things visible at once:

Where HAR70+ has coverage (left half of the x-axis, up through walking and stairs) and where it
doesn't (MET 6+ is HARTH-only). And where confidence is strongest (big bubbles, i.e. sitting and
walking) versus where we're working with small samples (the tails in both cohorts).

For the jury question about "what would change if the data volume 10x'd": the small bubbles
would grow proportionally, the left-right asymmetry would stay. You can't cover more activities
for HAR70+ by collecting more HARTH data.
            """)

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Weather coverage ----
    st.markdown("#### Weather data: live + archive")

    db_dates = load_db_weather_dates()
    st.markdown(f"""
<p style='color:{TEXT_SECONDARY}; font-size:0.88rem;'>
Weather enters the pipeline from two sources. The <strong style='color:{ACCENT};'>archive</strong>
sits in the SQLite database: {len(db_dates)} historical dates from the Open-Meteo archive,
matching the sessions when sensor subjects were recording. The <strong style='color:{ACCENT};'>live</strong>
feed hits Open-Meteo's API directly for any date outside the archive, including today's forecast
and any future date. Both come from the same underlying dataset, one is just cached.
</p>
    """, unsafe_allow_html=True)

    # Simple timeline of archive coverage
    dates_df = pd.DataFrame({
        "date": pd.to_datetime(db_dates),
        "value": 1,
    })
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates_df["date"], y=dates_df["value"],
        mode="markers",
        marker=dict(size=12, color=ACCENT, line=dict(color=ACCENT, width=1),
                    symbol="diamond"),
        hovertemplate="<b>%{x|%B %d, %Y}</b><br>in archive<extra></extra>",
        showlegend=False,
    ))
    fig.update_layout(
        title="Dates in the weather archive",
        xaxis_title="Date",
        yaxis=dict(visible=False),
        showlegend=False,
    )
    st.plotly_chart(style_plot(fig, height=220), use_container_width=True)

    with st.expander("Why hybrid storage makes sense here"):
        st.markdown(f"""
The pipeline could have gone one of two ways: pre-cache every plausible date (heavy, wasteful, stale),
or hit the API every time (slow, API-dependent, no offline capability).

We went hybrid. Dates that matter for joining to the sensor fact table (the {len(db_dates)} sessions
where HARTH/HAR70+ subjects were recording) get cached in the DB, indexed, and joined at query time.
Everything else, including future dates for the recommender, hits the Open-Meteo API live with a
short timeout and graceful fallback.

**Why that's the right trade-off:** the fact-table joins need to be deterministic and fast (sensor-to-weather
joins happen at pipeline build time, millions of times, and must be reproducible). User-facing recommendations
need freshness, not determinism, so the API is fine there. Splitting by use case lets each storage layer
do what it's good at.

If this data volume 10×'d, the archive portion would stay the same (still just the session dates),
and the live portion would sit behind a per-user cache with a 1-hour TTL. No architectural change needed.
        """)

    st.markdown("<br>", unsafe_allow_html=True)

    # ---- Data sources + governance summary ----
    st.markdown("#### Data sources and what we assume about them")

    with st.expander("HARTH dataset", expanded=False):
        st.markdown(f"""
**Origin:** Human Activity Recognition Trondheim, collected by NTNU (Norwegian University of Science and Technology).
Sensor data from back- and thigh-mounted accelerometers, labeled with ground-truth activity from video.

**Our slice:** {prov['n_harth']} subjects, {prov['windows_harth']:,} 2-second windows across {harth_activities} activities.

**Known limits:** HARTH's subjects are working-age adults, a convenience sample not designed to be
population-representative. Sedentary activities dominate (reflecting real life), which is a feature for our
use case but a limitation for gym-specific applications. Activity labels are from video, so label purity is
high but not perfect.
        """)

    with st.expander("HAR70+ dataset", expanded=False):
        st.markdown(f"""
**Origin:** Same NTNU group, extended to older adults. Fewer subjects, fewer sessions, narrower activity set
by design (vigorous activities not collected for safety).

**Our slice:** {prov['n_har70']} subjects, {prov['windows_har70']:,} windows across {har70_activities} activities.

**Known limits:** Smaller sample, narrower activity coverage, and no vigorous-intensity data. The
{size_ratio:.1f}× size gap with HARTH is the most important caveat when interpreting cross-cohort comparisons.
We surface this in both the Estimate and Recommend tabs by keeping cohorts separated rather than pooling data.
        """)

    with st.expander("Open-Meteo weather", expanded=False):
        st.markdown("""
**Origin:** Open-Meteo's historical archive + live forecast API. Free, no API key required, attribution requested.
Centered on Trondheim (lat 63.4305, lon 10.3951).

**Our usage:** Hourly temperature, precipitation, and weather codes. Aggregated to daily summaries for the
recommender's weather filter.

**Known limits:** Open-Meteo's weather codes follow the WMO standard. Our bad-weather threshold (codes 51-67, 71-77, 80-82, 95-99)
is a judgment call, not a clinical definition. Someone who enjoys running in light rain would legitimately disagree
with our filter, which is why outdoor options stay visible (dimmed) rather than disappearing entirely.
        """)


# =============================================================================
# Footer
# =============================================================================

st.markdown(f"""
<div class="app-footer">
Reference data: HARTH ({prov['n_harth']} working-age adults) + HAR70+ ({prov['n_har70']} adults 70+), NTNU Trondheim.
MET values: 2024 Compendium of Physical Activities.
Weather: Open-Meteo historical archive + live forecast API.
Estimates are based on population averages and shouldn't be treated as medical advice.
</div>
""", unsafe_allow_html=True)
