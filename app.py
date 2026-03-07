# =========================
# MENTORIA DO JHON - FASE 4 PREMIUM
# BLOCO 1/5
# BASE + BANCO + CSS + LOGIN + MENU
# =========================

import os
import sqlite3
import hashlib
import base64
import html
from datetime import datetime, date, timedelta
from typing import Optional
from textwrap import dedent

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Imports mantidos para futuras extensões de relatório PDF
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader


# =========================================================
# CONFIG APP
# =========================================================
APP_NAME = "🩺 Mentoria do Jhon"
APP_SUBTITLE = "Plataforma premium de acompanhamento para Residência Médica"
APP_VERSION = "FASE 4 PREMIUM"
DB_PATH = "mentoria_jhon_fase4.db"

DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = os.environ.get("MENTORIA_ADMIN_PASS", "admin123")

LOGO_CANDIDATES = [
    "/mnt/data/logo.png",
    "/mnt/data/mentoria_logo.png",
    "/mnt/data/photo_2026-02-08_22-17-28.jpg",
    "logo.png",
    "mentoria_logo.png",
    os.path.join("assets", "logo.png"),
    os.path.join("assets", "mentoria_logo.png"),
]

GREAT_AREAS = [
    "Clínica Médica",
    "Cirurgia",
    "Pediatria",
    "Ginecologia e Obstetrícia",
    "Preventiva",
]

SCHEDULE_CSV_CANDIDATES = [
    "/mnt/data/itens_teoria_por_semana.csv",
    "itens_teoria_por_semana.csv",
    os.path.join("assets", "itens_teoria_por_semana.csv"),
]

st.set_page_config(
    page_title=APP_NAME,
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# =========================================================
# HELPERS GERAIS
# =========================================================
def safe_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass


def render_html_block(content: str):
    if not content:
        return
    cleaned = dedent(str(content)).strip()
    if not cleaned:
        return
    st.markdown(cleaned, unsafe_allow_html=True)


def ensure_session_defaults():
    defaults = {
        "user_id": None,
        "username": None,
        "is_admin": False,
        "logged_in": False,
        "menu": "Visão Geral",
        "flashcard_fullscreen": False,
        "flashcard_index": 0,
        "flashcard_show_answer": False,
        "flashcard_show_note": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def reset_flashcard_state():
    st.session_state.flashcard_fullscreen = False
    st.session_state.flashcard_index = 0
    st.session_state.flashcard_show_answer = False
    st.session_state.flashcard_show_note = False


def reset_login_state():
    st.session_state.logged_in = False
    st.session_state.user_id = None
    st.session_state.username = None
    st.session_state.is_admin = False
    st.session_state.menu = "Visão Geral"
    reset_flashcard_state()


def hash_password(password: str) -> str:
    return hashlib.sha256((password or "").encode("utf-8")).hexdigest()


def normalize_text(value):
    return str(value or "").strip()


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def to_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def get_today_str():
    return date.today().isoformat()


def get_month_range(ref_date=None):
    ref_date = ref_date or date.today()
    start = ref_date.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start.isoformat(), end.isoformat()


def get_last_30_days_range():
    end = date.today()
    start = end - timedelta(days=29)
    return start.isoformat(), end.isoformat()


def get_logo_path():
    for path in LOGO_CANDIDATES:
        if path and os.path.exists(path):
            return path
    return None


def get_schedule_csv_path():
    for path in SCHEDULE_CSV_CANDIDATES:
        if path and os.path.exists(path):
            return path
    return None


def image_to_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def render_logo_html(height=70, css_class="brand-logo"):
    logo_path = get_logo_path()
    if not logo_path:
        return ""

    try:
        ext = os.path.splitext(logo_path)[1].lower().replace(".", "")
        if ext == "jpg":
            ext = "jpeg"
        img64 = image_to_base64(logo_path)
        return f'<img class="{css_class}" src="data:image/{ext};base64,{img64}" style="height:{height}px;" />'
    except Exception:
        return ""


def today_plus_days(days: int):
    return (date.today() + timedelta(days=days)).isoformat()


# =========================================================
# BANCO DE DADOS
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        is_admin INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS study_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        session_date TEXT NOT NULL,
        study_minutes INTEGER NOT NULL DEFAULT 0,
        questions_done INTEGER NOT NULL DEFAULT 0,
        correct_answers INTEGER NOT NULL DEFAULT 0,
        subject TEXT DEFAULT '',
        topic TEXT DEFAULT '',
        notes TEXT DEFAULT '',
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        daily_questions_goal INTEGER NOT NULL DEFAULT 60,
        daily_minutes_goal INTEGER NOT NULL DEFAULT 180,
        monthly_mock_goal INTEGER NOT NULL DEFAULT 4,
        phase_name TEXT DEFAULT 'Intermediária',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schedule_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        week_no INTEGER NOT NULL,
        area TEXT DEFAULT '',
        subject TEXT DEFAULT '',
        topic TEXT DEFAULT '',
        item_type TEXT DEFAULT '',
        title TEXT NOT NULL,
        planned_date TEXT,
        completed INTEGER NOT NULL DEFAULT 0,
        completed_at TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        mock_date TEXT NOT NULL,
        title TEXT DEFAULT '',
        score_percent REAL NOT NULL DEFAULT 0,
        questions_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS flashcards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        deck TEXT DEFAULT '',
        subject TEXT DEFAULT '',
        topic TEXT DEFAULT '',
        question TEXT NOT NULL,
        answer TEXT NOT NULL,
        note TEXT DEFAULT '',
        created_at TEXT NOT NULL
    )
    """)

    conn.commit()

    cur.execute("SELECT id FROM users WHERE username = ?", (DEFAULT_ADMIN_USER,))
    admin = cur.fetchone()
    if not admin:
        cur.execute(
            """
            INSERT INTO users (username, password_hash, is_admin, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                DEFAULT_ADMIN_USER,
                hash_password(DEFAULT_ADMIN_PASS),
                1,
                datetime.now().isoformat()
            )
        )
        conn.commit()

        cur.execute("SELECT id FROM users WHERE username = ?", (DEFAULT_ADMIN_USER,))
        admin = cur.fetchone()

        if admin:
            cur.execute(
                """
                INSERT INTO goals (
                    user_id, daily_questions_goal, daily_minutes_goal, monthly_mock_goal,
                    phase_name, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(admin["id"]),
                    60,
                    180,
                    4,
                    "Intermediária",
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                )
            )
            conn.commit()

    conn.close()


def ensure_schema_upgrades():
    conn = get_conn()
    cur = conn.cursor()

    existing_cols = []
    try:
        cur.execute("PRAGMA table_info(study_sessions)")
        existing_cols = [row[1] for row in cur.fetchall()]
    except Exception:
        existing_cols = []

    if "grande_area" not in existing_cols:
        cur.execute("ALTER TABLE study_sessions ADD COLUMN grande_area TEXT DEFAULT ''")

    existing_fc_cols = []
    try:
        cur.execute("PRAGMA table_info(flashcards)")
        existing_fc_cols = [row[1] for row in cur.fetchall()]
    except Exception:
        existing_fc_cols = []

    flashcard_alters = {
        "due_date": "ALTER TABLE flashcards ADD COLUMN due_date TEXT",
        "last_reviewed": "ALTER TABLE flashcards ADD COLUMN last_reviewed TEXT",
        "review_count": "ALTER TABLE flashcards ADD COLUMN review_count INTEGER NOT NULL DEFAULT 0",
        "lapse_count": "ALTER TABLE flashcards ADD COLUMN lapse_count INTEGER NOT NULL DEFAULT 0",
        "ease_factor": "ALTER TABLE flashcards ADD COLUMN ease_factor REAL NOT NULL DEFAULT 2.5",
        "interval_days": "ALTER TABLE flashcards ADD COLUMN interval_days INTEGER NOT NULL DEFAULT 0",
        "card_state": "ALTER TABLE flashcards ADD COLUMN card_state TEXT NOT NULL DEFAULT 'new'",
    }

    for col, ddl in flashcard_alters.items():
        if col not in existing_fc_cols:
            cur.execute(ddl)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mock_area_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mock_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        grande_area TEXT NOT NULL,
        correct_count INTEGER NOT NULL DEFAULT 0,
        question_count INTEGER NOT NULL DEFAULT 0,
        accuracy_percent REAL NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()


# =========================================================
# BASE DATAFRAME
# =========================================================
def fetch_dataframe(query: str, params=()):
    conn = get_conn()
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


# =========================================================
# USUÁRIOS / METAS
# =========================================================
def create_user(username: str, password: str, is_admin: int = 0):
    username = normalize_text(username)
    password = normalize_text(password)

    if not username:
        return False, "Digite um nome de usuário."
    if len(username) < 3:
        return False, "O usuário deve ter pelo menos 3 caracteres."
    if not password:
        return False, "Digite uma senha."
    if len(password) < 4:
        return False, "A senha deve ter pelo menos 4 caracteres."

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO users (username, password_hash, is_admin, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (username, hash_password(password), is_admin, datetime.now().isoformat())
        )
        user_id = cur.lastrowid

        cur.execute(
            """
            INSERT INTO goals (
                user_id, daily_questions_goal, daily_minutes_goal, monthly_mock_goal,
                phase_name, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                60,
                180,
                4,
                "Intermediária",
                datetime.now().isoformat(),
                datetime.now().isoformat()
            )
        )

        conn.commit()
        return True, "Usuário criado com sucesso."
    except sqlite3.IntegrityError:
        return False, "Esse usuário já existe."
    except Exception as e:
        return False, f"Erro ao criar usuário: {e}"
    finally:
        conn.close()


def authenticate_user(username: str, password: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, password_hash, is_admin
        FROM users
        WHERE username = ?
        """,
        (normalize_text(username),)
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    if row["password_hash"] != hash_password(password):
        return None

    return {
        "id": row["id"],
        "username": row["username"],
        "is_admin": bool(row["is_admin"]),
    }


def get_user_goal(user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM goals
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    conn.close()

    if row:
        return dict(row)

    return {
        "daily_questions_goal": 60,
        "daily_minutes_goal": 180,
        "monthly_mock_goal": 4,
        "phase_name": "Intermediária"
    }


def update_goal_settings(user_id: int, daily_questions_goal: int, daily_minutes_goal: int,
                         monthly_mock_goal: int, phase_name: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM goals WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,))
        row = cur.fetchone()

        if row:
            cur.execute(
                """
                UPDATE goals
                SET daily_questions_goal = ?, daily_minutes_goal = ?,
                    monthly_mock_goal = ?, phase_name = ?, updated_at = ?
                WHERE user_id = ?
                """,
                (
                    to_int(daily_questions_goal, 60),
                    to_int(daily_minutes_goal, 180),
                    to_int(monthly_mock_goal, 4),
                    normalize_text(phase_name) or "Intermediária",
                    datetime.now().isoformat(),
                    user_id
                )
            )
        else:
            cur.execute(
                """
                INSERT INTO goals (
                    user_id, daily_questions_goal, daily_minutes_goal,
                    monthly_mock_goal, phase_name, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    to_int(daily_questions_goal, 60),
                    to_int(daily_minutes_goal, 180),
                    to_int(monthly_mock_goal, 4),
                    normalize_text(phase_name) or "Intermediária",
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                )
            )

        conn.commit()
        return True, "Metas atualizadas com sucesso."
    except Exception as e:
        return False, f"Erro ao atualizar metas: {e}"
    finally:
        conn.close()


def fetch_users_df():
    df = fetch_dataframe("""
        SELECT id, username, is_admin, created_at
        FROM users
        ORDER BY id ASC
    """)
    if df.empty:
        return pd.DataFrame(columns=["id", "username", "is_admin", "created_at", "is_admin_label"])
    df["is_admin_label"] = df["is_admin"].apply(lambda x: "Sim" if int(x) == 1 else "Não")
    return df


# =========================================================
# CSS GLOBAL
# =========================================================
def inject_global_css():
    st.markdown(
        """
        <style>
        :root{
            --bg:#07111f;
            --card:#0f1c2e;
            --line:rgba(255,255,255,.08);
            --text:#eef4ff;
            --muted:#9fb0c8;
            --brand:#5ab2ff;
            --brand2:#8b5cf6;
            --shadow:0 12px 40px rgba(0,0,0,.28);
            --radius:22px;
        }

        html, body, [class*="css"]{
            font-family: "Inter", "Segoe UI", sans-serif;
        }

        .stApp{
            background:
                radial-gradient(circle at top left, rgba(90,178,255,0.10), transparent 28%),
                radial-gradient(circle at top right, rgba(139,92,246,0.12), transparent 24%),
                linear-gradient(180deg, #06101c 0%, #081321 45%, #07111f 100%);
            color: var(--text);
        }

        .block-container{
            padding-top: 0.6rem !important;
            padding-bottom: 1.6rem !important;
            max-width: 1380px;
        }

        .stApp header,
        [data-testid="stHeader"]{
            background: transparent !important;
        }

        [data-testid="stSidebar"],
        [data-testid="collapsedControl"]{
            display:none !important;
        }

        div[data-testid="stToolbar"]{
            right: 1rem;
        }

        .brand-shell{
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.08);
            background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
            box-shadow: var(--shadow);
            border-radius: 28px;
            padding: 20px 24px;
            margin-bottom: 14px;
            backdrop-filter: blur(10px);
        }

        .brand-shell::before{
            content:"";
            position:absolute;
            inset:0;
            background:
                radial-gradient(circle at 15% 20%, rgba(90,178,255,.18), transparent 24%),
                radial-gradient(circle at 85% 10%, rgba(139,92,246,.14), transparent 22%);
            pointer-events:none;
        }

        .brand-row{
            position: relative;
            z-index: 2;
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap:20px;
            flex-wrap:wrap;
        }

        .brand-left{
            display:flex;
            align-items:center;
            gap:18px;
            flex-wrap:wrap;
        }

        .brand-logo, .hero-logo{
            width:auto;
            object-fit:contain;
            border-radius:16px;
            filter: drop-shadow(0 10px 22px rgba(0,0,0,.28));
        }

        .brand-title{
            font-size: 1.95rem;
            font-weight: 800;
            line-height: 1.1;
            letter-spacing: -0.03em;
            color: #f7fbff;
            margin: 0;
        }

        .brand-subtitle{
            color: var(--muted);
            font-size: 0.98rem;
            margin-top: 6px;
        }

        .brand-badges{
            display:flex;
            gap:10px;
            flex-wrap:wrap;
            justify-content:flex-end;
        }

        .brand-badge{
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.05);
            color:#d9e7fb;
            padding:8px 12px;
            border-radius:999px;
            font-size:.82rem;
            font-weight:600;
        }

        .glass-card{
            border: 1px solid rgba(255,255,255,.07);
            background: linear-gradient(180deg, rgba(255,255,255,.045), rgba(255,255,255,.03));
            border-radius: 24px;
            padding: 18px 20px;
            box-shadow: var(--shadow);
        }

        .section-title{
            font-size:1.22rem;
            font-weight:800;
            color:#f7fbff;
            margin-bottom:4px;
            letter-spacing:-0.02em;
        }

        .section-subtitle{
            color:#9fb0c8;
            font-size:.93rem;
            margin-bottom:0;
        }

        .top-spacer-sm{ height:10px; }
        .top-spacer-md{ height:14px; }
        .top-spacer-lg{ height:22px; }

        .hero-card, .login-card{
            border-radius: 30px;
            border: 1px solid rgba(255,255,255,.08);
            box-shadow: 0 24px 60px rgba(0,0,0,.34);
            min-height: 100%;
        }

        .hero-card{
            padding: 28px 28px;
            background: linear-gradient(145deg, rgba(255,255,255,.06), rgba(255,255,255,.03));
        }

        .login-card{
            padding: 24px 24px;
            background: linear-gradient(180deg, rgba(13,22,37,.96) 0%, rgba(9,17,30,.98) 100%);
        }

        .hero-pill{
            display:inline-flex;
            border-radius:999px;
            padding:8px 12px;
            background: rgba(255,255,255,.06);
            border: 1px solid rgba(255,255,255,.08);
            color:#dbe8fb;
            font-size:.82rem;
            font-weight:700;
            margin-bottom:14px;
        }

        .hero-title{
            font-size: 2.65rem;
            line-height: 1.03;
            font-weight: 900;
            color: #ffffff;
            letter-spacing: -0.05em;
            margin-bottom: 12px;
        }

        .hero-title .grad{
            background: linear-gradient(135deg, #ffffff 0%, #9fd4ff 45%, #b29cff 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .login-title{
            font-size: 1.65rem;
            font-weight: 800;
            color: #f7fbff;
            margin-bottom: 6px;
        }

        .login-subtitle{
            color: var(--muted);
            font-size: .96rem;
            margin-bottom: 18px;
        }

        .section-chip{
            display:inline-block;
            padding:7px 12px;
            border-radius:999px;
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.04);
            color:#dce8fb;
            font-size:.8rem;
            font-weight:700;
            margin-bottom:14px;
        }

        .stTextInput > div > div > input,
        .stNumberInput input,
        .stDateInput input,
        .stTextArea textarea,
        .stSelectbox div[data-baseweb="select"] > div {
            background: rgba(255,255,255,0.045) !important;
            color: #f3f8ff !important;
            border: 1px solid rgba(255,255,255,0.09) !important;
            border-radius: 16px !important;
        }

        .stTextInput label,
        .stNumberInput label,
        .stDateInput label,
        .stTextArea label,
        .stSelectbox label,
        .stRadio label,
        .stCheckbox label {
            color:#dbe7f6 !important;
            font-weight:600 !important;
        }

        .stTabs [data-baseweb="tab-list"]{
            gap: 8px;
        }

        .stTabs [data-baseweb="tab"]{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.07);
            border-radius: 14px;
            color:#dce8fb;
            padding: 10px 16px;
            font-weight: 700;
        }

        .stTabs [aria-selected="true"]{
            background: linear-gradient(135deg, rgba(90,178,255,.16), rgba(139,92,246,.16));
            border-color: rgba(126,164,255,.28);
        }

        .stButton > button, .stDownloadButton > button{
            width: 100%;
            border: 1px solid rgba(255,255,255,.08);
            border-radius: 16px;
            padding: 0.82rem 1rem;
            color: white;
            font-weight: 800;
            background: linear-gradient(135deg, #1196ff 0%, #6e61ff 100%);
            box-shadow: 0 12px 28px rgba(43,117,255,.26);
        }

        .stButton > button:hover, .stDownloadButton > button:hover{
            filter: brightness(1.05);
            transform: translateY(-1px);
            transition: .18s ease;
        }

        .mini-note{
            color:#90a4c0;
            font-size:.84rem;
            line-height:1.6;
        }

        .auth-fix-gap{
            margin-top: 2px;
        }

        @media (max-width: 1100px){
            .hero-title{
                font-size:2.15rem;
            }
            .brand-row{
                flex-direction:column;
                align-items:flex-start;
            }
            .brand-badges{
                justify-content:flex-start;
            }
        }
        </style>
        """,
        unsafe_allow_html=True
    )


def inject_dashboard_css():
    st.markdown(
        """
        <style>
        .kpi-grid,.b3-kpi-grid,.b4-kpi-grid,.b5-kpi-grid{
            display:grid;
            grid-template-columns: repeat(4, 1fr);
            gap:16px;
            margin-bottom:12px;
        }

        .kpi-card,.b3-kpi,.b4-kpi,.b5-kpi{
            border:1px solid rgba(255,255,255,.08);
            border-radius:22px;
            padding:18px;
            background: linear-gradient(180deg, rgba(255,255,255,.045), rgba(255,255,255,.028));
            box-shadow: 0 14px 34px rgba(0,0,0,.22);
        }

        .kpi-label,.b3-kpi-label,.b4-kpi-label,.b5-kpi-label{
            color:#9fb0c8;
            font-size:.82rem;
            margin-bottom:8px;
            font-weight:700;
        }

        .kpi-value,.b3-kpi-value,.b4-kpi-value,.b5-kpi-value{
            color:#f7fbff;
            font-size:1.75rem;
            font-weight:900;
            letter-spacing:-0.03em;
            line-height:1.05;
            margin-bottom:6px;
        }

        .kpi-sub,.b3-kpi-sub,.b4-kpi-sub,.b5-kpi-sub{
            color:#dce8fb;
            font-size:.88rem;
        }

        .exec-card,.b3-card,.b4-card,.b5-card{
            border:1px solid rgba(255,255,255,.08);
            border-radius:26px;
            padding:20px;
            background: linear-gradient(180deg, rgba(255,255,255,.045), rgba(255,255,255,.03));
            box-shadow: 0 16px 42px rgba(0,0,0,.24);
            height:100%;
        }

        .exec-title,.b3-title,.b4-title,.b5-title{
            color:#f7fbff;
            font-size:1.08rem;
            font-weight:800;
            margin-bottom:4px;
            letter-spacing:-0.02em;
        }

        .exec-sub,.b3-sub,.b4-sub,.b5-sub{
            color:#9fb0c8;
            font-size:.9rem;
            margin-bottom:14px;
        }

        .b3-empty,.b4-empty,.b5-empty{
            border:1px dashed rgba(255,255,255,.12);
            border-radius:18px;
            padding:18px;
            color:#9fb0c8;
            text-align:center;
            background: rgba(255,255,255,.02);
        }

        .priority-item,.b3-item,.fc-list-item,.mock-item,.b5-stat-item,.overview-schedule-item{
            border:1px solid rgba(255,255,255,.07);
            border-radius:18px;
            padding:14px;
            background: rgba(255,255,255,.03);
        }

        .priority-top,.b3-item-top,.mock-top,.overview-schedule-top{
            display:flex;
            justify-content:space-between;
            gap:12px;
            align-items:flex-start;
            margin-bottom:8px;
        }

        .priority-name,.b3-item-title,.fc-list-q,.mock-title,.b5-stat-name,.overview-schedule-title{
            color:#eef5ff;
            font-size:.94rem;
            font-weight:800;
            line-height:1.35;
        }

        .priority-meta,.b3-item-meta,.fc-list-meta,.mock-meta,.b5-stat-meta,.overview-schedule-meta{
            color:#9fb0c8;
            font-size:.82rem;
            line-height:1.5;
        }

        .bar-shell{
            width:100%;
            height:10px;
            border-radius:999px;
            overflow:hidden;
            background: rgba(255,255,255,.07);
        }

        .bar-fill{
            height:100%;
            border-radius:999px;
            background: linear-gradient(135deg, #13a0ff 0%, #7b61ff 100%);
        }

        .rank-col{
            border:1px solid rgba(255,255,255,.07);
            border-radius:20px;
            padding:14px;
            background: rgba(255,255,255,.03);
            height:100%;
        }

        .rank-col-title{
            color:#f6fbff;
            font-size:.95rem;
            font-weight:800;
            margin-bottom:10px;
        }

        .rank-item{
            display:flex;
            justify-content:space-between;
            gap:12px;
            padding:10px 0;
            border-bottom:1px solid rgba(255,255,255,.06);
        }

        .rank-item:last-child{
            border-bottom:none;
        }

        .rank-name{
            color:#e9f2ff;
            font-size:.9rem;
            font-weight:700;
            line-height:1.35;
        }

        .rank-aux{
            color:#99aec9;
            font-size:.78rem;
            margin-top:2px;
        }

        .rank-score{
            color:#ffffff;
            font-weight:900;
            font-size:.92rem;
            white-space:nowrap;
        }

        .mini-stat{
            border:1px solid rgba(255,255,255,.07);
            border-radius:18px;
            padding:14px;
            background: rgba(255,255,255,.03);
            margin-bottom:12px;
        }

        .mini-stat-label{
            color:#9fb0c8;
            font-size:.82rem;
            margin-bottom:6px;
        }

        .mini-stat-value{
            color:#f7fbff;
            font-weight:800;
            font-size:1.2rem;
        }

        .b3-chip,.fc-chip,.mock-badge,.overview-chip{
            border-radius:999px;
            padding:6px 10px;
            font-size:.76rem;
            font-weight:800;
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.04);
            color:#e7f0ff;
            white-space:nowrap;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


# =========================================================
# HEADER / LOGIN
# =========================================================
def render_app_header(username: Optional[str] = None, is_admin: bool = False):
    logo_html = render_logo_html(height=66, css_class="brand-logo")
    today_str = datetime.now().strftime("%d/%m/%Y")

    badges = f"""
    <div class="brand-badges">
        <div class="brand-badge">Versão {html.escape(APP_VERSION)}</div>
        <div class="brand-badge">{today_str}</div>
        <div class="brand-badge">{'Administrador' if is_admin else 'Aluno'}</div>
        {f'<div class="brand-badge">Usuário: {html.escape(username)}</div>' if username else ''}
    </div>
    """

    header_html = f"""
    <div class="brand-shell">
        <div class="brand-row">
            <div class="brand-left">
                {logo_html}
                <div>
                    <div class="brand-title">{html.escape(APP_NAME)}</div>
                    <div class="brand-subtitle">{html.escape(APP_SUBTITLE)}</div>
                </div>
            </div>
            <div>{badges}</div>
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)


def render_auth_hero():
    logo_path = get_logo_path()

    st.markdown('<div class="hero-card auth-fix-gap">', unsafe_allow_html=True)

    st.markdown(
        '<div class="hero-pill">ALTO DESEMPENHO • ORGANIZAÇÃO • CONSISTÊNCIA</div>',
        unsafe_allow_html=True
    )

    if logo_path:
        st.image(logo_path, width=88)

    st.markdown(
        '<div class="hero-title">Transforme sua rotina em uma <span class="grad">operação de aprovação</span>.</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        """
Controle metas, questões, cronograma, desempenho, revisão e evolução diária
em uma experiência visual mais elegante, sólida e profissional.
        """
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("**Gestão diária**")
        st.caption("Questões + tempo")

    with c2:
        st.markdown("**Acompanhamento**")
        st.caption("Cronograma + metas")

    with c3:
        st.markdown("**Performance**")
        st.caption("Indicadores estratégicos")

    st.caption(
        "Uma interface premium melhora a percepção de valor e a experiência de uso desde a tela inicial."
    )

    st.markdown("</div>", unsafe_allow_html=True)


def render_login_screen():
    st.markdown('<div class="top-spacer-sm"></div>', unsafe_allow_html=True)

    col_left, col_right = st.columns([1.05, 0.95], gap="large")

    with col_left:
        render_auth_hero()

    with col_right:
        st.markdown('<div class="login-card auth-fix-gap">', unsafe_allow_html=True)
        st.markdown('<div class="section-chip">Mentoria do Jhon</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-title">Entrar na plataforma</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="login-subtitle">Acesse seu painel e continue sua execução estratégica de estudos.</div>',
            unsafe_allow_html=True
        )

        tab_login, tab_register = st.tabs(["Entrar", "Criar usuário"])

        with tab_login:
            with st.form("form_login", clear_on_submit=False):
                username = st.text_input("Usuário", placeholder="Digite seu usuário")
                password = st.text_input("Senha", type="password", placeholder="Digite sua senha")
                submitted = st.form_submit_button("Acessar plataforma")

            if submitted:
                user = authenticate_user(username, password)
                if user:
                    st.session_state.logged_in = True
                    st.session_state.user_id = user["id"]
                    st.session_state.username = user["username"]
                    st.session_state.is_admin = user["is_admin"]
                    st.success("Login realizado com sucesso.")
                    safe_rerun()
                else:
                    st.error("Usuário ou senha inválidos.")

        with tab_register:
            with st.form("form_register", clear_on_submit=True):
                new_username = st.text_input("Novo usuário", placeholder="Crie um nome de usuário")
                new_password = st.text_input("Nova senha", type="password", placeholder="Crie uma senha")
                submitted_register = st.form_submit_button("Criar usuário")

            if submitted_register:
                ok, msg = create_user(new_username, new_password, is_admin=0)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

        st.markdown(
            '<div class="mini-note" style="margin-top:14px;">'
            'Dica: '
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# MENU
# =========================================================
def render_top_menu():
    menus = [
        "Visão Geral",
        "Cronograma",
        "Questões",
        "Flashcards",
        "Simulados",
        "Relatórios",
        "Administração",
    ]

    allowed = menus if st.session_state.is_admin else [m for m in menus if m != "Administração"]
    current = st.session_state.get("menu", "Visão Geral")
    if current not in allowed:
        st.session_state.menu = allowed[0]

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Navegação</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Escolha a área que deseja abrir no painel.</div>', unsafe_allow_html=True)

    cols = st.columns(len(allowed))
    for i, item in enumerate(allowed):
        with cols[i]:
            if st.button(item, key=f"menu_{item}", use_container_width=True):
                st.session_state.menu = item
                if item != "Flashcards":
                    reset_flashcard_state()
                safe_rerun()

    st.markdown("</div>", unsafe_allow_html=True)
# =========================================================
# VISÃO GERAL
# =========================================================
def get_dashboard_base_data(user_id: int):
    today = get_today_str()
    month_start, month_end = get_month_range()
    last30_start, last30_end = get_last_30_days_range()

    sessions_df = fetch_sessions_df(user_id)
    schedule_df = fetch_schedule_df(user_id)
    mocks_df = fetch_mocks_df(user_id)
    goal = get_user_goal(user_id)

    return {
        "today": today,
        "month_start": month_start,
        "month_end": month_end,
        "last30_start": last30_start,
        "last30_end": last30_end,
        "sessions_df": sessions_df,
        "schedule_df": schedule_df,
        "mocks_df": mocks_df,
        "goal": goal,
    }


def build_dashboard_metrics(user_id: int):
    data = get_dashboard_base_data(user_id)
    sessions_df = data["sessions_df"].copy()
    schedule_df = data["schedule_df"].copy()
    mocks_df = data["mocks_df"].copy()
    goal = data["goal"]

    if sessions_df.empty:
        sessions_df = pd.DataFrame(columns=[
            "session_date", "study_minutes", "questions_done",
            "correct_answers", "subject", "topic"
        ])

    if schedule_df.empty:
        schedule_df = pd.DataFrame(columns=[
            "week_no", "title", "subject", "topic",
            "completed", "planned_date", "area"
        ])

    if mocks_df.empty:
        mocks_df = pd.DataFrame(columns=["mock_date", "score_percent", "questions_count"])

    for col in ["study_minutes", "questions_done", "correct_answers"]:
        if col in sessions_df.columns:
            sessions_df[col] = pd.to_numeric(sessions_df[col], errors="coerce").fillna(0)

    if "score_percent" in mocks_df.columns:
        mocks_df["score_percent"] = pd.to_numeric(mocks_df["score_percent"], errors="coerce").fillna(0)

    today_df = sessions_df[sessions_df.get("session_date", pd.Series(dtype=str)) == data["today"]].copy()
    questions_today = int(today_df["questions_done"].sum()) if not today_df.empty else 0
    minutes_today = int(today_df["study_minutes"].sum()) if not today_df.empty else 0

    month_mocks = mocks_df[
        (mocks_df.get("mock_date", pd.Series(dtype=str)) >= data["month_start"]) &
        (mocks_df.get("mock_date", pd.Series(dtype=str)) < data["month_end"])
    ].copy()
    mocks_this_month = int(len(month_mocks))

    streak_current = 0
    streak_best = 0
    consistency_30d = 0.0

    if not sessions_df.empty and "session_date" in sessions_df.columns:
        daily_any = sessions_df.groupby("session_date", as_index=False)[["study_minutes", "questions_done"]].sum()
        daily_any["did_study"] = (daily_any["study_minutes"] > 0) | (daily_any["questions_done"] > 0)
        studied_dates = set(daily_any.loc[daily_any["did_study"], "session_date"].astype(str).tolist())

        d = date.today()
        while d.isoformat() in studied_dates:
            streak_current += 1
            d = d - timedelta(days=1)

        rng_start = date.today() - timedelta(days=29)
        days = [rng_start + timedelta(days=i) for i in range(30)]
        best = 0
        current = 0
        for d2 in days:
            if d2.isoformat() in studied_dates:
                current += 1
                best = max(best, current)
            else:
                current = 0
        streak_best = best

        daily_any_30 = sessions_df[
            (sessions_df.get("session_date", pd.Series(dtype=str)) >= data["last30_start"]) &
            (sessions_df.get("session_date", pd.Series(dtype=str)) <= data["last30_end"])
        ].copy()

        if not daily_any_30.empty:
            grouped = daily_any_30.groupby("session_date", as_index=False)[["study_minutes", "questions_done"]].sum()
            grouped["did_study"] = (grouped["study_minutes"] > 0) | (grouped["questions_done"] > 0)
            consistency_30d = round((grouped["did_study"].sum() / 30) * 100, 1)

    planned_items = int(len(schedule_df))
    completed_items = int(pd.to_numeric(schedule_df.get("completed", 0), errors="coerce").fillna(0).sum()) if not schedule_df.empty else 0
    pending_items = max(planned_items - completed_items, 0)
    weeks_planned = int(schedule_df["week_no"].nunique()) if ("week_no" in schedule_df.columns and not schedule_df.empty) else 0

    if not schedule_df.empty:
        filled_week_df = schedule_df.copy()

        title_series = filled_week_df["title"].fillna("").astype(str) if "title" in filled_week_df.columns else pd.Series([""] * len(filled_week_df))
        subject_series = filled_week_df["subject"].fillna("").astype(str) if "subject" in filled_week_df.columns else pd.Series([""] * len(filled_week_df))
        topic_series = filled_week_df["topic"].fillna("").astype(str) if "topic" in filled_week_df.columns else pd.Series([""] * len(filled_week_df))

        filled_week_df["has_content"] = (
            title_series.str.strip().ne("") |
            subject_series.str.strip().ne("") |
            topic_series.str.strip().ne("")
        )
        weeks_with_content = int(filled_week_df.loc[filled_week_df["has_content"], "week_no"].nunique())
    else:
        weeks_with_content = 0

    ranking_df = pd.DataFrame()
    if not sessions_df.empty:
        rank_base = sessions_df.copy()

        if "topic" not in rank_base.columns:
            rank_base["topic"] = ""
        if "subject" not in rank_base.columns:
            rank_base["subject"] = ""

        rank_base["topic_display"] = rank_base["topic"].fillna("").astype(str).str.strip()
        rank_base["subject_display"] = rank_base["subject"].fillna("").astype(str).str.strip()
        rank_base["topic_display"] = rank_base["topic_display"].where(rank_base["topic_display"] != "", rank_base["subject_display"])
        rank_base["topic_display"] = rank_base["topic_display"].where(rank_base["topic_display"] != "", "Sem subtópico")

        grouped_rank = rank_base.groupby("topic_display", as_index=False)[["questions_done", "correct_answers"]].sum()
        grouped_rank = grouped_rank[grouped_rank["questions_done"] > 0].copy()

        if not grouped_rank.empty:
            grouped_rank["accuracy"] = (grouped_rank["correct_answers"] / grouped_rank["questions_done"]) * 100
            grouped_rank["accuracy"] = grouped_rank["accuracy"].round(1)
            ranking_df = grouped_rank.sort_values(["accuracy", "questions_done"], ascending=[False, False]).reset_index(drop=True)

    best_topics = ranking_df.head(5).copy() if not ranking_df.empty else pd.DataFrame(columns=["topic_display", "accuracy", "questions_done"])
    worst_topics = ranking_df.sort_values(["accuracy", "questions_done"], ascending=[True, False]).head(5).copy() if not ranking_df.empty else pd.DataFrame(columns=["topic_display", "accuracy", "questions_done"])

    daily_questions_goal = to_int(goal.get("daily_questions_goal", 60), 60)
    daily_minutes_goal = to_int(goal.get("daily_minutes_goal", 180), 180)
    monthly_mock_goal = to_int(goal.get("monthly_mock_goal", 4), 4)
    phase_name = str(goal.get("phase_name", "Intermediária") or "Intermediária")

    priorities_df = pd.DataFrame([
        {
            "label": "Questões do dia",
            "meta": f"{questions_today}/{daily_questions_goal}",
            "status": min((questions_today / daily_questions_goal) * 100, 100) if daily_questions_goal > 0 else 0,
        },
        {
            "label": "Tempo do dia",
            "meta": f"{minutes_today}/{daily_minutes_goal} min",
            "status": min((minutes_today / daily_minutes_goal) * 100, 100) if daily_minutes_goal > 0 else 0,
        },
        {
            "label": "Simulados do mês",
            "meta": f"{mocks_this_month}/{monthly_mock_goal}",
            "status": min((mocks_this_month / monthly_mock_goal) * 100, 100) if monthly_mock_goal > 0 else 0,
        },
        {
            "label": "Execução do cronograma",
            "meta": f"{completed_items}/{planned_items}" if planned_items > 0 else "0/0",
            "status": min((completed_items / planned_items) * 100, 100) if planned_items > 0 else 0,
        },
    ])

    line_df = pd.DataFrame({
        "day": [(date.today() - timedelta(days=29 - i)).isoformat() for i in range(30)]
    })

    if not sessions_df.empty:
        line_base = sessions_df.groupby("session_date", as_index=False)[["questions_done", "study_minutes"]].sum()
        line_df = line_df.merge(line_base, how="left", left_on="day", right_on="session_date")
        line_df["questions_done"] = pd.to_numeric(line_df["questions_done"], errors="coerce").fillna(0)
        line_df["study_minutes"] = pd.to_numeric(line_df["study_minutes"], errors="coerce").fillna(0)
    else:
        line_df["questions_done"] = 0
        line_df["study_minutes"] = 0

    avg_mock_score = round(float(month_mocks["score_percent"].mean()), 1) if not month_mocks.empty else 0.0

    return {
        "goal": goal,
        "phase_name": phase_name,
        "questions_today": questions_today,
        "minutes_today": minutes_today,
        "mocks_this_month": mocks_this_month,
        "avg_mock_score": avg_mock_score,
        "daily_questions_goal": daily_questions_goal,
        "daily_minutes_goal": daily_minutes_goal,
        "monthly_mock_goal": monthly_mock_goal,
        "streak_current": streak_current,
        "streak_best": streak_best,
        "consistency_30d": consistency_30d,
        "planned_items": planned_items,
        "completed_items": completed_items,
        "pending_items": pending_items,
        "weeks_planned": weeks_planned,
        "weeks_with_content": weeks_with_content,
        "best_topics": best_topics,
        "worst_topics": worst_topics,
        "priorities_df": priorities_df,
        "line_df": line_df,
        "schedule_df": schedule_df,
    }


def render_kpi_cards(metrics: dict):
    cards = [
        {
            "label": "Questões do dia",
            "value": metrics["questions_today"],
            "sub": f"Meta diária: {metrics['daily_questions_goal']}",
        },
        {
            "label": "Tempo do dia",
            "value": f"{metrics['minutes_today']} min",
            "sub": f"Meta diária: {metrics['daily_minutes_goal']} min",
        },
        {
            "label": "Simulados do mês",
            "value": metrics["mocks_this_month"],
            "sub": f"Meta mensal: {metrics['monthly_mock_goal']}",
        },
        {
            "label": "Consistência 30d",
            "value": f"{metrics['consistency_30d']}%",
            "sub": f"Sequência atual: {metrics['streak_current']} dia(s)",
        },
    ]

    cols = st.columns(4, gap="large")
    for col, card in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="kpi-card">'
                    f'<div class="kpi-label">{html.escape(str(card["label"]))}</div>'
                    f'<div class="kpi-value">{html.escape(str(card["value"]))}</div>'
                    f'<div class="kpi-sub">{html.escape(str(card["sub"]))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )


def render_priorities_panel(metrics: dict):
    priorities_df = metrics["priorities_df"].copy()

    st.markdown('<div class="exec-card">', unsafe_allow_html=True)
    st.markdown('<div class="exec-title">Prioridades do dia</div>', unsafe_allow_html=True)
    st.markdown('<div class="exec-sub">Leitura rápida das metas operacionais e da execução do plano.</div>', unsafe_allow_html=True)

    if priorities_df.empty:
        st.markdown('<div class="b3-empty">Nenhuma prioridade disponível.</div>', unsafe_allow_html=True)
    else:
        for _, row in priorities_df.iterrows():
            label = str(row.get("label", "-"))
            meta = str(row.get("meta", "-"))
            status = max(0.0, min(float(row.get("status", 0)), 100.0))

            st.markdown(
                (
                    '<div class="priority-item">'
                    '<div class="priority-top">'
                    f'<div class="priority-name">{html.escape(label)}</div>'
                    f'<div class="priority-meta">{html.escape(meta)}</div>'
                    '</div>'
                    f'<div class="bar-shell"><div class="bar-fill" style="width:{status:.1f}%"></div></div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

    st.markdown("</div>", unsafe_allow_html=True)


def render_ranking_panel(metrics: dict):
    best_df = metrics["best_topics"].copy()
    worst_df = metrics["worst_topics"].copy()

    st.markdown('<div class="exec-card">', unsafe_allow_html=True)
    st.markdown('<div class="exec-title">Melhores e piores subtópicos</div>', unsafe_allow_html=True)
    st.markdown('<div class="exec-sub">A leitura usa o subtópico como prioridade visual.</div>', unsafe_allow_html=True)

    st.markdown('<div class="rank-col" style="margin-bottom:14px;">', unsafe_allow_html=True)
    st.markdown('<div class="rank-col-title">Melhores</div>', unsafe_allow_html=True)

    if best_df.empty:
        st.markdown('<div class="b3-empty">Ainda não há dados suficientes.</div>', unsafe_allow_html=True)
    else:
        for _, row in best_df.iterrows():
            name = str(row.get("topic_display", "Sem nome"))
            acc = round(float(row.get("accuracy", 0)), 1)
            qtd = int(row.get("questions_done", 0))

            st.markdown(
                (
                    '<div class="rank-item">'
                    '<div>'
                    f'<div class="rank-name">{html.escape(name)}</div>'
                    f'<div class="rank-aux">{qtd} questões respondidas</div>'
                    '</div>'
                    f'<div class="rank-score">{acc:.1f}%</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="rank-col">', unsafe_allow_html=True)
    st.markdown('<div class="rank-col-title">Piores</div>', unsafe_allow_html=True)

    if worst_df.empty:
        st.markdown('<div class="b3-empty">Ainda não há dados suficientes.</div>', unsafe_allow_html=True)
    else:
        for _, row in worst_df.iterrows():
            name = str(row.get("topic_display", "Sem nome"))
            acc = round(float(row.get("accuracy", 0)), 1)
            qtd = int(row.get("questions_done", 0))

            st.markdown(
                (
                    '<div class="rank-item">'
                    '<div>'
                    f'<div class="rank-name">{html.escape(name)}</div>'
                    f'<div class="rank-aux">{qtd} questões respondidas</div>'
                    '</div>'
                    f'<div class="rank-score">{acc:.1f}%</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_strategy_panel(metrics: dict):
    stats = [
        ("Etapa atual", str(metrics["phase_name"])),
        ("Média de simulados no mês", f'{metrics["avg_mock_score"]:.1f}%'),
        ("Itens concluídos", str(metrics["completed_items"])),
        ("Itens pendentes", str(metrics["pending_items"])),
        ("Semanas planejadas", str(metrics["weeks_planned"])),
        ("Semanas com conteúdo", str(metrics["weeks_with_content"])),
        ("Sequência atual", f'{metrics["streak_current"]} dia(s)'),
        ("Melhor sequência 30d", f'{metrics["streak_best"]} dia(s)'),
    ]

    st.markdown('<div class="exec-card">', unsafe_allow_html=True)
    st.markdown('<div class="exec-title">Resumo estratégico</div>', unsafe_allow_html=True)
    st.markdown('<div class="exec-sub">Panorama central da rotina de estudo e da execução atual.</div>', unsafe_allow_html=True)

    for label, value in stats:
        st.markdown(
            (
                '<div class="mini-stat">'
                f'<div class="mini-stat-label">{html.escape(label)}</div>'
                f'<div class="mini-stat-value">{html.escape(value)}</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

    st.markdown("</div>", unsafe_allow_html=True)


def render_line_chart_panel(metrics: dict):
    line_df = metrics["line_df"].copy()

    st.markdown('<div class="exec-card">', unsafe_allow_html=True)
    st.markdown('<div class="exec-title">Ritmo dos últimos 30 dias</div>', unsafe_allow_html=True)
    st.markdown('<div class="exec-sub">Leitura simples da produção diária de questões.</div>', unsafe_allow_html=True)

    if line_df.empty or line_df["questions_done"].sum() == 0:
        st.info("Ainda não há dados suficientes para mostrar o ritmo dos últimos 30 dias.")
    else:
        fig, ax = plt.subplots(figsize=(10, 3.6))
        ax.plot(range(len(line_df)), line_df["questions_done"].tolist(), linewidth=2.2)
        ax.set_xticks(range(0, len(line_df), 4))
        ax.set_xticklabels([d[5:] for d in line_df["day"].tolist()[::4]], rotation=0)
        ax.set_ylabel("Questões")
        ax.set_xlabel("Data")
        ax.grid(alpha=0.18)
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

    st.markdown("</div>", unsafe_allow_html=True)


def render_dashboard_schedule_actions(metrics: dict):
    schedule_df = metrics["schedule_df"].copy()

    st.markdown('<div class="exec-card">', unsafe_allow_html=True)
    st.markdown('<div class="exec-title">Cronograma em execução</div>', unsafe_allow_html=True)
    st.markdown('<div class="exec-sub">Marque tarefas como concluídas diretamente pela Visão Geral.</div>', unsafe_allow_html=True)

    if schedule_df.empty:
        st.markdown('<div class="b3-empty">Nenhum item cadastrado no cronograma.</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    today_iso = date.today().isoformat()

    pending_df = schedule_df[schedule_df["completed"] == 0].copy()
    if pending_df.empty:
        st.markdown('<div class="b3-empty">Todos os itens do cronograma estão concluídos.</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    pending_df["priority_order"] = 3
    pending_df.loc[(pending_df["planned_date"] != "") & (pending_df["planned_date"] < today_iso), "priority_order"] = 1
    pending_df.loc[pending_df["planned_date"] == today_iso, "priority_order"] = 2

    pending_df = pending_df.sort_values(
        ["priority_order", "planned_date", "week_no", "id"],
        ascending=[True, True, True, True]
    ).head(6).copy()

    for _, row in pending_df.iterrows():
        item_id = int(row["id"])
        title = normalize_text(row.get("title", "Sem título"))
        area = normalize_text(row.get("area", ""))
        subject = normalize_text(row.get("subject", ""))
        topic = normalize_text(row.get("topic", ""))
        planned_date = normalize_text(row.get("planned_date", ""))
        week_no = to_int(row.get("week_no", 0), 0)

        chip_label = "Pendente"
        if planned_date:
            if planned_date < today_iso:
                chip_label = "Atrasado"
            elif planned_date == today_iso:
                chip_label = "Hoje"
            else:
                chip_label = "Próximo"

        meta_parts = []
        if week_no:
            meta_parts.append(f"Semana {week_no}")
        if area:
            meta_parts.append(area)
        if subject:
            meta_parts.append(subject)
        if topic:
            meta_parts.append(topic)
        if planned_date:
            meta_parts.append(planned_date)

        st.markdown(
            (
                '<div class="overview-schedule-item">'
                '<div class="overview-schedule-top">'
                '<div>'
                f'<div class="overview-schedule-title">{html.escape(title)}</div>'
                f'<div class="overview-schedule-meta">{html.escape(" • ".join(meta_parts) if meta_parts else "-")}</div>'
                '</div>'
                f'<div class="overview-chip">{html.escape(chip_label)}</div>'
                '</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

        if st.button("Marcar como concluído", key=f"overview_complete_{item_id}", use_container_width=True):
            ok = toggle_schedule_item(item_id, 1)
            if ok:
                st.success("Item marcado como concluído.")
                safe_rerun()

    if st.button("Abrir cronograma completo", key="overview_go_schedule", use_container_width=True):
        st.session_state.menu = "Cronograma"
        safe_rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def render_visao_geral():
    metrics = build_dashboard_metrics(st.session_state.user_id)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Visão Geral Executiva</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Seu cockpit premium com foco em rotina, desempenho e execução do cronograma.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_kpi_cards(metrics)

    col1, col2 = st.columns([1.1, 0.9], gap="large")
    with col1:
        render_priorities_panel(metrics)
    with col2:
        render_strategy_panel(metrics)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    col3, col4 = st.columns([1, 1], gap="large")
    with col3:
        render_ranking_panel(metrics)
    with col4:
        render_line_chart_panel(metrics)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_dashboard_schedule_actions(metrics)
# =========================================================
# CRONOGRAMA
# =========================================================
def fetch_schedule_df(user_id: int):
    df = fetch_dataframe(
        """
        SELECT *
        FROM schedule_items
        WHERE user_id = ?
        ORDER BY
            CASE WHEN week_no IS NULL THEN 999999 ELSE week_no END ASC,
            CASE WHEN planned_date IS NULL OR planned_date = '' THEN '9999-12-31' ELSE planned_date END ASC,
            id ASC
        """,
        (user_id,)
    )

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "week_no", "area", "subject", "topic", "item_type",
            "title", "planned_date", "completed", "completed_at", "created_at"
        ])

    for col in ["week_no", "completed"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["area", "subject", "topic", "item_type", "title", "planned_date"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    return df


def add_schedule_item(user_id: int, week_no: int, area: str, subject: str, topic: str,
                      item_type: str, title: str, planned_date):
    title = normalize_text(title)
    if not title:
        return False, "Digite o nome da tarefa."

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO schedule_items (
                user_id, week_no, area, subject, topic, item_type,
                title, planned_date, completed, completed_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)
            """,
            (
                user_id,
                to_int(week_no, 0),
                normalize_text(area),
                normalize_text(subject),
                normalize_text(topic),
                normalize_text(item_type),
                title,
                planned_date.isoformat() if planned_date else None,
                datetime.now().isoformat()
            )
        )
        conn.commit()
        return True, "Item adicionado ao cronograma."
    except Exception as e:
        return False, f"Erro ao adicionar item: {e}"
    finally:
        conn.close()


def import_schedule_from_csv(user_id: int):
    csv_path = get_schedule_csv_path()
    if not csv_path:
        return False, "CSV do cronograma não encontrado."

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        return False, f"Erro ao ler CSV: {e}"

    lower_map = {str(c).strip().lower(): c for c in df.columns}
    required_cols = {"semana", "nome_tarefa", "grande_area"}
    if not required_cols.issubset(set(lower_map.keys())):
        return False, "O CSV precisa ter colunas semana, nome_tarefa e grande_area."

    week_col = lower_map["semana"]
    title_col = lower_map["nome_tarefa"]
    area_col = lower_map["grande_area"]
    task_num_col = lower_map.get("tarefa_num")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM schedule_items WHERE user_id = ?", (user_id,))
        existing = cur.fetchone()[0]
        if existing > 0:
            return False, "Já existem itens no cronograma deste usuário. Exclua ou use outro usuário para importar."

        for _, row in df.iterrows():
            week_no = to_int(row.get(week_col, 0), 0)
            title = normalize_text(row.get(title_col, ""))
            area = normalize_text(row.get(area_col, ""))
            task_num = normalize_text(row.get(task_num_col, "")) if task_num_col else ""

            if not title:
                continue

            subject = area
            topic = title
            item_type = "Teoria"
            final_title = f"{task_num}. {title}" if task_num else title

            cur.execute(
                """
                INSERT INTO schedule_items (
                    user_id, week_no, area, subject, topic, item_type,
                    title, planned_date, completed, completed_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)
                """,
                (
                    user_id,
                    week_no,
                    area,
                    subject,
                    topic,
                    item_type,
                    final_title,
                    None,
                    datetime.now().isoformat()
                )
            )

        conn.commit()
        return True, "Cronograma importado com sucesso do CSV."
    except Exception as e:
        return False, f"Erro ao importar cronograma: {e}"
    finally:
        conn.close()


def toggle_schedule_item(item_id: int, completed: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        now_iso = datetime.now().isoformat() if int(completed) == 1 else None
        cur.execute(
            """
            UPDATE schedule_items
            SET completed = ?, completed_at = ?
            WHERE id = ?
            """,
            (int(completed), now_iso, item_id)
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def delete_schedule_item(item_id: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM schedule_items WHERE id = ?", (item_id,))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def build_schedule_summary(schedule_df: pd.DataFrame):
    if schedule_df.empty:
        return {
            "total": 0,
            "done": 0,
            "pending": 0,
            "weeks": 0,
            "today_count": 0,
            "overdue_count": 0,
            "upcoming_count": 0
        }

    today_iso = date.today().isoformat()
    total = int(len(schedule_df))
    done = int(schedule_df["completed"].sum()) if "completed" in schedule_df.columns else 0
    pending = max(total - done, 0)
    weeks = int(schedule_df["week_no"].nunique()) if "week_no" in schedule_df.columns else 0

    date_mask = schedule_df["planned_date"].fillna("").astype(str)
    completed_mask = schedule_df["completed"].fillna(0).astype(int)

    today_count = int(((date_mask == today_iso) & (completed_mask == 0)).sum())
    overdue_count = int(((date_mask != "") & (date_mask < today_iso) & (completed_mask == 0)).sum())
    upcoming_count = int(((date_mask > today_iso) & (completed_mask == 0)).sum())

    return {
        "total": total,
        "done": done,
        "pending": pending,
        "weeks": weeks,
        "today_count": today_count,
        "overdue_count": overdue_count,
        "upcoming_count": upcoming_count,
    }


def render_schedule_kpis(summary: dict):
    cards = [
        ("Itens planejados", summary["total"], f"Semanas: {summary['weeks']}"),
        ("Concluídos", summary["done"], f"Pendentes: {summary['pending']}"),
        ("Para hoje", summary["today_count"], f"Atrasados: {summary['overdue_count']}"),
        ("Próximos", summary["upcoming_count"], "Planejamento futuro"),
    ]

    cols = st.columns(4, gap="large")
    for col, (label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="b3-kpi">'
                    f'<div class="b3-kpi-label">{html.escape(str(label))}</div>'
                    f'<div class="b3-kpi-value">{html.escape(str(value))}</div>'
                    f'<div class="b3-kpi-sub">{html.escape(str(sub))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )


def render_schedule_preview_card(title: str, items_df: pd.DataFrame, empty_text: str):
    st.markdown('<div class="b3-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="b3-title">{html.escape(title)}</div>', unsafe_allow_html=True)
    st.markdown('<div class="b3-sub">Leitura rápida para orientar sua execução.</div>', unsafe_allow_html=True)

    if items_df.empty:
        st.markdown(f'<div class="b3-empty">{html.escape(empty_text)}</div>', unsafe_allow_html=True)
    else:
        for _, row in items_df.iterrows():
            item_title = normalize_text(row.get("title", "Sem título"))
            week_no = to_int(row.get("week_no", 0), 0)
            area = normalize_text(row.get("area", ""))
            planned_date = normalize_text(row.get("planned_date", ""))

            detail_parts = []
            if week_no:
                detail_parts.append(f"Semana {week_no}")
            if area:
                detail_parts.append(area)
            if planned_date:
                detail_parts.append(planned_date)

            st.markdown(
                (
                    '<div class="b3-item">'
                    f'<div class="b3-item-title">{html.escape(item_title)}</div>'
                    f'<div class="b3-item-meta">{html.escape(" • ".join(detail_parts) if detail_parts else "-")}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

    st.markdown("</div>", unsafe_allow_html=True)


def render_schedule_manager():
    schedule_df = fetch_schedule_df(st.session_state.user_id)
    summary = build_schedule_summary(schedule_df)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Cronograma Estratégico</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Cadastre, acompanhe e finalize tarefas com uma leitura mais executiva e organizada.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_schedule_kpis(summary)

    today_iso = date.today().isoformat()
    due_today_df = schedule_df[
        (schedule_df["planned_date"] == today_iso) & (schedule_df["completed"] == 0)
    ].head(5).copy() if not schedule_df.empty else pd.DataFrame()

    overdue_df = schedule_df[
        (schedule_df["planned_date"] != "") &
        (schedule_df["planned_date"] < today_iso) &
        (schedule_df["completed"] == 0)
    ].head(5).copy() if not schedule_df.empty else pd.DataFrame()

    col_a, col_b = st.columns([1, 1], gap="large")
    with col_a:
        render_schedule_preview_card("Prioridades de hoje", due_today_df, "Nenhuma tarefa pendente para hoje.")
    with col_b:
        render_schedule_preview_card("Itens atrasados", overdue_df, "Nenhum item atrasado no cronograma.")

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="b3-card">', unsafe_allow_html=True)
        st.markdown('<div class="b3-title">Adicionar item</div>', unsafe_allow_html=True)
        st.markdown('<div class="b3-sub">Preencha os campos para registrar uma nova tarefa no plano.</div>', unsafe_allow_html=True)

        with st.form("form_schedule_add", clear_on_submit=True):
            week_no = st.number_input("Semana", min_value=0, step=1, value=0)
            area = st.selectbox("Grande área", GREAT_AREAS, key="schedule_area")
            subject = st.text_input("Matéria", placeholder="Ex.: Cardiologia")
            topic = st.text_input("Subtópico", placeholder="Ex.: ICC")
            item_type = st.selectbox("Tipo", ["Teoria", "Questões", "Revisão", "Simulado", "Flashcards", "Outro"])
            title = st.text_input("Tarefa", placeholder="Ex.: Resolver 40 questões de ICC")
            planned_date = st.date_input("Data planejada", value=date.today())
            submitted = st.form_submit_button("Adicionar ao cronograma")

        if submitted:
            ok, msg = add_schedule_item(
                st.session_state.user_id, week_no, area, subject, topic, item_type, title, planned_date
            )
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        csv_exists = get_schedule_csv_path() is not None
        if st.button("Importar CSV como cronograma executável", use_container_width=True, disabled=not csv_exists):
            ok, msg = import_schedule_from_csv(st.session_state.user_id)
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        if not csv_exists:
            st.info("CSV do cronograma não encontrado no ambiente.")

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="b3-card">', unsafe_allow_html=True)
        st.markdown('<div class="b3-title">Gestão rápida do cronograma</div>', unsafe_allow_html=True)
        st.markdown('<div class="b3-sub">Marque como concluído ou exclua itens sem sair do painel.</div>', unsafe_allow_html=True)

        if schedule_df.empty:
            st.markdown('<div class="b3-empty">Nenhum item cadastrado no cronograma.</div>', unsafe_allow_html=True)
        else:
            filter_status = st.selectbox("Filtrar status", ["Todos", "Pendentes", "Concluídos"], key="schedule_filter_status")
            filter_week = st.text_input("Filtrar por semana", placeholder="Ex.: 1", key="schedule_filter_week")
            filter_area = st.selectbox("Filtrar grande área", ["Todas"] + GREAT_AREAS, key="schedule_filter_area")

            managed_df = schedule_df.copy()

            if filter_status == "Pendentes":
                managed_df = managed_df[managed_df["completed"] == 0]
            elif filter_status == "Concluídos":
                managed_df = managed_df[managed_df["completed"] == 1]

            if normalize_text(filter_week):
                try:
                    week_int = int(filter_week)
                    managed_df = managed_df[managed_df["week_no"] == week_int]
                except Exception:
                    pass

            if filter_area != "Todas":
                managed_df = managed_df[managed_df["area"] == filter_area]

            managed_df = managed_df.head(25).copy()

            if managed_df.empty:
                st.markdown('<div class="b3-empty">Nenhum item encontrado com esse filtro.</div>', unsafe_allow_html=True)
            else:
                for _, row in managed_df.iterrows():
                    item_id = int(row["id"])
                    title = normalize_text(row.get("title", "Sem título"))
                    area = normalize_text(row.get("area", ""))
                    subject = normalize_text(row.get("subject", ""))
                    topic = normalize_text(row.get("topic", ""))
                    week_no = to_int(row.get("week_no", 0), 0)
                    planned_date = normalize_text(row.get("planned_date", ""))
                    done = int(row.get("completed", 0)) == 1

                    meta_parts = []
                    if week_no:
                        meta_parts.append(f"Semana {week_no}")
                    if area:
                        meta_parts.append(area)
                    if subject:
                        meta_parts.append(subject)
                    if topic:
                        meta_parts.append(topic)
                    if planned_date:
                        meta_parts.append(planned_date)

                    st.markdown(
                        (
                            '<div class="b3-item">'
                            '<div class="b3-item-top">'
                            '<div>'
                            f'<div class="b3-item-title">{html.escape(title)}</div>'
                            f'<div class="b3-item-meta">{html.escape(" • ".join(meta_parts) if meta_parts else "-")}</div>'
                            '</div>'
                            f'<div class="b3-chip">{"Concluído" if done else "Pendente"}</div>'
                            '</div>'
                            '</div>'
                        ),
                        unsafe_allow_html=True
                    )

                    c1, c2 = st.columns([1, 1])
                    with c1:
                        btn_label = "Marcar pendente" if done else "Marcar concluído"
                        if st.button(btn_label, key=f"toggle_schedule_{item_id}", use_container_width=True):
                            if toggle_schedule_item(item_id, 0 if done else 1):
                                safe_rerun()
                    with c2:
                        if st.button("Excluir item", key=f"delete_schedule_{item_id}", use_container_width=True):
                            if delete_schedule_item(item_id):
                                safe_rerun()

        st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# QUESTÕES
# =========================================================
def add_study_session(user_id: int, session_date, study_minutes: int, questions_done: int,
                      correct_answers: int, subject: str, topic: str, notes: str, grande_area: str):
    if not session_date:
        return False, "Escolha a data."
    if to_int(study_minutes, 0) < 0:
        return False, "Tempo inválido."
    if to_int(questions_done, 0) < 0:
        return False, "Número de questões inválido."
    if to_int(correct_answers, 0) < 0:
        return False, "Número de acertos inválido."
    if to_int(correct_answers, 0) > to_int(questions_done, 0):
        return False, "Acertos não podem ser maiores que questões."

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO study_sessions (
                user_id, session_date, study_minutes, questions_done,
                correct_answers, subject, topic, notes, created_at, grande_area
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                session_date.isoformat() if hasattr(session_date, "isoformat") else str(session_date),
                to_int(study_minutes, 0),
                to_int(questions_done, 0),
                to_int(correct_answers, 0),
                normalize_text(subject),
                normalize_text(topic),
                normalize_text(notes),
                datetime.now().isoformat(),
                normalize_text(grande_area),
            )
        )
        conn.commit()
        return True, "Sessão registrada com sucesso."
    except Exception as e:
        return False, f"Erro ao registrar sessão: {e}"
    finally:
        conn.close()


def fetch_sessions_df(user_id: int):
    df = fetch_dataframe(
        """
        SELECT *
        FROM study_sessions
        WHERE user_id = ?
        ORDER BY session_date DESC, id DESC
        """,
        (user_id,)
    )

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "session_date", "study_minutes", "questions_done",
            "correct_answers", "subject", "topic", "notes", "created_at", "grande_area"
        ])

    for col in ["study_minutes", "questions_done", "correct_answers"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["subject", "topic", "notes", "session_date"]:
        df[col] = df[col].fillna("").astype(str)

    if "grande_area" not in df.columns:
        df["grande_area"] = ""
    else:
        df["grande_area"] = df["grande_area"].fillna("").astype(str)

    df["accuracy"] = 0.0
    mask = df["questions_done"] > 0
    df.loc[mask, "accuracy"] = (
        (df.loc[mask, "correct_answers"] / df.loc[mask, "questions_done"]) * 100
    ).round(1)

    return df


def delete_study_session(session_id: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM study_sessions WHERE id = ?", (session_id,))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def build_questions_summary(sessions_df: pd.DataFrame):
    if sessions_df.empty:
        return {
            "total_questions": 0,
            "total_correct": 0,
            "overall_accuracy": 0.0,
            "total_minutes": 0,
            "today_questions": 0,
            "today_minutes": 0,
        }

    total_questions = int(sessions_df["questions_done"].sum())
    total_correct = int(sessions_df["correct_answers"].sum())
    total_minutes = int(sessions_df["study_minutes"].sum())
    overall_accuracy = round((total_correct / total_questions) * 100, 1) if total_questions > 0 else 0.0

    today_iso = date.today().isoformat()
    today_df = sessions_df[sessions_df["session_date"] == today_iso].copy()
    today_questions = int(today_df["questions_done"].sum()) if not today_df.empty else 0
    today_minutes = int(today_df["study_minutes"].sum()) if not today_df.empty else 0

    return {
        "total_questions": total_questions,
        "total_correct": total_correct,
        "overall_accuracy": overall_accuracy,
        "total_minutes": total_minutes,
        "today_questions": today_questions,
        "today_minutes": today_minutes,
    }


def render_questions_kpis(summary: dict):
    cards = [
        ("Questões totais", summary["total_questions"], f"Acertos: {summary['total_correct']}"),
        ("Acurácia geral", f"{summary['overall_accuracy']}%", "Base acumulada"),
        ("Tempo total", f"{summary['total_minutes']} min", "Estudo registrado"),
        ("Hoje", summary["today_questions"], f"{summary['today_minutes']} min hoje"),
    ]

    cols = st.columns(4, gap="large")
    for col, (label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="b3-kpi">'
                    f'<div class="b3-kpi-label">{html.escape(str(label))}</div>'
                    f'<div class="b3-kpi-value">{html.escape(str(value))}</div>'
                    f'<div class="b3-kpi-sub">{html.escape(str(sub))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )


def render_questions_manager():
    sessions_df = fetch_sessions_df(st.session_state.user_id)
    summary = build_questions_summary(sessions_df)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Gestão de Questões</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Registre sessões, acompanhe a acurácia e mantenha o histórico limpo e organizado.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_questions_kpis(summary)

    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="b3-card">', unsafe_allow_html=True)
        st.markdown('<div class="b3-title">Registrar sessão</div>', unsafe_allow_html=True)
        st.markdown('<div class="b3-sub">Insira aqui uma sessão de questões com tempo, matéria e subtópico.</div>', unsafe_allow_html=True)

        with st.form("form_question_session", clear_on_submit=True):
            session_date = st.date_input("Data", value=date.today(), key="question_date")
            grande_area = st.selectbox("Grande área", GREAT_AREAS, key="question_area")
            study_minutes = st.number_input("Tempo estudado (min)", min_value=0, step=5, value=60)
            questions_done = st.number_input("Questões realizadas", min_value=0, step=1, value=20)
            correct_answers = st.number_input("Acertos", min_value=0, step=1, value=15)
            subject = st.text_input("Matéria", placeholder="Ex.: Pneumologia")
            topic = st.text_input("Subtópico", placeholder="Ex.: Asma")
            notes = st.text_area("Observações", placeholder="Ex.: errei fisiopatologia e tratamento inicial")
            submitted = st.form_submit_button("Registrar sessão")

        if submitted:
            ok, msg = add_study_session(
                st.session_state.user_id,
                session_date,
                study_minutes,
                questions_done,
                correct_answers,
                subject,
                topic,
                notes,
                grande_area
            )
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="b3-card">', unsafe_allow_html=True)
        st.markdown('<div class="b3-title">Histórico recente</div>', unsafe_allow_html=True)
        st.markdown('<div class="b3-sub">Visual executivo das últimas sessões com opção de exclusão.</div>', unsafe_allow_html=True)

        if sessions_df.empty:
            st.markdown('<div class="b3-empty">Nenhuma sessão registrada ainda.</div>', unsafe_allow_html=True)
        else:
            search_term = st.text_input(
                "Pesquisar por matéria ou subtópico",
                placeholder="Ex.: cardio, asma, antibiótico",
                key="questions_search_term"
            )
            filtered_df = sessions_df.copy()

            if normalize_text(search_term):
                needle = normalize_text(search_term).lower()
                filtered_df = filtered_df[
                    filtered_df["subject"].str.lower().str.contains(needle, na=False) |
                    filtered_df["topic"].str.lower().str.contains(needle, na=False) |
                    filtered_df["grande_area"].str.lower().str.contains(needle, na=False)
                ]

            filtered_df = filtered_df.head(20).copy()

            if filtered_df.empty:
                st.markdown('<div class="b3-empty">Nenhuma sessão encontrada com esse filtro.</div>', unsafe_allow_html=True)
            else:
                for _, row in filtered_df.iterrows():
                    session_id = int(row["id"])
                    session_date = normalize_text(row.get("session_date", ""))
                    grande_area = normalize_text(row.get("grande_area", ""))
                    subject = normalize_text(row.get("subject", ""))
                    topic = normalize_text(row.get("topic", ""))
                    questions_done = to_int(row.get("questions_done", 0), 0)
                    correct_answers = to_int(row.get("correct_answers", 0), 0)
                    study_minutes = to_int(row.get("study_minutes", 0), 0)
                    accuracy = float(row.get("accuracy", 0))
                    notes = normalize_text(row.get("notes", ""))

                    meta_parts = []
                    if grande_area:
                        meta_parts.append(grande_area)
                    if subject:
                        meta_parts.append(subject)
                    if topic:
                        meta_parts.append(topic)
                    meta_parts.append(f"{questions_done} questões")
                    meta_parts.append(f"{correct_answers} acertos")
                    meta_parts.append(f"{accuracy:.1f}%")
                    meta_parts.append(f"{study_minutes} min")

                    note_html = (
                        f'<div class="b3-item-meta" style="margin-top:6px;">{html.escape(notes)}</div>'
                        if notes else ""
                    )

                    st.markdown(
                        (
                            '<div class="b3-item">'
                            '<div class="b3-item-top">'
                            '<div>'
                            f'<div class="b3-item-title">{html.escape(session_date)}</div>'
                            f'<div class="b3-item-meta">{html.escape(" • ".join(meta_parts))}</div>'
                            f'{note_html}'
                            '</div>'
                            '<div class="b3-chip">Sessão</div>'
                            '</div>'
                            '</div>'
                        ),
                        unsafe_allow_html=True
                    )

                    if st.button("Excluir sessão", key=f"delete_session_{session_id}", use_container_width=True):
                        if delete_study_session(session_id):
                            safe_rerun()

        st.markdown("</div>", unsafe_allow_html=True)
# =========================================================
# FLASHCARDS
# =========================================================
def initialize_new_flashcard_defaults(card_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE flashcards
        SET due_date = COALESCE(due_date, ?),
            ease_factor = COALESCE(ease_factor, 2.5),
            interval_days = COALESCE(interval_days, 0),
            review_count = COALESCE(review_count, 0),
            lapse_count = COALESCE(lapse_count, 0),
            card_state = COALESCE(card_state, 'new')
        WHERE id = ?
        """,
        (date.today().isoformat(), card_id)
    )
    conn.commit()
    conn.close()


def review_flashcard(card_id: int, rating: str):
    rating = normalize_text(rating).lower()
    valid = {"again", "hard", "good", "easy"}
    if rating not in valid:
        return False, "Avaliação inválida."

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM flashcards WHERE id = ?", (card_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, "Flashcard não encontrado."

    review_count = to_int(row["review_count"], 0)
    lapse_count = to_int(row["lapse_count"], 0)
    ease_factor = to_float(row["ease_factor"], 2.5)
    interval_days = to_int(row["interval_days"], 0)

    if rating == "again":
        lapse_count += 1
        review_count += 1
        interval_days = 1
        ease_factor = max(1.3, ease_factor - 0.2)
        state = "learning"
    elif rating == "hard":
        review_count += 1
        interval_days = max(2, int(round(max(1, interval_days) * 1.2)))
        ease_factor = max(1.3, ease_factor - 0.15)
        state = "review"
    elif rating == "good":
        review_count += 1
        if interval_days <= 0:
            interval_days = 3
        elif interval_days == 1:
            interval_days = 6
        else:
            interval_days = int(round(interval_days * ease_factor))
        ease_factor = min(3.2, ease_factor + 0.05)
        state = "review"
    else:
        review_count += 1
        if interval_days <= 0:
            interval_days = 5
        elif interval_days == 1:
            interval_days = 8
        else:
            interval_days = int(round(interval_days * (ease_factor + 0.25)))
        ease_factor = min(3.4, ease_factor + 0.1)
        state = "review"

    due_date = today_plus_days(interval_days)

    cur.execute(
        """
        UPDATE flashcards
        SET due_date = ?,
            last_reviewed = ?,
            review_count = ?,
            lapse_count = ?,
            ease_factor = ?,
            interval_days = ?,
            card_state = ?
        WHERE id = ?
        """,
        (
            due_date,
            datetime.now().isoformat(),
            review_count,
            lapse_count,
            ease_factor,
            interval_days,
            state,
            card_id,
        )
    )
    conn.commit()
    conn.close()
    return True, "Revisão registrada."


def fetch_flashcards_df(user_id: int):
    df = fetch_dataframe(
        """
        SELECT *
        FROM flashcards
        WHERE user_id = ?
        ORDER BY
            CASE
                WHEN due_date IS NULL OR due_date = '' THEN '9999-12-31'
                ELSE due_date
            END ASC,
            id ASC
        """,
        (user_id,)
    )

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "deck", "subject", "topic", "question", "answer", "note",
            "created_at", "due_date", "last_reviewed", "review_count",
            "lapse_count", "ease_factor", "interval_days", "card_state"
        ])

    for col in ["deck", "subject", "topic", "question", "answer", "note", "card_state"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    for col in ["review_count", "lapse_count", "interval_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "ease_factor" in df.columns:
        df["ease_factor"] = pd.to_numeric(df["ease_factor"], errors="coerce").fillna(2.5)

    if "due_date" not in df.columns:
        df["due_date"] = date.today().isoformat()
    else:
        df["due_date"] = df["due_date"].fillna(date.today().isoformat()).astype(str)

    return df


def add_flashcard(user_id: int, deck: str, subject: str, topic: str, question: str, answer: str, note: str):
    question = normalize_text(question)
    answer = normalize_text(answer)

    if not question:
        return False, "Digite a frente/pergunta do flashcard."
    if not answer:
        return False, "Digite a resposta do flashcard."

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO flashcards (
                user_id, deck, subject, topic, question, answer, note, created_at,
                due_date, last_reviewed, review_count, lapse_count, ease_factor, interval_days, card_state
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                normalize_text(deck),
                normalize_text(subject),
                normalize_text(topic),
                question,
                answer,
                normalize_text(note),
                datetime.now().isoformat(),
                date.today().isoformat(),
                None,
                0,
                0,
                2.5,
                0,
                "new",
            )
        )
        conn.commit()
        card_id = cur.lastrowid
        initialize_new_flashcard_defaults(card_id)
        return True, "Flashcard adicionado com sucesso."
    except Exception as e:
        return False, f"Erro ao adicionar flashcard: {e}"
    finally:
        conn.close()


def delete_flashcard(card_id: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM flashcards WHERE id = ?", (card_id,))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def import_flashcards_csv(user_id: int, uploaded_file, deck_name: str, subject_name: str, topic_name: str):
    if uploaded_file is None:
        return False, "Envie um arquivo CSV."

    try:
        df = pd.read_csv(uploaded_file)
    except Exception:
        try:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, sep=";")
        except Exception as e:
            return False, f"Não foi possível ler o CSV: {e}"

    if df.empty:
        return False, "O CSV está vazio."

    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    q_col = None
    a_col = None
    n_col = None

    for candidate in ["question", "pergunta", "frente", "front"]:
        if candidate in cols_lower:
            q_col = cols_lower[candidate]
            break

    for candidate in ["answer", "resposta", "verso", "back"]:
        if candidate in cols_lower:
            a_col = cols_lower[candidate]
            break

    for candidate in ["note", "notes", "explicacao", "explicação", "comentario", "comentário"]:
        if candidate in cols_lower:
            n_col = cols_lower[candidate]
            break

    if q_col is None or a_col is None:
        return False, "O CSV precisa ter colunas de pergunta/frente e resposta/verso."

    insert_count = 0
    conn = get_conn()
    cur = conn.cursor()
    try:
        for _, row in df.iterrows():
            question = normalize_text(row.get(q_col, ""))
            answer = normalize_text(row.get(a_col, ""))
            note = normalize_text(row.get(n_col, "")) if n_col else ""

            if not question or not answer:
                continue

            cur.execute(
                """
                INSERT INTO flashcards (
                    user_id, deck, subject, topic, question, answer, note, created_at,
                    due_date, last_reviewed, review_count, lapse_count, ease_factor, interval_days, card_state
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    normalize_text(deck_name),
                    normalize_text(subject_name),
                    normalize_text(topic_name),
                    question,
                    answer,
                    note,
                    datetime.now().isoformat(),
                    date.today().isoformat(),
                    None,
                    0,
                    0,
                    2.5,
                    0,
                    "new",
                )
            )
            insert_count += 1

        conn.commit()
    except Exception as e:
        return False, f"Erro ao importar CSV: {e}"
    finally:
        conn.close()

    if insert_count == 0:
        return False, "Nenhum flashcard válido foi importado."
    return True, f"{insert_count} flashcards importados com sucesso."


def build_flashcard_filters(df: pd.DataFrame):
    decks = sorted([x for x in df["deck"].dropna().astype(str).unique().tolist() if normalize_text(x)]) if not df.empty else []
    subjects = sorted([x for x in df["subject"].dropna().astype(str).unique().tolist() if normalize_text(x)]) if not df.empty else []
    topics = sorted([x for x in df["topic"].dropna().astype(str).unique().tolist() if normalize_text(x)]) if not df.empty else []
    return decks, subjects, topics


def filter_flashcards_df(df: pd.DataFrame, deck_filter: str, subject_filter: str, topic_filter: str, search_term: str, due_only: bool = False):
    if df.empty:
        return df.copy()

    out = df.copy()

    if deck_filter != "Todos":
        out = out[out["deck"] == deck_filter]
    if subject_filter != "Todos":
        out = out[out["subject"] == subject_filter]
    if topic_filter != "Todos":
        out = out[out["topic"] == topic_filter]

    if due_only:
        today_iso = date.today().isoformat()
        out = out[out["due_date"].fillna(today_iso).astype(str) <= today_iso]

    needle = normalize_text(search_term).lower()
    if needle:
        out = out[
            out["question"].str.lower().str.contains(needle, na=False) |
            out["answer"].str.lower().str.contains(needle, na=False) |
            out["note"].str.lower().str.contains(needle, na=False) |
            out["subject"].str.lower().str.contains(needle, na=False) |
            out["topic"].str.lower().str.contains(needle, na=False) |
            out["deck"].str.lower().str.contains(needle, na=False)
        ]

    return out.reset_index(drop=True)


def render_flashcard_kpis(df: pd.DataFrame):
    total_cards = int(len(df)) if not df.empty else 0
    due_today = int((df["due_date"].fillna(date.today().isoformat()).astype(str) <= date.today().isoformat()).sum()) if not df.empty else 0
    total_subjects = int(df["subject"].astype(str).replace("", pd.NA).dropna().nunique()) if not df.empty else 0
    total_topics = int(df["topic"].astype(str).replace("", pd.NA).dropna().nunique()) if not df.empty else 0

    cards = [
        ("Flashcards", total_cards, "Base cadastrada"),
        ("Revisões para hoje", due_today, "Fila do dia"),
        ("Matérias", total_subjects, "Cobertura"),
        ("Subtópicos", total_topics, "Granularidade"),
    ]

    cols = st.columns(4, gap="large")
    for col, (label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="b4-kpi">'
                    f'<div class="b4-kpi-label">{html.escape(str(label))}</div>'
                    f'<div class="b4-kpi-value">{html.escape(str(value))}</div>'
                    f'<div class="b4-kpi-sub">{html.escape(str(sub))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )


def render_flashcard_player(filtered_df: pd.DataFrame):
    if filtered_df.empty:
        st.markdown('<div class="b4-empty">Nenhum flashcard encontrado com esse filtro.</div>', unsafe_allow_html=True)
        return

    total = len(filtered_df)
    current_index = st.session_state.get("flashcard_index", 0)
    current_index = max(0, min(current_index, total - 1))
    st.session_state.flashcard_index = current_index

    row = filtered_df.iloc[current_index]
    card_id = int(row["id"])
    question = normalize_text(row.get("question", ""))
    answer = normalize_text(row.get("answer", ""))
    note = normalize_text(row.get("note", ""))
    deck = normalize_text(row.get("deck", ""))
    subject = normalize_text(row.get("subject", ""))
    topic = normalize_text(row.get("topic", ""))
    due_date = normalize_text(row.get("due_date", ""))
    interval_days = to_int(row.get("interval_days", 0), 0)
    review_count = to_int(row.get("review_count", 0), 0)

    meta_html = ""
    if deck:
        meta_html += f'<div class="fc-chip">Deck: {html.escape(deck)}</div>'
    if subject:
        meta_html += f'<div class="fc-chip">Matéria: {html.escape(subject)}</div>'
    if topic:
        meta_html += f'<div class="fc-chip">Subtópico: {html.escape(topic)}</div>'
    if due_date:
        meta_html += f'<div class="fc-chip">Próxima revisão: {html.escape(due_date)}</div>'

    st.markdown(
        (
            '<div class="fc-player">'
            '<div class="fc-top">'
            '<div class="b4-title">Revisão de flashcards</div>'
            f'<div class="fc-counter">Card {current_index + 1} de {total}</div>'
            '</div>'
            f'<div style="display:flex; flex-wrap:wrap; gap:10px; margin-bottom:14px;">{meta_html}</div>'
            '<div class="fc-section-label">FRENTE</div>'
            f'<div class="fc-box"><div class="fc-question">{html.escape(question)}</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True
    )

    st.caption(f"Histórico: {review_count} revisão(ões) • Intervalo atual: {interval_days} dia(s)")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Mostrar resposta", key="fc_show_answer", use_container_width=True):
            st.session_state.flashcard_show_answer = True
    with c2:
        if st.button("Mostrar nota", key="fc_show_note", use_container_width=True):
            st.session_state.flashcard_show_note = True
    with c3:
        if st.button("Ocultar tudo", key="fc_hide_all", use_container_width=True):
            st.session_state.flashcard_show_answer = False
            st.session_state.flashcard_show_note = False
            safe_rerun()

    if st.session_state.get("flashcard_show_answer", False):
        st.markdown(
            f'<div class="fc-section-label">RESPOSTA</div><div class="fc-box"><div class="fc-answer">{html.escape(answer)}</div></div>',
            unsafe_allow_html=True
        )

    if st.session_state.get("flashcard_show_note", False):
        st.markdown(
            f'<div class="fc-section-label">NOTA / EXPLICAÇÃO</div><div class="fc-box"><div class="fc-note">{html.escape(note) if note else "Sem nota cadastrada."}</div></div>',
            unsafe_allow_html=True
        )

    st.markdown("### Avaliação da lembrança")
    st.caption("Again = errei / Hard = lembrei com dificuldade / Good = lembrei bem / Easy = muito fácil")

    r1, r2, r3, r4 = st.columns(4)
    with r1:
        if st.button("Again", key=f"fc_rate_again_{card_id}", use_container_width=True):
            ok, _ = review_flashcard(card_id, "again")
            if ok:
                st.session_state.flashcard_show_answer = False
                st.session_state.flashcard_show_note = False
                st.session_state.flashcard_index = min(current_index + 1, total - 1)
                safe_rerun()
    with r2:
        if st.button("Hard", key=f"fc_rate_hard_{card_id}", use_container_width=True):
            ok, _ = review_flashcard(card_id, "hard")
            if ok:
                st.session_state.flashcard_show_answer = False
                st.session_state.flashcard_show_note = False
                st.session_state.flashcard_index = min(current_index + 1, total - 1)
                safe_rerun()
    with r3:
        if st.button("Good", key=f"fc_rate_good_{card_id}", use_container_width=True):
            ok, _ = review_flashcard(card_id, "good")
            if ok:
                st.session_state.flashcard_show_answer = False
                st.session_state.flashcard_show_note = False
                st.session_state.flashcard_index = min(current_index + 1, total - 1)
                safe_rerun()
    with r4:
        if st.button("Easy", key=f"fc_rate_easy_{card_id}", use_container_width=True):
            ok, _ = review_flashcard(card_id, "easy")
            if ok:
                st.session_state.flashcard_show_answer = False
                st.session_state.flashcard_show_note = False
                st.session_state.flashcard_index = min(current_index + 1, total - 1)
                safe_rerun()

    p1, p2, p3 = st.columns([1, 1, 1])
    with p1:
        if st.button("Card anterior", key="fc_prev", use_container_width=True):
            st.session_state.flashcard_index = max(0, current_index - 1)
            st.session_state.flashcard_show_answer = False
            st.session_state.flashcard_show_note = False
            safe_rerun()
    with p2:
        if st.button("Próximo card", key="fc_next", use_container_width=True):
            st.session_state.flashcard_index = min(total - 1, current_index + 1)
            st.session_state.flashcard_show_answer = False
            st.session_state.flashcard_show_note = False
            safe_rerun()
    with p3:
        if st.button("Fechar flashcards", key="fc_close", use_container_width=True):
            st.session_state.flashcard_fullscreen = False
            st.session_state.flashcard_show_answer = False
            st.session_state.flashcard_show_note = False
            safe_rerun()


def render_flashcards_page():
    df = fetch_flashcards_df(st.session_state.user_id)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Flashcards</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Importe, organize e revise com lógica de repetição espaçada baseada em recuperação ativa.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_flashcard_kpis(df)

    decks, subjects, topics = build_flashcard_filters(df)
    deck_filter = st.selectbox("Filtrar deck", ["Todos"] + decks, key="fc_filter_deck")
    subject_filter = st.selectbox("Filtrar matéria", ["Todos"] + subjects, key="fc_filter_subject")
    topic_filter = st.selectbox("Filtrar subtópico", ["Todos"] + topics, key="fc_filter_topic")
    due_only = st.checkbox("Mostrar apenas cards vencidos para hoje", value=True, key="fc_due_only")
    search_term = st.text_input("Pesquisar flashcards", placeholder="Ex.: asma, antibiótico, ICC", key="fc_search_term")

    filtered_df = filter_flashcards_df(df, deck_filter, subject_filter, topic_filter, search_term, due_only=due_only)

    if st.session_state.get("flashcard_fullscreen", False):
        render_flashcard_player(filtered_df)
        return

    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Adicionar flashcard</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">Cadastre manualmente um novo card.</div>', unsafe_allow_html=True)

        with st.form("form_add_flashcard", clear_on_submit=True):
            deck = st.text_input("Deck", placeholder="Ex.: Clínica Médica")
            subject = st.text_input("Matéria", placeholder="Ex.: Pneumologia")
            topic = st.text_input("Subtópico", placeholder="Ex.: Asma")
            question = st.text_area("Frente / Pergunta", placeholder="Digite a pergunta ou frente do card")
            answer = st.text_area("Resposta / Verso", placeholder="Digite a resposta")
            note = st.text_area("Nota / Explicação", placeholder="Digite a explicação adicional")
            submitted = st.form_submit_button("Adicionar flashcard")

        if submitted:
            ok, msg = add_flashcard(st.session_state.user_id, deck, subject, topic, question, answer, note)
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)

        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Importar CSV</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">O arquivo pode ter colunas como pergunta, resposta e explicação.</div>', unsafe_allow_html=True)

        with st.form("form_import_flashcards", clear_on_submit=False):
            csv_deck = st.text_input("Deck do lote", placeholder="Ex.: Revisão R1")
            csv_subject = st.text_input("Matéria do lote", placeholder="Ex.: Cardiologia")
            csv_topic = st.text_input("Subtópico do lote", placeholder="Ex.: ICC")
            uploaded_file = st.file_uploader("Arquivo CSV", type=["csv"], key="fc_csv_upload")
            import_submitted = st.form_submit_button("Importar flashcards")

        if import_submitted:
            ok, msg = import_flashcards_csv(st.session_state.user_id, uploaded_file, csv_deck, csv_subject, csv_topic)
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Biblioteca e revisão</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">Abra a revisão do dia e faça a gestão dos cards.</div>', unsafe_allow_html=True)

        a1, a2 = st.columns([1, 1])
        with a1:
            if st.button("Abrir revisão dos flashcards", key="fc_open_player", use_container_width=True):
                st.session_state.flashcard_fullscreen = True
                st.session_state.flashcard_index = 0
                st.session_state.flashcard_show_answer = False
                st.session_state.flashcard_show_note = False
                safe_rerun()
        with a2:
            st.markdown(
                f'<div class="fc-chip" style="text-align:center; width:100%; display:block;">{len(filtered_df)} card(s) no filtro</div>',
                unsafe_allow_html=True
            )

        if filtered_df.empty:
            st.markdown('<div class="b4-empty" style="margin-top:12px;">Nenhum flashcard encontrado.</div>', unsafe_allow_html=True)
        else:
            preview_df = filtered_df.head(20).copy()
            st.markdown('<div style="margin-top:12px;"></div>', unsafe_allow_html=True)

            for _, row in preview_df.iterrows():
                card_id = int(row["id"])
                q = normalize_text(row.get("question", ""))
                deck = normalize_text(row.get("deck", ""))
                subject = normalize_text(row.get("subject", ""))
                topic = normalize_text(row.get("topic", ""))
                due_date = normalize_text(row.get("due_date", ""))

                meta_parts = []
                if deck:
                    meta_parts.append(deck)
                if subject:
                    meta_parts.append(subject)
                if topic:
                    meta_parts.append(topic)
                if due_date:
                    meta_parts.append(f"Revisão: {due_date}")

                st.markdown(
                    (
                        '<div class="fc-list-item">'
                        f'<div class="fc-list-q">{html.escape(q)}</div>'
                        f'<div class="fc-list-meta">{html.escape(" • ".join(meta_parts) if meta_parts else "-")}</div>'
                        '</div>'
                    ),
                    unsafe_allow_html=True
                )

                if st.button("Excluir flashcard", key=f"delete_flashcard_{card_id}", use_container_width=True):
                    if delete_flashcard(card_id):
                        safe_rerun()

        st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# SIMULADOS
# =========================================================
def fetch_mock_area_scores_df(user_id: int):
    df = fetch_dataframe(
        """
        SELECT mas.*, m.title AS mock_title, m.mock_date
        FROM mock_area_scores mas
        LEFT JOIN mocks m ON m.id = mas.mock_id
        WHERE mas.user_id = ?
        ORDER BY m.mock_date DESC, mas.id DESC
        """,
        (user_id,)
    )

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "mock_id", "user_id", "grande_area", "correct_count",
            "question_count", "accuracy_percent", "created_at", "mock_title", "mock_date"
        ])

    for col in ["correct_count", "question_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "accuracy_percent" in df.columns:
        df["accuracy_percent"] = pd.to_numeric(df["accuracy_percent"], errors="coerce").fillna(0.0)

    return df


def fetch_mocks_df(user_id: int):
    df = fetch_dataframe(
        """
        SELECT *
        FROM mocks
        WHERE user_id = ?
        ORDER BY mock_date DESC, id DESC
        """,
        (user_id,)
    )

    if df.empty:
        return pd.DataFrame(columns=["id", "mock_date", "title", "score_percent", "questions_count", "created_at"])

    for col in ["title", "mock_date"]:
        df[col] = df[col].fillna("").astype(str)

    for col in ["score_percent", "questions_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def add_mock(user_id: int, mock_date, title: str, score_percent: float, questions_count: int, area_scores: list):
    if not mock_date:
        return False, "Escolha a data do simulado."
    if to_float(score_percent, 0) < 0 or to_float(score_percent, 0) > 100:
        return False, "A porcentagem deve ficar entre 0 e 100."
    if to_int(questions_count, 0) < 0:
        return False, "O número de questões não pode ser negativo."

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO mocks (
                user_id, mock_date, title, score_percent, questions_count, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                mock_date.isoformat() if hasattr(mock_date, "isoformat") else str(mock_date),
                normalize_text(title),
                to_float(score_percent, 0),
                to_int(questions_count, 0),
                datetime.now().isoformat()
            )
        )
        mock_id = cur.lastrowid

        for item in area_scores:
            ga = normalize_text(item.get("grande_area", ""))
            correct_count = to_int(item.get("correct_count", 0), 0)
            question_count = to_int(item.get("question_count", 0), 0)
            acc = round((correct_count / question_count) * 100, 1) if question_count > 0 else 0.0

            if ga:
                cur.execute(
                    """
                    INSERT INTO mock_area_scores (
                        mock_id, user_id, grande_area, correct_count, question_count,
                        accuracy_percent, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mock_id,
                        user_id,
                        ga,
                        correct_count,
                        question_count,
                        acc,
                        datetime.now().isoformat()
                    )
                )

        conn.commit()
        return True, "Simulado registrado com sucesso."
    except Exception as e:
        return False, f"Erro ao registrar simulado: {e}"
    finally:
        conn.close()


def delete_mock(mock_id: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM mock_area_scores WHERE mock_id = ?", (mock_id,))
        cur.execute("DELETE FROM mocks WHERE id = ?", (mock_id,))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def build_mock_summary(mocks_df: pd.DataFrame):
    if mocks_df.empty:
        return {
            "count": 0,
            "avg_score": 0.0,
            "best_score": 0.0,
            "last_score": 0.0,
            "total_questions": 0
        }

    avg_score = round(float(mocks_df["score_percent"].mean()), 1)
    best_score = round(float(mocks_df["score_percent"].max()), 1)
    last_score = round(float(mocks_df.iloc[0]["score_percent"]), 1) if len(mocks_df) > 0 else 0.0
    total_questions = int(mocks_df["questions_count"].sum())

    return {
        "count": int(len(mocks_df)),
        "avg_score": avg_score,
        "best_score": best_score,
        "last_score": last_score,
        "total_questions": total_questions,
    }


def render_mock_kpis(summary: dict):
    cards = [
        ("Simulados", summary["count"], "Total cadastrado"),
        ("Média geral", f"{summary['avg_score']}%", "Desempenho médio"),
        ("Melhor resultado", f"{summary['best_score']}%", "Pico de performance"),
        ("Último resultado", f"{summary['last_score']}%", f"{summary['total_questions']} questões no total"),
    ]

    cols = st.columns(4, gap="large")
    for col, (label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="b4-kpi">'
                    f'<div class="b4-kpi-label">{html.escape(str(label))}</div>'
                    f'<div class="b4-kpi-value">{html.escape(str(value))}</div>'
                    f'<div class="b4-kpi-sub">{html.escape(str(sub))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )


def render_mock_chart(mocks_df: pd.DataFrame):
    st.markdown('<div class="b4-card">', unsafe_allow_html=True)
    st.markdown('<div class="b4-title">Evolução dos simulados</div>', unsafe_allow_html=True)
    st.markdown('<div class="b4-sub">Leitura visual simples da progressão dos resultados.</div>', unsafe_allow_html=True)

    if mocks_df.empty:
        st.markdown('<div class="b4-empty">Ainda não há simulados para mostrar no gráfico.</div>', unsafe_allow_html=True)
    else:
        chart_df = mocks_df.sort_values("mock_date", ascending=True).copy()
        fig, ax = plt.subplots(figsize=(10, 3.6))
        ax.plot(range(len(chart_df)), chart_df["score_percent"].tolist(), linewidth=2.2)
        ax.set_xticks(range(len(chart_df)))
        ax.set_xticklabels(chart_df["mock_date"].tolist(), rotation=45, ha="right")
        ax.set_ylabel("%")
        ax.set_xlabel("Data")
        ax.grid(alpha=0.18)
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

    st.markdown("</div>", unsafe_allow_html=True)


def render_mocks_page():
    mocks_df = fetch_mocks_df(st.session_state.user_id)
    area_scores_df = fetch_mock_area_scores_df(st.session_state.user_id)
    summary = build_mock_summary(mocks_df)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Simulados</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Cadastre resultados, acompanhe histórico e distribua acertos por grande área.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_mock_kpis(summary)

    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Registrar simulado</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">Cadastre a data, o nome, o percentual geral e os acertos por grande área.</div>', unsafe_allow_html=True)

        with st.form("form_add_mock", clear_on_submit=True):
            mock_date = st.date_input("Data do simulado", value=date.today(), key="mock_date_input")
            title = st.text_input("Título", placeholder="Ex.: Simulado ENARE 01")
            score_percent = st.number_input("Percentual geral (%)", min_value=0.0, max_value=100.0, step=0.1, value=75.0)
            questions_count = st.number_input("Número total de questões", min_value=0, step=1, value=100)

            st.markdown("#### Acertos por grande área")
            area_scores = []
            total_area_questions = 0
            total_area_correct = 0

            for ga in GREAT_AREAS:
                c1, c2 = st.columns(2)
                with c1:
                    q_count = st.number_input(f"{ga} - questões", min_value=0, step=1, value=0, key=f"mock_q_{ga}")
                with c2:
                    c_count = st.number_input(f"{ga} - acertos", min_value=0, step=1, value=0, key=f"mock_c_{ga}")

                area_scores.append({
                    "grande_area": ga,
                    "question_count": q_count,
                    "correct_count": c_count,
                })
                total_area_questions += q_count
                total_area_correct += c_count

            submitted = st.form_submit_button("Registrar simulado")

        if submitted:
            if total_area_correct > total_area_questions:
                st.error("Os acertos por grande área não podem ultrapassar o total de questões por área.")
            else:
                ok, msg = add_mock(
                    st.session_state.user_id,
                    mock_date,
                    title,
                    score_percent,
                    questions_count,
                    area_scores
                )
                if ok:
                    st.success(msg)
                    safe_rerun()
                else:
                    st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        render_mock_chart(mocks_df)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)

    c1, c2 = st.columns([1, 1], gap="large")

    with c1:
        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Histórico de simulados</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">Gerencie os lançamentos mais recentes.</div>', unsafe_allow_html=True)

        if mocks_df.empty:
            st.markdown('<div class="b4-empty">Nenhum simulado cadastrado ainda.</div>', unsafe_allow_html=True)
        else:
            hist_df = mocks_df.head(20).copy()
            for _, row in hist_df.iterrows():
                mock_id = int(row["id"])
                mock_date = normalize_text(row.get("mock_date", ""))
                title = normalize_text(row.get("title", "Sem título"))
                score_percent = round(float(row.get("score_percent", 0)), 1)
                questions_count = to_int(row.get("questions_count", 0), 0)

                st.markdown(
                    (
                        '<div class="mock-item">'
                        '<div class="mock-top">'
                        '<div>'
                        f'<div class="mock-title">{html.escape(title)}</div>'
                        f'<div class="mock-meta">{html.escape(mock_date)} • {questions_count} questões</div>'
                        '</div>'
                        f'<div class="mock-badge">{score_percent:.1f}%</div>'
                        '</div>'
                        '</div>'
                    ),
                    unsafe_allow_html=True
                )

                if st.button("Excluir simulado", key=f"delete_mock_{mock_id}", use_container_width=True):
                    if delete_mock(mock_id):
                        safe_rerun()

        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Desempenho por grande área</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">Resumo dos acertos por área ao longo dos simulados.</div>', unsafe_allow_html=True)

        if area_scores_df.empty:
            st.markdown('<div class="b4-empty">Ainda não há dados por grande área.</div>', unsafe_allow_html=True)
        else:
            grouped = area_scores_df.groupby("grande_area", as_index=False)[["correct_count", "question_count"]].sum()
            grouped["accuracy_percent"] = grouped.apply(
                lambda r: round((r["correct_count"] / r["question_count"]) * 100, 1) if r["question_count"] > 0 else 0.0,
                axis=1
            )
            grouped = grouped.rename(columns={
                "grande_area": "Grande área",
                "correct_count": "Acertos",
                "question_count": "Questões",
                "accuracy_percent": "Acurácia (%)"
            })
            st.dataframe(grouped, use_container_width=True, hide_index=True)

        st.markdown("</div>", unsafe_allow_html=True)
# =========================================================
# RELATÓRIOS
# =========================================================
def build_report_data(user_id: int):
    sessions_df = fetch_sessions_df(user_id)
    schedule_df = fetch_schedule_df(user_id)
    flashcards_df = fetch_flashcards_df(user_id)
    mocks_df = fetch_mocks_df(user_id)
    mock_area_scores_df = fetch_mock_area_scores_df(user_id)
    goal = get_user_goal(user_id)

    questions_summary = build_questions_summary(sessions_df)
    schedule_summary = build_schedule_summary(schedule_df)
    mock_summary = build_mock_summary(mocks_df)

    topic_ranking = pd.DataFrame(columns=["topic_display", "questions_done", "correct_answers", "accuracy"])
    if not sessions_df.empty:
        base = sessions_df.copy()
        base["topic_display"] = base["topic"].fillna("").astype(str).str.strip()
        base["subject_display"] = base["subject"].fillna("").astype(str).str.strip()
        base["topic_display"] = base["topic_display"].where(base["topic_display"] != "", base["subject_display"])
        base["topic_display"] = base["topic_display"].where(base["topic_display"] != "", "Sem subtópico")

        grouped = base.groupby("topic_display", as_index=False)[["questions_done", "correct_answers"]].sum()
        grouped = grouped[grouped["questions_done"] > 0].copy()
        if not grouped.empty:
            grouped["accuracy"] = ((grouped["correct_answers"] / grouped["questions_done"]) * 100).round(1)
            topic_ranking = grouped.sort_values(["accuracy", "questions_done"], ascending=[False, False]).reset_index(drop=True)

    subject_summary = pd.DataFrame(columns=["subject", "questions_done", "correct_answers", "accuracy", "study_minutes"])
    if not sessions_df.empty:
        base = sessions_df.copy()
        base["subject"] = base["subject"].fillna("").astype(str).str.strip()
        base["subject"] = base["subject"].where(base["subject"] != "", "Sem matéria")
        subject_summary = base.groupby("subject", as_index=False)[["questions_done", "correct_answers", "study_minutes"]].sum()
        subject_summary["accuracy"] = 0.0
        mask = subject_summary["questions_done"] > 0
        subject_summary.loc[mask, "accuracy"] = (
            (subject_summary.loc[mask, "correct_answers"] / subject_summary.loc[mask, "questions_done"]) * 100
        ).round(1)
        subject_summary = subject_summary.sort_values(["questions_done", "accuracy"], ascending=[False, False]).reset_index(drop=True)

    return {
        "sessions_df": sessions_df,
        "schedule_df": schedule_df,
        "flashcards_df": flashcards_df,
        "mocks_df": mocks_df,
        "mock_area_scores_df": mock_area_scores_df,
        "goal": goal,
        "questions_summary": questions_summary,
        "schedule_summary": schedule_summary,
        "mock_summary": mock_summary,
        "topic_ranking": topic_ranking,
        "subject_summary": subject_summary,
    }


def make_csv_download(df: pd.DataFrame):
    if df is None or df.empty:
        return None
    try:
        return df.to_csv(index=False).encode("utf-8-sig")
    except Exception:
        return None


def render_report_kpis(report: dict):
    qs = report["questions_summary"]
    ss = report["schedule_summary"]
    ms = report["mock_summary"]
    fc = report["flashcards_df"]

    cards = [
        ("Questões totais", qs["total_questions"], f"Acurácia geral: {qs['overall_accuracy']}%"),
        ("Tempo estudado", f"{qs['total_minutes']} min", f"Hoje: {qs['today_minutes']} min"),
        ("Cronograma", ss["done"], f"Concluídos • {ss['pending']} pendentes"),
        ("Flashcards", len(fc), f"Simulados: {ms['count']}"),
    ]

    cols = st.columns(4, gap="large")
    for col, (label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="b5-kpi">'
                    f'<div class="b5-kpi-label">{html.escape(str(label))}</div>'
                    f'<div class="b5-kpi-value">{html.escape(str(value))}</div>'
                    f'<div class="b5-kpi-sub">{html.escape(str(sub))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )


def render_reports_page():
    report = build_report_data(st.session_state.user_id)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Relatórios</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Leitura gerencial do seu desempenho com tabelas, ranking e exportações.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_report_kpis(report)

    qs = report["questions_summary"]
    ss = report["schedule_summary"]
    ms = report["mock_summary"]
    goal = report["goal"]
    topic_ranking = report["topic_ranking"]
    subject_summary = report["subject_summary"]
    mock_area_scores_df = report["mock_area_scores_df"]

    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        st.markdown('<div class="b5-card">', unsafe_allow_html=True)
        st.markdown('<div class="b5-title">Resumo executivo</div>', unsafe_allow_html=True)
        st.markdown('<div class="b5-sub">Panorama consolidado da operação de estudo.</div>', unsafe_allow_html=True)

        for name, text in [
            ("Metas atuais", f"Questões/dia: {to_int(goal.get('daily_questions_goal', 60), 60)} • Tempo/dia: {to_int(goal.get('daily_minutes_goal', 180), 180)} min • Simulados/mês: {to_int(goal.get('monthly_mock_goal', 4), 4)}"),
            ("Questões", f"{qs['total_questions']} feitas • {qs['total_correct']} acertos • {qs['overall_accuracy']}% de acurácia"),
            ("Cronograma", f"{ss['total']} itens planejados • {ss['done']} concluídos • {ss['pending']} pendentes"),
            ("Simulados", f"{ms['count']} registros • média {ms['avg_score']}% • melhor resultado {ms['best_score']}%"),
        ]:
            st.markdown(
                (
                    '<div class="b5-stat-item" style="margin-top:12px;">'
                    f'<div class="b5-stat-name">{html.escape(name)}</div>'
                    f'<div class="b5-stat-meta">{html.escape(text)}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        best_topic = "-"
        worst_topic = "-"

        if not topic_ranking.empty:
            best_topic = f"{topic_ranking.iloc[0]['topic_display']} ({topic_ranking.iloc[0]['accuracy']}%)"
            worst_sorted = topic_ranking.sort_values(["accuracy", "questions_done"], ascending=[True, False]).reset_index(drop=True)
            worst_topic = f"{worst_sorted.iloc[0]['topic_display']} ({worst_sorted.iloc[0]['accuracy']}%)"

        st.markdown(
            (
                '<div class="b5-highlight">'
                '<div class="b5-highlight-title">Leitura estratégica</div>'
                '<div class="b5-highlight-text">'
                f'Seu melhor subtópico atual é <b>{html.escape(str(best_topic))}</b>.<br><br>'
                f'O subtópico com maior necessidade de reforço é <b>{html.escape(str(worst_topic))}</b>.<br><br>'
                'Use essa leitura para ajustar revisões, priorizar subtópicos fracos e calibrar o cronograma semanal.'
                '</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    col3, col4 = st.columns([1, 1], gap="large")

    with col3:
        st.markdown('<div class="b5-card">', unsafe_allow_html=True)
        st.markdown('<div class="b5-title">Ranking por subtópico</div>', unsafe_allow_html=True)
        st.markdown('<div class="b5-sub">Ordenado por acurácia e volume de questões.</div>', unsafe_allow_html=True)

        if topic_ranking.empty:
            st.markdown('<div class="b5-empty">Ainda não há dados suficientes para ranking por subtópico.</div>', unsafe_allow_html=True)
        else:
            view_df = topic_ranking.rename(columns={
                "topic_display": "Subtópico",
                "questions_done": "Questões",
                "correct_answers": "Acertos",
                "accuracy": "Acurácia (%)"
            })
            st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.markdown("</div>", unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="b5-card">', unsafe_allow_html=True)
        st.markdown('<div class="b5-title">Resumo por matéria</div>', unsafe_allow_html=True)
        st.markdown('<div class="b5-sub">Volume, acurácia e tempo estudado por matéria.</div>', unsafe_allow_html=True)

        if subject_summary.empty:
            st.markdown('<div class="b5-empty">Ainda não há dados suficientes para resumo por matéria.</div>', unsafe_allow_html=True)
        else:
            view_df = subject_summary.rename(columns={
                "subject": "Matéria",
                "questions_done": "Questões",
                "correct_answers": "Acertos",
                "accuracy": "Acurácia (%)",
                "study_minutes": "Tempo (min)"
            })
            st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    st.markdown('<div class="b5-card">', unsafe_allow_html=True)
    st.markdown('<div class="b5-title">Simulados por grande área</div>', unsafe_allow_html=True)
    st.markdown('<div class="b5-sub">Acurácia consolidada por área.</div>', unsafe_allow_html=True)

    if mock_area_scores_df.empty:
        st.markdown('<div class="b5-empty">Ainda não há dados por grande área.</div>', unsafe_allow_html=True)
    else:
        ga = mock_area_scores_df.groupby("grande_area", as_index=False)[["correct_count", "question_count"]].sum()
        ga["accuracy_percent"] = ga.apply(
            lambda r: round((r["correct_count"] / r["question_count"]) * 100, 1) if r["question_count"] > 0 else 0.0,
            axis=1
        )
        ga = ga.rename(columns={
            "grande_area": "Grande área",
            "correct_count": "Acertos",
            "question_count": "Questões",
            "accuracy_percent": "Acurácia (%)"
        })
        st.dataframe(ga, use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    st.markdown('<div class="b5-card">', unsafe_allow_html=True)
    st.markdown('<div class="b5-title">Exportações</div>', unsafe_allow_html=True)
    st.markdown('<div class="b5-sub">Baixe suas bases para análise externa.</div>', unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        csv_data = make_csv_download(report["sessions_df"])
        st.download_button("Questões", data=csv_data if csv_data else b"", file_name="questoes.csv", mime="text/csv", use_container_width=True, disabled=(csv_data is None))
    with c2:
        csv_data = make_csv_download(report["schedule_df"])
        st.download_button("Cronograma", data=csv_data if csv_data else b"", file_name="cronograma.csv", mime="text/csv", use_container_width=True, disabled=(csv_data is None))
    with c3:
        csv_data = make_csv_download(report["flashcards_df"])
        st.download_button("Flashcards", data=csv_data if csv_data else b"", file_name="flashcards.csv", mime="text/csv", use_container_width=True, disabled=(csv_data is None))
    with c4:
        csv_data = make_csv_download(report["mocks_df"])
        st.download_button("Simulados", data=csv_data if csv_data else b"", file_name="simulados.csv", mime="text/csv", use_container_width=True, disabled=(csv_data is None))
    with c5:
        csv_data = make_csv_download(report["mock_area_scores_df"])
        st.download_button("Áreas", data=csv_data if csv_data else b"", file_name="simulados_por_area.csv", mime="text/csv", use_container_width=True, disabled=(csv_data is None))

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# ADMINISTRAÇÃO
# =========================================================
def render_admin_page():
    if not st.session_state.get("is_admin", False):
        st.warning("Você não tem permissão para acessar esta área.")
        return

    users_df = fetch_users_df()

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Administração</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Controle de usuários e parametrizações principais da plataforma.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)

    total_users = len(users_df)
    total_admins = int((users_df["is_admin"] == 1).sum()) if not users_df.empty else 0
    total_students = max(total_users - total_admins, 0)

    cards = [
        ("Usuários", total_users, "Total cadastrado"),
        ("Administradores", total_admins, "Acesso avançado"),
        ("Alunos", total_students, "Perfil padrão"),
        ("Usuário atual", st.session_state.get("username", "-"), "Sessão ativa"),
    ]

    cols = st.columns(4, gap="large")
    for col, (label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                (
                    '<div class="b5-kpi">'
                    f'<div class="b5-kpi-label">{html.escape(str(label))}</div>'
                    f'<div class="b5-kpi-value">{html.escape(str(value))}</div>'
                    f'<div class="b5-kpi-sub">{html.escape(str(sub))}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="b5-card">', unsafe_allow_html=True)
        st.markdown('<div class="b5-title">Criar usuário</div>', unsafe_allow_html=True)
        st.markdown('<div class="b5-sub">Cadastre novos acessos para a plataforma.</div>', unsafe_allow_html=True)

        with st.form("form_admin_create_user", clear_on_submit=True):
            new_username = st.text_input("Usuário", placeholder="Novo usuário", key="admin_new_user")
            new_password = st.text_input("Senha", type="password", placeholder="Senha", key="admin_new_pass")
            is_admin = st.checkbox("Criar como administrador", key="admin_new_is_admin")
            submitted = st.form_submit_button("Criar usuário")

        if submitted:
            ok, msg = create_user(new_username, new_password, is_admin=1 if is_admin else 0)
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="b5-card">', unsafe_allow_html=True)
        st.markdown('<div class="b5-title">Usuários cadastrados</div>', unsafe_allow_html=True)
        st.markdown('<div class="b5-sub">Leitura rápida da base de acessos.</div>', unsafe_allow_html=True)

        if users_df.empty:
            st.markdown('<div class="b5-empty">Nenhum usuário cadastrado.</div>', unsafe_allow_html=True)
        else:
            view_df = users_df[["id", "username", "is_admin_label", "created_at"]].copy()
            view_df.columns = ["ID", "Usuário", "Administrador", "Criado em"]
            st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    st.markdown('<div class="b5-card">', unsafe_allow_html=True)
    st.markdown('<div class="b5-title">Configurações da plataforma</div>', unsafe_allow_html=True)
    st.markdown('<div class="b5-sub">Ajuste metas e confira as grandes áreas padrão.</div>', unsafe_allow_html=True)

    current_user_id = st.session_state.get("user_id")
    current_goal = get_user_goal(current_user_id)

    with st.form("form_admin_goal_settings", clear_on_submit=False):
        phase_name = st.text_input("Etapa/Fase", value=str(current_goal.get("phase_name", "Intermediária") or "Intermediária"))
        daily_questions_goal = st.number_input("Meta diária de questões", min_value=0, step=1, value=to_int(current_goal.get("daily_questions_goal", 60), 60))
        daily_minutes_goal = st.number_input("Meta diária de tempo (min)", min_value=0, step=5, value=to_int(current_goal.get("daily_minutes_goal", 180), 180))
        monthly_mock_goal = st.number_input("Meta mensal de simulados", min_value=0, step=1, value=to_int(current_goal.get("monthly_mock_goal", 4), 4))

        st.markdown("#### Grandes áreas cadastradas")
        st.dataframe(pd.DataFrame({"Grande área": GREAT_AREAS}), use_container_width=True, hide_index=True)

        submitted = st.form_submit_button("Salvar metas")

    if submitted:
        ok, msg = update_goal_settings(current_user_id, daily_questions_goal, daily_minutes_goal, monthly_mock_goal, phase_name)
        if ok:
            st.success(msg)
            safe_rerun()
        else:
            st.error(msg)

    st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# RODAPÉ
# =========================================================
def render_footer_premium():
    render_html_block("""
    <div style="
        margin-top: 18px;
        padding: 18px 20px;
        border: 1px solid rgba(255,255,255,.08);
        border-radius: 22px;
        background: linear-gradient(180deg, rgba(255,255,255,.04), rgba(255,255,255,.025));
        color:#a8bbd6;
        font-size:.88rem;
        text-align:center;
        box-shadow: 0 12px 30px rgba(0,0,0,.18);
    ">
        🩺 <b>Mentoria do Jhon</b> • Plataforma premium de acompanhamento • Fase 4 concluída
    </div>
    """)


# =========================================================
# MAIN
# =========================================================
def main():
    ensure_session_defaults()
    init_db()
    ensure_schema_upgrades()
    inject_global_css()
    inject_dashboard_css()

    if not st.session_state.logged_in:
        render_login_screen()
        return

    render_app_header(
        username=st.session_state.username,
        is_admin=st.session_state.is_admin
    )
    render_top_menu()

    current_menu = st.session_state.get("menu", "Visão Geral")

    if current_menu == "Visão Geral":
        render_visao_geral()

    elif current_menu == "Cronograma":
        render_schedule_manager()

    elif current_menu == "Questões":
        render_questions_manager()

    elif current_menu == "Flashcards":
        render_flashcards_page()

    elif current_menu == "Simulados":
        render_mocks_page()

    elif current_menu == "Relatórios":
        render_reports_page()

    elif current_menu == "Administração":
        render_admin_page()

    render_footer_premium()

    st.markdown('<div class="top-spacer-lg"></div>', unsafe_allow_html=True)
    _, c2, _ = st.columns([1, 1, 1])
    with c2:
        if st.button("Encerrar sessão", use_container_width=True):
            reset_login_state()
            safe_rerun()


if __name__ == "__main__":
    main()
