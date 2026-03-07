# =========================
# BLOCO 1/5 - V6 PRO CLEAN FIX
# =========================

import os
import sqlite3
import hashlib
import secrets
import time
import re
import base64
import html
from datetime import datetime, date, timedelta
from io import BytesIO
from textwrap import dedent

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader


# =========================================================
# CONFIG APP
# =========================================================
APP_NAME = "Mentoria do Jhon"
APP_SUBTITLE = "Residência Médica — Plataforma premium de acompanhamento"
APP_VERSION = "Mentoria do Jhon"
DB_PATH = "mentoria_do_jhon_v6_fix.db"

DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = os.environ.get("MENTORIA_ADMIN_PASS", "admin123")

PRIMARY_LOGO_PATH = "/mnt/data/photo_2026-02-08_22-17-28.jpg"

LOGO_CANDIDATES = [
    PRIMARY_LOGO_PATH,
    "/mnt/data/photo_2026-02-08_22-17-28.jpg",
    "logo.png",
    "mentoria_logo.png",
    os.path.join("assets", "logo.png"),
    os.path.join("assets", "mentoria_logo.png"),
]

DEFAULT_SUBJECTS = [
    "Clínica Médica",
    "Cirurgia",
    "Pediatria",
    "Ginecologia e Obstetrícia",
    "Medicina Preventiva",
]

PREP_STAGES = [
    ("Base", 40, 70.0),
    ("Intermediária", 60, 78.0),
    ("Avançada", 80, 85.0),
    ("Reta Final", 100, 88.0),
]

MENU_ITEMS = [
    "Cockpit",
    "Hoje",
    "Registrar",
    "Revisões",
    "Dashboard",
    "Mapa de Rendimentos",
    "Cronograma",
    "Metas & Alertas",
    "Relatórios (PDF)",
    "Gerenciar (Editar/Excluir)",
    "Exportar/Importar CSV",
    "Matérias/Subtemas",
    "Usuários",
    "Auditoria",
]


# =========================================================
# TEMA
# =========================================================
def ensure_dark_theme_config():
    try:
        os.makedirs(".streamlit", exist_ok=True)
        cfg_path = os.path.join(".streamlit", "config.toml")
        cfg = """
[theme]
base="dark"
primaryColor="#D4A62A"
backgroundColor="#050608"
secondaryBackgroundColor="#0E1117"
textColor="#F5F2E8"
font="sans serif"
"""
        with open(cfg_path, "w", encoding="utf-8") as f:
            f.write(cfg.strip() + "\n")
    except Exception:
        pass


ensure_dark_theme_config()

st.set_page_config(
    page_title=f"{APP_NAME} | {APP_VERSION}",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# HELPERS
# =========================================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return date.today().isoformat()


def esc(value):
    return html.escape(str(value or ""))


def sha256(s: str):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def hash_password(password: str, salt: str):
    return sha256(salt + password)


def check_password(password: str, salt: str, pw_hash: str):
    return hash_password(password, salt) == pw_hash


def format_hms(seconds: int):
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def compute_review_days(accuracy_pct: float) -> int:
    if accuracy_pct < 80:
        return 20
    if accuracy_pct <= 90:
        return 30
    return 45


def valid_username(username: str) -> bool:
    username = (username or "").strip()
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{3,30}", username))


def valid_password(password: str) -> bool:
    return len(password or "") >= 4


def locate_logo_path():
    for p in LOGO_CANDIDATES:
        if p and os.path.exists(p):
            return p
    return None


def get_logo_b64():
    p = locate_logo_path()
    if not p:
        return None
    try:
        with open(p, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None


def safe_markdown(content: str):
    st.markdown(dedent(content).strip(), unsafe_allow_html=True)


def metric_card(label: str, value: str, delta: str = ""):
    st.markdown(
        f"""
        <div class="metric-v6">
            <div class="metric-label-v6">{esc(label)}</div>
            <div class="metric-value-v6">{esc(value)}</div>
            <div class="metric-delta-v6">{esc(delta)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def init_navigation():
    if "main_menu" not in st.session_state or st.session_state["main_menu"] not in MENU_ITEMS:
        st.session_state["main_menu"] = "Cockpit"


def goto_page(page_name: str):
    if page_name in MENU_ITEMS:
        st.session_state["main_menu"] = page_name
        st.rerun()


# =========================================================
# BANCO
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def fetch_one(query, params=()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    row = cur.fetchone()
    conn.close()
    return row


def fetch_all(query, params=()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def execute(query, params=()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    last_id = cur.lastrowid
    conn.close()
    return last_id


def audit(user_id: int, action: str, entity: str = None, entity_id: int = None, details: str = None):
    execute(
        """
        INSERT INTO audit_log (user_id, action, entity, entity_id, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, action, entity, entity_id, details, now_str()),
    )


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        salt TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        is_admin INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS topics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(subject_id, name),
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS question_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        topic_id INTEGER,
        tags TEXT,
        questions INTEGER NOT NULL,
        correct INTEGER NOT NULL,
        accuracy REAL NOT NULL,
        source TEXT,
        notes TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE,
        FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE SET NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS exams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        subject_id INTEGER,
        total_questions INTEGER NOT NULL,
        correct INTEGER NOT NULL,
        accuracy REAL NOT NULL,
        duration_seconds INTEGER DEFAULT 0,
        notes TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE SET NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS study_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        topic_id INTEGER,
        tags TEXT,
        duration_seconds INTEGER NOT NULL,
        session_type TEXT NOT NULL DEFAULT 'ESTUDO',
        notes TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE,
        FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE SET NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL UNIQUE,
        daily_questions_goal INTEGER NOT NULL DEFAULT 60,
        daily_minutes_goal INTEGER NOT NULL DEFAULT 180,
        monthly_exams_goal INTEGER NOT NULL DEFAULT 4,
        current_stage TEXT NOT NULL DEFAULT 'Intermediária',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        topic_id INTEGER,
        due_date TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDENTE',
        origin_type TEXT DEFAULT 'QUESTOES',
        origin_id INTEGER,
        last_accuracy REAL,
        created_at TEXT NOT NULL,
        completed_at TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE,
        FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE SET NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS prefs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL UNIQUE,
        inactive_days_alert INTEGER NOT NULL DEFAULT 7,
        drop_accuracy_alert REAL NOT NULL DEFAULT 5.0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        entity TEXT,
        entity_id INTEGER,
        details TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_q_user_date ON question_logs(user_id, created_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_e_user_date ON exams(user_id, created_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_s_user_date ON study_sessions(user_id, created_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_r_user_due ON reviews(user_id, due_date, status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")

    cur.execute("SELECT COUNT(*) FROM users;")
    count_users = cur.fetchone()[0]
    if count_users == 0:
        salt = secrets.token_hex(16)
        pw_hash = hash_password(DEFAULT_ADMIN_PASS, salt)
        cur.execute(
            """
            INSERT INTO users (username, salt, password_hash, is_admin, is_active, created_at)
            VALUES (?, ?, ?, 1, 1, ?)
            """,
            (DEFAULT_ADMIN_USER, salt, pw_hash, now_str())
        )

    for s in DEFAULT_SUBJECTS:
        cur.execute(
            "INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)",
            (s, now_str())
        )

    conn.commit()
    conn.close()


init_db()

# =========================
# BLOCO 2/5 - CSS + AUTH + BASE
# =========================

logo_b64 = get_logo_b64()

watermark_css = ""
if logo_b64:
    watermark_css = f"""
    .stApp::before {{
        content: "";
        position: fixed;
        inset: 0;
        background-image: url("data:image/jpeg;base64,{logo_b64}");
        background-repeat: no-repeat;
        background-position: center center;
        background-size: 30rem;
        opacity: 0.022;
        pointer-events: none;
        z-index: 0;
    }}
    """

safe_markdown(
    f"""
<style>
{watermark_css}

html, body, [class*="css"] {{
    font-feature-settings: "cv02","cv03","cv04","cv11";
}}

.block-container {{
    padding-top: 1rem;
    padding-bottom: 2rem;
    max-width: 1480px;
}}

.main > div {{
    position: relative;
    z-index: 1;
}}

section[data-testid="stSidebar"] {{
    background:
        linear-gradient(180deg, rgba(212,166,42,0.08) 0%, rgba(0,0,0,0) 22%),
        linear-gradient(180deg, #0D1118 0%, #07090D 100%);
    border-right: 1px solid rgba(212,166,42,0.12);
}}

.v6-login-wrap {{
    min-height: 86vh;
    display: flex;
    align-items: center;
    justify-content: center;
}}

.v6-login {{
    width: 100%;
    max-width: 980px;
    border-radius: 30px;
    overflow: hidden;
    background:
        linear-gradient(135deg, rgba(212,166,42,0.11), rgba(255,255,255,0.01)),
        linear-gradient(180deg, rgba(17,22,30,0.97), rgba(8,10,15,0.97));
    border: 1px solid rgba(212,166,42,0.18);
    box-shadow: 0 26px 60px rgba(0,0,0,0.38);
}}

.v6-login-grid {{
    display: grid;
    grid-template-columns: 1.05fr 1fr;
}}

.v6-login-brand {{
    padding: 34px 30px;
    border-right: 1px solid rgba(255,255,255,0.06);
    background:
        radial-gradient(circle at top left, rgba(212,166,42,0.12), transparent 45%),
        transparent;
}}

.v6-login-panel {{
    padding: 30px 28px;
}}

.v6-hero {{
    border-radius: 24px;
    padding: 20px 22px;
    background:
        linear-gradient(135deg, rgba(212,166,42,0.14), rgba(255,255,255,0.02)),
        linear-gradient(180deg, rgba(17,22,30,0.96), rgba(10,12,18,0.96));
    border: 1px solid rgba(212,166,42,0.18);
    box-shadow: 0 18px 40px rgba(0,0,0,0.28);
    margin-bottom: 14px;
}}

.v6-card {{
    border-radius: 20px;
    padding: 16px;
    background: rgba(255,255,255,0.028);
    border: 1px solid rgba(255,255,255,0.08);
    box-shadow: 0 14px 30px rgba(0,0,0,0.22);
}}

.metric-v6 {{
    border-radius: 20px;
    padding: 14px 16px;
    background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.015));
    border: 1px solid rgba(255,255,255,0.07);
    box-shadow: 0 12px 24px rgba(0,0,0,0.20);
    min-height: 110px;
}}

.metric-label-v6 {{
    font-size: 0.84rem;
    opacity: 0.75;
}}

.metric-value-v6 {{
    margin-top: 6px;
    font-size: 1.65rem;
    font-weight: 700;
}}

.metric-delta-v6 {{
    margin-top: 5px;
    font-size: 0.84rem;
    opacity: 0.72;
}}

.badge-v6 {{
    display: inline-block;
    padding: 0.25rem 0.72rem;
    border-radius: 999px;
    border: 1px solid rgba(212,166,42,0.30);
    background: rgba(212,166,42,0.10);
    font-size: 0.82rem;
    margin-right: 6px;
    margin-bottom: 6px;
}}

.soft-hr {{
    border: none;
    border-top: 1px solid rgba(255,255,255,0.08);
    margin: 12px 0 14px 0;
}}

.alert-box {{
    border-radius: 16px;
    padding: 12px 14px;
    margin-bottom: 10px;
    border: 1px solid rgba(255,255,255,0.08);
}}

.alert-red {{
    background: rgba(255, 89, 94, 0.08);
    border-color: rgba(255, 89, 94, 0.24);
}}

.alert-yellow {{
    background: rgba(255, 193, 7, 0.08);
    border-color: rgba(255, 193, 7, 0.24);
}}

.alert-green {{
    background: rgba(40, 167, 69, 0.08);
    border-color: rgba(40, 167, 69, 0.24);
}}

.heat-row {{
    display: grid;
    grid-template-columns: 120px minmax(220px, 1.6fr) minmax(180px, 1fr) 170px;
    align-items: center;
    gap: 14px;
    padding: 14px 16px;
    border-radius: 16px;
    margin-bottom: 12px;
    border: 1px solid rgba(255,255,255,0.06);
    background: rgba(255,255,255,0.02);
}}

.heat-tag {{
    width: 100%;
    min-width: 0;
    text-align: center;
    font-weight: 700;
    padding: 6px 10px;
    border-radius: 999px;
}}

.heat-topic {{
    min-width: 0;
    font-weight: 700;
    line-height: 1.45;
    word-break: break-word;
    overflow-wrap: anywhere;
}}

.heat-fill-wrap {{
    width: 100%;
    min-width: 0;
    height: 14px;
    background: rgba(255,255,255,0.08);
    border-radius: 999px;
    overflow: hidden;
}}

.heat-fill {{
    height: 100%;
    border-radius: 999px;
}}

.heat-meta {{
    min-width: 0;
    text-align: right;
    font-size: 0.92rem;
    opacity: 0.90;
    line-height: 1.45;
}}

.sidebar-user-card {{
    border-radius: 18px;
    padding: 14px;
    background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.015));
    border: 1px solid rgba(255,255,255,0.06);
    margin-bottom: 12px;
}}

.sidebar-user-name {{
    font-size: 1.10rem;
    font-weight: 700;
}}

div.stButton > button {{
    border-radius: 14px !important;
    border: 1px solid rgba(255,255,255,0.10) !important;
    min-height: 42px;
    font-weight: 600;
}}

div.stDownloadButton > button {{
    border-radius: 14px !important;
    min-height: 42px;
}}

[data-testid="stDataFrame"] {{
    border-radius: 18px;
    overflow: hidden;
    border: 1px solid rgba(255,255,255,0.06);
}}

@media (max-width: 980px) {{
    .v6-login-grid {{
        grid-template-columns: 1fr;
    }}
    .v6-login-brand {{
        border-right: none;
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }}
    .heat-row {{
        grid-template-columns: 1fr;
        align-items: stretch;
    }}
    .heat-meta {{
        text-align: left;
    }}
}}
</style>
"""
)


# =========================================================
# AUTH
# =========================================================
def get_user_by_username(username: str):
    row = fetch_one(
        """
        SELECT id, username, salt, password_hash, COALESCE(is_admin,0), COALESCE(is_active,1)
        FROM users
        WHERE username=?
        """,
        (username,)
    )
    if not row:
        return None
    return {
        "id": row[0],
        "username": row[1],
        "salt": row[2],
        "hash": row[3],
        "is_admin": int(row[4]),
        "is_active": int(row[5]),
    }


def create_user_account(username: str, password: str, is_admin: bool = False):
    username = (username or "").strip()

    if not valid_username(username):
        raise ValueError("Usuário deve ter 3 a 30 caracteres e usar apenas letras, números, ponto, hífen ou underline.")

    if not valid_password(password):
        raise ValueError("Senha deve ter pelo menos 4 caracteres.")

    if get_user_by_username(username):
        raise ValueError("Este usuário já existe.")

    salt = secrets.token_hex(16)
    pw_hash = hash_password(password, salt)

    uid = execute(
        """
        INSERT INTO users (username, salt, password_hash, is_admin, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, ?)
        """,
        (username, salt, pw_hash, 1 if is_admin else 0, now_str()),
    )
    return uid


def ensure_goal_row(user_id: int):
    row = fetch_one("SELECT id FROM goals WHERE user_id=?", (user_id,))
    if not row:
        execute(
            """
            INSERT INTO goals (
                user_id, daily_questions_goal, daily_minutes_goal,
                monthly_exams_goal, current_stage, created_at, updated_at
            )
            VALUES (?, 60, 180, 4, 'Intermediária', ?, ?)
            """,
            (user_id, now_str(), now_str()),
        )


def ensure_prefs_row(user_id: int):
    row = fetch_one("SELECT id FROM prefs WHERE user_id=?", (user_id,))
    if not row:
        execute(
            """
            INSERT INTO prefs (
                user_id, inactive_days_alert, drop_accuracy_alert, created_at, updated_at
            )
            VALUES (?, 7, 5.0, ?, ?)
            """,
            (user_id, now_str(), now_str()),
        )


def start_user_session(user_data: dict):
    st.session_state["auth_user"] = {
        "id": user_data["id"],
        "username": user_data["username"],
        "is_admin": user_data["is_admin"],
    }
    ensure_goal_row(user_data["id"])
    ensure_prefs_row(user_data["id"])
    init_navigation()


def login_box():
    left, center, right = st.columns([1, 1.2, 1])

    with center:
        st.markdown("<br>", unsafe_allow_html=True)
        st.title(f"🩺 {APP_NAME}")
        st.caption(APP_SUBTITLE)
        st.markdown("<br>", unsafe_allow_html=True)

        tabs = st.tabs(["Entrar", "Criar usuário"])

        with tabs[0]:
            with st.container(border=True):
                with st.form("login_form_v6", clear_on_submit=False):
                    username = st.text_input("Usuário", key="login_username")
                    password = st.text_input("Senha", type="password", key="login_password")
                    ok = st.form_submit_button("Entrar", use_container_width=True)

                if ok:
                    u = get_user_by_username(username.strip())
                    if not u:
                        st.error("Usuário não encontrado.")
                    elif not u["is_active"]:
                        st.error("Usuário inativo.")
                    elif not check_password(password, u["salt"], u["hash"]):
                        st.error("Senha incorreta.")
                    else:
                        start_user_session(u)
                        st.success("Login realizado com sucesso.")
                        st.rerun()

        with tabs[1]:
            with st.container(border=True):
                with st.form("create_account_form_v6", clear_on_submit=True):
                    new_username = st.text_input("Novo usuário", key="register_username")
                    new_password = st.text_input("Senha", type="password", key="register_password")
                    new_password_2 = st.text_input("Confirmar senha", type="password", key="register_password_2")
                    create_ok = st.form_submit_button("Criar conta", use_container_width=True)

                if create_ok:
                    try:
                        if new_password != new_password_2:
                            raise ValueError("As senhas não coincidem.")
                        uid = create_user_account(new_username, new_password, is_admin=False)
                        created = get_user_by_username(new_username.strip())
                        if created:
                            ensure_goal_row(created["id"])
                            ensure_prefs_row(created["id"])
                        st.success(f"Usuário criado com sucesso. ID {uid}. Agora faça login.")
                    except Exception as e:
                        st.error(f"Erro ao criar usuário: {e}")

        if DEFAULT_ADMIN_PASS == "admin123":
            st.warning("Senha padrão do administrador ainda ativa. Troque após entrar no sistema.")

def logout_button():
    if st.sidebar.button("Sair", use_container_width=True, key="sidebar_logout_btn_unique"):
        st.session_state.pop("auth_user", None)
        st.session_state.pop("main_menu", None)
        st.rerun()


# =========================================================
# BLOQUEIO DE ACESSO
# =========================================================
if "auth_user" not in st.session_state:
    login_box()
    st.stop()

user = st.session_state["auth_user"]
user_id = user["id"]
username = user["username"]
is_admin = bool(user.get("is_admin", 0))


# =========================================================
# BASE
# =========================================================
def get_subjects():
    rows = fetch_all("SELECT id, name FROM subjects ORDER BY name;")
    return [{"id": r[0], "name": r[1]} for r in rows]


def get_topics(subject_id: int):
    rows = fetch_all("SELECT id, name FROM topics WHERE subject_id=? ORDER BY name;", (subject_id,))
    return [{"id": r[0], "name": r[1]} for r in rows]


def subject_topic_picker(key_prefix=""):
    subjects = get_subjects()
    if not subjects:
        st.warning("Sem disciplinas cadastradas.")
        return None, None, None, None

    subj_names = [s["name"] for s in subjects]
    subj_idx = st.selectbox(
        "Disciplina",
        range(len(subj_names)),
        format_func=lambda i: subj_names[i],
        key=f"{key_prefix}_subj"
    )
    subject = subjects[subj_idx]

    topics = get_topics(subject["id"])
    topic_options = [{"id": None, "name": "(Sem tema específico)"}] + topics
    topic_names = [t["name"] for t in topic_options]
    topic_idx = st.selectbox(
        "Tema/Subtema",
        range(len(topic_names)),
        format_func=lambda i: topic_names[i],
        key=f"{key_prefix}_topic"
    )
    topic = topic_options[topic_idx]

    return subject["id"], subject["name"], topic["id"], topic["name"]
# =========================
# BLOCO 3/5 - TIMER + REGISTROS + DATAFRAMES + RESUMOS
# =========================

# =========================================================
# TIMER
# =========================================================
def timer_init():
    if "timer_running" not in st.session_state:
        st.session_state["timer_running"] = False
    if "timer_start_ts" not in st.session_state:
        st.session_state["timer_start_ts"] = None
    if "timer_accumulated" not in st.session_state:
        st.session_state["timer_accumulated"] = 0


def timer_current_seconds():
    acc = int(st.session_state.get("timer_accumulated", 0))
    if st.session_state.get("timer_running") and st.session_state.get("timer_start_ts") is not None:
        acc += int(time.time() - st.session_state["timer_start_ts"])
    return acc


def timer_start():
    if not st.session_state["timer_running"]:
        st.session_state["timer_running"] = True
        st.session_state["timer_start_ts"] = time.time()


def timer_pause():
    if st.session_state["timer_running"] and st.session_state["timer_start_ts"] is not None:
        elapsed = int(time.time() - st.session_state["timer_start_ts"])
        st.session_state["timer_accumulated"] += elapsed
        st.session_state["timer_running"] = False
        st.session_state["timer_start_ts"] = None


def timer_reset():
    st.session_state["timer_running"] = False
    st.session_state["timer_start_ts"] = None
    st.session_state["timer_accumulated"] = 0


def render_analog_timer(running: bool, start_ts, accumulated_seconds: int):
    start_ts_js = int(start_ts) if start_ts else 0
    running_js = "true" if running else "false"

    html_code = f"""
    <div style="display:flex;justify-content:center;align-items:center;">
      <div style="width:100%;max-width:560px;background:rgba(255,255,255,0.02);border:1px solid rgba(212,166,42,0.14);border-radius:24px;padding:18px;">
        <canvas id="analogClock" width="320" height="320" style="display:block;margin:0 auto;"></canvas>
        <div id="digitalTime" style="text-align:center;font-size:30px;font-weight:700;margin-top:8px;color:#F5F2E8;">00:00:00</div>
        <div style="text-align:center;font-size:13px;opacity:0.78;margin-top:6px;">alarme a cada 1 hora • som de clock enquanto roda</div>
      </div>
    </div>

    <script>
    const running = {running_js};
    const startTs = {start_ts_js};
    const accumulated = {int(accumulated_seconds)};

    const canvas = document.getElementById("analogClock");
    const ctx = canvas.getContext("2d");
    const radius = canvas.height / 2;
    ctx.translate(radius, radius);

    let audioCtx = null;
    let lastSecondBeep = -1;
    let lastHourAlarm = -1;

    function ensureAudio() {{
      try {{
        if (!audioCtx) {{
          audioCtx = new (window.AudioContext || window.webkitAudioContext)();
        }}
      }} catch (e) {{}}
    }}

    function beep(freq=900, duration=0.03, volume=0.01) {{
      try {{
        ensureAudio();
        if (!audioCtx) return;
        const osc = audioCtx.createOscillator();
        const gain = audioCtx.createGain();
        osc.connect(gain);
        gain.connect(audioCtx.destination);
        osc.frequency.value = freq;
        gain.gain.value = volume;
        osc.start();
        osc.stop(audioCtx.currentTime + duration);
      }} catch (e) {{}}
    }}

    function hourlyAlarm() {{
      beep(1200, 0.12, 0.04);
      setTimeout(() => beep(1400, 0.14, 0.04), 180);
      setTimeout(() => beep(1100, 0.16, 0.04), 380);
    }}

    function getElapsed() {{
      if (running && startTs > 0) {{
        return accumulated + Math.floor(Date.now() / 1000 - startTs);
      }}
      return accumulated;
    }}

    function pad(n) {{
      return String(n).padStart(2, "0");
    }}

    function drawFace(ctx, radius) {{
      ctx.beginPath();
      ctx.arc(0, 0, radius * 0.95, 0, 2 * Math.PI);
      ctx.fillStyle = "#0A1018";
      ctx.fill();

      const grad = ctx.createRadialGradient(0,0,radius*0.08,0,0,radius*0.98);
      grad.addColorStop(0, "#F2CC67");
      grad.addColorStop(0.5, "#D4A62A");
      grad.addColorStop(1, "#4B3603");
      ctx.strokeStyle = grad;
      ctx.lineWidth = radius * 0.06;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(0, 0, radius * 0.05, 0, 2 * Math.PI);
      ctx.fillStyle = "#D4A62A";
      ctx.fill();
    }}

    function drawNumbers(ctx, radius) {{
      ctx.font = radius * 0.14 + "px Arial";
      ctx.textBaseline = "middle";
      ctx.textAlign = "center";
      ctx.fillStyle = "#F4E2A3";
      for (let num = 1; num <= 12; num++) {{
        const ang = num * Math.PI / 6;
        ctx.rotate(ang);
        ctx.translate(0, -radius * 0.78);
        ctx.rotate(-ang);
        ctx.fillText(num.toString(), 0, 0);
        ctx.rotate(ang);
        ctx.translate(0, radius * 0.78);
        ctx.rotate(-ang);
      }}
    }}

    function drawTicks(ctx, radius) {{
      for (let i = 0; i < 60; i++) {{
        const ang = i * Math.PI / 30;
        ctx.beginPath();
        ctx.lineWidth = i % 5 === 0 ? 3 : 1;
        ctx.strokeStyle = i % 5 === 0 ? "#D4A62A" : "rgba(255,255,255,0.35)";
        const r1 = radius * (i % 5 === 0 ? 0.84 : 0.87);
        const r2 = radius * 0.92;
        ctx.moveTo(Math.cos(ang) * r1, Math.sin(ang) * r1);
        ctx.lineTo(Math.cos(ang) * r2, Math.sin(ang) * r2);
        ctx.stroke();
      }}
    }}

    function drawHand(ctx, pos, length, width, color) {{
      ctx.beginPath();
      ctx.lineWidth = width;
      ctx.lineCap = "round";
      ctx.strokeStyle = color;
      ctx.moveTo(0, 0);
      ctx.rotate(pos);
      ctx.lineTo(0, -length);
      ctx.stroke();
      ctx.rotate(-pos);
    }}

    function drawClock() {{
      const elapsed = getElapsed();
      const h = Math.floor(elapsed / 3600) % 12;
      const m = Math.floor((elapsed % 3600) / 60);
      const s = elapsed % 60;

      document.getElementById("digitalTime").innerText =
        `${{pad(Math.floor(elapsed / 3600))}}:${{pad(m)}}:${{pad(s)}}`;

      if (running && s !== lastSecondBeep) {{
        lastSecondBeep = s;
        beep(820, 0.018, 0.008);
      }}

      const wholeHours = Math.floor(elapsed / 3600);
      if (running && elapsed > 0 && elapsed % 3600 === 0 && lastHourAlarm !== wholeHours) {{
        lastHourAlarm = wholeHours;
        hourlyAlarm();
      }}

      ctx.clearRect(-radius, -radius, canvas.width, canvas.height);
      drawFace(ctx, radius);
      drawTicks(ctx, radius);
      drawNumbers(ctx, radius);

      const hour = (h * Math.PI / 6) + (m * Math.PI / (6 * 60)) + (s * Math.PI / (360 * 60));
      const minute = (m * Math.PI / 30) + (s * Math.PI / (30 * 60));
      const second = s * Math.PI / 30;

      drawHand(ctx, hour, radius * 0.48, radius * 0.045, "#F4E2A3");
      drawHand(ctx, minute, radius * 0.70, radius * 0.03, "#F5F5F5");
      drawHand(ctx, second, radius * 0.78, radius * 0.012, "#FF6B6B");
    }}

    setInterval(drawClock, 1000);
    drawClock();
    </script>
    """
    components.html(html_code, height=430)


# =========================================================
# REGISTROS
# =========================================================
def add_review(user_id: int, subject_id: int, topic_id, accuracy: float, origin_type: str, origin_id: int):
    days = compute_review_days(float(accuracy))
    due = (date.today() + timedelta(days=days)).isoformat()

    rid = execute(
        """
        INSERT INTO reviews (
            user_id, subject_id, topic_id, due_date, status,
            origin_type, origin_id, last_accuracy, created_at
        )
        VALUES (?, ?, ?, ?, 'PENDENTE', ?, ?, ?, ?)
        """,
        (user_id, subject_id, topic_id, due, origin_type, origin_id, float(accuracy), now_str())
    )

    audit(user_id, "CRIAR_REVISAO", "reviews", rid, f"venc={due}; acc={float(accuracy):.1f}; origem={origin_type}:{origin_id}")
    return rid, due, days


def add_question_log(user_id: int, subject_id: int, topic_id, tags: str, questions: int, correct: int, source: str, notes: str):
    questions = int(questions)
    correct = int(correct)

    if questions <= 0:
        raise ValueError("Número de questões deve ser maior que 0.")
    if correct < 0:
        raise ValueError("Acertos não podem ser negativos.")
    if correct > questions:
        raise ValueError("Acertos não podem ser maiores que o número de questões.")

    accuracy = (correct / questions) * 100.0

    qid = execute(
        """
        INSERT INTO question_logs (
            user_id, subject_id, topic_id, tags, questions, correct,
            accuracy, source, notes, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id, subject_id, topic_id,
            (tags or "").strip() or None,
            questions, correct, float(accuracy),
            (source or "").strip() or None,
            (notes or "").strip() or None,
            now_str()
        )
    )

    audit(user_id, "CRIAR_QUESTOES", "question_logs", qid, f"q={questions}; c={correct}; acc={accuracy:.1f}")
    add_review(user_id, subject_id, topic_id, accuracy, "QUESTOES", qid)
    return qid, accuracy


def add_exam(user_id: int, title: str, subject_id, total_questions: int, correct: int, duration_seconds: int, notes: str):
    title = (title or "").strip()
    total_questions = int(total_questions)
    correct = int(correct)
    duration_seconds = int(duration_seconds)

    if not title:
        raise ValueError("Título do simulado é obrigatório.")
    if total_questions <= 0:
        raise ValueError("Total de questões deve ser maior que 0.")
    if correct < 0:
        raise ValueError("Acertos não podem ser negativos.")
    if correct > total_questions:
        raise ValueError("Acertos não podem ser maiores que o total de questões.")

    accuracy = (correct / total_questions) * 100.0

    eid = execute(
        """
        INSERT INTO exams (
            user_id, title, subject_id, total_questions, correct,
            accuracy, duration_seconds, notes, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id, title, subject_id, total_questions, correct,
            float(accuracy), max(0, duration_seconds),
            (notes or "").strip() or None,
            now_str()
        )
    )

    audit(user_id, "CRIAR_SIMULADO", "exams", eid, f"titulo={title}; acc={accuracy:.1f}; dur={duration_seconds}")
    if subject_id is not None:
        add_review(user_id, subject_id, None, accuracy, "SIMULADO", eid)

    return eid, accuracy


def save_study_session(user_id: int, subject_id: int, topic_id, tags: str, duration_seconds: int, session_type: str, notes: str):
    duration_seconds = int(duration_seconds)
    session_type = (session_type or "ESTUDO").strip().upper()

    if duration_seconds <= 0:
        raise ValueError("A sessão deve ter duração maior que 0 segundos.")

    valid_types = ["ESTUDO", "REVISAO", "AULA", "LEITURA"]
    if session_type not in valid_types:
        session_type = "ESTUDO"

    sid = execute(
        """
        INSERT INTO study_sessions (
            user_id, subject_id, topic_id, tags, duration_seconds,
            session_type, notes, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id, subject_id, topic_id,
            (tags or "").strip() or None,
            duration_seconds, session_type,
            (notes or "").strip() or None,
            now_str()
        )
    )

    audit(user_id, "CRIAR_SESSAO", "study_sessions", sid, f"tipo={session_type}; seg={duration_seconds}")
    return sid


# =========================================================
# DATAFRAMES
# =========================================================
def df_question_logs(user_id: int, days_back: int = 60):
    since = (date.today() - timedelta(days=int(days_back))).isoformat()
    rows = fetch_all(
        """
        SELECT
            q.id, q.created_at, q.subject_id, q.topic_id,
            s.name, COALESCE(t.name, '(Sem tema)'),
            COALESCE(q.tags, ''), q.questions, q.correct, q.accuracy,
            COALESCE(q.source, ''), COALESCE(q.notes, '')
        FROM question_logs q
        JOIN subjects s ON s.id = q.subject_id
        LEFT JOIN topics t ON t.id = q.topic_id
        WHERE q.user_id=? AND DATE(q.created_at) >= DATE(?)
        ORDER BY q.created_at DESC
        """,
        (user_id, since)
    )

    df = pd.DataFrame(
        rows,
        columns=["id", "created_at", "subject_id", "topic_id", "subject", "topic", "tags", "questions", "correct", "accuracy", "source", "notes"]
    )

    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["date"] = df["created_at"].dt.date
        df["questions"] = pd.to_numeric(df["questions"], errors="coerce").fillna(0).astype(int)
        df["correct"] = pd.to_numeric(df["correct"], errors="coerce").fillna(0).astype(int)
        df["accuracy"] = pd.to_numeric(df["accuracy"], errors="coerce").fillna(0.0)
        df["topic_full"] = df["subject"].astype(str) + " • " + df["topic"].astype(str)

    return df


def df_study_sessions(user_id: int, days_back: int = 60):
    since = (date.today() - timedelta(days=int(days_back))).isoformat()
    rows = fetch_all(
        """
        SELECT
            ss.id, ss.created_at, ss.subject_id, ss.topic_id,
            s.name, COALESCE(t.name, '(Sem tema)'),
            COALESCE(ss.tags, ''), ss.duration_seconds, ss.session_type, COALESCE(ss.notes, '')
        FROM study_sessions ss
        JOIN subjects s ON s.id = ss.subject_id
        LEFT JOIN topics t ON t.id = ss.topic_id
        WHERE ss.user_id=? AND DATE(ss.created_at) >= DATE(?)
        ORDER BY ss.created_at DESC
        """,
        (user_id, since)
    )

    df = pd.DataFrame(
        rows,
        columns=["id", "created_at", "subject_id", "topic_id", "subject", "topic", "tags", "duration_seconds", "session_type", "notes"]
    )

    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["date"] = df["created_at"].dt.date
        df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0).astype(int)
        df["minutes"] = (df["duration_seconds"] / 60.0).round(1)
        df["topic_full"] = df["subject"].astype(str) + " • " + df["topic"].astype(str)

    return df


def df_exams(user_id: int, days_back: int = 180):
    since = (date.today() - timedelta(days=int(days_back))).isoformat()
    rows = fetch_all(
        """
        SELECT
            e.id, e.created_at, e.title, e.subject_id,
            COALESCE(s.name, '(Sem disciplina)'),
            e.total_questions, e.correct, e.accuracy, e.duration_seconds, COALESCE(e.notes, '')
        FROM exams e
        LEFT JOIN subjects s ON s.id = e.subject_id
        WHERE e.user_id=? AND DATE(e.created_at) >= DATE(?)
        ORDER BY e.created_at DESC
        """,
        (user_id, since)
    )

    df = pd.DataFrame(
        rows,
        columns=["id", "created_at", "title", "subject_id", "subject", "total_questions", "correct", "accuracy", "duration_seconds", "notes"]
    )

    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["date"] = df["created_at"].dt.date
        df["total_questions"] = pd.to_numeric(df["total_questions"], errors="coerce").fillna(0).astype(int)
        df["correct"] = pd.to_numeric(df["correct"], errors="coerce").fillna(0).astype(int)
        df["accuracy"] = pd.to_numeric(df["accuracy"], errors="coerce").fillna(0.0)
        df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0).astype(int)
        df["minutes"] = (df["duration_seconds"] / 60.0).round(1)
        df["week"] = df["created_at"].dt.to_period("W").astype(str)

    return df


# =========================================================
# AUX GERAIS
# =========================================================
def today_progress(user_id: int):
    d = today_str()

    q = fetch_one(
        """
        SELECT COALESCE(SUM(questions),0), COALESCE(SUM(correct),0)
        FROM question_logs
        WHERE user_id=? AND DATE(created_at)=DATE(?)
        """,
        (user_id, d)
    )
    qs, corr = int(q[0]), int(q[1])

    t = fetch_one(
        """
        SELECT COALESCE(SUM(duration_seconds),0)
        FROM study_sessions
        WHERE user_id=? AND DATE(created_at)=DATE(?)
        """,
        (user_id, d)
    )
    minutes = int(t[0]) / 60.0

    e = fetch_one(
        """
        SELECT COALESCE(COUNT(*),0)
        FROM exams
        WHERE user_id=? AND DATE(created_at)=DATE(?)
        """,
        (user_id, d)
    )
    exams_today = int(e[0])
    acc = (corr / qs * 100.0) if qs > 0 else None
    return qs, corr, acc, minutes, exams_today


def month_exam_count(user_id: int):
    first = date.today().replace(day=1).isoformat()
    row = fetch_one(
        """
        SELECT COALESCE(COUNT(*),0)
        FROM exams
        WHERE user_id=? AND DATE(created_at) >= DATE(?)
        """,
        (user_id, first)
    )
    return int(row[0])


def get_goals(user_id: int):
    row = fetch_one(
        "SELECT daily_questions_goal, daily_minutes_goal, monthly_exams_goal, current_stage FROM goals WHERE user_id=?",
        (user_id,)
    )
    if not row:
        return 60, 180, 4, "Intermediária"
    return int(row[0]), int(row[1]), int(row[2]), row[3]


def get_prefs(user_id: int):
    row = fetch_one("SELECT inactive_days_alert, drop_accuracy_alert FROM prefs WHERE user_id=?", (user_id,))
    if not row:
        return 7, 5.0
    return int(row[0]), float(row[1])


def set_goals(user_id: int, q_goal: int, min_goal: int, exams_goal: int, current_stage: str):
    execute(
        """
        UPDATE goals
        SET daily_questions_goal=?, daily_minutes_goal=?, monthly_exams_goal=?, current_stage=?, updated_at=?
        WHERE user_id=?
        """,
        (max(0, int(q_goal)), max(0, int(min_goal)), max(0, int(exams_goal)), current_stage, now_str(), user_id)
    )
    audit(user_id, "ATUALIZAR_METAS", "goals", None, f"q={q_goal}; min={min_goal}; sims={exams_goal}; etapa={current_stage}")


def set_prefs(user_id: int, inactive_days: int, drop_acc: float):
    execute(
        """
        UPDATE prefs
        SET inactive_days_alert=?, drop_accuracy_alert=?, updated_at=?
        WHERE user_id=?
        """,
        (max(1, int(inactive_days)), max(0.0, float(drop_acc)), now_str(), user_id)
    )
    audit(user_id, "ATUALIZAR_ALERTAS", "prefs", None, f"inativo={inactive_days}; queda={drop_acc}")


def suggestion_for_subject(subject_name: str, accuracy: float):
    accuracy = float(accuracy)
    if accuracy < 70:
        return "Aula completa + leitura dirigida + bloco de questões comentadas."
    if accuracy < 80:
        return "Leitura resumida + 20 questões + revisão leve."
    if accuracy < 85:
        return "Bloco extra de questões + revisão curta do tema."
    return "Manutenção com revisão espaçada e novo bloco prático."


def subject_strength_summary(user_id: int, days_back: int = 35):
    dfq = df_question_logs(user_id, days_back=days_back)
    empty_rank = pd.DataFrame(columns=["subject", "questions", "correct", "accuracy"])

    if dfq.empty:
        return {"best": None, "worst": None, "ranking": empty_rank}

    by_sub = dfq.groupby("subject", as_index=False)[["questions", "correct"]].sum()
    by_sub = by_sub[by_sub["questions"] > 0].copy()
    if by_sub.empty:
        return {"best": None, "worst": None, "ranking": empty_rank}

    by_sub["accuracy"] = (by_sub["correct"] / by_sub["questions"] * 100.0).round(1)
    ranking = by_sub.sort_values(["accuracy", "questions", "subject"], ascending=[False, False, True]).reset_index(drop=True)

    best = ranking.iloc[0].to_dict() if not ranking.empty else None
    worst = ranking.sort_values(["accuracy", "questions", "subject"], ascending=[True, False, True]).iloc[0].to_dict() if not ranking.empty else None

    return {"best": best, "worst": worst, "ranking": ranking}
# =========================
# BLOCO 4/5 - ALERTAS + MAPA + GRÁFICOS + PDF + LAYOUT FIXO
# =========================

# =========================================================
# ALERTAS / CONSISTÊNCIA / RESUMOS
# =========================================================
def study_consistency_summary(user_id: int, days_back: int = 30):
    days_back = max(1, int(days_back))
    start_date = date.today() - timedelta(days=days_back - 1)

    q_dates = fetch_all(
        """
        SELECT DISTINCT DATE(created_at)
        FROM question_logs
        WHERE user_id=? AND DATE(created_at) >= DATE(?)
        """,
        (user_id, start_date.isoformat())
    )

    s_dates = fetch_all(
        """
        SELECT DISTINCT DATE(created_at)
        FROM study_sessions
        WHERE user_id=? AND DATE(created_at) >= DATE(?)
        """,
        (user_id, start_date.isoformat())
    )

    e_dates = fetch_all(
        """
        SELECT DISTINCT DATE(created_at)
        FROM exams
        WHERE user_id=? AND DATE(created_at) >= DATE(?)
        """,
        (user_id, start_date.isoformat())
    )

    active_dates = set()
    for rows in [q_dates, s_dates, e_dates]:
        for r in rows:
            if r and r[0]:
                active_dates.add(r[0])

    active_days = len(active_dates)
    inactive_days = max(0, days_back - active_days)

    current_streak = 0
    probe = date.today()
    while probe.isoformat() in active_dates:
        current_streak += 1
        probe -= timedelta(days=1)

    longest_streak = 0
    running = 0
    for i in range(days_back):
        d = (start_date + timedelta(days=i)).isoformat()
        if d in active_dates:
            running += 1
            longest_streak = max(longest_streak, running)
        else:
            running = 0

    return {
        "active_days": active_days,
        "inactive_days": inactive_days,
        "current_streak": current_streak,
        "longest_streak": longest_streak,
        "consistency_pct": round((active_days / max(1, days_back)) * 100.0, 1),
    }


def alerts_summary(user_id: int):
    inactive_days, drop_acc = get_prefs(user_id)

    rows = fetch_all(
        """
        SELECT
            s.name,
            MAX(DATE(q.created_at)) AS last_q,
            MAX(DATE(ss.created_at)) AS last_study
        FROM subjects s
        LEFT JOIN question_logs q
            ON q.subject_id = s.id AND q.user_id = ?
        LEFT JOIN study_sessions ss
            ON ss.subject_id = s.id AND ss.user_id = ?
        GROUP BY s.id, s.name
        ORDER BY s.name
        """,
        (user_id, user_id)
    )

    stale = []
    today_dt = date.today()

    for name, last_q, last_study in rows:
        dates_found = []

        if last_q:
            try:
                dates_found.append(datetime.strptime(last_q, "%Y-%m-%d").date())
            except Exception:
                pass

        if last_study:
            try:
                dates_found.append(datetime.strptime(last_study, "%Y-%m-%d").date())
            except Exception:
                pass

        if not dates_found:
            stale.append((name, None))
            continue

        last_activity = max(dates_found)
        if (today_dt - last_activity).days >= inactive_days:
            stale.append((name, last_activity.isoformat()))

    dfq = df_question_logs(user_id, days_back=35)

    drop_msg = None
    weak_subjects = []
    strong_subjects = []

    if not dfq.empty:
        temp = dfq.copy()
        temp["d"] = pd.to_datetime(temp["created_at"], errors="coerce").dt.date
        cut = date.today() - timedelta(days=14)

        last = temp[temp["d"] >= cut].copy()
        prev = temp[(temp["d"] < cut) & (temp["d"] >= (cut - timedelta(days=14)))].copy()

        if not last.empty and not prev.empty:
            last_questions = last["questions"].sum()
            prev_questions = prev["questions"].sum()

            if last_questions > 0 and prev_questions > 0:
                last_acc = (last["correct"].sum() / last_questions) * 100.0
                prev_acc = (prev["correct"].sum() / prev_questions) * 100.0

                if (prev_acc - last_acc) >= drop_acc:
                    drop_msg = (prev_acc, last_acc)

        by_sub = temp.groupby("subject", as_index=False)[["questions", "correct"]].sum()
        by_sub = by_sub[by_sub["questions"] > 0].copy()

        if not by_sub.empty:
            by_sub["accuracy"] = (by_sub["correct"] / by_sub["questions"] * 100.0).round(1)

            weak_subjects = (
                by_sub.sort_values(["accuracy", "questions", "subject"], ascending=[True, False, True])
                .head(4)
                .to_dict("records")
            )

            strong_subjects = (
                by_sub.sort_values(["accuracy", "questions", "subject"], ascending=[False, False, True])
                .head(4)
                .to_dict("records")
            )

    overdue = fetch_one(
        """
        SELECT COUNT(*)
        FROM reviews
        WHERE user_id=? AND status='PENDENTE' AND DATE(due_date) < DATE(?)
        """,
        (user_id, today_str())
    )

    due_today = fetch_one(
        """
        SELECT COUNT(*)
        FROM reviews
        WHERE user_id=? AND status='PENDENTE' AND DATE(due_date) = DATE(?)
        """,
        (user_id, today_str())
    )

    return {
        "stale": stale,
        "drop_msg": drop_msg,
        "weak_subjects": weak_subjects,
        "strong_subjects": strong_subjects,
        "overdue_reviews": int(overdue[0] or 0),
        "due_today_reviews": int(due_today[0] or 0),
    }


def build_priority_actions(user_id: int):
    alerts = alerts_summary(user_id)
    actions = []

    if alerts["overdue_reviews"] > 0:
        actions.append({
            "priority": "MAX",
            "title": f"Revisar {alerts['overdue_reviews']} revisões vencidas",
            "desc": "Comece pelas revisões pendentes antes de abrir novos blocos."
        })

    if alerts["due_today_reviews"] > 0:
        actions.append({
            "priority": "ALTA",
            "title": f"Fazer {alerts['due_today_reviews']} revisões agendadas para hoje",
            "desc": "Mantenha a fila de revisão controlada."
        })

    if alerts["weak_subjects"]:
        ws = alerts["weak_subjects"][0]
        actions.append({
            "priority": "ALTA",
            "title": f"Reforçar {ws['subject']}",
            "desc": suggestion_for_subject(ws["subject"], ws["accuracy"])
        })

    if alerts["stale"]:
        stale_name = alerts["stale"][0][0]
        actions.append({
            "priority": "MÉDIA",
            "title": f"Retomar {stale_name}",
            "desc": "Disciplina sem registro recente. Faça um bloco curto hoje."
        })

    if alerts["strong_subjects"]:
        ss = alerts["strong_subjects"][0]
        actions.append({
            "priority": "OK",
            "title": f"Manter {ss['subject']}",
            "desc": "Melhor desempenho recente. Faça apenas manutenção e revisão espaçada."
        })

    if not actions:
        actions.append({
            "priority": "OK",
            "title": "Manter rotina de alto desempenho",
            "desc": "Fila controlada e sem alertas críticos."
        })

    return actions[:5]


def build_cronograma_df():
    subjects = [s["name"] for s in get_subjects()]
    rows = []

    for stage_name, min_q, min_acc in PREP_STAGES:
        for subj in subjects:
            bonus = 0
            if subj in ("Clínica Médica", "Cirurgia"):
                bonus = 15
            elif subj in ("Pediatria", "Ginecologia e Obstetrícia"):
                bonus = 8

            rows.append({
                "Etapa": stage_name,
                "Disciplina": subj,
                "Questões mínimas/semana": min_q + bonus,
                "% mínimo de acerto": min_acc,
                "Ação se abaixo da meta": "Aula + leitura + novo bloco de questões",
            })

    return pd.DataFrame(rows)


# =========================================================
# MAPA DE RENDIMENTOS
# =========================================================
def topic_performance_df(user_id: int, days_back: int = 180):
    dfq = df_question_logs(user_id, days_back=days_back)

    empty_df = pd.DataFrame(
        columns=["subject", "topic", "questions", "correct", "accuracy", "level", "topic_full"]
    )

    if dfq.empty:
        return empty_df

    grp = dfq.groupby(["subject", "topic"], as_index=False)[["questions", "correct"]].sum()
    grp = grp[grp["questions"] > 0].copy()

    if grp.empty:
        return empty_df

    grp["accuracy"] = (grp["correct"] / grp["questions"] * 100.0).round(1)
    grp["topic_full"] = grp["subject"].astype(str) + " • " + grp["topic"].astype(str)

    def classify(acc):
        if acc >= 90:
            return "EXCELENTE"
        if acc >= 80:
            return "BOM"
        if acc >= 70:
            return "ATENÇÃO"
        return "CRÍTICO"

    grp["level"] = grp["accuracy"].apply(classify)
    grp = grp.sort_values(["accuracy", "questions", "topic_full"], ascending=[False, False, True]).reset_index(drop=True)
    return grp


def render_heat_rows(df: pd.DataFrame, title: str, top_n: int = 10, best: bool = True):
    st.markdown(f"### {title}")

    if df.empty:
        st.info("Sem dados suficientes para exibir rendimento por tema/subtema.")
        return

    data = df.sort_values("accuracy", ascending=not best).head(top_n).copy()

    for _, r in data.iterrows():
        acc = float(r["accuracy"])
        q = int(r["questions"])
        corr = int(r["correct"])

        if acc >= 90:
            fill = "#2FBF71"
            label = "EXCELENTE"
            tag_bg = "#2FBF71"
        elif acc >= 80:
            fill = "#86C232"
            label = "BOM"
            tag_bg = "#86C232"
        elif acc >= 70:
            fill = "#D4A62A"
            label = "ATENÇÃO"
            tag_bg = "#D4A62A"
        else:
            fill = "#FF6B6B"
            label = "CRÍTICO"
            tag_bg = "#FF6B6B"

        width = max(6, min(100, acc))

        st.markdown(
            f"""
            <div class="heat-row">
                <div class="heat-tag" style="background:{tag_bg}20;border:1px solid {tag_bg}55;">{label}</div>
                <div class="heat-topic">{esc(r["topic_full"])}</div>
                <div class="heat-fill-wrap">
                    <div class="heat-fill" style="width:{width}%; background:{fill};"></div>
                </div>
                <div class="heat-meta">{acc:.1f}% • {corr}/{q} acertos</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# GRÁFICOS
# =========================================================
def plot_questions_chart(dfq: pd.DataFrame):
    fig = plt.figure(figsize=(8, 4.4))

    if dfq.empty:
        plt.text(0.5, 0.5, "Sem dados no período", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    temp = dfq.copy()
    temp["date"] = pd.to_datetime(temp["date"], errors="coerce")
    by_day = temp.groupby("date", as_index=False)[["questions", "correct"]].sum()

    plt.plot(by_day["date"], by_day["questions"], linewidth=2.2, marker="o")
    plt.title("Questões por dia")
    plt.ylabel("Questões")
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig


def plot_questions_accuracy_chart(dfq: pd.DataFrame):
    fig = plt.figure(figsize=(8, 4.4))

    if dfq.empty:
        plt.text(0.5, 0.5, "Sem dados no período", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    temp = dfq.copy()
    temp["date"] = pd.to_datetime(temp["date"], errors="coerce")
    by_day = temp.groupby("date", as_index=False)[["questions", "correct"]].sum()
    by_day = by_day[by_day["questions"] > 0].copy()

    if by_day.empty:
        plt.text(0.5, 0.5, "Sem dados válidos no período", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    by_day["accuracy"] = (by_day["correct"] / by_day["questions"]) * 100.0

    plt.plot(by_day["date"], by_day["accuracy"], linewidth=2.2, marker="o")
    plt.axhline(80, linestyle="--", linewidth=1)
    plt.axhline(85, linestyle=":", linewidth=1)
    plt.axhline(90, linestyle="-.", linewidth=1)
    plt.title("% de acerto por dia")
    plt.ylabel("%")
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig


def plot_exam_chart(dfe: pd.DataFrame):
    fig = plt.figure(figsize=(8, 4.4))

    if dfe.empty:
        plt.text(0.5, 0.5, "Sem simulados no período", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    order = dfe.sort_values("created_at").copy()
    x = list(range(1, len(order) + 1))

    plt.plot(x, order["accuracy"], marker="o", linewidth=2.2)
    plt.axhline(80, linestyle="--", linewidth=1)
    plt.axhline(85, linestyle=":", linewidth=1)
    plt.axhline(90, linestyle="-.", linewidth=1)
    plt.title("Aproveitamento dos simulados")
    plt.xlabel("Ordem cronológica")
    plt.ylabel("%")
    plt.tight_layout()
    return fig


def plot_subject_performance(dfq: pd.DataFrame):
    fig = plt.figure(figsize=(8, 4.4))

    if dfq.empty:
        plt.text(0.5, 0.5, "Sem dados por disciplina", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    by_sub = dfq.groupby("subject", as_index=False)[["questions", "correct"]].sum()
    by_sub = by_sub[by_sub["questions"] > 0].copy()

    if by_sub.empty:
        plt.text(0.5, 0.5, "Sem dados válidos por disciplina", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    by_sub["accuracy"] = (by_sub["correct"] / by_sub["questions"]) * 100.0
    by_sub = by_sub.sort_values("accuracy", ascending=True)

    plt.barh(by_sub["subject"], by_sub["accuracy"])
    plt.xlim(0, 100)
    plt.title("Desempenho por disciplina")
    plt.tight_layout()
    return fig


def plot_weekly_ranking(dfq: pd.DataFrame):
    fig = plt.figure(figsize=(8, 4.4))

    if dfq.empty:
        plt.text(0.5, 0.5, "Sem dados para ranking semanal", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    temp = dfq.copy()
    temp["week"] = pd.to_datetime(temp["created_at"], errors="coerce").dt.to_period("W").astype(str)
    last_week = temp["week"].max()
    wk = temp[temp["week"] == last_week].copy()

    if wk.empty:
        plt.text(0.5, 0.5, "Sem dados na última semana", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    by_sub = wk.groupby("subject", as_index=False)[["questions", "correct"]].sum()
    by_sub = by_sub[by_sub["questions"] > 0].copy()

    if by_sub.empty:
        plt.text(0.5, 0.5, "Sem dados válidos na última semana", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    by_sub["accuracy"] = (by_sub["correct"] / by_sub["questions"]) * 100.0
    by_sub["score"] = by_sub["accuracy"] * 0.7 + by_sub["questions"] * 0.3
    by_sub = by_sub.sort_values("score", ascending=False).head(8)

    plt.bar(by_sub["subject"], by_sub["score"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Ranking semanal das disciplinas ({last_week})")
    plt.tight_layout()
    return fig


def plot_heatmap_study(dfs: pd.DataFrame):
    fig = plt.figure(figsize=(10, 4.4))

    if dfs.empty:
        plt.text(0.5, 0.5, "Sem dados para mapa de calor", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    temp = dfs.copy()
    temp["dt"] = pd.to_datetime(temp["created_at"], errors="coerce")
    temp = temp.dropna(subset=["dt"]).copy()

    if temp.empty:
        plt.text(0.5, 0.5, "Sem dados válidos para mapa de calor", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        return fig

    temp["weekday"] = temp["dt"].dt.weekday
    temp["hour"] = temp["dt"].dt.hour

    pivot = temp.pivot_table(
        index="weekday",
        columns="hour",
        values="duration_seconds",
        aggfunc="sum",
        fill_value=0
    )

    for h in range(24):
        if h not in pivot.columns:
            pivot[h] = 0

    pivot = pivot.reindex(index=[0, 1, 2, 3, 4, 5, 6], fill_value=0)
    pivot = pivot[[h for h in range(24)]]
    pivot = pivot / 60.0

    cmap = LinearSegmentedColormap.from_list(
        "goldmap",
        ["#050608", "#6E5716", "#D4A62A"]
    )

    plt.imshow(pivot.values, aspect="auto", cmap=cmap)
    plt.colorbar(label="Minutos")
    plt.title("Mapa de calor do estudo")
    plt.xlabel("Hora do dia")
    plt.ylabel("Dia da semana")
    plt.yticks(ticks=range(7), labels=["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"])
    plt.xticks(ticks=range(24), labels=[str(h) for h in range(24)])
    plt.tight_layout()
    return fig


def fig_to_png_bytes(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=180)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# =========================================================
# FILTROS / PDF
# =========================================================
def subject_filter_ui(dfq: pd.DataFrame, dfe: pd.DataFrame):
    subjects_all = sorted(list(
        set(dfq["subject"].unique().tolist() if not dfq.empty else []) |
        set(dfe["subject"].unique().tolist() if not dfe.empty else [])
    ))
    selected = st.multiselect(
        "Filtrar disciplinas",
        options=subjects_all,
        default=subjects_all
    )
    return selected


def apply_subject_filter(dfq: pd.DataFrame, dfe: pd.DataFrame, dfs: pd.DataFrame, selected_subjects):
    if selected_subjects:
        if not dfq.empty:
            dfq = dfq[dfq["subject"].isin(selected_subjects)].copy()
        if not dfe.empty:
            dfe = dfe[dfe["subject"].isin(selected_subjects)].copy()
        if not dfs.empty:
            dfs = dfs[dfs["subject"].isin(selected_subjects)].copy()
    return dfq, dfe, dfs


def draw_pdf_logo(c, width, height):
    p = locate_logo_path()
    if p:
        try:
            img = ImageReader(p)
            c.drawImage(
                img,
                1.6 * cm,
                height - 3.0 * cm,
                width=2.8 * cm,
                height=2.8 * cm,
                preserveAspectRatio=True,
                mask="auto"
            )
        except Exception:
            pass


def draw_wrapped_text(c, text, x, y, max_width, line_height=12, font_name="Helvetica", font_size=10):
    c.setFont(font_name, font_size)
    words = str(text or "").split()
    lines = []
    current = ""

    for word in words:
        test = word if not current else current + " " + word
        if c.stringWidth(test, font_name, font_size) <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    for line in lines:
        c.drawString(x, y, line)
        y -= line_height

    return y


def generate_pdf_report(user_id: int, username: str, date_from: date, date_to: date):
    dfq = df_question_logs(user_id, days_back=3650)
    dfs = df_study_sessions(user_id, days_back=3650)
    dfe = df_exams(user_id, days_back=3650)

    if not dfq.empty:
        dfq["date_only"] = pd.to_datetime(dfq["created_at"], errors="coerce").dt.date
        dfq = dfq[(dfq["date_only"] >= date_from) & (dfq["date_only"] <= date_to)].copy()

    if not dfs.empty:
        dfs["date_only"] = pd.to_datetime(dfs["created_at"], errors="coerce").dt.date
        dfs = dfs[(dfs["date_only"] >= date_from) & (dfs["date_only"] <= date_to)].copy()

    if not dfe.empty:
        dfe["date_only"] = pd.to_datetime(dfe["created_at"], errors="coerce").dt.date
        dfe = dfe[(dfe["date_only"] >= date_from) & (dfe["date_only"] <= date_to)].copy()

    q_goal, min_goal, exams_goal, current_stage = get_goals(user_id)
    alerts = alerts_summary(user_id)
    consistency = study_consistency_summary(user_id, days_back=30)
    priorities = build_priority_actions(user_id)
    subj_strength = subject_strength_summary(user_id, days_back=35)

    total_q = int(dfq["questions"].sum()) if not dfq.empty else 0
    total_c = int(dfq["correct"].sum()) if not dfq.empty else 0
    acc = (total_c / total_q * 100.0) if total_q > 0 else 0.0
    total_minutes = float(dfs["duration_seconds"].sum() / 60.0) if not dfs.empty else 0.0
    total_exams = int(len(dfe)) if not dfe.empty else 0
    avg_exam_acc = float(dfe["accuracy"].mean()) if not dfe.empty else 0.0

    best_subj = subj_strength["best"]
    worst_subj = subj_strength["worst"]

    fig_q = plot_questions_chart(dfq)
    fig_acc = plot_questions_accuracy_chart(dfq)
    fig_exam = plot_exam_chart(dfe)
    fig_sub = plot_subject_performance(dfq)
    fig_rank = plot_weekly_ranking(dfq)
    fig_heat = plot_heatmap_study(dfs)

    img_q = fig_to_png_bytes(fig_q)
    img_acc = fig_to_png_bytes(fig_acc)
    img_exam = fig_to_png_bytes(fig_exam)
    img_sub = fig_to_png_bytes(fig_sub)
    img_rank = fig_to_png_bytes(fig_rank)
    img_heat = fig_to_png_bytes(fig_heat)

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    def header(title: str, subtitle: str):
        draw_pdf_logo(c, width, height)
        c.setFont("Helvetica-Bold", 18)
        c.drawString(4.8 * cm, height - 1.8 * cm, APP_NAME)
        c.setFont("Helvetica", 10)
        c.drawString(4.8 * cm, height - 2.35 * cm, APP_SUBTITLE)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(2 * cm, height - 3.5 * cm, title)
        c.setFont("Helvetica", 10)
        c.drawString(2 * cm, height - 4.0 * cm, subtitle)
        c.line(2 * cm, height - 4.25 * cm, width - 2 * cm, height - 4.25 * cm)

    def footer():
        c.setFont("Helvetica", 9)
        c.drawString(2 * cm, 1.2 * cm, f"Gerado em {now_str()} • Usuário: {username}")
        c.drawRightString(width - 2 * cm, 1.2 * cm, APP_NAME)

    header("Diagnóstico situacional", f"Período: {date_from.isoformat()} a {date_to.isoformat()}")
    y = height - 5.0 * cm

    c.setFont("Helvetica-Bold", 12)
    c.drawString(2 * cm, y, "Resumo executivo")
    y -= 0.7 * cm

    c.setFont("Helvetica", 11)
    lines = [
        f"Etapa da preparação: {current_stage}",
        f"Questões realizadas: {total_q} | Acertos: {total_c} | Aproveitamento: {acc:.1f}%",
        f"Tempo estudado: {total_minutes:.1f} minutos",
        f"Simulados: {total_exams} | Média dos simulados: {avg_exam_acc:.1f}%",
        f"Metas atuais: {q_goal} questões/dia | {min_goal} min/dia | {exams_goal} simulados/mês",
        f"Consistência 30d: {consistency['consistency_pct']:.1f}% | Dias ativos: {consistency['active_days']} | Sequência atual: {consistency['current_streak']}",
    ]

    if best_subj:
        lines.append(f"Melhor disciplina recente: {best_subj['subject']} ({float(best_subj['accuracy']):.1f}%)")
    if worst_subj:
        lines.append(f"Pior disciplina recente: {worst_subj['subject']} ({float(worst_subj['accuracy']):.1f}%)")

    for line in lines:
        c.drawString(2 * cm, y, line)
        y -= 0.55 * cm

    y -= 0.2 * cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2 * cm, y, "Prioridades")
    y -= 0.65 * cm

    for item in priorities[:4]:
        txt = f"[{item['priority']}] {item['title']} — {item['desc']}"
        y = draw_wrapped_text(c, "• " + txt, 2.2 * cm, y, width - 4.3 * cm, line_height=12, font_size=10)
        y -= 0.08 * cm

    y -= 0.2 * cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2 * cm, y, "Alertas")
    y -= 0.65 * cm

    alert_lines = []
    if alerts["overdue_reviews"] > 0:
        alert_lines.append(f"Revisões vencidas: {alerts['overdue_reviews']}")
    if alerts["due_today_reviews"] > 0:
        alert_lines.append(f"Revisões para hoje: {alerts['due_today_reviews']}")
    if alerts["weak_subjects"]:
        ws = alerts["weak_subjects"][0]
        alert_lines.append(f"Pior disciplina atual: {ws['subject']} ({ws['accuracy']:.1f}%)")
    if alerts["strong_subjects"]:
        ss = alerts["strong_subjects"][0]
        alert_lines.append(f"Melhor disciplina atual: {ss['subject']} ({ss['accuracy']:.1f}%)")
    if not alert_lines:
        alert_lines.append("Sem alertas críticos no período.")

    for line in alert_lines:
        y = draw_wrapped_text(c, "• " + line, 2.2 * cm, y, width - 4.3 * cm, line_height=12, font_size=10)
        y -= 0.08 * cm

    footer()
    c.showPage()

    header("Gráficos de questões", "Produção e aproveitamento")
    c.drawImage(ImageReader(BytesIO(img_q)), 2 * cm, height - 13.3 * cm, width=16.5 * cm, height=5.7 * cm, preserveAspectRatio=True, mask="auto")
    c.drawImage(ImageReader(BytesIO(img_acc)), 2 * cm, height - 20.0 * cm, width=16.5 * cm, height=5.7 * cm, preserveAspectRatio=True, mask="auto")
    footer()
    c.showPage()

    header("Gráficos de simulados e desempenho", "Simulados, disciplinas e ranking")
    c.drawImage(ImageReader(BytesIO(img_exam)), 2 * cm, height - 11.7 * cm, width=16.5 * cm, height=5.2 * cm, preserveAspectRatio=True, mask="auto")
    c.drawImage(ImageReader(BytesIO(img_sub)), 2 * cm, height - 18.2 * cm, width=16.5 * cm, height=5.2 * cm, preserveAspectRatio=True, mask="auto")
    footer()
    c.showPage()

    header("Ranking e mapa de calor", "Consistência semanal e padrão de estudo")
    c.drawImage(ImageReader(BytesIO(img_rank)), 2 * cm, height - 11.5 * cm, width=16.5 * cm, height=5.1 * cm, preserveAspectRatio=True, mask="auto")
    c.drawImage(ImageReader(BytesIO(img_heat)), 2 * cm, height - 18.0 * cm, width=16.5 * cm, height=5.1 * cm, preserveAspectRatio=True, mask="auto")
    footer()
    c.showPage()

    c.save()
    buffer.seek(0)
    return buffer.read()


def weekly_ranking_df(dfq: pd.DataFrame):
    if dfq.empty:
        return pd.DataFrame()

    temp = dfq.copy()
    temp["week"] = pd.to_datetime(temp["created_at"], errors="coerce").dt.to_period("W").astype(str)
    last_week = temp["week"].max()
    wk = temp[temp["week"] == last_week].copy()

    if wk.empty:
        return pd.DataFrame()

    by_sub = wk.groupby("subject", as_index=False)[["questions", "correct"]].sum()
    by_sub = by_sub[by_sub["questions"] > 0].copy()

    if by_sub.empty:
        return pd.DataFrame()

    by_sub["accuracy"] = (by_sub["correct"] / by_sub["questions"]) * 100.0
    by_sub["score"] = by_sub["accuracy"] * 0.7 + by_sub["questions"] * 0.3
    by_sub = by_sub.sort_values("score", ascending=False).reset_index(drop=True)
    by_sub["ranking"] = range(1, len(by_sub) + 1)
    by_sub["week"] = last_week

    return by_sub[["ranking", "subject", "questions", "accuracy", "score", "week"]]


# =========================================================
# SIDEBAR / TOPO - APENAS UMA VEZ
# =========================================================
init_navigation()

if logo_b64:
    st.sidebar.markdown(
        f"""
        <div style="text-align:center; margin-bottom:12px;">
            <img src="data:image/jpeg;base64,{logo_b64}"
                 style="width:100%; max-width:210px; border-radius:20px; box-shadow:0 14px 30px rgba(0,0,0,0.30); border:1px solid rgba(212,166,42,0.18);" />
        </div>
        """,
        unsafe_allow_html=True,
    )

st.sidebar.markdown(
    f"""
    <div class="sidebar-user-card">
        <div class="sidebar-user-name">👤 {esc(username)}</div>
        <div style="opacity:0.75; margin-top:4px;">{"Perfil: Administrador" if is_admin else "Perfil: Usuário"}</div>
        <div style="margin-top:8px;">
            <span class="badge-v6">{APP_VERSION}</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

logout_button()

menu = st.sidebar.radio(
    "Menu",
    MENU_ITEMS,
    index=MENU_ITEMS.index(st.session_state["main_menu"]) if st.session_state["main_menu"] in MENU_ITEMS else 0,
    key="sidebar_menu_radio_v6_unique"
)
st.session_state["main_menu"] = menu

hero_left, hero_right = st.columns([1.25, 1])

with hero_left:
    st.title(f"🩺 {APP_NAME}")
    st.caption(APP_SUBTITLE)

    info_cols = st.columns(4)
    with info_cols[0]:
        st.info(APP_VERSION)
    with info_cols[1]:
        st.info(f"Usuário: {username}")
    with info_cols[2]:
        st.info(date.today().strftime("%d/%m/%Y"))
    with info_cols[3]:
        st.info("Administrador" if is_admin else "Padrão")

with hero_right:
    st.empty()

# =========================
# BLOCO 5/5 - PÁGINAS
# =========================

# =========================================================
# PAGE: COCKPIT
# =========================================================
if menu == "Cockpit":
    timer_init()

    qs, corr, acc_today, min_today, exams_today = today_progress(user_id)
    q_goal, min_goal, exams_goal, current_stage = get_goals(user_id)
    exams_month = month_exam_count(user_id)
    alerts = alerts_summary(user_id)
    subj_strength = subject_strength_summary(user_id, days_back=35)
    consistency = study_consistency_summary(user_id, days_back=30)
    priorities = build_priority_actions(user_id)

    dfq_30 = df_question_logs(user_id, 30)
    dfs_30 = df_study_sessions(user_id, 30)
    dfe_90 = df_exams(user_id, 90)

    q30 = int(dfq_30["questions"].sum()) if not dfq_30.empty else 0
    c30 = int(dfq_30["correct"].sum()) if not dfq_30.empty else 0
    acc30 = (c30 / q30 * 100.0) if q30 > 0 else 0.0
    sim_avg = float(dfe_90["accuracy"].mean()) if not dfe_90.empty else 0.0

    best_subj = subj_strength["best"]
    worst_subj = subj_strength["worst"]

    a1, a2, a3, a4, a5 = st.columns(5)
    with a1:
        metric_card("Questões hoje", str(qs), f"Meta: {q_goal}")
    with a2:
        metric_card("% hoje", f"{acc_today:.1f}%" if acc_today is not None else "—", "Aproveitamento")
    with a3:
        metric_card("Tempo hoje", f"{min_today:.1f} min", f"Meta: {min_goal} min")
    with a4:
        metric_card("Questões 30d", str(q30), f"Acerto: {acc30:.1f}%")
    with a5:
        metric_card("Média simulados", f"{sim_avg:.1f}%", f"Mês: {exams_month}/{exams_goal}")

    st.markdown("<hr class='soft-hr'/>", unsafe_allow_html=True)

    if alerts["overdue_reviews"] > 0:
        st.markdown(
            f"<div class='alert-box alert-red'><b>Revisões vencidas:</b> {alerts['overdue_reviews']} pendências. Priorize revisão hoje.</div>",
            unsafe_allow_html=True,
        )
    elif alerts["due_today_reviews"] > 0:
        st.markdown(
            f"<div class='alert-box alert-yellow'><b>Revisões de hoje:</b> {alerts['due_today_reviews']} itens programados.</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div class='alert-box alert-green'><b>Revisões:</b> fila controlada no momento.</div>",
            unsafe_allow_html=True,
        )

    if worst_subj:
        st.markdown(
            f"<div class='alert-box alert-yellow'><b>Pior disciplina atual:</b> {esc(worst_subj['subject'])} ({float(worst_subj['accuracy']):.1f}%). Sugestão: {esc(suggestion_for_subject(worst_subj['subject'], float(worst_subj['accuracy'])))} </div>",
            unsafe_allow_html=True,
        )

    if best_subj:
        st.markdown(
            f"<div class='alert-box alert-green'><b>Melhor disciplina atual:</b> {esc(best_subj['subject'])} ({float(best_subj['accuracy']):.1f}%). Estratégia: manutenção com revisão espaçada e novos blocos práticos.</div>",
            unsafe_allow_html=True,
        )

    left, right = st.columns([1.15, 1])

    with left:
        st.markdown("<div class='v6-card'>", unsafe_allow_html=True)
        st.markdown("### Central de comando")

        n1, n2, n3, n4 = st.columns(4)
        with n1:
            if st.button("📅 Hoje", use_container_width=True, key="goto_today"):
                goto_page("Hoje")
        with n2:
            if st.button("✍️ Registrar", use_container_width=True, key="goto_registrar"):
                goto_page("Registrar")
        with n3:
            if st.button("📊 Dashboard", use_container_width=True, key="goto_dashboard"):
                goto_page("Dashboard")
        with n4:
            if st.button("🗂️ Revisões", use_container_width=True, key="goto_reviews"):
                goto_page("Revisões")

        n5, n6, n7, n8 = st.columns(4)
        with n5:
            if st.button("🔥 Rendimentos", use_container_width=True, key="goto_heat_page"):
                goto_page("Mapa de Rendimentos")
        with n6:
            if st.button("🎯 Metas", use_container_width=True, key="goto_goals"):
                goto_page("Metas & Alertas")
        with n7:
            if st.button("🧾 PDF", use_container_width=True, key="goto_pdf"):
                goto_page("Relatórios (PDF)")
        with n8:
            if st.button("🗓️ Cronograma", use_container_width=True, key="goto_cronograma"):
                goto_page("Cronograma")

        st.markdown("<hr class='soft-hr'/>", unsafe_allow_html=True)
        st.write(f"**Etapa atual:** {current_stage}")
        st.write(f"**Meta diária:** {q_goal} questões • {min_goal} min")
        st.write(f"**Meta mensal de simulados:** {exams_goal}")
        st.write(f"**Cumprimento do mês:** {exams_month}/{exams_goal}")
        st.write(f"**Consistência 30d:** {consistency['consistency_pct']:.1f}%")
        st.write(f"**Sequência atual:** {consistency['current_streak']} dia(s)")
        st.write(f"**Melhor sequência 30d:** {consistency['longest_streak']} dia(s)")

        prog_q = min((qs / q_goal), 1.0) if q_goal > 0 else 0
        prog_t = min((min_today / min_goal), 1.0) if min_goal > 0 else 0
        prog_e = min((exams_month / exams_goal), 1.0) if exams_goal > 0 else 0

        st.write("Questões do dia")
        st.progress(prog_q)
        st.write("Tempo do dia")
        st.progress(prog_t)
        st.write("Simulados do mês")
        st.progress(prog_e)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='v6-card'>", unsafe_allow_html=True)
        st.markdown("### O que fazer agora")
        for item in priorities:
            st.markdown(f"**{item['priority']} • {item['title']}**")
            st.caption(item["desc"])
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Visão rápida")
    v1, v2, v3 = st.columns(3)

    with v1:
        st.markdown("<div class='v6-card'>", unsafe_allow_html=True)
        st.markdown("### Resumo de desempenho")
        st.write(f"**Acerto em 30 dias:** {acc30:.1f}%")
        st.write(f"**Simulados hoje:** {exams_today}")
        st.write(f"**Questões em 30 dias:** {q30}")
        if not dfs_30.empty:
            st.write(f"**Tempo estudado em 30 dias:** {dfs_30['minutes'].sum():.1f} min")
        else:
            st.write("**Tempo estudado em 30 dias:** 0.0 min")
        st.markdown("</div>", unsafe_allow_html=True)

    with v2:
        st.markdown("<div class='v6-card'>", unsafe_allow_html=True)
        st.markdown("### Melhor e pior matéria")
        if best_subj:
            st.write(f"**Melhor matéria (35d):** {best_subj['subject']} ({float(best_subj['accuracy']):.1f}%)")
        if worst_subj:
            st.write(f"**Pior matéria (35d):** {worst_subj['subject']} ({float(worst_subj['accuracy']):.1f}%)")
        if not best_subj and not worst_subj:
            st.info("Sem dados suficientes.")
        st.markdown("</div>", unsafe_allow_html=True)

    with v3:
        st.markdown("<div class='v6-card'>", unsafe_allow_html=True)
        st.markdown("### Disciplinas paradas")
        stale = alerts["stale"][:4]
        if stale:
            for name, last in stale:
                st.write(f"- {name} ({'sem histórico' if last is None else 'último registro: ' + last})")
        else:
            st.write("Nenhuma disciplina parada pelo critério atual.")
        st.markdown("</div>", unsafe_allow_html=True)

    g1, g2 = st.columns(2)
    with g1:
        st.pyplot(plot_questions_chart(dfq_30), use_container_width=True)
    with g2:
        st.pyplot(plot_exam_chart(dfe_90), use_container_width=True)

    g3, g4 = st.columns(2)
    with g3:
        st.pyplot(plot_weekly_ranking(dfq_30), use_container_width=True)
    with g4:
        st.pyplot(plot_heatmap_study(dfs_30), use_container_width=True)


# =========================================================
# PAGE: HOJE
# =========================================================
elif menu == "Hoje":
    timer_init()

    st.subheader("📅 Painel do dia")
    qs, corr, acc_today, min_today, exams_today = today_progress(user_id)
    q_goal, min_goal, exams_goal, current_stage = get_goals(user_id)
    alerts = alerts_summary(user_id)
    priorities = build_priority_actions(user_id)
    exams_month = month_exam_count(user_id)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Questões hoje", str(qs), f"Meta {q_goal}")
    with c2:
        metric_card("Aproveitamento", f"{acc_today:.1f}%" if acc_today is not None else "—", "Hoje")
    with c3:
        metric_card("Tempo", f"{min_today:.1f} min", f"Meta {min_goal}")
    with c4:
        metric_card("Revisões hoje", str(alerts["due_today_reviews"]), "Agenda")

    st.markdown("### Prioridades do dia")
    for item in priorities:
        st.markdown(f"**{item['priority']} • {item['title']}**")
        st.caption(item["desc"])

    st.markdown("### Ações rápidas")
    a1, a2, a3, a4 = st.columns(4)
    with a1:
        if st.button("✍️ Registrar questões", use_container_width=True, key="today_to_register"):
            goto_page("Registrar")
    with a2:
        if st.button("🗂️ Abrir revisões", use_container_width=True, key="today_to_reviews"):
            goto_page("Revisões")
    with a3:
        if st.button("📊 Ver dashboard", use_container_width=True, key="today_to_dashboard"):
            goto_page("Dashboard")
    with a4:
        if st.button("🔥 Ver rendimentos", use_container_width=True, key="today_to_heat"):
            goto_page("Mapa de Rendimentos")

    st.markdown("### Cronômetro rápido")
    st.markdown("<div class='v6-card'>", unsafe_allow_html=True)

    render_analog_timer(
        running=st.session_state["timer_running"],
        start_ts=st.session_state["timer_start_ts"],
        accumulated_seconds=st.session_state["timer_accumulated"],
    )

    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("▶️ Iniciar / Retomar", use_container_width=True, key="today_timer_start"):
            timer_start()
            st.rerun()
    with b2:
        if st.button("⏸️ Pausar", use_container_width=True, key="today_timer_pause"):
            timer_pause()
            st.rerun()
    with b3:
        if st.button("🔁 Zerar", use_container_width=True, key="today_timer_reset"):
            timer_reset()
            st.rerun()

    st.write(f"**Tempo atual:** {format_hms(timer_current_seconds())}")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Status do mês")
    st.write(f"**Simulados realizados no mês:** {exams_month}/{exams_goal}")
    st.write(f"**Etapa atual:** {current_stage}")


# =========================================================
# PAGE: REGISTRAR
# =========================================================
elif menu == "Registrar":
    st.subheader("✍️ Registrar desempenho")
    t1, t2, t3 = st.tabs(["Questões", "Simulados", "Cronômetro"])

    with t1:
        subj_id, subj_name, topic_id, topic_name = subject_topic_picker("qlog")

        c1, c2 = st.columns(2)
        with c1:
            questions = st.number_input("Número de questões", min_value=1, step=1, value=20, key="reg_questions")
        with c2:
            correct = st.number_input("Acertos", min_value=0, step=1, value=15, key="reg_correct")

        tags = st.text_input("Tags", key="reg_tags")
        source = st.text_input("Fonte", key="reg_source")
        notes = st.text_area("Observações", key="reg_notes")

        if st.button("Salvar questões + revisão automática", type="primary", use_container_width=True, key="btn_save_questions"):
            try:
                qid, acc = add_question_log(
                    user_id,
                    subj_id,
                    topic_id,
                    tags,
                    int(questions),
                    int(correct),
                    source,
                    notes
                )
                st.success(f"Registro salvo. ID {qid} • {acc:.1f}% • revisão em {compute_review_days(acc)} dias.")
            except Exception as e:
                st.error(f"Erro: {e}")

    with t2:
        title = st.text_input("Título do simulado", value="Simulado", key="exam_title")
        subjects = [{"id": None, "name": "(Sem disciplina)"}] + get_subjects()
        subj_names = [x["name"] for x in subjects]

        idx = st.selectbox(
            "Disciplina",
            range(len(subj_names)),
            format_func=lambda i: subj_names[i],
            key="exam_subj"
        )
        exam_subject_id = subjects[idx]["id"]

        c1, c2, c3 = st.columns(3)
        with c1:
            total_q = st.number_input("Total de questões", min_value=1, step=1, value=100, key="exam_total")
        with c2:
            corr = st.number_input("Acertos", min_value=0, step=1, value=75, key="exam_corr")
        with c3:
            mins = st.number_input("Duração (min)", min_value=0, step=5, value=120, key="exam_mins")

        notes = st.text_area("Observações do simulado", key="exam_notes")

        if st.button("Salvar simulado + revisão automática", type="primary", use_container_width=True, key="save_exam"):
            try:
                eid, acc = add_exam(
                    user_id,
                    title,
                    exam_subject_id,
                    int(total_q),
                    int(corr),
                    int(mins) * 60,
                    notes
                )
                st.success(f"Simulado salvo. ID {eid} • {acc:.1f}%")
            except Exception as e:
                st.error(f"Erro: {e}")

    with t3:
        timer_init()

        st.markdown("<div class='v6-card'>", unsafe_allow_html=True)

        render_analog_timer(
            running=st.session_state["timer_running"],
            start_ts=st.session_state["timer_start_ts"],
            accumulated_seconds=st.session_state["timer_accumulated"],
        )

        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("▶️ Iniciar / Retomar", use_container_width=True, key="timer_start_btn"):
                timer_start()
                st.rerun()
        with b2:
            if st.button("⏸️ Pausar", use_container_width=True, key="timer_pause_btn"):
                timer_pause()
                st.rerun()
        with b3:
            if st.button("🔁 Zerar", use_container_width=True, key="timer_reset_btn"):
                timer_reset()
                st.rerun()

        st.write(f"**Tempo atual:** {format_hms(timer_current_seconds())}")

        subj_id, subj_name, topic_id, topic_name = subject_topic_picker("timer_save")
        session_type = st.selectbox("Tipo da sessão", ["ESTUDO", "REVISAO", "AULA", "LEITURA"], key="timer_session_type")
        tags = st.text_input("Tags da sessão", key="timer_tags")
        notes = st.text_area("Observações da sessão", key="timer_notes")

        if st.button("✅ Finalizar e salvar sessão", type="primary", use_container_width=True, key="timer_save_btn"):
            try:
                timer_pause()
                duration = timer_current_seconds()

                save_study_session(
                    user_id,
                    subj_id,
                    topic_id,
                    tags,
                    duration,
                    session_type,
                    notes
                )

                timer_reset()
                st.success(f"Sessão salva: {format_hms(duration)} em {subj_name} / {topic_name}")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar sessão: {e}")

        st.markdown("</div>", unsafe_allow_html=True)


# =========================================================
# PAGE: REVISÕES
# =========================================================
elif menu == "Revisões":
    st.subheader("🗂️ Revisões espaçadas")
    tab1, tab2 = st.tabs(["Pendentes", "Concluídas"])

    with tab1:
        rows = fetch_all(
            """
            SELECT
                r.id,
                r.due_date,
                s.name,
                COALESCE(t.name,'(Sem tema)'),
                r.origin_type,
                COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id = r.subject_id
            LEFT JOIN topics t ON t.id = r.topic_id
            WHERE r.user_id = ? AND r.status='PENDENTE'
            ORDER BY r.due_date ASC, r.id ASC
            """,
            (user_id,)
        )

        if not rows:
            st.info("Sem revisões pendentes.")
        else:
            df = pd.DataFrame(
                rows,
                columns=["id", "due_date", "subject", "topic", "origin", "last_accuracy"]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)

            c1, c2, c3 = st.columns(3)
            with c1:
                rid = st.number_input(
                    "ID da revisão",
                    min_value=1,
                    step=1,
                    value=int(df.iloc[0]["id"]),
                    key="rev_id"
                )
            with c2:
                if st.button("✅ Concluir", use_container_width=True, key="rev_done"):
                    execute(
                        """
                        UPDATE reviews
                        SET status='CONCLUIDA', completed_at=?
                        WHERE id=? AND user_id=?
                        """,
                        (now_str(), int(rid), user_id)
                    )
                    audit(user_id, "CONCLUIR_REVISAO", "reviews", int(rid), "done")
                    st.success("Revisão concluída.")
                    st.rerun()
            with c3:
                if st.button("🗑️ Excluir", use_container_width=True, key="rev_delete"):
                    execute(
                        "DELETE FROM reviews WHERE id=? AND user_id=?",
                        (int(rid), user_id)
                    )
                    audit(user_id, "EXCLUIR_REVISAO", "reviews", int(rid), "deleted")
                    st.success("Revisão excluída.")
                    st.rerun()

    with tab2:
        rows = fetch_all(
            """
            SELECT
                r.id,
                r.due_date,
                r.completed_at,
                s.name,
                COALESCE(t.name,'(Sem tema)'),
                r.origin_type,
                COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id = r.subject_id
            LEFT JOIN topics t ON t.id = r.topic_id
            WHERE r.user_id = ? AND r.status='CONCLUIDA'
            ORDER BY r.completed_at DESC, r.id DESC
            LIMIT 400
            """,
            (user_id,)
        )

        if not rows:
            st.info("Sem revisões concluídas.")
        else:
            df = pd.DataFrame(
                rows,
                columns=["id", "due_date", "completed_at", "subject", "topic", "origin", "last_accuracy"]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: DASHBOARD
# =========================================================
elif menu == "Dashboard":
    st.subheader("📊 Dashboard de resultados")

    days_back = st.slider("Janela de análise (dias)", 7, 365, 90, key="dash_days")

    dfq = df_question_logs(user_id, days_back=days_back)
    dfs = df_study_sessions(user_id, days_back=days_back)
    dfe = df_exams(user_id, days_back=max(90, days_back))

    selected_subjects = subject_filter_ui(dfq, dfe)
    dfq, dfe, dfs = apply_subject_filter(dfq, dfe, dfs, selected_subjects)

    total_q = int(dfq["questions"].sum()) if not dfq.empty else 0
    total_c = int(dfq["correct"].sum()) if not dfq.empty else 0
    acc = (total_c / total_q * 100.0) if total_q > 0 else 0.0
    total_min = float(dfs["duration_seconds"].sum() / 60.0) if not dfs.empty else 0.0
    total_exams = int(len(dfe)) if not dfe.empty else 0
    avg_exam_acc = float(dfe["accuracy"].mean()) if not dfe.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Questões", total_q)
    c2.metric("Acertos", total_c)
    c3.metric("% Questões", f"{acc:.1f}%")
    c4.metric("Tempo", f"{total_min:.1f} min")
    c5.metric("Simulados", f"{total_exams} • {avg_exam_acc:.1f}%")

    g1, g2 = st.columns(2)
    with g1:
        st.pyplot(plot_questions_chart(dfq), use_container_width=True)
    with g2:
        st.pyplot(plot_exam_chart(dfe), use_container_width=True)

    g3, g4 = st.columns(2)
    with g3:
        st.pyplot(plot_questions_accuracy_chart(dfq), use_container_width=True)
    with g4:
        st.pyplot(plot_subject_performance(dfq), use_container_width=True)

    g5, g6 = st.columns(2)
    with g5:
        st.pyplot(plot_weekly_ranking(dfq), use_container_width=True)
    with g6:
        st.pyplot(plot_heatmap_study(dfs), use_container_width=True)

    st.markdown("### Ranking semanal das disciplinas")
    ranking_df = weekly_ranking_df(dfq)
    if ranking_df.empty:
        st.info("Sem dados suficientes para ranking semanal.")
    else:
        st.dataframe(ranking_df, use_container_width=True, hide_index=True)

    if not dfq.empty:
        st.markdown("### Diagnóstico por disciplina")
        by_sub = dfq.groupby("subject", as_index=False)[["questions", "correct"]].sum()
        by_sub = by_sub[by_sub["questions"] > 0].copy()
        by_sub["accuracy"] = by_sub["correct"] / by_sub["questions"] * 100.0
        by_sub["Sugestão"] = by_sub.apply(
            lambda r: suggestion_for_subject(r["subject"], r["accuracy"]),
            axis=1
        )
        by_sub = by_sub.sort_values("accuracy", ascending=True).reset_index(drop=True)
        st.dataframe(by_sub, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: MAPA DE RENDIMENTOS
# =========================================================
elif menu == "Mapa de Rendimentos":
    st.subheader("🔥 Mapa de rendimentos por tema e subtema")
    st.caption("Desempenho em faixas visuais, destacando os melhores e os piores tópicos.")

    days_back = st.slider("Janela do mapa (dias)", 15, 365, 180, key="heat_days")
    perf = topic_performance_df(user_id, days_back=days_back)

    if perf.empty:
        st.info("Sem dados suficientes para gerar o mapa.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("Tópicos avaliados", str(len(perf)))
        with c2:
            metric_card("Melhor tópico", f"{perf.iloc[0]['accuracy']:.1f}%")
        with c3:
            metric_card("Pior tópico", f"{perf.sort_values('accuracy').iloc[0]['accuracy']:.1f}%")
        with c4:
            metric_card("Média geral", f"{perf['accuracy'].mean():.1f}%")

        st.markdown("<hr class='soft-hr'/>", unsafe_allow_html=True)

        left, right = st.columns(2)
        with left:
            render_heat_rows(perf, "Melhores rendimentos", top_n=10, best=True)
        with right:
            render_heat_rows(perf, "Piores rendimentos", top_n=10, best=False)

        st.markdown("### Lista completa")
        all_subjects = sorted(perf["subject"].unique().tolist())
        show_subject = st.multiselect(
            "Filtrar disciplinas",
            all_subjects,
            default=all_subjects,
            key="heat_subj_filter"
        )

        filtered = perf[perf["subject"].isin(show_subject)].copy() if show_subject else perf.copy()
        filtered = filtered.sort_values(["accuracy", "questions"], ascending=[False, False])

        st.dataframe(
            filtered[["subject", "topic", "questions", "correct", "accuracy", "level"]],
            use_container_width=True,
            hide_index=True
        )


# =========================================================
# PAGE: CRONOGRAMA
# =========================================================
elif menu == "Cronograma":
    st.subheader("🗓️ Cronograma de preparação")

    df_crono = build_cronograma_df()
    _, _, _, current_stage = get_goals(user_id)

    stage_names = [x[0] for x in PREP_STAGES]
    stage_filter = st.selectbox(
        "Etapa",
        options=stage_names,
        index=stage_names.index(current_stage) if current_stage in stage_names else 0,
        key="cronograma_stage"
    )

    view = df_crono[df_crono["Etapa"] == stage_filter].copy()

    st.info("Quantidade mínima de questões por disciplina e percentual mínimo de acerto para cada etapa da preparação.")
    st.dataframe(view, use_container_width=True, hide_index=True)

    st.markdown("### Regras gerais")
    for stage_name, min_q, min_acc in PREP_STAGES:
        st.write(f"- **{stage_name}**: mínimo base de **{min_q} questões/semana por disciplina** e **{min_acc:.0f}% de acerto**.")


# =========================================================
# PAGE: METAS & ALERTAS
# =========================================================
elif menu == "Metas & Alertas":
    st.subheader("🎯 Metas e alertas")

    q_goal, min_goal, exams_goal, current_stage = get_goals(user_id)
    inactive_days, drop_acc = get_prefs(user_id)
    alerts = alerts_summary(user_id)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        new_q = st.number_input("Meta diária de questões", min_value=0, step=10, value=int(q_goal), key="goal_q")
    with c2:
        new_min = st.number_input("Meta diária de tempo (min)", min_value=0, step=10, value=int(min_goal), key="goal_min")
    with c3:
        new_exams = st.number_input("Meta de simulados por mês", min_value=0, step=1, value=int(exams_goal), key="goal_exam")
    with c4:
        new_stage = st.selectbox(
            "Etapa atual",
            options=[x[0] for x in PREP_STAGES],
            index=[x[0] for x in PREP_STAGES].index(current_stage) if current_stage in [x[0] for x in PREP_STAGES] else 0,
            key="goal_stage"
        )

    c5, c6 = st.columns(2)
    with c5:
        new_inactive = st.number_input(
            "Alertar disciplina parada após (dias)",
            min_value=1,
            step=1,
            value=int(inactive_days),
            key="pref_inactive"
        )
    with c6:
        new_drop = st.number_input(
            "Alertar queda de % em 14 dias",
            min_value=1.0,
            step=1.0,
            value=float(drop_acc),
            key="pref_drop"
        )

    if st.button("Salvar metas e alertas", type="primary", use_container_width=True, key="save_goals"):
        set_goals(user_id, int(new_q), int(new_min), int(new_exams), new_stage)
        set_prefs(user_id, int(new_inactive), float(new_drop))
        st.success("Configurações salvas.")
        st.rerun()

    st.markdown("<hr class='soft-hr'/>", unsafe_allow_html=True)
    st.markdown("### Barra de alertas")

    if alerts["overdue_reviews"] > 0:
        st.error(f"Revisões vencidas: {alerts['overdue_reviews']}")
    else:
        st.success("Sem revisões vencidas no momento.")

    if alerts["due_today_reviews"] > 0:
        st.warning(f"Revisões para hoje: {alerts['due_today_reviews']}")

    if alerts["stale"]:
        st.warning("Disciplinas paradas:")
        for name, last in alerts["stale"][:10]:
            st.write(f"- {name} • {'sem histórico' if last is None else 'último registro: ' + last}")

    if alerts["weak_subjects"]:
        st.error("Disciplinas com pior desempenho:")
        for ws in alerts["weak_subjects"]:
            st.write(f"- **{ws['subject']}** — {ws['accuracy']:.1f}%")
            st.write(f"  Sugestão: {suggestion_for_subject(ws['subject'], ws['accuracy'])}")

    if alerts["strong_subjects"]:
        st.success("Disciplinas com melhor desempenho:")
        for ss in alerts["strong_subjects"]:
            st.write(f"- **{ss['subject']}** — {ss['accuracy']:.1f}%")

    if alerts["drop_msg"]:
        prev_acc, last_acc = alerts["drop_msg"]
        st.error(f"Queda detectada: {prev_acc:.1f}% → {last_acc:.1f}% nos últimos 14 dias.")


# =========================================================
# PAGE: RELATÓRIOS PDF
# =========================================================
elif menu == "Relatórios (PDF)":
    st.subheader("🧾 PDF diagnóstico situacional")
    st.write("Relatório com progresso, metas, gráficos e pontos a melhorar.")

    c1, c2 = st.columns(2)
    with c1:
        d1 = st.date_input("Data inicial", value=date.today() - timedelta(days=30), key="pdf_d1")
    with c2:
        d2 = st.date_input("Data final", value=date.today(), key="pdf_d2")

    if d2 < d1:
        st.error("Data final não pode ser menor que a inicial.")
    else:
        if st.button("Gerar PDF", type="primary", use_container_width=True, key="pdf_btn"):
            try:
                pdf_bytes = generate_pdf_report(user_id, username, d1, d2)
                st.download_button(
                    "⬇️ Baixar PDF",
                    data=pdf_bytes,
                    file_name=f"diagnostico_mentoria_do_jhon_{d1.isoformat()}_{d2.isoformat()}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="pdf_dl"
                )
                audit(user_id, "GERAR_PDF", "report", None, f"{d1}..{d2}")
                st.success("PDF gerado.")
            except Exception as e:
                st.error(f"Erro ao gerar PDF: {e}")


# =========================================================
# PAGE: GERENCIAR
# =========================================================
elif menu == "Gerenciar (Editar/Excluir)":
    st.subheader("🧰 Gerenciar dados")
    t1, t2, t3, t4 = st.tabs(["Questões", "Sessões", "Simulados", "Revisões"])

    with t1:
        dfq = df_question_logs(user_id, days_back=3650)
        if dfq.empty:
            st.info("Sem registros.")
        else:
            st.dataframe(
                dfq.drop(columns=["date", "topic_full"], errors="ignore"),
                use_container_width=True,
                hide_index=True
            )

            qid = st.number_input(
                "ID do registro de questões",
                min_value=1,
                step=1,
                value=int(dfq.iloc[0]["id"]),
                key="edit_qid"
            )

            row = fetch_one(
                """
                SELECT
                    id, subject_id, topic_id, COALESCE(tags,''), questions,
                    correct, COALESCE(source,''), COALESCE(notes,'')
                FROM question_logs
                WHERE id=? AND user_id=?
                """,
                (int(qid), user_id)
            )

            if row:
                st.markdown("### Editar registro de questões")

                subjects = get_subjects()
                subj_idx = next((i for i, s in enumerate(subjects) if s["id"] == row[1]), 0)

                subj_choice = st.selectbox(
                    "Disciplina",
                    range(len(subjects)),
                    format_func=lambda i: subjects[i]["name"],
                    index=subj_idx,
                    key="edit_q_subj"
                )
                chosen_subject_id = subjects[subj_choice]["id"]

                topics = [{"id": None, "name": "(Sem tema específico)"}] + get_topics(chosen_subject_id)
                topic_idx = next((i for i, t in enumerate(topics) if t["id"] == row[2]), 0)

                topic_choice = st.selectbox(
                    "Tema/Subtema",
                    range(len(topics)),
                    format_func=lambda i: topics[i]["name"],
                    index=topic_idx,
                    key="edit_q_topic"
                )
                chosen_topic_id = topics[topic_choice]["id"]

                col1, col2 = st.columns(2)
                with col1:
                    new_questions = st.number_input("Questões", min_value=1, value=int(row[4]), key="edit_questions")
                with col2:
                    new_correct = st.number_input("Acertos", min_value=0, value=int(row[5]), key="edit_correct")

                new_tags = st.text_input("Tags", value=row[3], key="edit_q_tags")
                new_source = st.text_input("Fonte", value=row[6], key="edit_q_source")
                new_notes = st.text_area("Observações", value=row[7], key="edit_q_notes")

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Salvar edição", use_container_width=True, key="save_q_edit"):
                        if new_correct > new_questions:
                            st.error("Acertos não podem ser maiores que o número de questões.")
                        else:
                            new_acc = (new_correct / new_questions) * 100.0
                            execute(
                                """
                                UPDATE question_logs
                                SET subject_id=?, topic_id=?, tags=?, questions=?, correct=?,
                                    accuracy=?, source=?, notes=?
                                WHERE id=? AND user_id=?
                                """,
                                (
                                    chosen_subject_id,
                                    chosen_topic_id,
                                    new_tags.strip() or None,
                                    int(new_questions),
                                    int(new_correct),
                                    float(new_acc),
                                    new_source.strip() or None,
                                    new_notes.strip() or None,
                                    int(qid),
                                    user_id
                                )
                            )
                            audit(user_id, "EDITAR_QUESTOES", "question_logs", int(qid), f"q={new_questions}; c={new_correct}; acc={new_acc:.1f}")
                            st.success("Registro atualizado.")
                            st.rerun()
                with c2:
                    if st.button("🗑️ Excluir registro", use_container_width=True, key="delete_q"):
                        execute("DELETE FROM question_logs WHERE id=? AND user_id=?", (int(qid), user_id))
                        audit(user_id, "EXCLUIR_QUESTOES", "question_logs", int(qid), "deleted")
                        st.success("Registro excluído.")
                        st.rerun()

    with t2:
        dfs = df_study_sessions(user_id, days_back=3650)
        if dfs.empty:
            st.info("Sem sessões.")
        else:
            st.dataframe(
                dfs.drop(columns=["date", "topic_full"], errors="ignore"),
                use_container_width=True,
                hide_index=True
            )

            sid = st.number_input(
                "ID da sessão",
                min_value=1,
                step=1,
                value=int(dfs.iloc[0]["id"]),
                key="edit_sid"
            )

            row = fetch_one(
                """
                SELECT
                    id, subject_id, topic_id, COALESCE(tags,''), duration_seconds,
                    session_type, COALESCE(notes,'')
                FROM study_sessions
                WHERE id=? AND user_id=?
                """,
                (int(sid), user_id)
            )

            if row:
                st.markdown("### Editar sessão")

                subjects = get_subjects()
                subj_idx = next((i for i, s in enumerate(subjects) if s["id"] == row[1]), 0)

                subj_choice = st.selectbox(
                    "Disciplina",
                    range(len(subjects)),
                    format_func=lambda i: subjects[i]["name"],
                    index=subj_idx,
                    key="edit_s_subj"
                )
                chosen_subject_id = subjects[subj_choice]["id"]

                topics = [{"id": None, "name": "(Sem tema específico)"}] + get_topics(chosen_subject_id)
                topic_idx = next((i for i, t in enumerate(topics) if t["id"] == row[2]), 0)

                topic_choice = st.selectbox(
                    "Tema/Subtema",
                    range(len(topics)),
                    format_func=lambda i: topics[i]["name"],
                    index=topic_idx,
                    key="edit_s_topic"
                )
                chosen_topic_id = topics[topic_choice]["id"]

                minutes = st.number_input("Minutos", min_value=1, value=max(1, int(row[4] / 60)), key="edit_s_minutes")
                s_type = st.selectbox(
                    "Tipo",
                    ["ESTUDO", "REVISAO", "AULA", "LEITURA"],
                    index=["ESTUDO", "REVISAO", "AULA", "LEITURA"].index(row[5]) if row[5] in ["ESTUDO", "REVISAO", "AULA", "LEITURA"] else 0,
                    key="edit_s_type"
                )
                tags = st.text_input("Tags", value=row[3], key="edit_s_tags")
                notes = st.text_area("Observações", value=row[6], key="edit_s_notes")

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Salvar edição", use_container_width=True, key="save_s_edit"):
                        execute(
                            """
                            UPDATE study_sessions
                            SET subject_id=?, topic_id=?, tags=?, duration_seconds=?, session_type=?, notes=?
                            WHERE id=? AND user_id=?
                            """,
                            (
                                chosen_subject_id,
                                chosen_topic_id,
                                tags.strip() or None,
                                int(minutes) * 60,
                                s_type,
                                notes.strip() or None,
                                int(sid),
                                user_id
                            )
                        )
                        audit(user_id, "EDITAR_SESSAO", "study_sessions", int(sid), f"min={minutes}")
                        st.success("Sessão atualizada.")
                        st.rerun()
                with c2:
                    if st.button("🗑️ Excluir sessão", use_container_width=True, key="delete_s"):
                        execute("DELETE FROM study_sessions WHERE id=? AND user_id=?", (int(sid), user_id))
                        audit(user_id, "EXCLUIR_SESSAO", "study_sessions", int(sid), "deleted")
                        st.success("Sessão excluída.")
                        st.rerun()

    with t3:
        dfe = df_exams(user_id, days_back=3650)
        if dfe.empty:
            st.info("Sem simulados.")
        else:
            st.dataframe(
                dfe.drop(columns=["date", "week"], errors="ignore"),
                use_container_width=True,
                hide_index=True
            )

            eid = st.number_input(
                "ID do simulado",
                min_value=1,
                step=1,
                value=int(dfe.iloc[0]["id"]),
                key="edit_eid"
            )

            row = fetch_one(
                """
                SELECT id, title, subject_id, total_questions, correct, duration_seconds, COALESCE(notes,'')
                FROM exams
                WHERE id=? AND user_id=?
                """,
                (int(eid), user_id)
            )

            if row:
                st.markdown("### Editar simulado")

                title = st.text_input("Título", value=row[1], key="edit_exam_title")
                subjects = [{"id": None, "name": "(Sem disciplina)"}] + get_subjects()
                subj_idx = next((i for i, s in enumerate(subjects) if s["id"] == row[2]), 0)

                subj_choice = st.selectbox(
                    "Disciplina",
                    range(len(subjects)),
                    format_func=lambda i: subjects[i]["name"],
                    index=subj_idx,
                    key="edit_exam_subj"
                )
                chosen_subject_id = subjects[subj_choice]["id"]

                c1, c2, c3 = st.columns(3)
                with c1:
                    total_q = st.number_input("Total de questões", min_value=1, value=int(row[3]), key="edit_exam_total")
                with c2:
                    correct = st.number_input("Acertos", min_value=0, value=int(row[4]), key="edit_exam_correct")
                with c3:
                    mins = st.number_input("Duração (min)", min_value=0, value=int(row[5] / 60), key="edit_exam_mins")

                notes = st.text_area("Observações", value=row[6], key="edit_exam_notes")

                d1, d2 = st.columns(2)
                with d1:
                    if st.button("💾 Salvar edição", use_container_width=True, key="save_exam_edit"):
                        if correct > total_q:
                            st.error("Acertos não podem ser maiores que o total.")
                        else:
                            acc = (correct / total_q) * 100.0
                            execute(
                                """
                                UPDATE exams
                                SET title=?, subject_id=?, total_questions=?, correct=?, accuracy=?, duration_seconds=?, notes=?
                                WHERE id=? AND user_id=?
                                """,
                                (
                                    title.strip(),
                                    chosen_subject_id,
                                    int(total_q),
                                    int(correct),
                                    float(acc),
                                    int(mins) * 60,
                                    notes.strip() or None,
                                    int(eid),
                                    user_id
                                )
                            )
                            audit(user_id, "EDITAR_SIMULADO", "exams", int(eid), f"acc={acc:.1f}")
                            st.success("Simulado atualizado.")
                            st.rerun()
                with d2:
                    if st.button("🗑️ Excluir simulado", use_container_width=True, key="delete_exam"):
                        execute("DELETE FROM exams WHERE id=? AND user_id=?", (int(eid), user_id))
                        audit(user_id, "EXCLUIR_SIMULADO", "exams", int(eid), "deleted")
                        st.success("Simulado excluído.")
                        st.rerun()

    with t4:
        rows = fetch_all(
            """
            SELECT
                r.id,
                r.due_date,
                r.status,
                s.name,
                COALESCE(t.name,'(Sem tema)'),
                r.origin_type,
                COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id=r.subject_id
            LEFT JOIN topics t ON t.id=r.topic_id
            WHERE r.user_id=?
            ORDER BY r.status ASC, r.due_date ASC, r.id ASC
            """,
            (user_id,)
        )

        if not rows:
            st.info("Sem revisões.")
        else:
            df = pd.DataFrame(
                rows,
                columns=["id", "due_date", "status", "subject", "topic", "origin", "last_accuracy"]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)

            rid = st.number_input(
                "ID da revisão",
                min_value=1,
                step=1,
                value=int(df.iloc[0]["id"]),
                key="edit_rid"
            )

            row = fetch_one(
                """
                SELECT id, due_date, status
                FROM reviews
                WHERE id=? AND user_id=?
                """,
                (int(rid), user_id)
            )

            if row:
                due_date = st.date_input(
                    "Nova data",
                    value=datetime.strptime(row[1], "%Y-%m-%d").date(),
                    key="edit_rev_date"
                )
                status = st.selectbox(
                    "Status",
                    ["PENDENTE", "CONCLUIDA"],
                    index=0 if row[2] == "PENDENTE" else 1,
                    key="edit_rev_status"
                )

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Salvar edição", use_container_width=True, key="save_rev_edit"):
                        completed_at = now_str() if status == "CONCLUIDA" else None
                        execute(
                            """
                            UPDATE reviews
                            SET due_date=?, status=?, completed_at=?
                            WHERE id=? AND user_id=?
                            """,
                            (due_date.isoformat(), status, completed_at, int(rid), user_id)
                        )
                        audit(user_id, "EDITAR_REVISAO", "reviews", int(rid), f"status={status}; data={due_date.isoformat()}")
                        st.success("Revisão atualizada.")
                        st.rerun()
                with c2:
                    if st.button("🗑️ Excluir revisão", use_container_width=True, key="delete_rev"):
                        execute("DELETE FROM reviews WHERE id=? AND user_id=?", (int(rid), user_id))
                        audit(user_id, "EXCLUIR_REVISAO", "reviews", int(rid), "deleted")
                        st.success("Revisão excluída.")
                        st.rerun()


# =========================================================
# PAGE: EXPORTAR / IMPORTAR CSV
# =========================================================
elif menu == "Exportar/Importar CSV":
    st.subheader("🔁 Exportar / backup CSV")

    dfq = df_question_logs(user_id, days_back=3650)
    dfs = df_study_sessions(user_id, days_back=3650)
    dfe = df_exams(user_id, days_back=3650)

    dfr = pd.DataFrame(
        fetch_all(
            """
            SELECT
                r.id, r.due_date, r.status, s.name, COALESCE(t.name,'(Sem tema)'),
                r.origin_type, COALESCE(r.last_accuracy,0), r.created_at, COALESCE(r.completed_at,'')
            FROM reviews r
            JOIN subjects s ON s.id=r.subject_id
            LEFT JOIN topics t ON t.id=r.topic_id
            WHERE r.user_id=?
            """,
            (user_id,)
        ),
        columns=["id", "due_date", "status", "subject", "topic", "origin_type", "last_accuracy", "created_at", "completed_at"]
    )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.download_button(
            "Questões CSV",
            dfq.to_csv(index=False).encode("utf-8"),
            "questoes.csv",
            "text/csv",
            use_container_width=True
        )
    with c2:
        st.download_button(
            "Sessões CSV",
            dfs.to_csv(index=False).encode("utf-8"),
            "sessoes.csv",
            "text/csv",
            use_container_width=True
        )
    with c3:
        st.download_button(
            "Simulados CSV",
            dfe.to_csv(index=False).encode("utf-8"),
            "simulados.csv",
            "text/csv",
            use_container_width=True
        )
    with c4:
        st.download_button(
            "Revisões CSV",
            dfr.to_csv(index=False).encode("utf-8"),
            "revisoes.csv",
            "text/csv",
            use_container_width=True
        )


# =========================================================
# PAGE: MATÉRIAS / SUBTEMAS
# =========================================================
elif menu == "Matérias/Subtemas":
    st.subheader("📚 Disciplinas e temas")
    tab1, tab2 = st.tabs(["Disciplinas", "Temas/Subtemas"])

    with tab1:
        new_subject = st.text_input("Nome da disciplina", key="new_subject")

        if st.button("Adicionar disciplina", type="primary", key="add_subject"):
            if not new_subject.strip():
                st.error("Digite um nome.")
            else:
                execute(
                    "INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)",
                    (new_subject.strip(), now_str())
                )
                audit(user_id, "CRIAR_DISCIPLINA", "subjects", None, new_subject.strip())
                st.success("Disciplina adicionada.")
                st.rerun()

        subs = fetch_all("SELECT id, name, created_at FROM subjects ORDER BY name;")
        df = pd.DataFrame(subs, columns=["id", "name", "created_at"])
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab2:
        subjects = get_subjects()
        if not subjects:
            st.warning("Crie uma disciplina primeiro.")
        else:
            subj_names = [s["name"] for s in subjects]
            idx = st.selectbox(
                "Disciplina",
                range(len(subj_names)),
                format_func=lambda i: subj_names[i],
                key="topic_pick"
            )
            subject_id = subjects[idx]["id"]
            topic_name = st.text_input("Nome do tema/subtema", key="topic_name")

            if st.button("Adicionar tema", type="primary", key="topic_add"):
                if not topic_name.strip():
                    st.error("Digite um nome.")
                else:
                    execute(
                        "INSERT OR IGNORE INTO topics (subject_id, name, created_at) VALUES (?, ?, ?)",
                        (subject_id, topic_name.strip(), now_str())
                    )
                    audit(user_id, "CRIAR_TEMA", "topics", None, f"{subject_id}:{topic_name.strip()}")
                    st.success("Tema adicionado.")
                    st.rerun()

            topics = fetch_all(
                "SELECT id, name, created_at FROM topics WHERE subject_id=? ORDER BY name;",
                (subject_id,)
            )
            df2 = pd.DataFrame(topics, columns=["id", "name", "created_at"])
            st.dataframe(df2, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: USUÁRIOS
# =========================================================
elif menu == "Usuários":
    if not is_admin:
        st.error("Acesso restrito ao administrador.")
        st.stop()

    st.subheader("👥 Usuários")
    st.info("Padrão inicial: admin / admin123")
    tab1, tab2, tab3, tab4 = st.tabs(["Criar usuário", "Trocar senha", "Listar usuários", "Ativar/Inativar"])

    with tab1:
        new_u = st.text_input("Novo usuário", key="new_user")
        new_p = st.text_input("Nova senha", type="password", key="new_pass")
        new_p2 = st.text_input("Confirmar senha", type="password", key="new_pass_2")
        new_admin = st.checkbox("Criar como administrador", key="new_admin_flag")

        if st.button("Criar usuário", type="primary", key="create_user"):
            try:
                if new_p != new_p2:
                    raise ValueError("As senhas não coincidem.")
                uid = create_user_account(new_u, new_p, is_admin=bool(new_admin))
                ensure_goal_row(uid)
                ensure_prefs_row(uid)
                audit(user_id, "CRIAR_USUARIO", "users", uid, f"{new_u.strip()} | admin={bool(new_admin)}")
                st.success("Usuário criado.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")

    with tab2:
        current = st.text_input("Senha atual", type="password", key="pw_current")
        newpass = st.text_input("Nova senha", type="password", key="pw_new")
        newpass2 = st.text_input("Repetir nova senha", type="password", key="pw_new2")

        if st.button("Atualizar senha", type="primary", key="update_pw"):
            u = get_user_by_username(username)
            if not u:
                st.error("Usuário logado não encontrado.")
            elif not check_password(current, u["salt"], u["hash"]):
                st.error("Senha atual incorreta.")
            elif newpass != newpass2:
                st.error("As novas senhas não conferem.")
            elif not valid_password(newpass):
                st.error("Senha deve ter pelo menos 4 caracteres.")
            else:
                salt = secrets.token_hex(16)
                pw_hash = hash_password(newpass, salt)
                execute("UPDATE users SET salt=?, password_hash=? WHERE id=?", (salt, pw_hash, user_id))
                audit(user_id, "TROCAR_SENHA", "users", user_id, "changed")
                st.success("Senha atualizada.")

    with tab3:
        rows = fetch_all(
            """
            SELECT id, username, COALESCE(is_admin,0), COALESCE(is_active,1), created_at
            FROM users
            ORDER BY created_at DESC
            """
        )
        df = pd.DataFrame(rows, columns=["id", "username", "is_admin", "is_active", "created_at"])
        df["perfil"] = df["is_admin"].apply(lambda x: "Administrador" if int(x) == 1 else "Usuário")
        df["status"] = df["is_active"].apply(lambda x: "Ativo" if int(x) == 1 else "Inativo")
        st.dataframe(df[["id", "username", "perfil", "status", "created_at"]], use_container_width=True, hide_index=True)

    with tab4:
        rows = fetch_all(
            """
            SELECT id, username, COALESCE(is_admin,0), COALESCE(is_active,1), created_at
            FROM users
            ORDER BY username ASC
            """
        )

        if not rows:
            st.info("Sem usuários.")
        else:
            df = pd.DataFrame(rows, columns=["id", "username", "is_admin", "is_active", "created_at"])
            st.dataframe(df, use_container_width=True, hide_index=True)

            target_user_id = st.number_input(
                "ID do usuário",
                min_value=1,
                step=1,
                value=int(df.iloc[0]["id"]),
                key="toggle_user_id"
            )

            target = fetch_one(
                """
                SELECT id, username, COALESCE(is_admin,0), COALESCE(is_active,1)
                FROM users
                WHERE id=?
                """,
                (int(target_user_id),)
            )

            if target:
                st.write(f"**Usuário selecionado:** {target[1]}")
                st.write(f"**Perfil:** {'Administrador' if int(target[2]) == 1 else 'Usuário'}")
                st.write(f"**Status atual:** {'Ativo' if int(target[3]) == 1 else 'Inativo'}")

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Ativar/Inativar", use_container_width=True, key="toggle_user_btn"):
                        if int(target[0]) == int(user_id):
                            st.error("Você não pode inativar seu próprio usuário.")
                        elif int(target[0]) == 1 and int(target[2]) == 1 and int(target[3]) == 1:
                            st.error("Não é permitido inativar o administrador principal padrão.")
                        else:
                            new_status = 0 if int(target[3]) == 1 else 1
                            execute("UPDATE users SET is_active=? WHERE id=?", (new_status, int(target[0])))
                            audit(user_id, "ATIVAR_INATIVAR_USUARIO", "users", int(target[0]), f"status={new_status}")
                            st.success("Status atualizado.")
                            st.rerun()

                with c2:
                    if st.button("Tornar admin / remover admin", use_container_width=True, key="toggle_admin_btn"):
                        if int(target[0]) == 1 and int(target[2]) == 1:
                            st.error("Não é permitido remover admin do administrador principal padrão.")
                        else:
                            new_admin_status = 0 if int(target[2]) == 1 else 1
                            execute("UPDATE users SET is_admin=? WHERE id=?", (new_admin_status, int(target[0])))
                            audit(user_id, "ALTERAR_PERFIL_USUARIO", "users", int(target[0]), f"is_admin={new_admin_status}")
                            st.success("Perfil atualizado.")
                            st.rerun()


# =========================================================
# PAGE: AUDITORIA
# =========================================================
elif menu == "Auditoria":
    if not is_admin:
        st.error("Acesso restrito ao administrador.")
        st.stop()

    st.subheader("🧾 Auditoria")

    rows = fetch_all(
        """
        SELECT
            a.id,
            a.created_at,
            u.username,
            a.action,
            COALESCE(a.entity,''),
            COALESCE(a.entity_id,''),
            COALESCE(a.details,'')
        FROM audit_log a
        JOIN users u ON u.id = a.user_id
        ORDER BY a.created_at DESC
        LIMIT 500
        """
    )

    df = pd.DataFrame(rows, columns=["id", "created_at", "user", "action", "entity", "entity_id", "details"])
    st.dataframe(df, use_container_width=True, hide_index=True)


# =========================================================
# FALLBACK
# =========================================================
else:
    st.warning("By Jhonatan Jason.")
