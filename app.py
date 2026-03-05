import os
import sqlite3
import hashlib
import secrets
import time
import math
import re
from datetime import datetime, date, timedelta
from io import BytesIO, StringIO

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm

# =========================================================
# APP: Mentoria do Jhon — Estudos de alto rendimento
# =========================================================

APP_NAME = "Mentoria do Jhon"
DB_PATH = "jhonatan_jason.db"

DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = "admin123"

# Regras de revisão (questões manuais):
# <80% = 20 dias; 80–90% = 30 dias; >90% = 45 dias
def compute_review_days(accuracy_pct: float) -> int:
    if accuracy_pct < 80:
        return 20
    if accuracy_pct <= 90:
        return 30
    return 45


# =========================================================
# ===== FLASHCARDS: NOVA LÓGICA (4 BOTÕES) =====
# =========================================================
# FACINHO: 5 dias; depois 2,5x
# MEDIANO: 3 dias; depois 2,5x
# NÃO SABIA: 2 dias; depois 2,5x
# IMPOSSÍVEL LEMBRAR: 1 dia; depois 2 dias; depois 2,0x

def flash_next_interval(prev_days: int, result: str) -> int:
    prev_days = int(prev_days or 0)
    result = (result or "").strip().upper()

    if result == "FACINHO":
        if prev_days <= 0:
            return 5
        return int(math.ceil(prev_days * 2.5))

    if result == "MEDIANO":
        if prev_days <= 0:
            return 3
        return int(math.ceil(prev_days * 2.5))

    if result == "NAO_SABIA":
        if prev_days <= 0:
            return 2
        return int(math.ceil(prev_days * 2.5))

    if result == "IMPOSSIVEL":
        if prev_days <= 0:
            return 1
        if prev_days == 1:
            return 2
        return int(math.ceil(prev_days * 2.0))

    if prev_days <= 0:
        return 3
    return int(math.ceil(prev_days * 2.0))


# =========================================================
# THEME (tema escuro premium via config.toml)
# =========================================================
def ensure_dark_theme_config():
    try:
        os.makedirs(".streamlit", exist_ok=True)
        cfg_path = os.path.join(".streamlit", "config.toml")
        if not os.path.exists(cfg_path):
            cfg = """
[theme]
base="dark"
primaryColor="#7C3AED"
backgroundColor="#0E1117"
secondaryBackgroundColor="#161B22"
textColor="#E6E6E6"
font="sans serif"
"""
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(cfg.strip() + "\n")
    except Exception:
        pass

ensure_dark_theme_config()
st.set_page_config(page_title=APP_NAME, page_icon="📚", layout="wide")


# =========================================================
# HELPERS
# =========================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def today_str() -> str:
    return date.today().isoformat()

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def hash_password(password: str, salt: str) -> str:
    return sha256(salt + password)

def check_password(password: str, salt: str, pw_hash: str) -> bool:
    return hash_password(password, salt) == pw_hash

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

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def format_hms(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def audit(user_id: int, action: str, entity: str = None, entity_id: int = None, details: str = None):
    execute("""
        INSERT INTO audit_log (user_id, action, entity, entity_id, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, action, entity, entity_id, details, now_str()))


# =========================================================
# NORMALIZAÇÃO / HASH
# =========================================================
def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def hash_text(s: str) -> str:
    return hashlib.sha256(norm_text(s).encode("utf-8")).hexdigest()


# =========================================================
# DB INIT
# =========================================================
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Usuários
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        salt TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """)

    # Matérias
    cur.execute("""
    CREATE TABLE IF NOT EXISTS subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL
    );
    """)

    # Subtemas
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

    # Registros de questões
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

    # Simulados
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

    # Sessões de estudo
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

    # Metas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL UNIQUE,
        daily_questions_goal INTEGER NOT NULL DEFAULT 0,
        daily_minutes_goal INTEGER NOT NULL DEFAULT 0,
        monthly_exams_goal INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    # Revisões
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

    # Preferências
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

    # Auditoria
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

    # =========================================================
    # FLASHCARDS + FILA (3 respostas)
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flashcards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deck TEXT,
        assunto TEXT NOT NULL,
        tags TEXT,
        f_front TEXT NOT NULL,
        f_back1 TEXT,
        f_back2 TEXT,
        f_back3 TEXT,
        f_cloze TEXT,
        card_type TEXT NOT NULL DEFAULT 'BASIC',
        source TEXT,
        c_hash TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS flash_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        card_id INTEGER NOT NULL,
        due_date TEXT NOT NULL,
        interval_days INTEGER NOT NULL DEFAULT 0,
        last_result TEXT,
        status TEXT NOT NULL DEFAULT 'PENDENTE',
        last_reviewed_at TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(card_id) REFERENCES flashcards(id) ON DELETE CASCADE
    );
    """)

    # índices (ajudam MUITO na pesquisa)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_flash_assunto ON flashcards(assunto);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_flash_deck ON flashcards(deck);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_flash_front ON flashcards(f_front);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_flash_due ON flash_reviews(user_id, due_date, status);")

    conn.commit()

    # Seed: admin
    cur.execute("SELECT COUNT(*) FROM users;")
    count_users = cur.fetchone()[0]
    if count_users == 0:
        salt = secrets.token_hex(16)
        pw_hash = hash_password(DEFAULT_ADMIN_PASS, salt)
        cur.execute("""
            INSERT INTO users (username, salt, password_hash, created_at)
            VALUES (?, ?, ?, ?)
        """, (DEFAULT_ADMIN_USER, salt, pw_hash, now_str()))
        conn.commit()

    # Seed: 5 grandes áreas
    default_subjects = ["Clínica Médica", "Cirurgia", "Pediatria", "Ginecologia e Obstetrícia", "Medicina Preventiva"]
    for s in default_subjects:
        cur.execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (s, now_str()))
    conn.commit()

    conn.close()

init_db()


# =========================================================
# AUTH + SETUP
# =========================================================
def get_user_by_username(username: str):
    row = fetch_one("SELECT id, username, salt, password_hash FROM users WHERE username = ?", (username,))
    if not row:
        return None
    return {"id": row[0], "username": row[1], "salt": row[2], "hash": row[3]}

def ensure_goal_row(user_id: int):
    row = fetch_one("SELECT id FROM goals WHERE user_id = ?", (user_id,))
    if not row:
        execute("""
            INSERT INTO goals (user_id, daily_questions_goal, daily_minutes_goal, monthly_exams_goal, created_at, updated_at)
            VALUES (?, 0, 0, 0, ?, ?)
        """, (user_id, now_str(), now_str()))

def ensure_prefs_row(user_id: int):
    row = fetch_one("SELECT id FROM prefs WHERE user_id = ?", (user_id,))
    if not row:
        execute("""
            INSERT INTO prefs (user_id, inactive_days_alert, drop_accuracy_alert, created_at, updated_at)
            VALUES (?, 7, 5.0, ?, ?)
        """, (user_id, now_str(), now_str()))

def login_box():
    st.markdown(f"## 🔐 {APP_NAME} — Login")
    st.caption("Digite usuário e senha")
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Usuário", value="")
        password = st.text_input("Senha", type="password", value="")
        ok = st.form_submit_button("Entrar")
    if ok:
        u = get_user_by_username(username.strip())
        if not u:
            st.error("Usuário não encontrado.")
            return
        if not check_password(password, u["salt"], u["hash"]):
            st.error("Senha incorreta.")
            return
        st.session_state["auth_user"] = {"id": u["id"], "username": u["username"]}
        ensure_goal_row(u["id"])
        ensure_prefs_row(u["id"])
        st.success("Login feito com sucesso!")
        st.rerun()

def logout_button():
    if st.sidebar.button("Sair"):
        st.session_state.pop("auth_user", None)
        st.rerun()


# =========================================================
# SUBJECTS & TOPICS
# =========================================================
def get_subjects():
    rows = fetch_all("SELECT id, name FROM subjects ORDER BY name;")
    return [{"id": r[0], "name": r[1]} for r in rows]

def get_topics(subject_id: int):
    rows = fetch_all("SELECT id, name FROM topics WHERE subject_id = ? ORDER BY name;", (subject_id,))
    return [{"id": r[0], "name": r[1]} for r in rows]

def subject_topic_picker(key_prefix=""):
    subjects = get_subjects()
    if not subjects:
        st.warning("Sem matérias cadastradas.")
        return None, None, None, None

    subj_names = [s["name"] for s in subjects]
    subj_idx = st.selectbox("Matéria", range(len(subj_names)), format_func=lambda i: subj_names[i], key=f"{key_prefix}_subj")
    subject = subjects[subj_idx]

    topics = get_topics(subject["id"])
    topic_options = [{"id": None, "name": "(Sem subtema)"}] + topics
    topic_names = [t["name"] for t in topic_options]
    topic_idx = st.selectbox("Subtema", range(len(topic_names)), format_func=lambda i: topic_names[i], key=f"{key_prefix}_topic")
    topic = topic_options[topic_idx]

    return subject["id"], subject["name"], topic["id"], topic["name"]


# =========================================================
# LOGGING + AUTO REVIEWS
# =========================================================
def add_review(user_id: int, subject_id: int, topic_id, accuracy: float, origin_type: str, origin_id: int):
    days = compute_review_days(accuracy)
    due = (date.today() + timedelta(days=days)).isoformat()
    rid = execute("""
        INSERT INTO reviews (user_id, subject_id, topic_id, due_date, status, origin_type, origin_id, last_accuracy, created_at)
        VALUES (?, ?, ?, ?, 'PENDENTE', ?, ?, ?, ?)
    """, (user_id, subject_id, topic_id, due, origin_type, origin_id, accuracy, now_str()))
    audit(user_id, "CRIAR_REVISAO", "reviews", rid, f"venc={due}, acc={accuracy:.1f}, origem={origin_type}:{origin_id}")
    return rid, due, days

def add_question_log(user_id: int, subject_id: int, topic_id, tags: str, questions: int, correct: int, source: str, notes: str):
    if questions <= 0:
        raise ValueError("Número de questões deve ser > 0.")
    if correct < 0 or correct > questions:
        raise ValueError("Acertos inválidos.")
    accuracy = (correct / questions) * 100.0
    log_id = execute("""
        INSERT INTO question_logs (user_id, subject_id, topic_id, tags, questions, correct, accuracy, source, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, subject_id, topic_id, (tags or "").strip() or None, questions, correct, accuracy,
          (source or "").strip() or None, (notes or "").strip() or None, now_str()))
    audit(user_id, "CRIAR_QUESTOES", "question_logs", log_id, f"q={questions}, c={correct}, acc={accuracy:.1f}")
    add_review(user_id, subject_id, topic_id, accuracy, "QUESTOES", log_id)
    return log_id, accuracy

def add_exam(user_id: int, title: str, subject_id, total_questions: int, correct: int, duration_seconds: int, notes: str):
    if not title.strip():
        raise ValueError("Título do simulado é obrigatório.")
    if total_questions <= 0:
        raise ValueError("Total de questões deve ser > 0.")
    if correct < 0 or correct > total_questions:
        raise ValueError("Acertos inválidos.")
    accuracy = (correct / total_questions) * 100.0
    exam_id = execute("""
        INSERT INTO exams (user_id, title, subject_id, total_questions, correct, accuracy, duration_seconds, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, title.strip(), subject_id, total_questions, correct, accuracy, duration_seconds,
          (notes or "").strip() or None, now_str()))
    audit(user_id, "CRIAR_SIMULADO", "exams", exam_id, f"titulo={title}, acc={accuracy:.1f}")
    if subject_id is not None:
        add_review(user_id, subject_id, None, accuracy, "SIMULADO", exam_id)
    return exam_id, accuracy


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
    acc = st.session_state["timer_accumulated"]
    if st.session_state["timer_running"] and st.session_state["timer_start_ts"] is not None:
        acc += int(time.time() - st.session_state["timer_start_ts"])
    return acc

def timer_start():
    if not st.session_state["timer_running"]:
        st.session_state["timer_running"] = True
        st.session_state["timer_start_ts"] = time.time()

def timer_pause():
    if st.session_state["timer_running"]:
        elapsed = int(time.time() - st.session_state["timer_start_ts"])
        st.session_state["timer_accumulated"] += elapsed
        st.session_state["timer_running"] = False
        st.session_state["timer_start_ts"] = None

def timer_reset():
    st.session_state["timer_running"] = False
    st.session_state["timer_start_ts"] = None
    st.session_state["timer_accumulated"] = 0

def save_study_session(user_id: int, subject_id: int, topic_id, tags: str, duration_seconds: int, session_type: str, notes: str):
    if duration_seconds <= 0:
        raise ValueError("Tempo deve ser > 0.")
    sid = execute("""
        INSERT INTO study_sessions (user_id, subject_id, topic_id, tags, duration_seconds, session_type, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, subject_id, topic_id, (tags or "").strip() or None, duration_seconds, session_type,
          (notes or "").strip() or None, now_str()))
    audit(user_id, "CRIAR_SESSAO", "study_sessions", sid, f"tipo={session_type}, seg={duration_seconds}")
    return sid


# =========================================================
# DATAFRAMES
# =========================================================
def df_question_logs(user_id: int, days_back: int = 60) -> pd.DataFrame:
    since = (date.today() - timedelta(days=days_back)).isoformat()
    rows = fetch_all("""
        SELECT q.id, q.created_at, s.name, COALESCE(t.name,'(Sem subtema)'),
               COALESCE(q.tags,''), q.questions, q.correct, q.accuracy, COALESCE(q.source,''), COALESCE(q.notes,'')
        FROM question_logs q
        JOIN subjects s ON s.id = q.subject_id
        LEFT JOIN topics t ON t.id = q.topic_id
        WHERE q.user_id = ? AND DATE(q.created_at) >= DATE(?)
        ORDER BY q.created_at DESC
    """, (user_id, since))
    df = pd.DataFrame(rows, columns=["id","created_at","subject","topic","tags","questions","correct","accuracy","source","notes"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["created_at"]).dt.date
    return df

def df_study_sessions(user_id: int, days_back: int = 60) -> pd.DataFrame:
    since = (date.today() - timedelta(days=days_back)).isoformat()
    rows = fetch_all("""
        SELECT ss.id, ss.created_at, s.name, COALESCE(t.name,'(Sem subtema)'),
               COALESCE(ss.tags,''), ss.duration_seconds, ss.session_type, COALESCE(ss.notes,'')
        FROM study_sessions ss
        JOIN subjects s ON s.id = ss.subject_id
        LEFT JOIN topics t ON t.id = ss.topic_id
        WHERE ss.user_id = ? AND DATE(ss.created_at) >= DATE(?)
        ORDER BY ss.created_at DESC
    """, (user_id, since))
    df = pd.DataFrame(rows, columns=["id","created_at","subject","topic","tags","duration_seconds","session_type","notes"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["created_at"]).dt.date
        df["minutes"] = (df["duration_seconds"] / 60).round(1)
    return df

def df_exams(user_id: int, days_back: int = 180) -> pd.DataFrame:
    since = (date.today() - timedelta(days=days_back)).isoformat()
    rows = fetch_all("""
        SELECT e.id, e.created_at, e.title, COALESCE(s.name,'(Sem matéria)'),
               e.total_questions, e.correct, e.accuracy, e.duration_seconds, COALESCE(e.notes,'')
        FROM exams e
        LEFT JOIN subjects s ON s.id = e.subject_id
        WHERE e.user_id = ? AND DATE(e.created_at) >= DATE(?)
        ORDER BY e.created_at DESC
    """, (user_id, since))
    df = pd.DataFrame(rows, columns=["id","created_at","title","subject","total_questions","correct","accuracy","duration_seconds","notes"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["created_at"]).dt.date
        df["minutes"] = (df["duration_seconds"] / 60).round(1)
    return df


# =========================================================
# GOALS + PREFS + TODAY PROGRESS
# =========================================================
def get_goals(user_id: int):
    row = fetch_one("SELECT daily_questions_goal, daily_minutes_goal, monthly_exams_goal FROM goals WHERE user_id = ?", (user_id,))
    if not row:
        return 0, 0, 0
    return int(row[0]), int(row[1]), int(row[2])

def set_goals(user_id: int, q_goal: int, min_goal: int, exams_goal: int):
    execute("""
        UPDATE goals
        SET daily_questions_goal = ?, daily_minutes_goal = ?, monthly_exams_goal = ?, updated_at = ?
        WHERE user_id = ?
    """, (int(q_goal), int(min_goal), int(exams_goal), now_str(), user_id))
    audit(user_id, "ATUALIZAR_METAS", "goals", None, f"q={q_goal}, min={min_goal}, sims={exams_goal}")

def get_prefs(user_id: int):
    row = fetch_one("SELECT inactive_days_alert, drop_accuracy_alert FROM prefs WHERE user_id = ?", (user_id,))
    if not row:
        return 7, 5.0
    return int(row[0]), float(row[1])

def set_prefs(user_id: int, inactive_days: int, drop_acc: float):
    execute("""
        UPDATE prefs
        SET inactive_days_alert = ?, drop_accuracy_alert = ?, updated_at = ?
        WHERE user_id = ?
    """, (int(inactive_days), float(drop_acc), now_str(), user_id))
    audit(user_id, "ATUALIZAR_ALERTAS", "prefs", None, f"inativo={inactive_days}, queda={drop_acc}")

def today_progress(user_id: int):
    d = today_str()
    q = fetch_one("""
        SELECT COALESCE(SUM(questions),0), COALESCE(SUM(correct),0)
        FROM question_logs
        WHERE user_id = ? AND DATE(created_at) = DATE(?)
    """, (user_id, d))
    qs, corr = int(q[0]), int(q[1])

    t = fetch_one("""
        SELECT COALESCE(SUM(duration_seconds),0)
        FROM study_sessions
        WHERE user_id = ? AND DATE(created_at) = DATE(?)
    """, (user_id, d))
    seconds = int(t[0])
    minutes = seconds / 60.0

    exams = fetch_one("""
        SELECT COALESCE(COUNT(*),0)
        FROM exams
        WHERE user_id = ? AND DATE(created_at) = DATE(?)
    """, (user_id, d))
    exams_today = int(exams[0])

    acc = (corr / qs * 100.0) if qs > 0 else None
    return qs, corr, acc, minutes, exams_today

def month_exam_count(user_id: int):
    first = date.today().replace(day=1).isoformat()
    row = fetch_one("""
        SELECT COALESCE(COUNT(*),0)
        FROM exams
        WHERE user_id = ? AND DATE(created_at) >= DATE(?)
    """, (user_id, first))
    return int(row[0])

def alerts_summary(user_id: int):
    inactive_days, drop_acc = get_prefs(user_id)
    rows = fetch_all("""
        SELECT s.name,
               MAX(DATE(q.created_at)) AS last_q,
               MAX(DATE(ss.created_at)) AS last_study
        FROM subjects s
        LEFT JOIN question_logs q ON q.subject_id=s.id AND q.user_id=?
        LEFT JOIN study_sessions ss ON ss.subject_id=s.id AND ss.user_id=?
        GROUP BY s.id
        ORDER BY s.name
    """, (user_id, user_id))

    stale = []
    today = date.today()
    for name, last_q, last_study in rows:
        last_dates = []
        if last_q:
            last_dates.append(datetime.strptime(last_q, "%Y-%m-%d").date())
        if last_study:
            last_dates.append(datetime.strptime(last_study, "%Y-%m-%d").date())
        if not last_dates:
            stale.append((name, None))
            continue
        last = max(last_dates)
        if (today - last).days >= inactive_days:
            stale.append((name, last.isoformat()))

    dfq = df_question_logs(user_id, days_back=35)
    drop_msg = None
    if not dfq.empty:
        dfq["d"] = pd.to_datetime(dfq["created_at"]).dt.date
        cut = date.today() - timedelta(days=14)
        last = dfq[dfq["d"] >= cut]
        prev = dfq[(dfq["d"] < cut) & (dfq["d"] >= (cut - timedelta(days=14)))]
        if not last.empty and not prev.empty:
            last_acc = (last["correct"].sum() / max(1, last["questions"].sum())) * 100.0
            prev_acc = (prev["correct"].sum() / max(1, prev["questions"].sum())) * 100.0
            if (prev_acc - last_acc) >= drop_acc:
                drop_msg = (prev_acc, last_acc)

    return stale, drop_msg


# =========================================================
# PDF REPORT
# =========================================================
def generate_pdf_report(user_id: int, username: str, date_from: date, date_to: date) -> bytes:
    dfq = df_question_logs(user_id, days_back=3650)
    dfs = df_study_sessions(user_id, days_back=3650)
    dfe = df_exams(user_id, days_back=3650)

    if not dfq.empty:
        dfq["date_only"] = pd.to_datetime(dfq["created_at"]).dt.date
        dfq = dfq[(dfq["date_only"] >= date_from) & (dfq["date_only"] <= date_to)].copy()
    if not dfs.empty:
        dfs["date_only"] = pd.to_datetime(dfs["created_at"]).dt.date
        dfs = dfs[(dfs["date_only"] >= date_from) & (dfs["date_only"] <= date_to)].copy()
    if not dfe.empty:
        dfe["date_only"] = pd.to_datetime(dfe["created_at"]).dt.date
        dfe = dfe[(dfe["date_only"] >= date_from) & (dfe["date_only"] <= date_to)].copy()

    total_q = int(dfq["questions"].sum()) if not dfq.empty else 0
    total_c = int(dfq["correct"].sum()) if not dfq.empty else 0
    acc = (total_c / total_q * 100.0) if total_q > 0 else 0.0
    total_minutes = float(dfs["duration_seconds"].sum() / 60.0) if not dfs.empty else 0.0

    total_exams = int(len(dfe)) if not dfe.empty else 0
    avg_exam_acc = float(dfe["accuracy"].mean()) if not dfe.empty else 0.0

    overdue = fetch_all("""
        SELECT r.due_date, s.name, COALESCE(t.name,'(Sem subtema)'), COALESCE(r.last_accuracy,0)
        FROM reviews r
        JOIN subjects s ON s.id = r.subject_id
        LEFT JOIN topics t ON t.id = r.topic_id
        WHERE r.user_id = ? AND r.status='PENDENTE' AND DATE(r.due_date) < DATE(?)
        ORDER BY r.due_date ASC
        LIMIT 250
    """, (user_id, today_str()))

    upcoming = fetch_all("""
        SELECT r.due_date, s.name, COALESCE(t.name,'(Sem subtema)'), COALESCE(r.last_accuracy,0)
        FROM reviews r
        JOIN subjects s ON s.id = r.subject_id
        LEFT JOIN topics t ON t.id = r.topic_id
        WHERE r.user_id = ? AND r.status='PENDENTE' AND DATE(r.due_date) >= DATE(?)
        ORDER BY r.due_date ASC
        LIMIT 250
    """, (user_id, today_str()))

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    def header(title: str, subtitle: str):
        c.setFont("Helvetica-Bold", 16)
        c.drawString(2*cm, height - 2*cm, title)
        c.setFont("Helvetica", 10)
        c.drawString(2*cm, height - 2.7*cm, subtitle)
        c.setLineWidth(1)
        c.line(2*cm, height - 3.0*cm, width - 2*cm, height - 3.0*cm)

    def footer():
        c.setFont("Helvetica", 9)
        c.drawString(2*cm, 1.3*cm, f"Gerado em: {now_str()} | Usuário: {username}")
        c.drawRightString(width - 2*cm, 1.3*cm, APP_NAME)

    header(APP_NAME + " — Relatório", f"Período: {date_from.isoformat()} a {date_to.isoformat()}")
    y = height - 4*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "Resumo")
    y -= 0.8*cm
    c.setFont("Helvetica", 11)
    c.drawString(2*cm, y, f"Questões: {total_q} | Acertos: {total_c} | Aproveitamento: {acc:.1f}%")
    y -= 0.6*cm
    c.drawString(2*cm, y, f"Tempo estudado: {total_minutes:.1f} min")
    y -= 0.6*cm
    c.drawString(2*cm, y, f"Simulados: {total_exams} | Média (%): {avg_exam_acc:.1f}%")
    footer()
    c.showPage()

    header(APP_NAME + " — Revisões", "Vencidas (pendentes)")
    y = height - 4*cm
    c.setFont("Helvetica", 10)
    if overdue:
        for due_date, subj, topic, last_acc in overdue:
            c.drawString(2*cm, y, f"- {due_date} | {subj} — {topic} | última: {float(last_acc):.1f}%")
            y -= 0.45*cm
            if y < 3*cm:
                footer(); c.showPage()
                header(APP_NAME + " — Revisões", "Vencidas (pendentes)")
                y = height - 4*cm
                c.setFont("Helvetica", 10)
    else:
        c.drawString(2*cm, y, "Nenhuma revisão vencida. ✅")
    footer()
    c.showPage()

    header(APP_NAME + " — Revisões", "Próximas (pendentes)")
    y = height - 4*cm
    c.setFont("Helvetica", 10)
    if upcoming:
        for due_date, subj, topic, last_acc in upcoming:
            c.drawString(2*cm, y, f"- {due_date} | {subj} — {topic} | última: {float(last_acc):.1f}%")
            y -= 0.45*cm
            if y < 3*cm:
                footer(); c.showPage()
                header(APP_NAME + " — Revisões", "Próximas (pendentes)")
                y = height - 4*cm
                c.setFont("Helvetica", 10)
    else:
        c.drawString(2*cm, y, "Nenhuma revisão pendente. ✅")
    footer()
    c.showPage()

    c.save()
    buffer.seek(0)
    return buffer.read()


# =========================================================
# UI STYLE (layout tipo “lista” igual seu exemplo)
# =========================================================
st.markdown(
    """
<style>
.block-container { padding-top: 1.2rem; }

div[data-testid="stMetric"] {
  padding: 12px; border-radius: 14px;
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.06);
}

.badge {
  display:inline-block; padding: 0.15rem 0.55rem; border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.14);
  background: rgba(255,255,255,0.04);
  font-size: 0.85rem;
}

hr.soft {
  border: none;
  border-top: 1px solid rgba(255,255,255,0.08);
  margin: 12px 0;
}

/* LIST ROW (assuntos/decks) */
.list-row {
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:14px 14px;
  border-radius:14px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.02);
  margin-bottom:10px;
}
.list-left { display:flex; flex-direction:column; gap:2px; }
.list-title { font-size:1.05rem; font-weight:600; }
.list-sub { opacity:0.75; font-size:0.9rem; }

.chev {
  opacity:0.7;
  font-size:1.4rem;
  padding:0 8px;
}

/* FLASH CARD */
.flash-card {
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.03);
  padding: 18px 18px 14px 18px;
}
.flash-front {
  font-size: 1.15rem;
  line-height: 1.45;
  margin: 0.25rem 0 0.65rem 0;
}
.flash-meta { opacity: 0.85; font-size: 0.9rem; margin-bottom: 0.5rem; }

.answer-box {
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.02);
  padding: 10px 12px;
  min-height: 44px;
  margin-bottom: 10px;
}
.answer-title { font-weight:600; margin-bottom:6px; opacity:0.9; }
</style>
""",
    unsafe_allow_html=True,
)


# =========================================================
# FLASH: FUNÇÕES + IMPORT + LISTA
# =========================================================
def flash_ensure_queue_for_user(user_id: int):
    rows = fetch_all("""
        SELECT f.id
        FROM flashcards f
        LEFT JOIN flash_reviews fr ON fr.card_id=f.id AND fr.user_id=?
        WHERE fr.id IS NULL
        LIMIT 5000
    """, (user_id,))
    for (cid,) in rows:
        execute("""
            INSERT INTO flash_reviews (user_id, card_id, due_date, interval_days, last_result, status, last_reviewed_at, created_at)
            VALUES (?, ?, ?, 0, NULL, 'PENDENTE', NULL, ?)
        """, (user_id, int(cid), today_str(), now_str()))

def flash_mark_result(user_id: int, fr_id: int, result: str):
    row = fetch_one("SELECT interval_days FROM flash_reviews WHERE id=? AND user_id=?", (int(fr_id), user_id))
    if not row:
        return None
    prev = int(row[0] or 0)
    nxt = flash_next_interval(prev, result=result)
    due = (date.today() + timedelta(days=nxt)).isoformat()
    execute("""
        UPDATE flash_reviews
        SET due_date=?, interval_days=?, last_result=?, last_reviewed_at=?, status='PENDENTE'
        WHERE id=? AND user_id=?
    """, (due, int(nxt), result, now_str(), int(fr_id), user_id))
    audit(user_id, "FLASH_REVISAR", "flash_reviews", int(fr_id), f"res={result}, prox={nxt}d, venc={due}")
    return due, nxt

def flash_counts_by_assunto(search: str = ""):
    search = (search or "").strip()
    if search:
        like = f"%{search}%"
        rows = fetch_all("""
            SELECT assunto, COUNT(*) total
            FROM flashcards
            WHERE assunto LIKE ?
            GROUP BY assunto
            ORDER BY total DESC, assunto ASC
        """, (like,))
    else:
        rows = fetch_all("""
            SELECT assunto, COUNT(*) total
            FROM flashcards
            GROUP BY assunto
            ORDER BY total DESC, assunto ASC
        """)
    return [{"assunto": r[0], "total": int(r[1] or 0)} for r in rows]

def flash_decks_by_assunto(assunto: str, search: str = ""):
    assunto = (assunto or "").strip()
    search = (search or "").strip()
    if not assunto:
        return []
    if search:
        like = f"%{search}%"
        rows = fetch_all("""
            SELECT COALESCE(deck,''), COUNT(*) total
            FROM flashcards
            WHERE assunto=? AND COALESCE(deck,'') LIKE ?
            GROUP BY COALESCE(deck,'')
            ORDER BY total DESC, COALESCE(deck,'') ASC
        """, (assunto, like))
    else:
        rows = fetch_all("""
            SELECT COALESCE(deck,''), COUNT(*) total
            FROM flashcards
            WHERE assunto=?
            GROUP BY COALESCE(deck,'')
            ORDER BY total DESC, COALESCE(deck,'') ASC
        """, (assunto,))
    return [{"deck": r[0] or "", "total": int(r[1] or 0)} for r in rows]

def flash_due_list(user_id: int, assunto: str = None, deck: str = None, search: str = "", limit: int = 500):
    """
    Lista cards vencidos com filtro por assunto/deck e pesquisa.
    """
    sql = """
        SELECT fr.id, fr.card_id, fr.due_date, fr.interval_days, COALESCE(fr.last_result,''),
               COALESCE(f.deck,''), f.assunto, f.card_type,
               f.f_front, COALESCE(f.f_back1,''), COALESCE(f.f_back2,''), COALESCE(f.f_back3,''),
               COALESCE(f.f_cloze,''), COALESCE(f.tags,''), COALESCE(f.source,'')
        FROM flash_reviews fr
        JOIN flashcards f ON f.id=fr.card_id
        WHERE fr.user_id=? AND fr.status='PENDENTE' AND DATE(fr.due_date) <= DATE(?)
    """
    params = [user_id, today_str()]

    if assunto:
        sql += " AND f.assunto=?"
        params.append(assunto)

    if deck is not None:
        # deck pode ser "" (sem deck)
        sql += " AND COALESCE(f.deck,'')=?"
        params.append(deck)

    search = (search or "").strip()
    if search:
        like = f"%{search}%"
        sql += " AND (f.f_front LIKE ? OR COALESCE(f.tags,'') LIKE ? OR COALESCE(f.source,'') LIKE ?)"
        params.extend([like, like, like])

    sql += " ORDER BY fr.due_date ASC, fr.id ASC LIMIT ?"
    params.append(int(limit))
    return fetch_all(sql, tuple(params))

def flash_search_any(search: str, assunto: str = None, deck: str = None, limit: int = 200):
    """
    Pesquisa geral (não só vencidos). Usado para “Pesquisar flashcards”.
    """
    search = (search or "").strip()
    if not search:
        return []

    like = f"%{search}%"
    sql = """
        SELECT f.id, COALESCE(f.deck,''), f.assunto, f.f_front,
               COALESCE(f.f_back1,''), COALESCE(f.f_back2,''), COALESCE(f.f_back3,''),
               COALESCE(f.tags,''), COALESCE(f.source,''), f.created_at
        FROM flashcards f
        WHERE (f.f_front LIKE ? OR COALESCE(f.tags,'') LIKE ? OR COALESCE(f.source,'') LIKE ?)
    """
    params = [like, like, like]

    if assunto:
        sql += " AND f.assunto=?"
        params.append(assunto)

    if deck is not None:
        sql += " AND COALESCE(f.deck,'')=?"
        params.append(deck)

    sql += " ORDER BY f.id DESC LIMIT ?"
    params.append(int(limit))

    rows = fetch_all(sql, tuple(params))
    return rows

def flash_upsert_card(deck: str, assunto: str, tags: str, front: str, back1: str, back2: str, back3: str,
                      cloze: str, card_type: str, source: str):
    """
    IMPORTANTE: NÃO INFERE ASSUNTO.
    O ASSUNTO vem do usuário na hora do import (ou do CSV se você escolher).
    """
    if not front:
        return None
    if not assunto:
        return None

    h = hash_text(f"{deck}|{assunto}|{tags}|{front}|{back1}|{back2}|{back3}|{cloze}|{card_type}")
    try:
        cid = execute("""
            INSERT OR IGNORE INTO flashcards
            (deck, assunto, tags, f_front, f_back1, f_back2, f_back3, f_cloze, card_type, source, c_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ((deck or "").strip() or None,
              assunto.strip(),
              (tags or "").strip() or None,
              front.strip(),
              (back1 or "").strip() or None,
              (back2 or "").strip() or None,
              (back3 or "").strip() or None,
              (cloze or "").strip() or None,
              (card_type or "BASIC").strip().upper(),
              (source or "").strip() or None,
              h, now_str()))
        return cid
    except Exception:
        return None

def _read_csv_robusto(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.getvalue()

    text = None
    for enc in ["utf-8", "utf-8-sig", "latin-1"]:
        try:
            text = raw.decode(enc)
            break
        except Exception:
            pass

    if text is None:
        raise ValueError("Não consegui ler o arquivo. Salve como CSV em UTF-8 (ou UTF-8 com BOM).")

    sep = ";" if text.count(";") > text.count(",") else ","

    df = pd.read_csv(StringIO(text), sep=sep, engine="python", header="infer")

    # se a primeira coluna parece texto grande, era dado => sem cabeçalho
    first_col = str(df.columns[0])
    if len(first_col) > 30 and (" " in first_col):
        df = pd.read_csv(StringIO(text), sep=sep, engine="python", header=None)

    return df

def flash_import_df(
    user_id: int,
    df: pd.DataFrame,
    tema_assunto: str,
    deck_padrao: str = "",
    tags_padrao: str = "",
    fonte_padrao: str = "CSV",
    usar_coluna_assunto_csv: bool = False,
    usar_coluna_deck_csv: bool = False,
) -> int:
    """
    Import robusto:
    - aceita sem cabeçalho (2 a 4 colunas): frente, resp1, resp2, resp3
    - aceita com cabeçalho (frente/front/pergunta etc)
    - ASSUNTO: por padrão usa o tema_assunto que o usuário escolher.
      Se marcar usar_coluna_assunto_csv, e o CSV tiver coluna assunto/tema/subject, usa ela.
    - DECK: por padrão usa deck_padrao que o usuário escolher (pode ficar vazio).
      Se marcar usar_coluna_deck_csv, e o CSV tiver coluna deck/baralho, usa ela.
    """
    tema_assunto = (tema_assunto or "").strip()
    if not tema_assunto:
        raise ValueError("Escolha um TEMA/ASSUNTO para importar.")

    imported = 0

    # sem cabeçalho => colunas numéricas
    if all(str(c).isdigit() for c in df.columns) or all(isinstance(c, int) for c in df.columns):
        df2 = df.copy()
        df2.columns = [str(i) for i in range(len(df2.columns))]
        for _, r in df2.iterrows():
            front = str(r.get("0", "")).strip()
            if not front:
                continue
            back1 = str(r.get("1", "")).strip()
            back2 = str(r.get("2", "")).strip() if "2" in df2.columns else ""
            back3 = str(r.get("3", "")).strip() if "3" in df2.columns else ""

            assunto = tema_assunto
            deck = deck_padrao
            tags = tags_padrao

            cid = flash_upsert_card(deck, assunto, tags, front, back1, back2, back3, "", "BASIC", fonte_padrao)
            if cid:
                imported += 1

        flash_ensure_queue_for_user(user_id)
        audit(user_id, "FLASH_IMPORTAR_CSV", "flashcards", None, f"importados={imported}; tema={tema_assunto}")
        return imported

    cols = {str(c).lower().strip(): c for c in df.columns}

    def get_any(row, keys, default=""):
        for k in keys:
            col = cols.get(k)
            if col is not None:
                v = row.get(col)
                if pd.notna(v):
                    return str(v).strip()
        return default

    for _, r in df.iterrows():
        front = get_any(r, ["frente", "front", "pergunta", "question", "q"], "")
        if not front:
            first_col = df.columns[0]
            v = r.get(first_col)
            front = str(v).strip() if pd.notna(v) else ""
        if not front:
            continue

        back1 = get_any(r, ["resposta1", "back1", "verso1", "a1", "resposta", "back", "verso", "answer"], "")
        back2 = get_any(r, ["resposta2", "back2", "verso2", "a2"], "")
        back3 = get_any(r, ["resposta3", "back3", "verso3", "a3"], "")

        # assunto: tema escolhido OU coluna do CSV
        assunto = tema_assunto
        if usar_coluna_assunto_csv:
            a_csv = get_any(r, ["assunto", "tema", "subject"], "")
            if a_csv:
                assunto = a_csv

        deck = deck_padrao
        if usar_coluna_deck_csv:
            d_csv = get_any(r, ["deck", "baralho"], "")
            if d_csv:
                deck = d_csv

        tags = tags_padrao
        t_csv = get_any(r, ["tags", "tag"], "")
        if t_csv:
            tags = (tags + ("; " if tags and t_csv else "") + t_csv).strip()

        cloze = get_any(r, ["cloze", "texto", "text"], "")
        card_type = "CLOZE" if (cloze and "{{c" in cloze) else "BASIC"

        cid = flash_upsert_card(deck, assunto, tags, front, back1, back2, back3, cloze, card_type, fonte_padrao)
        if cid:
            imported += 1

    flash_ensure_queue_for_user(user_id)
    audit(user_id, "FLASH_IMPORTAR_CSV", "flashcards", None, f"importados={imported}; tema={tema_assunto}")
    return imported


# =========================================================
# MAIN AUTH
# =========================================================
if "auth_user" not in st.session_state:
    login_box()
    st.stop()

user = st.session_state["auth_user"]
user_id = user["id"]
username = user["username"]

st.sidebar.markdown(f"### 👤 {username}")
logout_button()

menu = st.sidebar.radio(
    "Menu",
    [
        "Hoje",
        "Registrar",
        "Flashcards",
        "Importar Flashcards",
        "Revisões",
        "Dashboard",
        "Metas & Alertas",
        "Relatórios (PDF)",
        "Gerenciar (Editar/Excluir)",
        "Exportar/Importar CSV",
        "Matérias/Subtemas",
        "Usuários",
        "Auditoria"
    ],
)

st.title(f"📚 {APP_NAME}")
st.caption("Plataforma completa: questões, simulados, tempo, metas, revisões inteligentes, alertas e relatórios.")


# =========================================================
# PAGE: HOJE
# =========================================================
if menu == "Hoje":
    timer_init()

    qs, corr, acc_today, min_today, exams_today = today_progress(user_id)
    q_goal, min_goal, exams_goal = get_goals(user_id)
    exams_month = month_exam_count(user_id)

    colA, colB, colC, colD, colE = st.columns(5)
    colA.metric("Questões hoje", f"{qs}")
    colB.metric("Acertos hoje", f"{corr}")
    colC.metric("% hoje", f"{acc_today:.1f}%" if acc_today is not None else "—")
    colD.metric("Tempo hoje", f"{min_today:.1f} min")
    colE.metric("Simulados hoje", f"{exams_today}")

    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

    stale, drop_msg = alerts_summary(user_id)
    with st.expander("⚠️ Alertas inteligentes", expanded=True):
        if stale:
            st.warning("Matérias paradas (sem estudo/questões):")
            for name, last in stale[:20]:
                if last is None:
                    st.write(f"- **{name}**: sem registros ainda")
                else:
                    st.write(f"- **{name}**: último registro em **{last}**")
        else:
            st.success("Nenhuma matéria parada no critério atual. ✅")

        if drop_msg:
            prev_acc, last_acc = drop_msg
            st.error(f"Queda de desempenho detectada: média anterior {prev_acc:.1f}% → últimos 14 dias {last_acc:.1f}%.")
        else:
            st.info("Sem queda de desempenho relevante (no critério atual).")

    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

    left, right = st.columns([1.25, 1])

    with left:
        st.subheader("⏱️ Cronômetro (Estudo)")
        seconds = timer_current_seconds()
        st.markdown(f"### {format_hms(seconds)}  <span class='badge'>{'RODANDO' if st.session_state['timer_running'] else 'PAUSADO'}</span>", unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button("▶️ Iniciar", use_container_width=True, key="timer_start"):
                timer_start()
                st.rerun()
        with c2:
            if st.button("⏸️ Pausar", use_container_width=True, key="timer_pause"):
                timer_pause()
                st.rerun()
        with c3:
            if st.button("⏩ Retomar", use_container_width=True, key="timer_resume"):
                timer_start()
                st.rerun()
        with c4:
            if st.button("🔁 Zerar", use_container_width=True, key="timer_reset"):
                timer_reset()
                st.rerun()

        st.markdown("#### Finalizar e salvar sessão")
        subj_id, subj_name, topic_id, topic_name = subject_topic_picker("timer_save")
        tags = st.text_input("Tags (opcional) — ex.: cardio; prova; revisão", key="timer_tags")
        notes = st.text_area("Observações (opcional)", key="timer_notes")

        if st.button("✅ Finalizar e salvar", type="primary", use_container_width=True, key="timer_save_btn"):
            try:
                timer_pause()
                duration = timer_current_seconds()
                save_study_session(user_id, subj_id, topic_id, tags, duration, "ESTUDO", notes)
                timer_reset()
                st.success(f"Sessão salva: {format_hms(duration)} em {subj_name} / {topic_name}")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar sessão: {e}")

    with right:
        st.subheader("🎯 Metas do dia")
        prog_q = (qs / q_goal) if q_goal > 0 else 0
        prog_t = (min_today / min_goal) if min_goal > 0 else 0
        prog_e = (exams_month / exams_goal) if exams_goal > 0 else 0

        st.write(f"**Questões:** {qs} / {q_goal}")
        st.progress(min(max(prog_q, 0), 1.0))
        st.write(f"**Tempo:** {min_today:.1f} / {min_goal} min")
        st.progress(min(max(prog_t, 0), 1.0))
        st.write(f"**Simulados (mês):** {exams_month} / {exams_goal}")
        st.progress(min(max(prog_e, 0), 1.0))


# =========================================================
# PAGE: REGISTRAR
# =========================================================
elif menu == "Registrar":
    st.subheader("🧾 Registrar")
    t1, t2, t3 = st.tabs(["Questões", "Simulados", "Sessão manual"])

    with t1:
        st.markdown("### Questões")
        subj_id, subj_name, topic_id, topic_name = subject_topic_picker("qlog")
        c1, c2 = st.columns(2)
        with c1:
            questions = st.number_input("Número de questões", min_value=1, step=1, value=20, key="q_questions")
        with c2:
            correct = st.number_input("Acertos", min_value=0, step=1, value=15, key="q_correct")
        tags = st.text_input("Tags (opcional) — ex.: prova; cardio; revisão", key="q_tags")
        source = st.text_input("Fonte (opcional) — ex.: Banco X, Simulado Y", key="q_source")
        notes = st.text_area("Observações (opcional)", key="q_notes")

        if st.button("Salvar questões + criar revisão automática", type="primary", key="save_questions"):
            try:
                log_id, acc = add_question_log(user_id, subj_id, topic_id, tags, int(questions), int(correct), source, notes)
                days = compute_review_days(acc)
                st.success(f"Salvo (ID {log_id}) — {acc:.1f}% | Revisão: +{days} dias.")
            except Exception as e:
                st.error(f"Erro: {e}")

    with t2:
        st.markdown("### Simulados")
        title = st.text_input("Título do simulado", value="Simulado", key="exam_title")
        subjects = get_subjects()
        subj_opts = [{"id": None, "name": "(Sem matéria)"}] + subjects
        subj_names = [x["name"] for x in subj_opts]
        exam_subj_idx = st.selectbox("Matéria (opcional)", range(len(subj_names)), format_func=lambda i: subj_names[i], key="exam_subj")
        exam_subject_id = subj_opts[exam_subj_idx]["id"]

        c1, c2, c3 = st.columns(3)
        with c1:
            total_q = st.number_input("Total de questões", min_value=1, step=1, value=100, key="exam_total")
        with c2:
            corr = st.number_input("Acertos", min_value=0, step=1, value=80, key="exam_corr")
        with c3:
            mins = st.number_input("Duração (min)", min_value=0, step=10, value=120, key="exam_mins")

        notes = st.text_area("Observações (opcional)", key="exam_notes")

        if st.button("Salvar simulado + criar revisão automática", type="primary", key="save_exam"):
            try:
                exam_id, acc = add_exam(user_id, title, exam_subject_id, int(total_q), int(corr), int(mins)*60, notes)
                days = compute_review_days(acc)
                st.success(f"Simulado salvo (ID {exam_id}) — {acc:.1f}% | Revisão: +{days} dias (se matéria escolhida).")
            except Exception as e:
                st.error(f"Erro: {e}")

    with t3:
        st.markdown("### Sessão manual (sem cronômetro)")
        subj_id2, subj_name2, topic_id2, topic_name2 = subject_topic_picker("manual_session")
        tags2 = st.text_input("Tags (opcional)", key="man_tags")
        minutes = st.number_input("Minutos estudados", min_value=1, step=5, value=30, key="man_minutes")
        notes2 = st.text_area("Observações (opcional)", key="man_notes")

        if st.button("Salvar sessão manual", type="primary", key="save_manual"):
            try:
                save_study_session(user_id, subj_id2, topic_id2, tags2, int(minutes) * 60, "ESTUDO", notes2)
                st.success("Sessão manual salva.")
            except Exception as e:
                st.error(f"Erro: {e}")


# =========================================================
# PAGE: IMPORTAR FLASHCARDS (SESSÃO SEPARADA)
# =========================================================
elif menu == "Importar Flashcards":
    st.subheader("📥 Importar Flashcards (CSV)")

    st.caption("Você escolhe o **TEMA/ASSUNTO** aqui. Não tem escolha automática.")
    colA, colB, colC = st.columns([1.2, 1.0, 1.0])
    with colA:
        tema = st.text_input("Tema/Assunto para estes cards (obrigatório)", value="", key="imp_tema")
    with colB:
        deck_padrao = st.text_input("Deck padrão (opcional)", value="", key="imp_deck")
    with colC:
        tags_padrao = st.text_input("Tags padrão (opcional)", value="", key="imp_tags")

    c1, c2 = st.columns(2)
    with c1:
        usar_assunto_csv = st.checkbox("Se o CSV tiver coluna 'assunto', usar ela (senão usa o tema acima)", value=False, key="imp_use_assunto_csv")
    with c2:
        usar_deck_csv = st.checkbox("Se o CSV tiver coluna 'deck', usar ela (senão usa o deck padrão)", value=False, key="imp_use_deck_csv")

    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

    up = st.file_uploader("Enviar CSV", type=["csv"], key="imp_uploader")
    st.caption("Aceita separador **;** ou **,**, com ou sem cabeçalho. Sem cabeçalho: 2 a 4 colunas (Frente/Resp1/Resp2/Resp3).")

    if up is not None:
        try:
            df_csv = _read_csv_robusto(up)
            st.dataframe(df_csv.head(20), use_container_width=True)

            if st.button("✅ Importar agora", type="primary", key="imp_btn"):
                if not tema.strip():
                    st.error("Preencha o Tema/Assunto antes de importar.")
                else:
                    qtd = flash_import_df(
                        user_id=user_id,
                        df=df_csv,
                        tema_assunto=tema.strip(),
                        deck_padrao=deck_padrao.strip(),
                        tags_padrao=tags_padrao.strip(),
                        fonte_padrao="CSV",
                        usar_coluna_assunto_csv=bool(usar_assunto_csv),
                        usar_coluna_deck_csv=bool(usar_deck_csv),
                    )
                    st.success(f"Importados: {qtd} flashcards.")
                    st.info("Agora vá em **Flashcards** para revisar.")
        except Exception as e:
            st.error(f"Erro ao importar: {e}")


# =========================================================
# PAGE: FLASHCARDS (SESSÃO APENAS REVISÃO + FILTROS + PESQUISA)
# =========================================================
elif menu == "Flashcards":
    st.subheader("🧠 Flashcards")

    flash_ensure_queue_for_user(user_id)

    # Estado de navegação (assunto -> deck -> revisar)
    if "fc_assunto" not in st.session_state:
        st.session_state["fc_assunto"] = ""
    if "fc_deck" not in st.session_state:
        st.session_state["fc_deck"] = None  # None = todos; "" = sem deck
    if "fc_idx" not in st.session_state:
        st.session_state["fc_idx"] = 0

    # Topo: pesquisa
    top1, top2, top3 = st.columns([2.2, 1.0, 1.0])
    with top1:
        search = st.text_input("Pesquisar flashcards (frente/tags/fonte)", value="", key="fc_search")
    with top2:
        if st.button("🔄 Recarregar", use_container_width=True, key="fc_reload"):
            st.session_state["fc_idx"] = 0
            flash_ensure_queue_for_user(user_id)
            st.rerun()
    with top3:
        mostrar_respostas = st.checkbox("Mostrar respostas", value=False, key="fc_show_ans")

    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

    # Layout tipo lista (igual seu exemplo): coluna esquerda (assuntos/decks) e direita (card)
    left, right = st.columns([1.0, 1.55])

    # ========== COLUNA ESQUERDA: ASSUNTOS E DECKS ==========
    with left:
        st.markdown("### Assuntos")

        ass_q = st.text_input("Filtrar assuntos", value="", key="fc_assunto_search")
        assuntos = flash_counts_by_assunto(search=ass_q)

        if not assuntos:
            st.info("Nenhum assunto encontrado.")
        else:
            for item in assuntos[:60]:
                a = item["assunto"]
                total = item["total"]
                selected = (a == st.session_state["fc_assunto"])
                btn_label = f"{'✅ ' if selected else ''}{a}"
                rowA, rowB = st.columns([5, 1])
                with rowA:
                    if st.button(btn_label, use_container_width=True, key=f"pick_ass_{hash_text(a)}"):
                        st.session_state["fc_assunto"] = a
                        st.session_state["fc_deck"] = None
                        st.session_state["fc_idx"] = 0
                        st.rerun()
                with rowB:
                    st.markdown(f"<div class='chev'>›</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='list-sub'>{total} cards</div>", unsafe_allow_html=True)
                st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

        st.markdown("### Decks")

        if not st.session_state["fc_assunto"]:
            st.info("Selecione um assunto acima.")
        else:
            deck_q = st.text_input("Filtrar decks", value="", key="fc_deck_search")
            decks = flash_decks_by_assunto(st.session_state["fc_assunto"], search=deck_q)

            # opção “Todos os decks”
            sel_all = (st.session_state["fc_deck"] is None)
            if st.button(f"{'✅ ' if sel_all else ''}Todos os decks", use_container_width=True, key="pick_deck_all"):
                st.session_state["fc_deck"] = None
                st.session_state["fc_idx"] = 0
                st.rerun()

            # decks list
            for d in decks[:80]:
                deck_name = d["deck"]  # pode ser ""
                total = d["total"]
                label = deck_name if deck_name else "(Sem deck)"
                selected = (deck_name == (st.session_state["fc_deck"] if st.session_state["fc_deck"] is not None else "__NONE__"))
                rowA, rowB = st.columns([5, 1])
                with rowA:
                    if st.button(f"{'✅ ' if selected else ''}{label}", use_container_width=True, key=f"pick_deck_{hash_text(st.session_state['fc_assunto']+'|'+label)}"):
                        st.session_state["fc_deck"] = deck_name
                        st.session_state["fc_idx"] = 0
                        st.rerun()
                with rowB:
                    st.markdown(f"<div class='chev'>›</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='list-sub'>{total} cards</div>", unsafe_allow_html=True)

    # ========== COLUNA DIREITA: REVISÃO (VENCIDOS) + CARD ==========
    with right:
        st.markdown("### Revisão (vencidos)")

        if not st.session_state["fc_assunto"]:
            st.info("Escolha um assunto à esquerda para começar.")
        else:
            # filtros aplicados
            due = flash_due_list(
                user_id=user_id,
                assunto=st.session_state["fc_assunto"],
                deck=st.session_state["fc_deck"],
                search=search,
                limit=500
            )

            # métricas
            m1, m2, m3 = st.columns(3)
            m1.metric("Vencidos", f"{len(due)}")
            total_cards_ass = fetch_one("SELECT COUNT(*) FROM flashcards WHERE assunto=?", (st.session_state["fc_assunto"],))
            m2.metric("Total no assunto", f"{int(total_cards_ass[0]) if total_cards_ass else 0}")
            m3.metric("Deck selecionado", (st.session_state["fc_deck"] if st.session_state["fc_deck"] is not None else "Todos") or "(Sem deck)")

            st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

            if not due:
                st.success("Nenhum flashcard vencido com esses filtros. ✅")
                st.caption("Dica: troque o deck, limpe a pesquisa, ou aguarde os próximos dias.")
            else:
                total = len(due)
                idx = max(0, min(int(st.session_state["fc_idx"]), total - 1))
                st.session_state["fc_idx"] = idx

                (fr_id, card_id, due_date, interval_days, last_result,
                 deck, assunto, ctype,
                 front, back1, back2, back3,
                 cloze, tags, source) = due[idx]

                # Navegação horizontal
                navL, navM, navR = st.columns([1, 3, 1])
                with navL:
                    if st.button("⬅️ Anterior", use_container_width=True, key="fc_prev"):
                        st.session_state["fc_idx"] = max(0, idx - 1)
                        st.rerun()
                with navM:
                    st.progress((idx + 1) / max(1, total))
                    st.caption(f"Card {idx+1} de {total} | Vencido: {due_date} | Intervalo: {interval_days} dias | Último: {last_result or '—'}")
                with navR:
                    if st.button("Próximo ➡️", use_container_width=True, key="fc_next"):
                        st.session_state["fc_idx"] = min(total - 1, idx + 1)
                        st.rerun()

                # Card
                st.markdown("<div class='flash-card'>", unsafe_allow_html=True)
                meta = f"<span class='badge'>{assunto}</span>  <span class='badge'>{deck or '(Sem deck)'}</span>  <span class='badge'>{source or 'Sem fonte'}</span>"
                st.markdown(f"<div class='flash-meta'>{meta}</div>", unsafe_allow_html=True)
                if tags:
                    st.markdown(f"<div class='flash-meta'>Tags: {tags}</div>", unsafe_allow_html=True)
                if ctype == "CLOZE" and cloze:
                    st.markdown(cloze)
                st.markdown(f"<div class='flash-front'>{front}</div>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

                # Respostas (3 campos UM ABAIXO DO OUTRO)
                if mostrar_respostas:
                    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

                    st.markdown("<div class='answer-box'>", unsafe_allow_html=True)
                    st.markdown("<div class='answer-title'>Resposta 1</div>", unsafe_allow_html=True)
                    st.write(back1 or "—")
                    st.markdown("</div>", unsafe_allow_html=True)

                    st.markdown("<div class='answer-box'>", unsafe_allow_html=True)
                    st.markdown("<div class='answer-title'>Resposta 2</div>", unsafe_allow_html=True)
                    st.write(back2 or "—")
                    st.markdown("</div>", unsafe_allow_html=True)

                    st.markdown("<div class='answer-box'>", unsafe_allow_html=True)
                    st.markdown("<div class='answer-title'>Resposta 3</div>", unsafe_allow_html=True)
                    st.write(back3 or "—")
                    st.markdown("</div>", unsafe_allow_html=True)

                st.markdown("<hr class='soft'/>", unsafe_allow_html=True)
                st.markdown("### Como foi?")
                b1, b2, b3, b4 = st.columns(4)

                if b1.button("😊 Facinho", use_container_width=True, key="fc_facinho"):
                    res = flash_mark_result(user_id, int(fr_id), "FACINHO")
                    if res:
                        due2, nxt = res
                        st.success(f"Próxima revisão: {due2} (intervalo {nxt} dias)")
                    st.rerun()

                if b2.button("🙂 Mediano", use_container_width=True, key="fc_mediano"):
                    res = flash_mark_result(user_id, int(fr_id), "MEDIANO")
                    if res:
                        due2, nxt = res
                        st.success(f"Próxima revisão: {due2} (intervalo {nxt} dias)")
                    st.rerun()

                if b3.button("😕 Não sabia", use_container_width=True, key="fc_nao_sabia"):
                    res = flash_mark_result(user_id, int(fr_id), "NAO_SABIA")
                    if res:
                        due2, nxt = res
                        st.warning(f"Próxima revisão: {due2} (intervalo {nxt} dias)")
                    st.rerun()

                if b4.button("🥶 Impossível lembrar", use_container_width=True, key="fc_impossivel"):
                    res = flash_mark_result(user_id, int(fr_id), "IMPOSSIVEL")
                    if res:
                        due2, nxt = res
                        st.error(f"Próxima revisão: {due2} (intervalo {nxt} dias)")
                    st.rerun()


# =========================================================
# PAGE: REVISÕES
# =========================================================
elif menu == "Revisões":
    st.subheader("🗂️ Fila de revisões")
    tab1, tab2 = st.tabs(["Pendentes", "Concluídas"])

    with tab1:
        rows = fetch_all("""
            SELECT r.id, r.due_date, s.name, COALESCE(t.name,'(Sem subtema)'),
                   r.origin_type, COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id = r.subject_id
            LEFT JOIN topics t ON t.id = r.topic_id
            WHERE r.user_id = ? AND r.status='PENDENTE'
            ORDER BY r.due_date ASC
        """, (user_id,))
        if not rows:
            st.info("Sem revisões pendentes.")
        else:
            df = pd.DataFrame(rows, columns=["id","due_date","subject","topic","origin","last_accuracy"])
            st.dataframe(df, use_container_width=True, hide_index=True)

            c1, c2, c3 = st.columns(3)
            with c1:
                rid = st.number_input("ID", min_value=1, step=1, value=int(df.iloc[0]["id"]), key="rev_id")
            with c2:
                if st.button("✅ Concluir", use_container_width=True, key="rev_done"):
                    execute("""
                        UPDATE reviews SET status='CONCLUIDA', completed_at=?
                        WHERE id=? AND user_id=?
                    """, (now_str(), int(rid), user_id))
                    audit(user_id, "CONCLUIR_REVISAO", "reviews", int(rid), "done")
                    st.success("Concluída!")
                    st.rerun()
            with c3:
                if st.button("🗑️ Excluir", use_container_width=True, key="rev_del"):
                    execute("DELETE FROM reviews WHERE id=? AND user_id=?", (int(rid), user_id))
                    audit(user_id, "EXCLUIR_REVISAO", "reviews", int(rid), "deleted")
                    st.success("Excluída!")
                    st.rerun()

    with tab2:
        rows = fetch_all("""
            SELECT r.id, r.due_date, r.completed_at, s.name, COALESCE(t.name,'(Sem subtema)'),
                   r.origin_type, COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id = r.subject_id
            LEFT JOIN topics t ON t.id = r.topic_id
            WHERE r.user_id = ? AND r.status='CONCLUIDA'
            ORDER BY r.completed_at DESC
            LIMIT 400
        """, (user_id,))
        if not rows:
            st.info("Sem revisões concluídas ainda.")
        else:
            df = pd.DataFrame(rows, columns=["id","due_date","completed_at","subject","topic","origin","last_accuracy"])
            st.dataframe(df, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: DASHBOARD
# =========================================================
elif menu == "Dashboard":
    st.subheader("📊 Dashboard BI")
    days_back = st.slider("Janela (dias)", 7, 365, 90, key="dash_days")

    dfq = df_question_logs(user_id, days_back=days_back)
    dfs = df_study_sessions(user_id, days_back=days_back)
    dfe = df_exams(user_id, days_back=days_back)

    total_q = int(dfq["questions"].sum()) if not dfq.empty else 0
    total_c = int(dfq["correct"].sum()) if not dfq.empty else 0
    acc = (total_c / total_q * 100.0) if total_q > 0 else 0.0
    total_min = float(dfs["duration_seconds"].sum() / 60.0) if not dfs.empty else 0.0
    total_exams = int(len(dfe)) if not dfe.empty else 0
    avg_exam_acc = float(dfe["accuracy"].mean()) if not dfe.empty else 0.0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Questões", f"{total_q}")
    col2.metric("Acertos", f"{total_c}")
    col3.metric("% Questões", f"{acc:.1f}%")
    col4.metric("Tempo", f"{total_min:.1f} min")
    col5.metric("Simulados", f"{total_exams} (média {avg_exam_acc:.1f}%)")

    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

    cA, cB = st.columns(2)
    with cA:
        st.markdown("### Questões por dia")
        if dfq.empty:
            st.info("Sem registros de questões nesse período.")
        else:
            by_day = dfq.groupby("date")[["questions","correct"]].sum().reset_index()
            fig = plt.figure()
            plt.plot(by_day["date"], by_day["questions"])
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

    with cB:
        st.markdown("### % de acerto por dia")
        if dfq.empty:
            st.info("Sem registros de questões nesse período.")
        else:
            by_day = dfq.groupby("date")[["questions","correct"]].sum().reset_index()
            by_day["accuracy"] = by_day["correct"] / by_day["questions"] * 100.0
            fig = plt.figure()
            plt.plot(by_day["date"], by_day["accuracy"])
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)


# =========================================================
# PAGE: METAS & ALERTAS
# =========================================================
elif menu == "Metas & Alertas":
    st.subheader("🎯 Metas e Alertas")
    q_goal, min_goal, exams_goal = get_goals(user_id)
    inactive_days, drop_acc = get_prefs(user_id)

    c1, c2, c3 = st.columns(3)
    with c1:
        new_q = st.number_input("Meta diária de questões", min_value=0, step=10, value=int(q_goal), key="goal_q")
    with c2:
        new_min = st.number_input("Meta diária de tempo (min)", min_value=0, step=10, value=int(min_goal), key="goal_min")
    with c3:
        new_exams = st.number_input("Meta de simulados por mês", min_value=0, step=1, value=int(exams_goal), key="goal_ex")

    st.markdown("<hr class='soft'/>", unsafe_allow_html=True)

    st.markdown("### Alertas inteligentes (configurações)")
    c4, c5 = st.columns(2)
    with c4:
        new_inactive = st.number_input("Alertar matéria parada após (dias)", min_value=1, step=1, value=int(inactive_days), key="pref_inactive")
    with c5:
        new_drop = st.number_input("Alertar queda de % (pontos) em 14 dias", min_value=1.0, step=1.0, value=float(drop_acc), key="pref_drop")

    if st.button("Salvar metas e alertas", type="primary", key="save_goals_prefs"):
        set_goals(user_id, int(new_q), int(new_min), int(new_exams))
        set_prefs(user_id, int(new_inactive), float(new_drop))
        st.success("Salvo.")


# =========================================================
# PAGE: RELATÓRIOS PDF
# =========================================================
elif menu == "Relatórios (PDF)":
    st.subheader("🧾 Relatório em PDF")
    st.write("Resumo do período + revisões vencidas/próximas + desempenho geral.")

    c1, c2 = st.columns(2)
    with c1:
        d1 = st.date_input("Data inicial", value=date.today() - timedelta(days=30), key="pdf_d1")
    with c2:
        d2 = st.date_input("Data final", value=date.today(), key="pdf_d2")

    if d2 < d1:
        st.error("Data final não pode ser menor que a inicial.")
    else:
        if st.button("Gerar PDF", type="primary", key="pdf_btn"):
            try:
                pdf_bytes = generate_pdf_report(user_id, username, d1, d2)
                st.download_button(
                    "⬇️ Baixar PDF",
                    data=pdf_bytes,
                    file_name=f"{APP_NAME}_relatorio_{d1.isoformat()}_a_{d2.isoformat()}.pdf",
                    mime="application/pdf",
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
    st.subheader("🧰 Gerenciar dados (editar / excluir)")
    t1, t2, t3, t4 = st.tabs(["Questões", "Sessões", "Simulados", "Revisões"])

    with t1:
        dfq = df_question_logs(user_id, days_back=3650)
        if dfq.empty:
            st.info("Sem registros.")
        else:
            st.dataframe(dfq.drop(columns=["date"], errors="ignore"), use_container_width=True, hide_index=True)
            rid = st.number_input("ID do registro", min_value=1, step=1, value=int(dfq.iloc[0]["id"]), key="del_q_id")
            if st.button("🗑️ Excluir registro", use_container_width=True, key="del_q_btn"):
                execute("DELETE FROM question_logs WHERE id=? AND user_id=?", (int(rid), user_id))
                audit(user_id, "EXCLUIR_QUESTOES", "question_logs", int(rid), "deleted")
                st.success("Excluído.")
                st.rerun()

    with t2:
        dfs = df_study_sessions(user_id, days_back=3650)
        if dfs.empty:
            st.info("Sem sessões.")
        else:
            st.dataframe(dfs.drop(columns=["date"], errors="ignore"), use_container_width=True, hide_index=True)
            sid = st.number_input("ID da sessão", min_value=1, step=1, value=int(dfs.iloc[0]["id"]), key="del_sess_id")
            if st.button("🗑️ Excluir sessão", use_container_width=True, key="del_sess_btn"):
                execute("DELETE FROM study_sessions WHERE id=? AND user_id=?", (int(sid), user_id))
                audit(user_id, "EXCLUIR_SESSAO", "study_sessions", int(sid), "deleted")
                st.success("Sessão excluída.")
                st.rerun()

    with t3:
        dfe = df_exams(user_id, days_back=3650)
        if dfe.empty:
            st.info("Sem simulados.")
        else:
            st.dataframe(dfe.drop(columns=["date"], errors="ignore"), use_container_width=True, hide_index=True)
            eid = st.number_input("ID do simulado", min_value=1, step=1, value=int(dfe.iloc[0]["id"]), key="del_exam_id")
            if st.button("🗑️ Excluir simulado", use_container_width=True, key="del_exam_btn"):
                execute("DELETE FROM exams WHERE id=? AND user_id=?", (int(eid), user_id))
                audit(user_id, "EXCLUIR_SIMULADO", "exams", int(eid), "deleted")
                st.success("Simulado excluído.")
                st.rerun()

    with t4:
        rows = fetch_all("""
            SELECT r.id, r.due_date, r.status, s.name, COALESCE(t.name,'(Sem subtema)'),
                   r.origin_type, COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id=r.subject_id
            LEFT JOIN topics t ON t.id=r.topic_id
            WHERE r.user_id=?
            ORDER BY r.status ASC, r.due_date ASC
        """, (user_id,))
        if not rows:
            st.info("Sem revisões.")
        else:
            df = pd.DataFrame(rows, columns=["id","due_date","status","subject","topic","origin","last_accuracy"])
            st.dataframe(df, use_container_width=True, hide_index=True)
            rid = st.number_input("ID da revisão", min_value=1, step=1, value=int(df.iloc[0]["id"]), key="del_rev_id")
            if st.button("🗑️ Excluir revisão", use_container_width=True, key="del_rev_btn"):
                execute("DELETE FROM reviews WHERE id=? AND user_id=?", (int(rid), user_id))
                audit(user_id, "EXCLUIR_REVISAO", "reviews", int(rid), "deleted")
                st.success("Revisão excluída.")
                st.rerun()


# =========================================================
# PAGE: EXPORT/IMPORT CSV
# =========================================================
elif menu == "Exportar/Importar CSV":
    st.subheader("🔁 Exportar / Importar CSV (backup e migração)")
    t1, t2 = st.tabs(["Exportar", "Importar"])

    with t1:
        dfq = df_question_logs(user_id, days_back=3650)
        dfs = df_study_sessions(user_id, days_back=3650)
        dfe = df_exams(user_id, days_back=3650)

        dfr = pd.DataFrame(fetch_all("""
            SELECT r.id, r.due_date, r.status, s.name, COALESCE(t.name,'(Sem subtema)'),
                   r.origin_type, COALESCE(r.last_accuracy,0), r.created_at, COALESCE(r.completed_at,'')
            FROM reviews r
            JOIN subjects s ON s.id=r.subject_id
            LEFT JOIN topics t ON t.id=r.topic_id
            WHERE r.user_id=?
        """, (user_id,)), columns=["id","due_date","status","subject","topic","origin_type","last_accuracy","created_at","completed_at"])

        df_fc = pd.DataFrame(fetch_all("""
            SELECT id, COALESCE(deck,''), assunto, COALESCE(tags,''), f_front,
                   COALESCE(f_back1,''), COALESCE(f_back2,''), COALESCE(f_back3,''),
                   card_type, COALESCE(source,''), created_at
            FROM flashcards
        """), columns=["id","deck","assunto","tags","frente","resposta1","resposta2","resposta3","tipo","fonte","created_at"])

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.download_button("⬇️ Questões CSV", dfq.to_csv(index=False).encode("utf-8"), "questoes.csv", "text/csv")
        with c2:
            st.download_button("⬇️ Sessões CSV", dfs.to_csv(index=False).encode("utf-8"), "sessoes.csv", "text/csv")
        with c3:
            st.download_button("⬇️ Simulados CSV", dfe.to_csv(index=False).encode("utf-8"), "simulados.csv", "text/csv")
        with c4:
            st.download_button("⬇️ Revisões CSV", dfr.to_csv(index=False).encode("utf-8"), "revisoes.csv", "text/csv")
        with c5:
            st.download_button("⬇️ Flashcards CSV", df_fc.to_csv(index=False).encode("utf-8"), "flashcards.csv", "text/csv")

    with t2:
        st.warning("Importação aqui é só para backup geral. Para flashcards, use o menu **Importar Flashcards**.")
        st.info("Se quiser, eu adapto importação completa de todos os CSVs aqui também.")


# =========================================================
# PAGE: MATÉRIAS / SUBTEMAS
# =========================================================
elif menu == "Matérias/Subtemas":
    st.subheader("📚 Gerenciar matérias e subtemas")
    tab1, tab2 = st.tabs(["Matérias", "Subtemas"])

    with tab1:
        new_subject = st.text_input("Nome da matéria", key="subj_new_name")
        if st.button("Adicionar matéria", type="primary", key="subj_add"):
            if not new_subject.strip():
                st.error("Digite um nome.")
            else:
                execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (new_subject.strip(), now_str()))
                audit(user_id, "CRIAR_MATERIA", "subjects", None, new_subject.strip())
                st.success("Matéria adicionada (ou já existia).")
                st.rerun()

        subs = fetch_all("SELECT id, name, created_at FROM subjects ORDER BY name;")
        df = pd.DataFrame(subs, columns=["id","name","created_at"])
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab2:
        subjects = get_subjects()
        if not subjects:
            st.warning("Crie uma matéria primeiro.")
        else:
            subj_names = [s["name"] for s in subjects]
            idx = st.selectbox("Matéria", range(len(subj_names)), format_func=lambda i: subj_names[i], key="topic_subj_pick")
            subject_id = subjects[idx]["id"]
            topic_name = st.text_input("Nome do subtema", key="topic_new_name")
            if st.button("Adicionar subtema", type="primary", key="topic_add"):
                if not topic_name.strip():
                    st.error("Digite um nome.")
                else:
                    execute("INSERT OR IGNORE INTO topics (subject_id, name, created_at) VALUES (?, ?, ?)",
                            (subject_id, topic_name.strip(), now_str()))
                    audit(user_id, "CRIAR_SUBTEMA", "topics", None, f"{subject_id}:{topic_name.strip()}")
                    st.success("Subtema adicionado (ou já existia).")
                    st.rerun()

            topics = fetch_all("SELECT id, name, created_at FROM topics WHERE subject_id=? ORDER BY name;", (subject_id,))
            df2 = pd.DataFrame(topics, columns=["id","name","created_at"])
            st.dataframe(df2, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: USUÁRIOS (CORRIGIDO)
# =========================================================
elif menu == "Usuários":
    st.subheader("👥 Usuários")
    st.info("Padrão: **admin / admin123**. Crie usuários e troque senhas aqui.")
    tab1, tab2, tab3 = st.tabs(["Criar usuário", "Trocar senha (logado)", "Listar usuários"])

    with tab1:
        new_u = st.text_input("Novo usuário", key="usr_new_username")
        new_p = st.text_input("Nova senha", type="password", key="usr_new_password")
        if st.button("Criar usuário", type="primary", key="usr_create_btn"):
            if not new_u.strip() or not new_p:
                st.error("Preencha usuário e senha.")
            else:
                salt = secrets.token_hex(16)
                pw_hash = hash_password(new_p, salt)
                try:
                    uid = execute("""
                        INSERT INTO users (username, salt, password_hash, created_at)
                        VALUES (?, ?, ?, ?)
                    """, (new_u.strip(), salt, pw_hash, now_str()))
                    audit(user_id, "CRIAR_USUARIO", "users", uid, new_u.strip())
                    st.success("Usuário criado.")
                except Exception as e:
                    st.error(f"Erro: {e}")

    with tab2:
        current = st.text_input("Senha atual", type="password", key="usr_pw_current")
        newpass = st.text_input("Nova senha", type="password", key="usr_pw_new")
        newpass2 = st.text_input("Repetir nova senha", type="password", key="usr_pw_new2")
        if st.button("Atualizar senha", type="primary", key="usr_pw_btn"):
            u = get_user_by_username(username)
            if not u:
                st.error("Usuário logado não encontrado.")
            elif not check_password(current, u["salt"], u["hash"]):
                st.error("Senha atual incorreta.")
            elif newpass != newpass2 or not newpass:
                st.error("Nova senha inválida ou não confere.")
            else:
                salt = secrets.token_hex(16)
                pw_hash = hash_password(newpass, salt)
                execute("UPDATE users SET salt=?, password_hash=? WHERE id=?", (salt, pw_hash, user_id))
                audit(user_id, "TROCAR_SENHA", "users", user_id, "changed")
                st.success("Senha atualizada. ✅")

    with tab3:
        rows = fetch_all("SELECT id, username, created_at FROM users ORDER BY created_at DESC;")
        df = pd.DataFrame(rows, columns=["id","username","created_at"])
        st.dataframe(df, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: AUDITORIA
# =========================================================
elif menu == "Auditoria":
    st.subheader("🧾 Auditoria (ações registradas)")
    rows = fetch_all("""
        SELECT a.id, a.created_at, u.username, a.action, COALESCE(a.entity,''), COALESCE(a.entity_id,''), COALESCE(a.details,'')
        FROM audit_log a
        JOIN users u ON u.id=a.user_id
        ORDER BY a.created_at DESC
        LIMIT 500
    """)
    df = pd.DataFrame(rows, columns=["id","created_at","user","action","entity","entity_id","details"])
    st.dataframe(df, use_container_width=True, hide_index=True)
