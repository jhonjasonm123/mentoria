import os
import sqlite3
import hashlib
import secrets
import time
import math
import re
from datetime import datetime, date, timedelta
from io import BytesIO

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

# Regras de revisão:
# <80% = 20 dias; 80–90% = 30 dias; >90% = 45 dias
def compute_review_days(accuracy_pct: float) -> int:
    if accuracy_pct < 80:
        return 20
    if accuracy_pct <= 90:
        return 30
    return 45


# =========================================================
# ===== NOVO: FLASHCARDS (REVISÃO 7 / 2 dias e 1.5x) =====
# =========================================================
FLASH_KNOW_DAYS = 7
FLASH_DONTKNOW_DAYS = 2
FLASH_MULT = 1.5

def flash_next_interval(prev_days: int, knew: bool) -> int:
    """
    Regra:
    - se primeira vez: 7 (soube) ou 2 (não soube)
    - depois: multiplica por 1.5x sempre e arredonda pra cima
    """
    if not prev_days or prev_days <= 0:
        base = FLASH_KNOW_DAYS if knew else FLASH_DONTKNOW_DAYS
        return int(base)
    return int(math.ceil(prev_days * FLASH_MULT))


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

def format_minutes(mins: float) -> str:
    return f"{mins:.1f} min"

def audit(user_id: int, action: str, entity: str = None, entity_id: int = None, details: str = None):
    execute("""
        INSERT INTO audit_log (user_id, action, entity, entity_id, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, action, entity, entity_id, details, now_str()))


# =========================================================
# ===== NOVO: NORMALIZAÇÃO / HASH para banco de questões =====
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

    # Sessões de estudo (cronômetro)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS study_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        topic_id INTEGER,
        tags TEXT,
        duration_seconds INTEGER NOT NULL,
        session_type TEXT NOT NULL DEFAULT 'ESTUDO', -- ESTUDO / POMODORO
        notes TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE,
        FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE SET NULL
    );
    """)

    # Metas (questões e tempo por dia + simulados/mês)
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

    # Revisões (fila)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        topic_id INTEGER,
        due_date TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDENTE', -- PENDENTE / CONCLUIDA
        origin_type TEXT DEFAULT 'QUESTOES',     -- QUESTOES / SIMULADO / MANUAL
        origin_id INTEGER,                      -- id do log/simulado
        last_accuracy REAL,
        created_at TEXT NOT NULL,
        completed_at TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE,
        FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE SET NULL
    );
    """)

    # Preferências (alertas)
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
    # ===== NOVO: BANCO DE QUESTÕES + TENTATIVAS POR ASSUNTO =====
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS question_bank (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        assunto TEXT NOT NULL,
        subassunto TEXT,
        enunciado TEXT NOT NULL,
        alternativas TEXT,
        gabarito TEXT,
        explicacao TEXT,
        source TEXT,
        q_hash TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS question_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        question_id INTEGER NOT NULL,
        assunto TEXT NOT NULL,
        is_correct INTEGER NOT NULL,
        seconds_spent INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(question_id) REFERENCES question_bank(id) ON DELETE CASCADE
    );
    """)

    # =========================================================
    # ===== NOVO: FLASHCARDS + FILA DE REVISÃO =====
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flashcards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deck TEXT,
        assunto TEXT NOT NULL,
        tags TEXT,
        f_front TEXT NOT NULL,
        f_back TEXT,
        f_cloze TEXT,
        card_type TEXT NOT NULL DEFAULT 'BASIC', -- BASIC / CLOZE
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
        last_result TEXT, -- KNEW / DONTKNOW
        status TEXT NOT NULL DEFAULT 'PENDENTE', -- PENDENTE / CONCLUIDA
        last_reviewed_at TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY(card_id) REFERENCES flashcards(id) ON DELETE CASCADE
    );
    """)

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
# ===== NOVO: FUNÇÕES DO BANCO DE QUESTÕES / FLASHCARDS =====
# =========================================================
def qb_upsert_question(assunto: str, subassunto: str, enunciado: str,
                       alternativas: str = None, gabarito: str = None,
                       explicacao: str = None, source: str = None):
    if not assunto or not enunciado:
        return None
    h = hash_text(f"{assunto}|{subassunto}|{enunciado}|{alternativas}|{gabarito}")
    try:
        qid = execute("""
            INSERT OR IGNORE INTO question_bank
            (assunto, subassunto, enunciado, alternativas, gabarito, explicacao, source, q_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (assunto.strip(), (subassunto or "").strip() or None, enunciado.strip(),
              (alternativas or "").strip() or None, (gabarito or "").strip() or None,
              (explicacao or "").strip() or None, (source or "").strip() or None,
              h, now_str()))
        return qid
    except Exception:
        return None

def qb_list_assuntos():
    rows = fetch_all("SELECT DISTINCT assunto FROM question_bank ORDER BY assunto;")
    return [r[0] for r in rows]

def qb_search(assunto: str = None, q: str = None, limit: int = 200):
    sql = """
        SELECT id, assunto, COALESCE(subassunto,''), enunciado, COALESCE(source,'')
        FROM question_bank
        WHERE 1=1
    """
    params = []
    if assunto and assunto != "Todos":
        sql += " AND assunto = ?"
        params.append(assunto)
    if q and q.strip():
        sql += " AND (enunciado LIKE ? OR subassunto LIKE ?)"
        params.extend([f"%{q.strip()}%", f"%{q.strip()}%"])
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))
    return fetch_all(sql, tuple(params))

def qb_register_attempt(user_id: int, question_id: int, assunto: str, is_correct: bool, seconds_spent: int = 0):
    execute("""
        INSERT INTO question_attempts (user_id, question_id, assunto, is_correct, seconds_spent, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, int(question_id), assunto, 1 if is_correct else 0, int(seconds_spent or 0), now_str()))
    audit(user_id, "ATTEMPT_QUESTION", "question_attempts", None, f"qid={question_id}, assunto={assunto}, ok={is_correct}")

def stats_attempts_today_by_assunto(user_id: int):
    rows = fetch_all("""
        SELECT assunto,
               COUNT(*) as total,
               SUM(is_correct) as correct
        FROM question_attempts
        WHERE user_id=? AND DATE(created_at)=DATE(?)
        GROUP BY assunto
        ORDER BY total DESC
    """, (user_id, today_str()))
    out = []
    for a, total, corr in rows:
        total = int(total or 0)
        corr = int(corr or 0)
        acc = (corr/total*100.0) if total > 0 else 0.0
        out.append((a, total, corr, acc))
    return out

def stats_attempts_window_by_assunto(user_id: int, days_back: int = 90):
    since = (date.today() - timedelta(days=days_back)).isoformat()
    rows = fetch_all("""
        SELECT assunto,
               COUNT(*) as total,
               SUM(is_correct) as correct
        FROM question_attempts
        WHERE user_id=? AND DATE(created_at) >= DATE(?)
        GROUP BY assunto
        ORDER BY total DESC
    """, (user_id, since))
    out = []
    for a, total, corr in rows:
        total = int(total or 0)
        corr = int(corr or 0)
        acc = (corr/total*100.0) if total > 0 else 0.0
        out.append((a, total, corr, acc))
    return out

def simulado_auto_pick_questions(user_id: int, n: int = 20, days_back: int = 180):
    """
    Puxa mais questões dos assuntos com pior desempenho recente.
    Se não houver desempenho, distribui uniforme.
    """
    assuntos = qb_list_assuntos()
    if not assuntos:
        return []

    stats = stats_attempts_window_by_assunto(user_id, days_back=days_back)
    stat_map = {a: (total, corr, acc) for a, total, corr, acc in stats}

    weights = []
    for a in assuntos:
        if a in stat_map:
            _, _, acc = stat_map[a]
            w = max(1.0, 100.0 - float(acc))  # pior acc => maior peso
        else:
            w = 50.0  # sem histórico: peso médio
        weights.append(w)

    # normaliza e define quantas questões por assunto
    total_w = sum(weights) or 1.0
    quotas = {}
    for a, w in zip(assuntos, weights):
        quotas[a] = max(1, int(round((w / total_w) * n)))

    # ajusta para ficar exatamente n
    picked = []
    for a in sorted(quotas.keys(), key=lambda x: quotas[x], reverse=True):
        rows = qb_search(assunto=a, q=None, limit=500)
        # evita pegar só as mais novas sempre: dá uma "andada" com offset simples
        if not rows:
            continue
        need = quotas[a]
        for r in rows[:need]:
            picked.append(r)
            if len(picked) >= n:
                break
        if len(picked) >= n:
            break

    return picked[:n]

def simple_variation_text(enunciado: str) -> str:
    """
    Variação offline leve (sem IA):
    - troca alguns conectores, remove duplicidades, muda pequenas frases
    """
    s = (enunciado or "").strip()
    if not s:
        return s
    rep = [
        ("Assinale a alternativa correta", "Marque a opção correta"),
        ("assinale a alternativa correta", "marque a opção correta"),
        ("qual é", "qual alternativa representa"),
        ("sobre", "a respeito de"),
        ("é correto afirmar", "é verdadeiro dizer"),
    ]
    for a, b in rep:
        if a in s:
            s = s.replace(a, b)
            break
    s = re.sub(r"\s+", " ", s).strip()
    return s

def flash_upsert_card(deck: str, assunto: str, tags: str, front: str, back: str, cloze: str, card_type: str, source: str):
    if not assunto or not front:
        return None
    h = hash_text(f"{deck}|{assunto}|{tags}|{front}|{back}|{cloze}|{card_type}")
    try:
        cid = execute("""
            INSERT OR IGNORE INTO flashcards
            (deck, assunto, tags, f_front, f_back, f_cloze, card_type, source, c_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ((deck or "").strip() or None, assunto.strip(), (tags or "").strip() or None,
              front.strip(), (back or "").strip() or None, (cloze or "").strip() or None,
              (card_type or "BASIC").strip().upper(), (source or "").strip() or None,
              h, now_str()))
        return cid
    except Exception:
        return None

def flash_ensure_queue(user_id: int, card_id: int):
    # se não existir fila para esse card, cria pendente com due hoje
    row = fetch_one("SELECT id FROM flash_reviews WHERE user_id=? AND card_id=?", (user_id, card_id))
    if row:
        return row[0]
    rid = execute("""
        INSERT INTO flash_reviews (user_id, card_id, due_date, interval_days, last_result, status, last_reviewed_at, created_at)
        VALUES (?, ?, ?, 0, NULL, 'PENDENTE', NULL, ?)
    """, (user_id, card_id, today_str(), now_str()))
    return rid

def flash_due_cards(user_id: int, limit: int = 30):
    rows = fetch_all("""
        SELECT fr.id, fr.card_id, fr.due_date, fr.interval_days, COALESCE(fr.last_result,''), f.assunto, COALESCE(f.deck,''), f.card_type,
               f.f_front, COALESCE(f.f_back,''), COALESCE(f.f_cloze,''), COALESCE(f.tags,''), COALESCE(f.source,'')
        FROM flash_reviews fr
        JOIN flashcards f ON f.id = fr.card_id
        WHERE fr.user_id=? AND fr.status='PENDENTE' AND DATE(fr.due_date) <= DATE(?)
        ORDER BY fr.due_date ASC
        LIMIT ?
    """, (user_id, today_str(), int(limit)))
    return rows

def flash_mark(user_id: int, fr_id: int, knew: bool):
    row = fetch_one("SELECT interval_days, card_id FROM flash_reviews WHERE id=? AND user_id=?", (int(fr_id), user_id))
    if not row:
        return None
    prev = int(row[0] or 0)
    card_id = int(row[1])
    nxt = flash_next_interval(prev, knew=knew)
    due = (date.today() + timedelta(days=nxt)).isoformat()
    execute("""
        UPDATE flash_reviews
        SET due_date=?, interval_days=?, last_result=?, last_reviewed_at=?, status='PENDENTE'
        WHERE id=? AND user_id=?
    """, (due, int(nxt), "KNEW" if knew else "DONTKNOW", now_str(), int(fr_id), user_id))
    audit(user_id, "FLASH_REVIEW", "flash_reviews", int(fr_id), f"knew={knew}, next={nxt}d, due={due}")
    return due, nxt, card_id


# =========================================================
# AUTH + SETUP ROWS
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
    audit(user_id, "CREATE_REVIEW", "reviews", rid, f"due={due}, acc={accuracy:.1f}, origin={origin_type}:{origin_id}")
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
    audit(user_id, "CREATE_QUESTIONS", "question_logs", log_id, f"q={questions}, c={correct}, acc={accuracy:.1f}")
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
    audit(user_id, "CREATE_EXAM", "exams", exam_id, f"title={title}, acc={accuracy:.1f}")

    # revisão vinculada ao simulado (associada à matéria, sem subtema)
    if subject_id is not None:
        add_review(user_id, subject_id, None, accuracy, "SIMULADO", exam_id)

    return exam_id, accuracy


# =========================================================
# TIMER + POMODORO
# =========================================================
def timer_init():
    if "timer_running" not in st.session_state:
        st.session_state["timer_running"] = False
    if "timer_start_ts" not in st.session_state:
        st.session_state["timer_start_ts"] = None
    if "timer_accumulated" not in st.session_state:
        st.session_state["timer_accumulated"] = 0  # segundos
    if "pomo_mode" not in st.session_state:
        st.session_state["pomo_mode"] = False
    if "pomo_phase" not in st.session_state:
        st.session_state["pomo_phase"] = "FOCO"  # FOCO / PAUSA
    if "pomo_focus_min" not in st.session_state:
        st.session_state["pomo_focus_min"] = 25
    if "pomo_break_min" not in st.session_state:
        st.session_state["pomo_break_min"] = 5
    if "pomo_cycles_done" not in st.session_state:
        st.session_state["pomo_cycles_done"] = 0

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
    audit(user_id, "CREATE_SESSION", "study_sessions", sid, f"type={session_type}, sec={duration_seconds}")
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
    audit(user_id, "UPDATE_GOALS", "goals", None, f"q={q_goal}, min={min_goal}, exams={exams_goal}")

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
    audit(user_id, "UPDATE_PREFS", "prefs", None, f"inactive_days={inactive_days}, drop_acc={drop_acc}")

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

    # 1) matérias paradas
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

    # 2) queda de desempenho (comparar últimos 14 dias vs 14 dias anteriores)
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
# PDF REPORT (completo)
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

    # Página 1: resumo
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
    y -= 1.0*cm

    # Top matérias
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "Top matérias (por questões)")
    y -= 0.6*cm
    c.setFont("Helvetica", 10)
    if not dfq.empty:
        top = dfq.groupby("subject")["questions"].sum().sort_values(ascending=False).head(10)
        for subj, val in top.items():
            c.drawString(2*cm, y, f"- {subj}: {int(val)}")
            y -= 0.45*cm
            if y < 3*cm:
                footer(); c.showPage()
                header(APP_NAME + " — Relatório", "Continuação")
                y = height - 4*cm
                c.setFont("Helvetica", 10)
    else:
        c.drawString(2*cm, y, "Sem registros de questões no período.")
        y -= 0.6*cm

    footer()
    c.showPage()

    # Página 2: revisões vencidas
    header(APP_NAME + " — Revisões", "Vencidas (pendentes)")
    y = height - 4*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "Vencidas")
    y -= 0.6*cm
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
        y -= 0.6*cm
    footer()
    c.showPage()

    # Página 3: próximas revisões
    header(APP_NAME + " — Revisões", "Próximas (pendentes)")
    y = height - 4*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "Próximas")
    y -= 0.6*cm
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
        y -= 0.6*cm
    footer()
    c.showPage()

    c.save()
    buffer.seek(0)
    return buffer.read()


# =========================================================
# UI STYLE
# =========================================================
st.markdown(
    """
<style>
.block-container { padding-top: 1.2rem; }
div[data-testid="stMetric"] { padding: 12px; border-radius: 14px;
  background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.06);
}
.small-muted { opacity: 0.8; font-size: 0.9rem; }
.badge { display:inline-block; padding: 0.15rem 0.5rem; border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.14); background: rgba(255,255,255,0.04);
  font-size: 0.85rem;
}
</style>
""",
    unsafe_allow_html=True,
)


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
        "Banco de Questões (Assuntos)",
        "Flashcards",
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

    # métricas principais
    colA, colB, colC, colD, colE = st.columns(5)
    colA.metric("Questões hoje", f"{qs}")
    colB.metric("Acertos hoje", f"{corr}")
    colC.metric("% hoje", f"{acc_today:.1f}%" if acc_today is not None else "—")
    colD.metric("Tempo hoje", f"{min_today:.1f} min")
    colE.metric("Simulados hoje", f"{exams_today}")

    st.divider()

    # ===== NOVO: Estatística do dia por ASSUNTO (banco de questões) =====
    with st.expander("📌 Estatísticas do dia por ASSUNTO (Banco de Questões)", expanded=True):
        rows = stats_attempts_today_by_assunto(user_id)
        if not rows:
            st.info("Sem tentativas no Banco de Questões hoje. Use o menu **Banco de Questões** para responder questões por assunto.")
        else:
            df_ass = pd.DataFrame(rows, columns=["assunto", "total", "acertos", "accuracy_%"])
            st.dataframe(df_ass, use_container_width=True, hide_index=True)

    st.divider()

    # Alertas inteligentes
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

    st.divider()

    left, right = st.columns([1.25, 1])

    with left:
        st.subheader("⏱️ Cronômetro (Estudo)")
        seconds = timer_current_seconds()
        st.markdown(f"### {format_hms(seconds)}  <span class='badge'>{'RODANDO' if st.session_state['timer_running'] else 'PAUSADO'}</span>", unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button("▶️ Iniciar", use_container_width=True):
                st.session_state["pomo_mode"] = False
                timer_start()
                st.rerun()
        with c2:
            if st.button("⏸️ Pausar", use_container_width=True):
                timer_pause()
                st.rerun()
        with c3:
            if st.button("⏩ Retomar", use_container_width=True):
                timer_start()
                st.rerun()
        with c4:
            if st.button("🔁 Zerar", use_container_width=True):
                timer_reset()
                st.rerun()

        st.markdown("#### Finalizar e salvar sessão")
        subj_id, subj_name, topic_id, topic_name = subject_topic_picker("timer_save")
        tags = st.text_input("Tags (opcional) — ex.: cardio; prova; revisão", key="timer_tags")
        notes = st.text_area("Observações (opcional)", key="timer_notes")

        if st.button("✅ Finalizar e salvar", type="primary", use_container_width=True):
            try:
                timer_pause()
                duration = timer_current_seconds()
                save_study_session(user_id, subj_id, topic_id, tags, duration, "ESTUDO", notes)
                timer_reset()
                st.success(f"Sessão salva: {format_hms(duration)} em {subj_name} / {topic_name}")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar sessão: {e}")

        st.divider()
        st.subheader("🍅 Pomodoro (FOCO/PAUSA)")
        preset = st.selectbox("Preset", ["25/5 (clássico)", "50/10 (intenso)", "Custom"], index=0)
        if preset == "25/5 (clássico)":
            focus_min, break_min = 25, 5
        elif preset == "50/10 (intenso)":
            focus_min, break_min = 50, 10
        else:
            cfm, cbm = st.columns(2)
            with cfm:
                focus_min = st.number_input("Foco (min)", min_value=5, max_value=180, value=int(st.session_state["pomo_focus_min"]), step=5)
            with cbm:
                break_min = st.number_input("Pausa (min)", min_value=1, max_value=60, value=int(st.session_state["pomo_break_min"]), step=1)

        st.session_state["pomo_focus_min"] = int(focus_min)
        st.session_state["pomo_break_min"] = int(break_min)

        pomo_cols = st.columns(4)
        with pomo_cols[0]:
            if st.button("▶️ Iniciar Pomodoro", use_container_width=True):
                st.session_state["pomo_mode"] = True
                st.session_state["pomo_phase"] = "FOCO"
                st.session_state["pomo_cycles_done"] = 0
                timer_reset()
                timer_start()
                st.rerun()
        with pomo_cols[1]:
            if st.button("🔄 Trocar fase", use_container_width=True):
                # encerra fase atual e troca (salva como sessão POMODORO)
                try:
                    timer_pause()
                    dur = timer_current_seconds()
                    if dur > 0:
                        subj_idp, subj_namep, topic_idp, topic_namep = subject_topic_picker("pomo_save_quick")
                        tagsp = st.text_input("Tags Pomodoro (opcional)", key="pomo_tags_quick")
                        notep = st.text_input("Obs Pomodoro (opcional)", key="pomo_note_quick")
                        save_study_session(user_id, subj_idp, topic_idp, tagsp, dur, "POMODORO", f"{st.session_state['pomo_phase']} | {notep}")
                    # troca
                    st.session_state["pomo_phase"] = "PAUSA" if st.session_state["pomo_phase"] == "FOCO" else "FOCO"
                    if st.session_state["pomo_phase"] == "FOCO":
                        st.session_state["pomo_cycles_done"] += 1
                    timer_reset()
                    timer_start()
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")
        with pomo_cols[2]:
            if st.button("⏸️ Pausar Pomodoro", use_container_width=True):
                timer_pause()
                st.rerun()
        with pomo_cols[3]:
            if st.button("🛑 Encerrar Pomodoro", use_container_width=True):
                timer_pause()
                timer_reset()
                st.session_state["pomo_mode"] = False
                st.success("Pomodoro encerrado.")
                st.rerun()

        if st.session_state["pomo_mode"]:
            target = (st.session_state["pomo_focus_min"] if st.session_state["pomo_phase"] == "FOCO" else st.session_state["pomo_break_min"]) * 60
            elapsed = timer_current_seconds()
            remaining = target - elapsed
            st.markdown(
                f"**Fase:** <span class='badge'>{st.session_state['pomo_phase']}</span> | "
                f"**Ciclos concluídos:** {st.session_state['pomo_cycles_done']}  \n"
                f"**Meta da fase:** {format_hms(target)} | **Decorrido:** {format_hms(elapsed)} | **Restante:** {format_hms(remaining)}",
                unsafe_allow_html=True
            )
            st.info("Dica: clique em **Trocar fase** ao terminar o FOCO/PAUSA para registrar automaticamente.")

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

        st.divider()
        st.subheader("📌 Revisões para hoje / vencidas")
        rev_rows = fetch_all("""
            SELECT r.id, r.due_date, s.name, COALESCE(t.name,'(Sem subtema)'), COALESCE(r.last_accuracy,0)
            FROM reviews r
            JOIN subjects s ON s.id = r.subject_id
            LEFT JOIN topics t ON t.id = r.topic_id
            WHERE r.user_id = ? AND r.status='PENDENTE' AND DATE(r.due_date) <= DATE(?)
            ORDER BY r.due_date ASC
            LIMIT 60
        """, (user_id, today_str()))

        if not rev_rows:
            st.success("Nenhuma revisão vencida ou para hoje. ✅")
        else:
            for rid, due, sname, tname, last_acc in rev_rows:
                cols = st.columns([2.2, 2.6, 1.2])
                cols[0].write(f"**{due}**")
                cols[1].write(f"{sname} — {tname}")
                cols[2].write(f"{float(last_acc):.1f}%")
                b1, b2 = st.columns(2)
                if b1.button(f"✅ Concluir #{rid}", key=f"done_{rid}", use_container_width=True):
                    execute("""
                        UPDATE reviews SET status='CONCLUIDA', completed_at=?
                        WHERE id=? AND user_id=?
                    """, (now_str(), rid, user_id))
                    audit(user_id, "COMPLETE_REVIEW", "reviews", rid, "done")
                    st.rerun()
                if b2.button(f"🗑️ Excluir #{rid}", key=f"del_{rid}", use_container_width=True):
                    execute("DELETE FROM reviews WHERE id=? AND user_id=?", (rid, user_id))
                    audit(user_id, "DELETE_REVIEW", "reviews", rid, "deleted")
                    st.rerun()


# =========================================================
# PAGE: REGISTRAR (questões, simulado, sessão manual)
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
# ===== NOVO PAGE: BANCO DE QUESTÕES (ASSUNTOS + FILTRO) =====
# =========================================================
elif menu == "Banco de Questões (Assuntos)":
    st.subheader("🏷️ Banco de Questões — Filtro por ASSUNTO + Simulado automático")

    tA, tB, tC = st.tabs(["Responder", "Cadastrar/Importar", "Simulado automático"])

    with tA:
        assuntos = ["Todos"] + qb_list_assuntos()
        a = st.selectbox("Filtrar por assunto", assuntos, index=0)
        qtxt = st.text_input("Pesquisar no enunciado / subassunto (opcional)")
        rows = qb_search(assunto=a, q=qtxt, limit=200)

        if not rows:
            st.info("Nenhuma questão encontrada. Use a aba **Cadastrar/Importar** para adicionar.")
        else:
            df = pd.DataFrame(rows, columns=["id", "assunto", "subassunto", "enunciado", "source"])
            st.dataframe(df[["id","assunto","subassunto","source","enunciado"]], use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("### Responder uma questão por ID (gera estatística por assunto)")
            qid = st.number_input("ID da questão", min_value=1, step=1, value=int(df.iloc[0]["id"]))
            qrow = fetch_one("""
                SELECT id, assunto, COALESCE(subassunto,''), enunciado, COALESCE(alternativas,''), COALESCE(gabarito,''), COALESCE(explicacao,''), COALESCE(source,'')
                FROM question_bank WHERE id=?
            """, (int(qid),))
            if qrow:
                _, assunto, subassunto, enunciado, alternativas, gabarito, explicacao, source = qrow
                st.write(f"**Assunto:** {assunto}  |  **Subassunto:** {subassunto}  |  **Fonte:** {source}")
                st.markdown(f"**Enunciado:** {enunciado}")

                alt_list = []
                if alternativas:
                    alt_list = [x.strip() for x in alternativas.split("\n") if x.strip()]

                user_ans = None
                if alt_list:
                    user_ans = st.radio("Alternativas", alt_list, index=0)
                else:
                    user_ans = st.text_input("Sua resposta (texto livre)")

                spent = st.number_input("Tempo gasto (seg)", min_value=0, step=10, value=0)
                if st.button("✅ Registrar resposta"):
                    # Checagem simples: se houver gabarito e alternativas, compara letra ou texto
                    ok = False
                    if gabarito:
                        gb = gabarito.strip().upper()
                        if alt_list:
                            # tenta interpretar gabarito como letra A/B/C...
                            letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                            if gb in letters[:len(alt_list)]:
                                ok = (user_ans == alt_list[letters.index(gb)])
                            else:
                                ok = (norm_text(user_ans) == norm_text(gabarito))
                        else:
                            ok = (norm_text(user_ans) == norm_text(gabarito))
                    else:
                        # sem gabarito: usuário decide
                        ok = st.checkbox("Marcar como correto (sem gabarito)", value=False, key=f"ok_{qid}")

                    qb_register_attempt(user_id, int(qid), assunto, bool(ok), int(spent))
                    st.success(f"Registrado. Correto={ok}. (Assunto: {assunto})")

                    if explicacao:
                        st.info(f"Explicação: {explicacao}")

                    st.rerun()

    with tB:
        st.markdown("### Cadastrar questão manual (leve e sem travar)")
        c1, c2 = st.columns(2)
        with c1:
            assunto = st.text_input("ASSUNTO (obrigatório)", value="")
            subassunto = st.text_input("Subassunto (opcional)", value="")
        with c2:
            source = st.text_input("Fonte (opcional) — ex.: Bradia.pdf", value="")
            gabarito = st.text_input("Gabarito (opcional) — ex.: A ou texto", value="")

        enunciado = st.text_area("Enunciado (obrigatório)", value="", height=120)
        alternativas = st.text_area("Alternativas (opcional) — 1 por linha", value="", height=120)
        explicacao = st.text_area("Explicação (opcional)", value="", height=100)

        if st.button("➕ Salvar questão no banco", type="primary"):
            if not assunto.strip() or not enunciado.strip():
                st.error("Assunto e enunciado são obrigatórios.")
            else:
                qb_upsert_question(assunto.strip(), subassunto.strip(), enunciado.strip(),
                                   alternativas.strip() or None, gabarito.strip() or None,
                                   explicacao.strip() or None, source.strip() or None)
                st.success("Questão salva (ou já existia pelo hash).")
                st.rerun()

        st.divider()
        st.markdown("### Importar questões via CSV (para teste)")
        st.caption("CSV esperado: assunto, subassunto, enunciado, alternativas, gabarito, explicacao, source")
        up = st.file_uploader("Enviar CSV de questões", type=["csv"], key="qb_csv")
        if up is not None:
            df = pd.read_csv(up)
            st.dataframe(df.head(20), use_container_width=True)
            if st.button("Importar CSV de questões", type="primary"):
                okc = 0
                for _, r in df.iterrows():
                    qb_upsert_question(
                        str(r.get("assunto","")).strip(),
                        str(r.get("subassunto","")).strip(),
                        str(r.get("enunciado","")).strip(),
                        str(r.get("alternativas","")).strip() or None,
                        str(r.get("gabarito","")).strip() or None,
                        str(r.get("explicacao","")).strip() or None,
                        str(r.get("source","")).strip() or None,
                    )
                    okc += 1
                audit(user_id, "IMPORT_QB_CSV", "question_bank", None, f"rows={okc}")
                st.success(f"Importado: {okc} linhas (duplicatas ignoradas por hash).")
                st.rerun()

    with tC:
        st.markdown("### Simulado automático por rendimento (assuntos piores aparecem mais)")
        n = st.number_input("Quantidade de questões", min_value=5, max_value=200, value=20, step=5)
        days_back = st.slider("Janela de desempenho (dias)", 30, 365, 180)

        if st.button("🎯 Gerar simulado automático", type="primary"):
            picked = simulado_auto_pick_questions(user_id, n=int(n), days_back=int(days_back))
            if not picked:
                st.error("Não há questões no banco. Cadastre/importa primeiro.")
            else:
                st.session_state["auto_simulado"] = picked
                st.success("Simulado gerado. Responda abaixo.")

        picked = st.session_state.get("auto_simulado", [])
        if picked:
            st.divider()
            st.write(f"**Questões no simulado:** {len(picked)}")
            total_ok = 0
            total_done = 0

            for idx, row in enumerate(picked, start=1):
                qid, assunto, subassunto, enunciado, source = row
                st.markdown(f"#### Q{idx} — {assunto} / {subassunto}  <span class='badge'>{source}</span>", unsafe_allow_html=True)

                qrow = fetch_one("""
                    SELECT enunciado, COALESCE(alternativas,''), COALESCE(gabarito,''), COALESCE(explicacao,'')
                    FROM question_bank WHERE id=?
                """, (int(qid),))
                if not qrow:
                    continue
                enun, alts, gb, exp = qrow
                st.markdown(simple_variation_text(enun))  # variação leve offline

                alt_list = []
                if alts:
                    alt_list = [x.strip() for x in alts.split("\n") if x.strip()]
                if alt_list:
                    ans = st.radio(f"Resposta Q{idx}", alt_list, index=0, key=f"sim_ans_{qid}_{idx}")
                else:
                    ans = st.text_input(f"Resposta Q{idx}", key=f"sim_ans_free_{qid}_{idx}")

                mark = st.selectbox(f"Marcar resultado Q{idx}", ["(não responder ainda)", "Correto", "Errado"], index=0, key=f"sim_mark_{qid}_{idx}")
                if mark != "(não responder ainda)":
                    total_done += 1
                    ok = (mark == "Correto")
                    if ok:
                        total_ok += 1
                    # registra tentativa (para estatística por assunto)
                    qb_register_attempt(user_id, int(qid), assunto, ok, 0)
                    if gb:
                        st.caption(f"Gabarito: {gb}")
                    if exp:
                        st.caption(f"Explicação: {exp}")

                st.divider()

            if total_done > 0:
                acc = (total_ok / total_done) * 100.0
                st.success(f"Parcial do simulado: {total_ok}/{total_done} = {acc:.1f}%")
            else:
                st.info("Marque 'Correto/Errado' em cada questão para computar o simulado.")


# =========================================================
# ===== NOVO PAGE: FLASHCARDS =====
# =========================================================
elif menu == "Flashcards":
    st.subheader("🧠 Flashcards — revisão 7 / 2 dias + 1.5x")

    t1, t2, t3 = st.tabs(["Revisar (hoje/vencidas)", "Cadastrar/Importar", "Estatísticas"])

    with t1:
        due = flash_due_cards(user_id, limit=30)
        if not due:
            st.success("Nenhum flashcard vencido para hoje. ✅")
        else:
            st.warning(f"Você tem **{len(due)}** flashcards para revisar agora.")
            for fr_id, card_id, due_date, interval_days, last_result, assunto, deck, ctype, front, back, cloze, tags, source in due:
                st.markdown(f"### {assunto}  <span class='badge'>{deck or 'sem deck'}</span>  <span class='badge'>venc.: {due_date}</span>", unsafe_allow_html=True)
                st.caption(f"Tipo: {ctype} | Tags: {tags} | Fonte: {source} | Intervalo atual: {interval_days}d")

                if ctype == "CLOZE" and cloze:
                    st.markdown(f"**Cloze:** {cloze}")
                st.markdown(f"**Frente:** {front}")
                with st.expander("Ver resposta"):
                    if ctype == "CLOZE" and cloze and back:
                        st.markdown(f"**Resposta:** {back}")
                    else:
                        st.markdown(f"**Verso:** {back}")

                cA, cB = st.columns(2)
                if cA.button("✅ Soube", key=f"knew_{fr_id}", use_container_width=True):
                    due2, nxt, _ = flash_mark(user_id, int(fr_id), knew=True)
                    st.success(f"Próxima revisão: {due2} (intervalo {nxt} dias)")
                    st.rerun()
                if cB.button("❌ Não soube", key=f"dk_{fr_id}", use_container_width=True):
                    due2, nxt, _ = flash_mark(user_id, int(fr_id), knew=False)
                    st.error(f"Próxima revisão: {due2} (intervalo {nxt} dias)")
                    st.rerun()
                st.divider()

    with t2:
        st.markdown("### Cadastrar flashcard manual")
        c1, c2 = st.columns(2)
        with c1:
            deck = st.text_input("Deck (opcional)", value="")
            assunto = st.text_input("Assunto (obrigatório)", value="")
        with c2:
            tags = st.text_input("Tags (opcional)", value="")
            source = st.text_input("Fonte (opcional)", value="")

        ctype = st.selectbox("Tipo", ["BASIC", "CLOZE"], index=0)
        front = st.text_area("Frente (obrigatório)", value="", height=100)
        back = st.text_area("Verso (BASIC) / Resposta (CLOZE)", value="", height=100)
        cloze = ""
        if ctype == "CLOZE":
            cloze = st.text_area("Texto Cloze (ex.: ... {{c1::lacuna}} ...)", value="", height=100)

        if st.button("➕ Salvar flashcard", type="primary"):
            if not assunto.strip() or not front.strip():
                st.error("Assunto e frente são obrigatórios.")
            else:
                cid = flash_upsert_card(deck, assunto, tags, front, back, cloze, ctype, source)
                if cid is None:
                    st.warning("Não foi possível inserir (pode ser duplicado).")
                else:
                    flash_ensure_queue(user_id, int(cid))
                    audit(user_id, "CREATE_FLASH", "flashcards", int(cid), f"{assunto}")
                    st.success("Flashcard salvo e colocado na fila (due hoje).")
                st.rerun()

        st.divider()
        st.markdown("### Importar flashcards via CSV (para teste)")
        st.caption("Aceita CSV do Anki simples. Colunas aceitas: Front/Back ou frente/verso ou cloze/text.")
        up = st.file_uploader("Enviar CSV de flashcards", type=["csv"], key="flash_csv")
        if up is not None:
            df = pd.read_csv(up)
            st.dataframe(df.head(20), use_container_width=True)

            default_assunto = st.text_input("Assunto padrão (se o CSV não tiver)", value="Geral")
            default_deck = st.text_input("Deck padrão (se o CSV não tiver)", value="Importado")

            if st.button("Importar CSV de flashcards", type="primary"):
                imported = 0
                cols = {c.lower(): c for c in df.columns}
                for _, r in df.iterrows():
                    # tenta detectar campos
                    front_v = str(r.get(cols.get("front",""), r.get(cols.get("frente",""), r.get("Front","")))).strip()
                    back_v = str(r.get(cols.get("back",""), r.get(cols.get("verso",""), r.get("Back","")))).strip()
                    cloze_v = str(r.get(cols.get("cloze",""), r.get(cols.get("text",""), ""))).strip()

                    assunto_v = str(r.get(cols.get("assunto",""), default_assunto)).strip() or default_assunto
                    deck_v = str(r.get(cols.get("deck",""), default_deck)).strip() or default_deck
                    tags_v = str(r.get(cols.get("tags",""), "")).strip()

                    card_type = "CLOZE" if (cloze_v and "{{c" in cloze_v) else "BASIC"
                    if not front_v:
                        continue
                    cid = flash_upsert_card(deck_v, assunto_v, tags_v, front_v, back_v, cloze_v, card_type, "CSV")
                    if cid:
                        flash_ensure_queue(user_id, int(cid))
                        imported += 1

                audit(user_id, "IMPORT_FLASH_CSV", "flashcards", None, f"imported={imported}")
                st.success(f"Importados: {imported} (duplicatas ignoradas por hash).")
                st.rerun()

    with t3:
        st.markdown("### Estatísticas de Flashcards")
        rows = fetch_all("""
            SELECT f.assunto,
                   COUNT(*) as total_cards,
                   SUM(CASE WHEN fr.last_result='KNEW' THEN 1 ELSE 0 END) as knew,
                   SUM(CASE WHEN fr.last_result='DONTKNOW' THEN 1 ELSE 0 END) as dontknow,
                   SUM(CASE WHEN DATE(fr.due_date) <= DATE(?) AND fr.status='PENDENTE' THEN 1 ELSE 0 END) as due_now
            FROM flashcards f
            LEFT JOIN flash_reviews fr ON fr.card_id=f.id AND fr.user_id=?
            GROUP BY f.assunto
            ORDER BY total_cards DESC
        """, (today_str(), user_id))

        if not rows:
            st.info("Sem flashcards ainda.")
        else:
            df = pd.DataFrame(rows, columns=["assunto","total_cards","knew","dontknow","due_now"])
            st.dataframe(df, use_container_width=True, hide_index=True)


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
                if st.button("✅ Concluir", use_container_width=True):
                    execute("""
                        UPDATE reviews SET status='CONCLUIDA', completed_at=?
                        WHERE id=? AND user_id=?
                    """, (now_str(), int(rid), user_id))
                    audit(user_id, "COMPLETE_REVIEW", "reviews", int(rid), "done")
                    st.success("Concluída!")
                    st.rerun()
            with c3:
                if st.button("🗑️ Excluir", use_container_width=True):
                    execute("DELETE FROM reviews WHERE id=? AND user_id=?", (int(rid), user_id))
                    audit(user_id, "DELETE_REVIEW", "reviews", int(rid), "deleted")
                    st.success("Excluída!")
                    st.rerun()

            st.divider()
            st.markdown("### Criar revisão manual")
            subj_id, subj_name, topic_id, topic_name = subject_topic_picker("manual_review")
            acc = st.number_input("Último % (opcional)", min_value=0.0, max_value=100.0, value=85.0, step=1.0, key="man_rev_acc")
            if st.button("Criar revisão manual", type="primary"):
                rid, due, days = add_review(user_id, subj_id, topic_id, float(acc), "MANUAL", 0)
                st.success(f"Criada revisão #{rid} para {due} (+{days} dias).")

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
# PAGE: DASHBOARD (BI completo + ranking)
# =========================================================
elif menu == "Dashboard":
    st.subheader("📊 Dashboard BI")
    days_back = st.slider("Janela (dias)", 7, 365, 90)

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

    st.divider()

    # ===== NOVO: desempenho por ASSUNTO (Banco de Questões) =====
    st.markdown("### 🏷️ Banco de Questões — desempenho por ASSUNTO")
    stats = stats_attempts_window_by_assunto(user_id, days_back=days_back)
    if not stats:
        st.info("Sem tentativas no Banco de Questões nesse período.")
    else:
        dfA = pd.DataFrame(stats, columns=["assunto","total","acertos","accuracy_%"])
        st.dataframe(dfA, use_container_width=True, hide_index=True)

    st.divider()

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

    st.divider()

    cC, cD = st.columns(2)
    with cC:
        st.markdown("### Questões por matéria")
        if dfq.empty:
            st.info("Sem dados.")
        else:
            by_subj = dfq.groupby("subject")["questions"].sum().sort_values(ascending=False).head(12)
            fig = plt.figure()
            plt.bar(by_subj.index, by_subj.values)
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

    with cD:
        st.markdown("### Tempo por matéria (min)")
        if dfs.empty:
            st.info("Sem dados.")
        else:
            by_subj = (dfs.groupby("subject")["duration_seconds"].sum().sort_values(ascending=False).head(12) / 60.0)
            fig = plt.figure()
            plt.bar(by_subj.index, by_subj.values)
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

    st.divider()

    st.markdown("### Ranking (prioridades)")
    if dfq.empty:
        st.info("Sem dados suficientes.")
    else:
        # ranking por: menor % médio + maior volume = prioridade
        agg = dfq.groupby("subject").agg(
            questions=("questions","sum"),
            avg_accuracy=("accuracy","mean")
        ).reset_index()
        agg["priority_score"] = (100 - agg["avg_accuracy"]) * (agg["questions"].clip(lower=1) ** 0.5)
        agg = agg.sort_values("priority_score", ascending=False)
        st.dataframe(agg, use_container_width=True, hide_index=True)


# =========================================================
# PAGE: METAS & ALERTAS
# =========================================================
elif menu == "Metas & Alertas":
    st.subheader("🎯 Metas e Alertas")
    q_goal, min_goal, exams_goal = get_goals(user_id)
    inactive_days, drop_acc = get_prefs(user_id)

    c1, c2, c3 = st.columns(3)
    with c1:
        new_q = st.number_input("Meta diária de questões", min_value=0, step=10, value=int(q_goal))
    with c2:
        new_min = st.number_input("Meta diária de tempo (min)", min_value=0, step=10, value=int(min_goal))
    with c3:
        new_exams = st.number_input("Meta de simulados por mês", min_value=0, step=1, value=int(exams_goal))

    st.divider()
    st.markdown("### Alertas inteligentes (configurações)")
    c4, c5 = st.columns(2)
    with c4:
        new_inactive = st.number_input("Alertar matéria parada após (dias)", min_value=1, step=1, value=int(inactive_days))
    with c5:
        new_drop = st.number_input("Alertar queda de % (pontos) em 14 dias", min_value=1.0, step=1.0, value=float(drop_acc))

    if st.button("Salvar metas e alertas", type="primary"):
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
        d1 = st.date_input("Data inicial", value=date.today() - timedelta(days=30))
    with c2:
        d2 = st.date_input("Data final", value=date.today())

    if d2 < d1:
        st.error("Data final não pode ser menor que a inicial.")
    else:
        if st.button("Gerar PDF", type="primary"):
            try:
                pdf_bytes = generate_pdf_report(user_id, username, d1, d2)
                st.download_button(
                    "⬇️ Baixar PDF",
                    data=pdf_bytes,
                    file_name=f"{APP_NAME}_relatorio_{d1.isoformat()}_a_{d2.isoformat()}.pdf",
                    mime="application/pdf"
                )
                audit(user_id, "GENERATE_PDF", "report", None, f"{d1}..{d2}")
                st.success("PDF gerado.")
            except Exception as e:
                st.error(f"Erro ao gerar PDF: {e}")


# =========================================================
# PAGE: GERENCIAR (editar/excluir)
# =========================================================
elif menu == "Gerenciar (Editar/Excluir)":
    st.subheader("🧰 Gerenciar dados (editar / excluir)")

    t1, t2, t3, t4 = st.tabs(["Questões", "Sessões", "Simulados", "Revisões"])

    # ---- QUESTOES
    with t1:
        dfq = df_question_logs(user_id, days_back=3650)
        if dfq.empty:
            st.info("Sem registros.")
        else:
            st.dataframe(dfq.drop(columns=["date"], errors="ignore"), use_container_width=True, hide_index=True)
            st.markdown("#### Editar / Excluir por ID")
            rid = st.number_input("ID do registro", min_value=1, step=1, value=int(dfq.iloc[0]["id"]))
            row = fetch_one("""
                SELECT subject_id, topic_id, COALESCE(tags,''), questions, correct, COALESCE(source,''), COALESCE(notes,'')
                FROM question_logs WHERE id=? AND user_id=?
            """, (int(rid), user_id))
            if row:
                subject_id, topic_id, tags, questions, correct, source, notes = row
                st.caption("Atualize os campos e clique em **Salvar edição**.")
                # pick subject/topic for edit
                subjects = get_subjects()
                subj_map = {s["name"]: s["id"] for s in subjects}
                subj_names = list(subj_map.keys())
                current_subj_name = fetch_one("SELECT name FROM subjects WHERE id=?", (subject_id,))[0]
                subj_idx = subj_names.index(current_subj_name) if current_subj_name in subj_names else 0
                new_subj_name = st.selectbox("Matéria", subj_names, index=subj_idx, key="edit_q_subj")
                new_subject_id = subj_map[new_subj_name]

                topics = get_topics(new_subject_id)
                topic_options = ["(Sem subtema)"] + [t["name"] for t in topics]
                current_topic_name = "(Sem subtema)"
                if topic_id:
                    r2 = fetch_one("SELECT name FROM topics WHERE id=?", (topic_id,))
                    if r2:
                        current_topic_name = r2[0]
                topic_idx = topic_options.index(current_topic_name) if current_topic_name in topic_options else 0
                new_topic_name = st.selectbox("Subtema", topic_options, index=topic_idx, key="edit_q_topic")
                new_topic_id = None
                if new_topic_name != "(Sem subtema)":
                    for t in topics:
                        if t["name"] == new_topic_name:
                            new_topic_id = t["id"]
                            break

                c1, c2 = st.columns(2)
                with c1:
                    new_questions = st.number_input("Questões", min_value=1, step=1, value=int(questions), key="edit_q_questions")
                with c2:
                    new_correct = st.number_input("Acertos", min_value=0, step=1, value=int(correct), key="edit_q_correct")
                new_tags = st.text_input("Tags", value=tags, key="edit_q_tags")
                new_source = st.text_input("Fonte", value=source, key="edit_q_source")
                new_notes = st.text_area("Observações", value=notes, key="edit_q_notes")

                cA, cB = st.columns(2)
                if cA.button("💾 Salvar edição", type="primary", use_container_width=True):
                    if int(new_correct) > int(new_questions):
                        st.error("Acertos não pode ser maior que questões.")
                    else:
                        acc = (int(new_correct) / int(new_questions)) * 100.0
                        execute("""
                            UPDATE question_logs
                            SET subject_id=?, topic_id=?, tags=?, questions=?, correct=?, accuracy=?, source=?, notes=?
                            WHERE id=? AND user_id=?
                        """, (new_subject_id, new_topic_id, (new_tags or "").strip() or None,
                              int(new_questions), int(new_correct), float(acc),
                              (new_source or "").strip() or None,
                              (new_notes or "").strip() or None,
                              int(rid), user_id))
                        audit(user_id, "UPDATE_QUESTIONS", "question_logs", int(rid), f"acc={acc:.1f}")
                        st.success("Atualizado.")
                        st.rerun()

                if cB.button("🗑️ Excluir registro", use_container_width=True):
                    execute("DELETE FROM question_logs WHERE id=? AND user_id=?", (int(rid), user_id))
                    audit(user_id, "DELETE_QUESTIONS", "question_logs", int(rid), "deleted")
                    st.success("Excluído.")
                    st.rerun()
            else:
                st.error("ID não encontrado.")

    # ---- SESSOES
    with t2:
        dfs = df_study_sessions(user_id, days_back=3650)
        if dfs.empty:
            st.info("Sem sessões.")
        else:
            st.dataframe(dfs.drop(columns=["date"], errors="ignore"), use_container_width=True, hide_index=True)
            sid = st.number_input("ID da sessão", min_value=1, step=1, value=int(dfs.iloc[0]["id"]), key="sess_id")
            row = fetch_one("""
                SELECT subject_id, topic_id, COALESCE(tags,''), duration_seconds, session_type, COALESCE(notes,'')
                FROM study_sessions WHERE id=? AND user_id=?
            """, (int(sid), user_id))
            if row:
                subject_id, topic_id, tags, dur, stype, notes = row
                subjects = get_subjects()
                subj_map = {s["name"]: s["id"] for s in subjects}
                subj_names = list(subj_map.keys())
                current_subj_name = fetch_one("SELECT name FROM subjects WHERE id=?", (subject_id,))[0]
                subj_idx = subj_names.index(current_subj_name) if current_subj_name in subj_names else 0
                new_subj_name = st.selectbox("Matéria", subj_names, index=subj_idx, key="edit_s_subj")
                new_subject_id = subj_map[new_subj_name]

                topics = get_topics(new_subject_id)
                topic_options = ["(Sem subtema)"] + [t["name"] for t in topics]
                current_topic_name = "(Sem subtema)"
                if topic_id:
                    r2 = fetch_one("SELECT name FROM topics WHERE id=?", (topic_id,))
                    if r2:
                        current_topic_name = r2[0]
                topic_idx = topic_options.index(current_topic_name) if current_topic_name in topic_options else 0
                new_topic_name = st.selectbox("Subtema", topic_options, index=topic_idx, key="edit_s_topic")
                new_topic_id = None
                if new_topic_name != "(Sem subtema)":
                    for t in topics:
                        if t["name"] == new_topic_name:
                            new_topic_id = t["id"]
                            break

                c1, c2, c3 = st.columns(3)
                with c1:
                    new_minutes = st.number_input("Duração (min)", min_value=1, step=5, value=max(1, int(dur//60)), key="edit_s_min")
                with c2:
                    new_stype = st.selectbox("Tipo", ["ESTUDO", "POMODORO"], index=0 if stype == "ESTUDO" else 1, key="edit_s_type")
                with c3:
                    new_tags = st.text_input("Tags", value=tags, key="edit_s_tags")
                new_notes = st.text_area("Observações", value=notes, key="edit_s_notes")

                cA, cB = st.columns(2)
                if cA.button("💾 Salvar sessão", type="primary", use_container_width=True):
                    execute("""
                        UPDATE study_sessions
                        SET subject_id=?, topic_id=?, tags=?, duration_seconds=?, session_type=?, notes=?
                        WHERE id=? AND user_id=?
                    """, (new_subject_id, new_topic_id, (new_tags or "").strip() or None,
                          int(new_minutes)*60, new_stype, (new_notes or "").strip() or None,
                          int(sid), user_id))
                    audit(user_id, "UPDATE_SESSION", "study_sessions", int(sid), f"min={new_minutes}")
                    st.success("Sessão atualizada.")
                    st.rerun()
                if cB.button("🗑️ Excluir sessão", use_container_width=True):
                    execute("DELETE FROM study_sessions WHERE id=? AND user_id=?", (int(sid), user_id))
                    audit(user_id, "DELETE_SESSION", "study_sessions", int(sid), "deleted")
                    st.success("Sessão excluída.")
                    st.rerun()
            else:
                st.error("ID não encontrado.")

    # ---- SIMULADOS
    with t3:
        dfe = df_exams(user_id, days_back=3650)
        if dfe.empty:
            st.info("Sem simulados.")
        else:
            st.dataframe(dfe.drop(columns=["date"], errors="ignore"), use_container_width=True, hide_index=True)
            eid = st.number_input("ID do simulado", min_value=1, step=1, value=int(dfe.iloc[0]["id"]), key="exam_id_manage")
            row = fetch_one("""
                SELECT title, subject_id, total_questions, correct, duration_seconds, COALESCE(notes,'')
                FROM exams WHERE id=? AND user_id=?
            """, (int(eid), user_id))
            if row:
                title, subject_id, total_q, correct, dur, notes = row
                title2 = st.text_input("Título", value=title, key="edit_e_title")

                subjects = get_subjects()
                subj_opts = [{"id": None, "name": "(Sem matéria)"}] + subjects
                subj_names = [x["name"] for x in subj_opts]
                current_name = "(Sem matéria)"
                if subject_id:
                    r = fetch_one("SELECT name FROM subjects WHERE id=?", (subject_id,))
                    if r:
                        current_name = r[0]
                idx = subj_names.index(current_name) if current_name in subj_names else 0
                new_subj_name = st.selectbox("Matéria", subj_names, index=idx, key="edit_e_subj")
                new_subject_id = None
                for o in subj_opts:
                    if o["name"] == new_subj_name:
                        new_subject_id = o["id"]
                        break

                c1, c2, c3 = st.columns(3)
                with c1:
                    total2 = st.number_input("Total", min_value=1, step=1, value=int(total_q), key="edit_e_total")
                with c2:
                    corr2 = st.number_input("Acertos", min_value=0, step=1, value=int(correct), key="edit_e_corr")
                with c3:
                    min2 = st.number_input("Duração (min)", min_value=0, step=10, value=int(dur//60), key="edit_e_min")
                notes2 = st.text_area("Observações", value=notes, key="edit_e_notes")

                cA, cB = st.columns(2)
                if cA.button("💾 Salvar simulado", type="primary", use_container_width=True):
                    if int(corr2) > int(total2):
                        st.error("Acertos não pode ser maior que total.")
                    else:
                        acc = (int(corr2) / int(total2)) * 100.0
                        execute("""
                            UPDATE exams
                            SET title=?, subject_id=?, total_questions=?, correct=?, accuracy=?, duration_seconds=?, notes=?
                            WHERE id=? AND user_id=?
                        """, (title2.strip(), new_subject_id, int(total2), int(corr2), float(acc), int(min2)*60,
                              (notes2 or "").strip() or None, int(eid), user_id))
                        audit(user_id, "UPDATE_EXAM", "exams", int(eid), f"acc={acc:.1f}")
                        st.success("Simulado atualizado.")
                        st.rerun()
                if cB.button("🗑️ Excluir simulado", use_container_width=True):
                    execute("DELETE FROM exams WHERE id=? AND user_id=?", (int(eid), user_id))
                    audit(user_id, "DELETE_EXAM", "exams", int(eid), "deleted")
                    st.success("Simulado excluído.")
                    st.rerun()
            else:
                st.error("ID não encontrado.")

    # ---- REVISOES
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

            rid = st.number_input("ID da revisão", min_value=1, step=1, value=int(df.iloc[0]["id"]), key="edit_rev_id")
            row = fetch_one("SELECT due_date, status FROM reviews WHERE id=? AND user_id=?", (int(rid), user_id))
            if row:
                due, status = row
                new_due = st.date_input("Nova data", value=datetime.strptime(due, "%Y-%m-%d").date(), key="edit_rev_due")
                new_status = st.selectbox("Status", ["PENDENTE", "CONCLUIDA"], index=0 if status == "PENDENTE" else 1, key="edit_rev_status")
                cA, cB = st.columns(2)
                if cA.button("💾 Salvar revisão", type="primary", use_container_width=True):
                    execute("""
                        UPDATE reviews
                        SET due_date=?, status=?, completed_at=CASE WHEN ?='CONCLUIDA' THEN COALESCE(completed_at, ?) ELSE NULL END
                        WHERE id=? AND user_id=?
                    """, (new_due.isoformat(), new_status, new_status, now_str(), int(rid), user_id))
                    audit(user_id, "UPDATE_REVIEW", "reviews", int(rid), f"due={new_due}, status={new_status}")
                    st.success("Revisão atualizada.")
                    st.rerun()
                if cB.button("🗑️ Excluir revisão", use_container_width=True):
                    execute("DELETE FROM reviews WHERE id=? AND user_id=?", (int(rid), user_id))
                    audit(user_id, "DELETE_REVIEW", "reviews", int(rid), "deleted")
                    st.success("Revisão excluída.")
                    st.rerun()
            else:
                st.error("ID não encontrado.")


# =========================================================
# PAGE: EXPORT/IMPORT CSV
# =========================================================
elif menu == "Exportar/Importar CSV":
    st.subheader("🔁 Exportar / Importar CSV (backup e migração)")
    st.caption("Exporta seus dados para CSV e permite reimportar (cuidado para não duplicar).")

    t1, t2 = st.tabs(["Exportar", "Importar"])

    with t1:
        st.markdown("### Exportar")
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

        # ===== NOVO: export banco de questões e flashcards =====
        df_qb = pd.DataFrame(fetch_all("""
            SELECT id, assunto, COALESCE(subassunto,''), enunciado, COALESCE(alternativas,''), COALESCE(gabarito,''), COALESCE(explicacao,''), COALESCE(source,''), created_at
            FROM question_bank
        """), columns=["id","assunto","subassunto","enunciado","alternativas","gabarito","explicacao","source","created_at"])

        df_fc = pd.DataFrame(fetch_all("""
            SELECT id, COALESCE(deck,''), assunto, COALESCE(tags,''), f_front, COALESCE(f_back,''), COALESCE(f_cloze,''), card_type, COALESCE(source,''), created_at
            FROM flashcards
        """), columns=["id","deck","assunto","tags","front","back","cloze","card_type","source","created_at"])

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1:
            st.download_button("⬇️ Questões CSV", dfq.to_csv(index=False).encode("utf-8"), "questoes.csv", "text/csv")
        with c2:
            st.download_button("⬇️ Sessões CSV", dfs.to_csv(index=False).encode("utf-8"), "sessoes.csv", "text/csv")
        with c3:
            st.download_button("⬇️ Simulados CSV", dfe.to_csv(index=False).encode("utf-8"), "simulados.csv", "text/csv")
        with c4:
            st.download_button("⬇️ Revisões CSV", dfr.to_csv(index=False).encode("utf-8"), "revisoes.csv", "text/csv")
        with c5:
            st.download_button("⬇️ Banco Questões", df_qb.to_csv(index=False).encode("utf-8"), "banco_questoes.csv", "text/csv")
        with c6:
            st.download_button("⬇️ Flashcards", df_fc.to_csv(index=False).encode("utf-8"), "flashcards.csv", "text/csv")

    with t2:
        st.markdown("### Importar (opcional)")
        st.warning("Importação é útil para recuperar backup, mas pode duplicar dados. Use com cuidado.")
        uploaded = st.file_uploader("Envie um CSV (questões/sessões/simulados/revisões)", type=["csv"])
        mode = st.selectbox("Tipo do CSV", ["questoes", "sessoes", "simulados", "revisoes", "banco_questoes", "flashcards"])
        if uploaded is not None:
            df = pd.read_csv(uploaded)
            st.dataframe(df.head(20), use_container_width=True)
            if st.button("Importar agora", type="primary"):
                try:
                    if mode == "questoes":
                        for _, r in df.iterrows():
                            subj_name = str(r.get("subject","")).strip()
                            if not subj_name:
                                continue
                            execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (subj_name, now_str()))
                            sid = fetch_one("SELECT id FROM subjects WHERE name=?", (subj_name,))[0]
                            topic_name = str(r.get("topic","(Sem subtema)")).strip()
                            tid = None
                            if topic_name and topic_name != "(Sem subtema)":
                                execute("INSERT OR IGNORE INTO topics (subject_id, name, created_at) VALUES (?, ?, ?)", (sid, topic_name, now_str()))
                                tid = fetch_one("SELECT id FROM topics WHERE subject_id=? AND name=?", (sid, topic_name))[0]
                            q = safe_int(r.get("questions", 0), 0)
                            crr = safe_int(r.get("correct", 0), 0)
                            if q <= 0 or crr < 0 or crr > q:
                                continue
                            acc = (crr/q)*100.0
                            execute("""
                                INSERT INTO question_logs (user_id, subject_id, topic_id, tags, questions, correct, accuracy, source, notes, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (user_id, sid, tid, str(r.get("tags","")).strip() or None, q, crr, acc,
                                  str(r.get("source","")).strip() or None,
                                  str(r.get("notes","")).strip() or None,
                                  str(r.get("created_at", now_str()))))
                        audit(user_id, "IMPORT_CSV", "question_logs", None, "import questoes")
                        st.success("Importação de questões concluída.")
                    elif mode == "sessoes":
                        for _, r in df.iterrows():
                            subj_name = str(r.get("subject","")).strip()
                            if not subj_name:
                                continue
                            execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (subj_name, now_str()))
                            sid = fetch_one("SELECT id FROM subjects WHERE name=?", (subj_name,))[0]
                            topic_name = str(r.get("topic","(Sem subtema)")).strip()
                            tid = None
                            if topic_name and topic_name != "(Sem subtema)":
                                execute("INSERT OR IGNORE INTO topics (subject_id, name, created_at) VALUES (?, ?, ?)", (sid, topic_name, now_str()))
                                tid = fetch_one("SELECT id FROM topics WHERE subject_id=? AND name=?", (sid, topic_name))[0]
                            dur = safe_int(r.get("duration_seconds", 0), 0)
                            if dur <= 0:
                                mins = safe_int(r.get("minutes", 0), 0)
                                dur = mins*60
                            if dur <= 0:
                                continue
                            execute("""
                                INSERT INTO study_sessions (user_id, subject_id, topic_id, tags, duration_seconds, session_type, notes, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (user_id, sid, tid, str(r.get("tags","")).strip() or None, dur,
                                  str(r.get("session_type","ESTUDO")).strip() or "ESTUDO",
                                  str(r.get("notes","")).strip() or None,
                                  str(r.get("created_at", now_str()))))
                        audit(user_id, "IMPORT_CSV", "study_sessions", None, "import sessoes")
                        st.success("Importação de sessões concluída.")
                    elif mode == "simulados":
                        for _, r in df.iterrows():
                            title = str(r.get("title","Simulado")).strip() or "Simulado"
                            subj_name = str(r.get("subject","(Sem matéria)")).strip()
                            sid = None
                            if subj_name and subj_name != "(Sem matéria)":
                                execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (subj_name, now_str()))
                                sid = fetch_one("SELECT id FROM subjects WHERE name=?", (subj_name,))[0]
                            total = safe_int(r.get("total_questions", 0), 0)
                            corr = safe_int(r.get("correct", 0), 0)
                            if total <= 0 or corr < 0 or corr > total:
                                continue
                            acc = (corr/total)*100.0
                            dur = safe_int(r.get("duration_seconds", 0), 0)
                            execute("""
                                INSERT INTO exams (user_id, title, subject_id, total_questions, correct, accuracy, duration_seconds, notes, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (user_id, title, sid, total, corr, acc, dur,
                                  str(r.get("notes","")).strip() or None,
                                  str(r.get("created_at", now_str()))))
                        audit(user_id, "IMPORT_CSV", "exams", None, "import simulados")
                        st.success("Importação de simulados concluída.")
                    elif mode == "revisoes":
                        for _, r in df.iterrows():
                            subj_name = str(r.get("subject","")).strip()
                            if not subj_name:
                                continue
                            execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (subj_name, now_str()))
                            sid = fetch_one("SELECT id FROM subjects WHERE name=?", (subj_name,))[0]
                            topic_name = str(r.get("topic","(Sem subtema)")).strip()
                            tid = None
                            if topic_name and topic_name != "(Sem subtema)":
                                execute("INSERT OR IGNORE INTO topics (subject_id, name, created_at) VALUES (?, ?, ?)", (sid, topic_name, now_str()))
                                tid = fetch_one("SELECT id FROM topics WHERE subject_id=? AND name=?", (sid, topic_name))[0]
                            due = str(r.get("due_date", today_str()))
                            status = str(r.get("status","PENDENTE")).strip().upper()
                            acc = safe_float(r.get("last_accuracy", 0), 0.0)
                            execute("""
                                INSERT INTO reviews (user_id, subject_id, topic_id, due_date, status, origin_type, origin_id, last_accuracy, created_at, completed_at)
                                VALUES (?, ?, ?, ?, ?, 'MANUAL', 0, ?, ?, ?)
                            """, (user_id, sid, tid, due, "CONCLUIDA" if status == "CONCLUIDA" else "PENDENTE",
                                  float(acc), now_str(), now_str() if status == "CONCLUIDA" else None))
                        audit(user_id, "IMPORT_CSV", "reviews", None, "import revisoes")
                        st.success("Importação de revisões concluída.")
                    elif mode == "banco_questoes":
                        count = 0
                        for _, r in df.iterrows():
                            qb_upsert_question(
                                str(r.get("assunto","")).strip(),
                                str(r.get("subassunto","")).strip(),
                                str(r.get("enunciado","")).strip(),
                                str(r.get("alternativas","")).strip() or None,
                                str(r.get("gabarito","")).strip() or None,
                                str(r.get("explicacao","")).strip() or None,
                                str(r.get("source","")).strip() or None,
                            )
                            count += 1
                        audit(user_id, "IMPORT_CSV", "question_bank", None, f"rows={count}")
                        st.success(f"Importação banco de questões concluída ({count}).")
                    elif mode == "flashcards":
                        count = 0
                        for _, r in df.iterrows():
                            cid = flash_upsert_card(
                                str(r.get("deck","")).strip(),
                                str(r.get("assunto","Geral")).strip() or "Geral",
                                str(r.get("tags","")).strip(),
                                str(r.get("front","")).strip(),
                                str(r.get("back","")).strip(),
                                str(r.get("cloze","")).strip(),
                                str(r.get("card_type","BASIC")).strip(),
                                str(r.get("source","")).strip()
                            )
                            if cid:
                                flash_ensure_queue(user_id, int(cid))
                            count += 1
                        audit(user_id, "IMPORT_CSV", "flashcards", None, f"rows={count}")
                        st.success(f"Importação flashcards concluída ({count}).")
                except Exception as e:
                    st.error(f"Erro ao importar: {e}")


# =========================================================
# PAGE: MATÉRIAS / SUBTEMAS
# =========================================================
elif menu == "Matérias/Subtemas":
    st.subheader("📚 Gerenciar matérias e subtemas")

    tab1, tab2 = st.tabs(["Matérias", "Subtemas"])

    with tab1:
        st.markdown("#### Adicionar matéria")
        new_subject = st.text_input("Nome da matéria")
        if st.button("Adicionar matéria", type="primary"):
            if not new_subject.strip():
                st.error("Digite um nome.")
            else:
                execute("INSERT OR IGNORE INTO subjects (name, created_at) VALUES (?, ?)", (new_subject.strip(), now_str()))
                audit(user_id, "CREATE_SUBJECT", "subjects", None, new_subject.strip())
                st.success("Matéria adicionada (ou já existia).")
                st.rerun()

        st.markdown("#### Lista de matérias")
        subs = fetch_all("SELECT id, name, created_at FROM subjects ORDER BY name;")
        df = pd.DataFrame(subs, columns=["id","name","created_at"])
        st.dataframe(df, use_container_width=True, hide_index=True)

        if not df.empty:
            sid = st.number_input("ID da matéria", min_value=1, step=1, value=int(df.iloc[0]["id"]))
            new_name = st.text_input("Novo nome (opcional)", value="")
            c1, c2 = st.columns(2)
            if c1.button("✏️ Renomear", use_container_width=True):
                if not new_name.strip():
                    st.error("Digite o novo nome.")
                else:
                    execute("UPDATE subjects SET name=? WHERE id=?", (new_name.strip(), int(sid)))
                    audit(user_id, "RENAME_SUBJECT", "subjects", int(sid), new_name.strip())
                    st.success("Renomeado.")
                    st.rerun()
            if c2.button("🗑️ Excluir (cuidado!)", use_container_width=True):
                execute("DELETE FROM subjects WHERE id=?", (int(sid),))
                audit(user_id, "DELETE_SUBJECT", "subjects", int(sid), "deleted")
                st.success("Excluído.")
                st.rerun()

    with tab2:
        st.markdown("#### Adicionar subtema")
        subjects = get_subjects()
        if not subjects:
            st.warning("Crie uma matéria primeiro.")
        else:
            subj_names = [s["name"] for s in subjects]
            idx = st.selectbox("Matéria", range(len(subj_names)), format_func=lambda i: subj_names[i])
            subject_id = subjects[idx]["id"]
            topic_name = st.text_input("Nome do subtema")
            if st.button("Adicionar subtema", type="primary"):
                if not topic_name.strip():
                    st.error("Digite um nome.")
                else:
                    execute("INSERT OR IGNORE INTO topics (subject_id, name, created_at) VALUES (?, ?, ?)",
                            (subject_id, topic_name.strip(), now_str()))
                    audit(user_id, "CREATE_TOPIC", "topics", None, f"{subject_id}:{topic_name.strip()}")
                    st.success("Subtema adicionado (ou já existia).")
                    st.rerun()

            st.markdown("#### Subtemas da matéria selecionada")
            topics = fetch_all("SELECT id, name, created_at FROM topics WHERE subject_id=? ORDER BY name;", (subject_id,))
            df2 = pd.DataFrame(topics, columns=["id","name","created_at"])
            st.dataframe(df2, use_container_width=True, hide_index=True)

            if not df2.empty:
                tid = st.number_input("ID do subtema", min_value=1, step=1, value=int(df2.iloc[0]["id"]))
                new_tname = st.text_input("Novo nome do subtema (opcional)", value="")
                c1, c2 = st.columns(2)
                if c1.button("✏️ Renomear subtema", use_container_width=True):
                    if not new_tname.strip():
                        st.error("Digite o novo nome.")
                    else:
                        execute("UPDATE topics SET name=? WHERE id=?", (new_tname.strip(), int(tid)))
                        audit(user_id, "RENAME_TOPIC", "topics", int(tid), new_tname.strip())
                        st.success("Renomeado.")
                        st.rerun()
                if c2.button("🗑️ Excluir subtema", use_container_width=True):
                    execute("DELETE FROM topics WHERE id=?", (int(tid),))
                    audit(user_id, "DELETE_TOPIC", "topics", int(tid), "deleted")
                    st.success("Excluído.")
                    st.rerun()


# =========================================================
# PAGE: USUÁRIOS
# =========================================================
elif menu == "Usuários":
    st.subheader("👥 Usuários")
    st.info("Padrão: **admin / admin123**. Crie usuários e troque senhas aqui.")

    tab1, tab2, tab3 = st.tabs(["Criar usuário", "Trocar senha (logado)", "Listar usuários"])

    with tab1:
        new_u = st.text_input("Novo usuário", key="create_user_username")
        new_p = st.text_input("Nova senha", type="password", key="create_user_password")
        if st.button("Criar", type="primary", key="create_user_btn"):
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
                    audit(user_id, "CREATE_USER", "users", uid, new_u.strip())
                    st.success("Usuário criado.")
                except Exception as e:
                    st.error(f"Erro: {e}")

    with tab2:
        st.markdown("Trocar senha do usuário logado:")
        current = st.text_input("Senha atual", type="password", key="pw_current")
        newpass = st.text_input("Nova senha", type="password", key="pw_new")
        newpass2 = st.text_input("Repetir nova senha", type="password", key="pw_new2")

        if st.button("Atualizar senha", type="primary", key="pw_update_btn"):
            u = get_user_by_username(username)
            if not check_password(current, u["salt"], u["hash"]):
                st.error("Senha atual incorreta.")
            elif newpass != newpass2 or not newpass:
                st.error("Nova senha inválida ou não confere.")
            else:
                salt = secrets.token_hex(16)
                pw_hash = hash_password(newpass, salt)
                execute("UPDATE users SET salt=?, password_hash=? WHERE id=?", (salt, pw_hash, user_id))
                audit(user_id, "CHANGE_PASSWORD", "users", user_id, "changed")
                st.success("Senha atualizada. ✅")

    with tab3:
        rows = fetch_all("SELECT id, username, created_at FROM users ORDER BY created_at DESC;")
        df = pd.DataFrame(rows, columns=["id","username","created_at"])
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab1:
        new_u = st.text_input("Novo usuário")
        new_p = st.text_input("Nova senha", type="password")
        if st.button("Criar", type="primary"):
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
                    audit(user_id, "CREATE_USER", "users", uid, new_u.strip())
                    st.success("Usuário criado.")
                except Exception as e:
                    st.error(f"Erro: {e}")

    with tab2:
        st.markdown("Trocar senha do usuário logado:")
        current = st.text_input("Senha atual", type="password")
        newpass = st.text_input("Nova senha", type="password")
        newpass2 = st.text_input("Repetir nova senha", type="password")

        if st.button("Atualizar senha", type="primary"):
            u = get_user_by_username(username)
            if not check_password(current, u["salt"], u["hash"]):
                st.error("Senha atual incorreta.")
            elif newpass != newpass2 or not newpass:
                st.error("Nova senha inválida ou não confere.")
            else:
                salt = secrets.token_hex(16)
                pw_hash = hash_password(newpass, salt)
                execute("UPDATE users SET salt=?, password_hash=? WHERE id=?", (salt, pw_hash, user_id))
                audit(user_id, "CHANGE_PASSWORD", "users", user_id, "changed")
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
