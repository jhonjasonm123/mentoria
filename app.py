# =========================
# NEXUS MED - FASE 4 PREMIUM
# TÓPICO 1/9
# BASE + BANCO + CSS + LOGIN + MENU
# =========================

import os
import re
import io
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

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader

# =========================================================
# HELPERS
# RANKING MULTIUSUÁRIO + PDF PREMIUM DE SIMULADOS
# =========================================================

import io
import sqlite3
import pandas as pd
import streamlit as st

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)

# ---------------------------------------------------------
# UTILITÁRIOS
# ---------------------------------------------------------

def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except:
        return default


def safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(v)
    except:
        return default


def normalize_colname(col):
    return str(col).strip().lower().replace(" ", "_")


def first_existing_column(df, candidates, default=None):
    cols = {normalize_colname(c): c for c in df.columns}
    for c in candidates:
        key = normalize_colname(c)
        if key in cols:
            return cols[key]
    return default


# ---------------------------------------------------------
# DESCOBERTA DE TABELAS
# ---------------------------------------------------------

def list_db_tables():
    conn = sqlite3.connect(DB_PATH)
    try:
        q = """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        """
        df = pd.read_sql_query(q, conn)
        return df["name"].astype(str).tolist()
    except:
        return []
    finally:
        conn.close()


def get_mock_table_name():
    """
    Tenta identificar automaticamente a tabela de simulados.
    Ajuste manualmente se quiser fixar um nome.
    """
    candidates_priority = [
        "mock_exams",
        "mocks",
        "simulados",
        "simulado_resultados",
        "simulados_resultados",
        "simulations",
        "mock_results",
    ]

    tables = list_db_tables()
    normalized = {t.lower(): t for t in tables}

    for cand in candidates_priority:
        if cand.lower() in normalized:
            return normalized[cand.lower()]

    for t in tables:
        tl = t.lower()
        if "mock" in tl or "simulado" in tl:
            return t

    return None


def get_users_table_name():
    candidates_priority = [
        "users",
        "usuarios",
        "user",
        "usuario",
    ]

    tables = list_db_tables()
    normalized = {t.lower(): t for t in tables}

    for cand in candidates_priority:
        if cand.lower() in normalized:
            return normalized[cand.lower()]

    for t in tables:
        tl = t.lower()
        if "user" in tl or "usuario" in tl:
            return t

    return None


def get_table_columns(table_name):
    if not table_name:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = f"PRAGMA table_info({table_name})"
        df = pd.read_sql_query(q, conn)
        if "name" in df.columns:
            return df["name"].astype(str).tolist()
        return []
    except:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------
# LEITURA DOS SIMULADOS
# ---------------------------------------------------------

def fetch_raw_mocks_df():
    """
    Lê a tabela real `mocks` e normaliza para:
    user_id, mock_name, total_questions, correct_answers, wrong_answers, exam_date, score_percent
    """
    df = fetch_dataframe("""
        SELECT
            id,
            user_id,
            mock_date,
            title,
            score_percent,
            questions_count,
            created_at
        FROM mocks
        ORDER BY mock_date DESC, id DESC
    """)

    if df.empty:
        return pd.DataFrame(columns=[
            "id",
            "user_id",
            "mock_name",
            "total_questions",
            "correct_answers",
            "wrong_answers",
            "exam_date",
            "score_percent",
            "created_at",
        ])

    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").fillna(0).astype(int)
    df["score_percent"] = pd.to_numeric(df["score_percent"], errors="coerce").fillna(0.0)
    df["questions_count"] = pd.to_numeric(df["questions_count"], errors="coerce").fillna(0).astype(int)
    df["title"] = df["title"].fillna("").astype(str).str.strip()
    df["mock_date"] = df["mock_date"].fillna("").astype(str)

    normalized = pd.DataFrame()
    normalized["id"] = df["id"]
    normalized["user_id"] = df["user_id"]
    normalized["mock_name"] = df["title"].replace("", "Simulado")
    normalized["total_questions"] = df["questions_count"]
    normalized["score_percent"] = df["score_percent"]
    normalized["exam_date"] = df["mock_date"]
    normalized["created_at"] = df["created_at"]

    normalized["correct_answers"] = (
        (normalized["score_percent"] / 100.0) * normalized["total_questions"]
    ).round().astype(int)

    normalized["wrong_answers"] = (
        normalized["total_questions"] - normalized["correct_answers"]
    ).clip(lower=0).astype(int)

    return normalized

def fetch_users_df():
    """
    Retorna dataframe de usuários com:
    id, user_name
    """
    users_table = get_users_table_name()
    if not users_table:
        return pd.DataFrame(columns=["id", "user_name"])

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {users_table}", conn)
    except:
        conn.close()
        return pd.DataFrame(columns=["id", "user_name"])
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=["id", "user_name"])

    id_col = first_existing_column(df, ["id", "user_id", "usuario_id"])
    name_col = first_existing_column(df, ["name", "nome", "full_name"])
    username_col = first_existing_column(df, ["username", "user", "login"])

    result = pd.DataFrame()
    result["id"] = df[id_col] if id_col else None

    if name_col:
        result["user_name"] = df[name_col]
    elif username_col:
        result["user_name"] = df[username_col]
    else:
        result["user_name"] = result["id"].apply(lambda x: f"Usuário {x}")

    result["user_name"] = result["user_name"].fillna("").astype(str).str.strip()
    result.loc[result["user_name"] == "", "user_name"] = result["id"].apply(lambda x: f"Usuário {x}")

    return result[["id", "user_name"]]


def fetch_available_mock_names():
    df = fetch_raw_mocks_df()
    if df.empty or "mock_name" not in df.columns:
        return []
    names = (
        df["mock_name"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    names = names[names != ""].unique().tolist()
    names.sort()
    return names


def fetch_mock_ranking_df(simulado_nome=None):
    mocks_df = fetch_raw_mocks_df()
    if mocks_df.empty:
        return pd.DataFrame()

    if simulado_nome:
        mocks_df = mocks_df[
            mocks_df["mock_name"].astype(str).str.strip() == str(simulado_nome).strip()
        ].copy()

    if mocks_df.empty:
        return pd.DataFrame()

    users_df = fetch_users_df()

    if not users_df.empty:
        users_base = users_df[["id", "username"]].copy()
        users_base["user_name"] = users_base["username"].fillna("").astype(str).str.strip()

        df = mocks_df.merge(
            users_base[["id", "user_name"]],
            how="left",
            left_on="user_id",
            right_on="id"
        )
    else:
        df = mocks_df.copy()
        df["user_name"] = df["user_id"].apply(lambda x: f"Usuário {x}")

    if "user_name" not in df.columns:
        df["user_name"] = df["user_id"].apply(lambda x: f"Usuário {x}")

    df["user_name"] = df["user_name"].fillna("").astype(str).str.strip()
    df.loc[df["user_name"] == "", "user_name"] = df["user_id"].apply(lambda x: f"Usuário {x}")

    df["total_questions"] = pd.to_numeric(df["total_questions"], errors="coerce").fillna(0).astype(int)
    df["correct_answers"] = pd.to_numeric(df["correct_answers"], errors="coerce").fillna(0).astype(int)
    df["wrong_answers"] = pd.to_numeric(df["wrong_answers"], errors="coerce").fillna(0).astype(int)
    df["score_percent"] = pd.to_numeric(df["score_percent"], errors="coerce").fillna(0.0)

    df = df.sort_values(
        by=["score_percent", "correct_answers", "total_questions", "user_name"],
        ascending=[False, False, False, True]
    ).reset_index(drop=True)

    df["rank"] = range(1, len(df) + 1)

    leader_score = float(df.iloc[0]["score_percent"]) if not df.empty else 0.0
    df["diff_to_leader"] = df["score_percent"].apply(lambda x: round(leader_score - float(x), 2))

    return df

# ---------------------------------------------------------
# DIAGNÓSTICOS
# ---------------------------------------------------------

def build_mock_diagnostics(df):
    if df.empty:
        return {
            "media": 0.0,
            "mediana": 0.0,
            "melhor": 0.0,
            "pior": 0.0,
            "amplitude": 0.0,
            "desvio": 0.0,
            "n_usuarios": 0,
        }

    media = round(df["score_percent"].mean(), 2)
    mediana = round(df["score_percent"].median(), 2)
    melhor = round(df["score_percent"].max(), 2)
    pior = round(df["score_percent"].min(), 2)
    amplitude = round(melhor - pior, 2)
    desvio = round(df["score_percent"].std(ddof=0), 2) if len(df) > 1 else 0.0

    return {
        "media": media,
        "mediana": mediana,
        "melhor": melhor,
        "pior": pior,
        "amplitude": amplitude,
        "desvio": desvio,
        "n_usuarios": len(df),
    }


def classify_performance(score):
    score = safe_float(score)
    if score >= 85:
        return "Elite"
    elif score >= 80:
        return "Alta performance"
    elif score >= 70:
        return "Boa performance"
    elif score >= 60:
        return "Intermediária"
    else:
        return "Crítica"


def build_user_diagnostic_text(row, geral):
    nome = str(row.get("user_name", "Usuário"))
    score = safe_float(row.get("score_percent", 0))
    rank = safe_int(row.get("rank", 0))
    correct = safe_int(row.get("correct_answers", 0))
    total = safe_int(row.get("total_questions", 0))
    diff = safe_float(row.get("diff_to_leader", 0))
    media = safe_float(geral.get("media", 0))

    faixa = classify_performance(score)

    if score > media:
        rel_media = f"{round(score - media, 2)} ponto(s) acima da média"
    elif score < media:
        rel_media = f"{round(media - score, 2)} ponto(s) abaixo da média"
    else:
        rel_media = "exatamente na média"

    if rank == 1:
        posicao = "lidera o grupo"
    elif rank == 2:
        posicao = "ocupa a 2ª posição"
    elif rank == 3:
        posicao = "ocupa a 3ª posição"
    else:
        posicao = f"ocupa a {rank}ª posição"

    if score >= 85:
        recomendacao = "Manter constância, revisar erros residuais e buscar estabilidade de excelência."
    elif score >= 80:
        recomendacao = "Desempenho forte; vale refinar detalhes e reduzir oscilações pontuais."
    elif score >= 70:
        recomendacao = "Boa base; o principal ganho agora está em aumentar precisão e consistência."
    elif score >= 60:
        recomendacao = "Precisa revisão direcionada e reforço dos temas mais errados."
    else:
        recomendacao = "Necessita intervenção intensiva com revisão estruturada e retomada de base."

    return (
        f"{nome} acertou {correct} de {total} questões ({score}%), "
        f"{posicao}, está {rel_media}, encontra-se na faixa {faixa} "
        f"e está a {diff} ponto(s) do líder. "
        f"Recomendação: {recomendacao}"
    )


# ---------------------------------------------------------
# PDF PREMIUM
# ---------------------------------------------------------
def fetch_mock_area_history_df():
    """
    Histórico completo por área de todos os simulados de todos os usuários.
    """
    df = fetch_dataframe("""
        SELECT
            mas.id,
            mas.mock_id,
            mas.user_id,
            mas.grande_area,
            mas.correct_count,
            mas.question_count,
            mas.accuracy_percent,
            m.title AS mock_name,
            m.mock_date,
            u.username
        FROM mock_area_scores mas
        LEFT JOIN mocks m
            ON m.id = mas.mock_id
        LEFT JOIN users u
            ON u.id = mas.user_id
        ORDER BY m.mock_date ASC, mas.id ASC
    """)

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "mock_id", "user_id", "grande_area", "correct_count",
            "question_count", "accuracy_percent", "mock_name", "mock_date", "username"
        ])

    for col in ["correct_count", "question_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["accuracy_percent"] = pd.to_numeric(df["accuracy_percent"], errors="coerce").fillna(0.0)

    for col in ["grande_area", "mock_name", "mock_date", "username"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def build_area_ranking_for_mock(simulado_nome):
    """
    Ranking por grande área para o simulado selecionado.
    Retorna dict:
    {
        "Clínica Médica": df_ranking_area,
        ...
    }
    """
    df = fetch_mock_area_history_df()
    if df.empty:
        return {}

    df = df[df["mock_name"].astype(str).str.strip() == str(simulado_nome).strip()].copy()
    if df.empty:
        return {}

    out = {}

    for area in sorted(df["grande_area"].dropna().astype(str).unique().tolist()):
        area_df = df[df["grande_area"] == area].copy()
        if area_df.empty:
            continue

        area_df["user_name"] = area_df["username"].replace("", pd.NA).fillna(
            area_df["user_id"].apply(lambda x: f"Usuário {x}")
        )

        area_df = area_df.sort_values(
            by=["accuracy_percent", "correct_count", "question_count", "user_name"],
            ascending=[False, False, False, True]
        ).reset_index(drop=True)

        area_df["rank"] = range(1, len(area_df) + 1)

        leader_score = float(area_df.iloc[0]["accuracy_percent"]) if not area_df.empty else 0.0
        area_df["diff_to_leader"] = area_df["accuracy_percent"].apply(lambda x: round(leader_score - float(x), 2))

        out[area] = area_df

    return out


def build_area_evolution_summary(simulado_nome):
    """
    Analisa evolução por grande área usando todos os simulados anteriores
    com o mesmo nome.
    Retorna dict por área com média atual, média anterior e tendência.
    """
    df = fetch_mock_area_history_df()
    if df.empty:
        return {}

    df = df[df["mock_name"].astype(str).str.strip() == str(simulado_nome).strip()].copy()
    if df.empty:
        return {}

    df = df.sort_values(["mock_date", "id"], ascending=[True, True]).copy()

    result = {}

    for area in sorted(df["grande_area"].dropna().astype(str).unique().tolist()):
        area_df = df[df["grande_area"] == area].copy()
        if area_df.empty:
            continue

        grouped = area_df.groupby("mock_date", as_index=False)["accuracy_percent"].mean()
        grouped = grouped.sort_values("mock_date", ascending=True).reset_index(drop=True)

        media_atual = round(float(grouped.iloc[-1]["accuracy_percent"]), 2) if len(grouped) >= 1 else 0.0
        media_anterior = round(float(grouped.iloc[-2]["accuracy_percent"]), 2) if len(grouped) >= 2 else media_atual
        delta = round(media_atual - media_anterior, 2)

        if len(grouped) == 1:
            tendencia = "Sem histórico comparativo suficiente."
        elif delta > 0:
            tendencia = f"Evolução positiva de {delta} ponto(s) em relação ao simulado anterior."
        elif delta < 0:
            tendencia = f"Queda de {abs(delta)} ponto(s) em relação ao simulado anterior."
        else:
            tendencia = "Estabilidade em relação ao simulado anterior."

        media_geral_area = round(float(grouped["accuracy_percent"].mean()), 2) if not grouped.empty else 0.0

        result[area] = {
            "media_atual": media_atual,
            "media_anterior": media_anterior,
            "delta": delta,
            "media_geral_area": media_geral_area,
            "tendencia": tendencia,
            "n_pontos_historicos": len(grouped),
        }

    return result

def generate_mock_ranking_pdf(simulado_nome, df):
    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.2 * cm,
        rightMargin=1.2 * cm,
        topMargin=2.4 * cm,
        bottomMargin=1.4 * cm,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "PremiumTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=21,
        leading=25,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#10213A"),
        spaceAfter=5,
    )

    sub_style = ParagraphStyle(
        "PremiumSub",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=13,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#5B6472"),
        spaceAfter=10,
    )

    section_style = ParagraphStyle(
        "PremiumSection",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12.5,
        leading=16,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#14233C"),
        spaceBefore=10,
        spaceAfter=8,
    )

    body_style = ParagraphStyle(
        "PremiumBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.2,
        leading=13.5,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#293241"),
        spaceAfter=6,
    )

    small_style = ParagraphStyle(
        "PremiumSmall",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=11.5,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#394454"),
        spaceAfter=5,
    )

    def draw_header_footer(canvas_obj, doc_obj):
        width, height = A4
        canvas_obj.saveState()

        # fundo creme suave
        canvas_obj.setFillColor(colors.HexColor("#F7F1E8"))
        canvas_obj.rect(0, 0, width, height, fill=1, stroke=0)

        # faixa do cabeçalho
        canvas_obj.setFillColor(colors.HexColor("#0B1730"))
        canvas_obj.rect(0, height - 2.1 * cm, width, 2.1 * cm, fill=1, stroke=0)

        logo_path = get_logo_path()
        if logo_path:
            try:
                canvas_obj.drawImage(
                    logo_path,
                    1.1 * cm,
                    height - 1.65 * cm,
                    width=1.15 * cm,
                    height=1.15 * cm,
                    preserveAspectRatio=True,
                    mask="auto"
                )
            except Exception:
                pass

        canvas_obj.setFillColor(colors.white)
        canvas_obj.setFont("Helvetica-Bold", 15)
        canvas_obj.drawString(2.55 * cm, height - 0.95 * cm, "NEXUS MED")

        canvas_obj.setFont("Helvetica", 8.6)
        canvas_obj.drawString(2.55 * cm, height - 1.34 * cm, "Relatório Premium de Ranking de Simulados")

        # rodapé
        canvas_obj.setFillColor(colors.HexColor("#6A7280"))
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.drawString(1.1 * cm, 0.75 * cm, f"Nexus Med • Simulado: {simulado_nome}")
        canvas_obj.drawRightString(width - 1.1 * cm, 0.75 * cm, f"Página {doc_obj.page}")

        canvas_obj.restoreState()

    story = []

    story.append(Paragraph("Ranking Premium de Simulado", title_style))
    story.append(Paragraph(f"Simulado analisado: <b>{simulado_nome}</b>", sub_style))
    story.append(Paragraph("Análise comparativa global, por grande área e evolução histórica dos simulados.", sub_style))
    story.append(Spacer(1, 0.15 * cm))

    if df.empty:
        story.append(Paragraph("Nenhum dado encontrado para este simulado.", body_style))
        doc.build(story, onFirstPage=draw_header_footer, onLaterPages=draw_header_footer)
        pdf = buffer.getvalue()
        buffer.close()
        return pdf

    geral = build_mock_diagnostics(df)
    area_rankings = build_area_ranking_for_mock(simulado_nome)
    area_evolution = build_area_evolution_summary(simulado_nome)

    leader = df.iloc[0]
    last = df.iloc[-1]
    top3 = df.head(3).copy()
    bottom3 = df.tail(3).copy()

    elite = len(df[df["score_percent"] >= 85])
    alta = len(df[(df["score_percent"] >= 80) & (df["score_percent"] < 85)])
    boa = len(df[(df["score_percent"] >= 70) & (df["score_percent"] < 80)])
    inter = len(df[(df["score_percent"] >= 60) & (df["score_percent"] < 70)])
    critica = len(df[df["score_percent"] < 60])

    story.append(Paragraph("1. Resumo Executivo", section_style))
    story.append(Paragraph(
        (
            f"Foram analisados <b>{geral['n_usuarios']}</b> participantes. "
            f"A média global foi <b>{geral['media']}%</b>, mediana <b>{geral['mediana']}%</b>, "
            f"melhor resultado <b>{geral['melhor']}%</b> e pior resultado <b>{geral['pior']}%</b>. "
            f"A amplitude foi de <b>{geral['amplitude']}</b> ponto(s) e o desvio padrão foi <b>{geral['desvio']}</b>."
        ),
        body_style
    ))

    story.append(Paragraph(
        (
            f"O líder geral foi <b>{leader['user_name']}</b> com <b>{leader['score_percent']}%</b> "
            f"({safe_int(leader['correct_answers'])}/{safe_int(leader['total_questions'])}), "
            f"enquanto a última posição ficou com <b>{last['user_name']}</b> em <b>{last['score_percent']}%</b>. "
            f"A distância entre topo e base foi de <b>{round(float(leader['score_percent']) - float(last['score_percent']), 2)} ponto(s)</b>."
        ),
        body_style
    ))

    story.append(Paragraph("2. Leitura Estratégica do Grupo", section_style))
    story.append(Paragraph(
        (
            f"Distribuição de performance: Elite = <b>{elite}</b>, Alta performance = <b>{alta}</b>, "
            f"Boa performance = <b>{boa}</b>, Intermediária = <b>{inter}</b>, Crítica = <b>{critica}</b>."
        ),
        body_style
    ))

    if geral["desvio"] <= 5:
        dispersao = "O grupo apresenta baixa dispersão, sugerindo homogeneidade competitiva."
    elif geral["desvio"] <= 10:
        dispersao = "O grupo apresenta dispersão moderada, com diferença relevante entre faixas."
    else:
        dispersao = "O grupo apresenta alta dispersão, com clara distância entre topo e base."

    story.append(Paragraph(dispersao, body_style))

    story.append(Paragraph("3. Destaques do Ranking Geral", section_style))
    story.append(Paragraph("<b>Top 3</b>", body_style))
    for _, row in top3.iterrows():
        story.append(Paragraph(
            f"#{safe_int(row['rank'])} • <b>{row['user_name']}</b> — {row['score_percent']}% "
            f"({safe_int(row['correct_answers'])}/{safe_int(row['total_questions'])})",
            small_style
        ))

    story.append(Spacer(1, 0.08 * cm))
    story.append(Paragraph("<b>Últimos 3</b>", body_style))
    for _, row in bottom3.iterrows():
        story.append(Paragraph(
            f"#{safe_int(row['rank'])} • <b>{row['user_name']}</b> — {row['score_percent']}% "
            f"({safe_int(row['correct_answers'])}/{safe_int(row['total_questions'])})",
            small_style
        ))

    story.append(Paragraph("4. Ranking Geral Completo", section_style))

    table_data = [["Pos.", "Usuário", "Acertos", "Total", "%", "Dif. líder", "Faixa"]]
    for _, row in df.iterrows():
        table_data.append([
            str(safe_int(row["rank"])),
            str(row["user_name"]),
            str(safe_int(row["correct_answers"])),
            str(safe_int(row["total_questions"])),
            f"{safe_float(row['score_percent'])}%",
            f"{safe_float(row['diff_to_leader'])}",
            classify_performance(row["score_percent"]),
        ])

    ranking_table = Table(
        table_data,
        colWidths=[1.0*cm, 4.7*cm, 1.6*cm, 1.6*cm, 1.6*cm, 1.8*cm, 3.6*cm],
        repeatRows=1,
    )
    ranking_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0B1730")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (1, 1), (1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#D7CFC4")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#FFFDF9"), colors.HexColor("#F9F4EC")]),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(ranking_table)

    story.append(Paragraph("5. Comparação entre Grandes Áreas", section_style))

    if not area_rankings:
        story.append(Paragraph("Não há dados por grande área disponíveis para este simulado.", body_style))
    else:
        for area, area_df in area_rankings.items():
            if area_df.empty:
                continue

            media_area = round(float(area_df["accuracy_percent"].mean()), 2)
            melhor_area = area_df.iloc[0]

            story.append(Paragraph(f"<b>{area}</b>", body_style))
            story.append(Paragraph(
                (
                    f"Média da área: <b>{media_area}%</b>. "
                    f"Líder da área: <b>{melhor_area['user_name']}</b> com "
                    f"<b>{round(float(melhor_area['accuracy_percent']), 2)}%</b> "
                    f"({safe_int(melhor_area['correct_count'])}/{safe_int(melhor_area['question_count'])})."
                ),
                small_style
            ))

            podium = area_df.head(3).copy()
            for _, row in podium.iterrows():
                story.append(Paragraph(
                    f"#{safe_int(row['rank'])} • {row['user_name']} — {round(float(row['accuracy_percent']), 2)}% "
                    f"({safe_int(row['correct_count'])}/{safe_int(row['question_count'])})",
                    small_style
                ))

            story.append(Spacer(1, 0.08 * cm))

    story.append(PageBreak())
    story.append(Paragraph("6. Evolução Histórica por Grande Área", section_style))

    if not area_evolution:
        story.append(Paragraph("Não há histórico suficiente para análise evolutiva por área.", body_style))
    else:
        for area, info in area_evolution.items():
            story.append(Paragraph(f"<b>{area}</b>", body_style))
            story.append(Paragraph(
                (
                    f"Média atual: <b>{info['media_atual']}%</b> • "
                    f"Média anterior: <b>{info['media_anterior']}%</b> • "
                    f"Média histórica da área: <b>{info['media_geral_area']}%</b> • "
                    f"Pontos históricos avaliados: <b>{info['n_pontos_historicos']}</b>."
                ),
                small_style
            ))
            story.append(Paragraph(info["tendencia"], small_style))
            story.append(Spacer(1, 0.06 * cm))

    story.append(Paragraph("7. Diagnóstico Individual Detalhado", section_style))

    for _, row in df.iterrows():
        story.append(Paragraph(f"<b>{row['user_name']}</b>", body_style))
        story.append(Paragraph(build_user_diagnostic_text(row, geral), body_style))

        score = safe_float(row.get("score_percent", 0))
        if score >= 85:
            extra = "Leitura estratégica: perfil de excelência, com foco em manutenção de topo."
        elif score >= 80:
            extra = "Leitura estratégica: desempenho competitivo, muito próximo da elite."
        elif score >= 70:
            extra = "Leitura estratégica: boa base, ainda com espaço claro para refinamento."
        elif score >= 60:
            extra = "Leitura estratégica: desempenho intermediário, exigindo revisão direcionada."
        else:
            extra = "Leitura estratégica: desempenho crítico, com necessidade de reconstrução de base."

        story.append(Paragraph(extra, small_style))
        story.append(Spacer(1, 0.10 * cm))

    doc.build(story, onFirstPage=draw_header_footer, onLaterPages=draw_header_footer)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf

# =========================================================
# CONFIG APP
# =========================================================
APP_NAME = "🩺 Nexus Med"
APP_SUBTITLE = "Plataforma premium de acompanhamento para Residência Médica - Mentoria do Jhon"
APP_VERSION = "PREMIUM"
DB_PATH = "nexo_med.db"

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

SCHEDULE_CSV_CANDIDATES = [
    "/mnt/data/itens_teoria_por_semana.csv",
    "itens_teoria_por_semana.csv",
    os.path.join("assets", "itens_teoria_por_semana.csv"),
]

GREAT_AREAS = [
    "Clínica Médica",
    "Cirurgia",
    "Pediatria",
    "Ginecologia e Obstetrícia",
    "Preventiva",
]

STUDY_STAGES = {
    "Iniciante": {
        "daily_questions_goal": 30,
        "daily_flashcard_goal": 50,
        "daily_minutes_goal": 120,
        "monthly_mock_goal": 2,
    },
    "Amador": {
        "daily_questions_goal": 50,
        "daily_flashcard_goal": 100,
        "daily_minutes_goal": 150,
        "monthly_mock_goal": 3,
    },
    "Profissional": {
        "daily_questions_goal": 70,
        "daily_flashcard_goal": 200,
        "daily_minutes_goal": 180,
        "monthly_mock_goal": 4,
    },
    "Lenda": {
        "daily_questions_goal": 100,
        "daily_flashcard_goal": 300,
        "daily_minutes_goal": 240,
        "monthly_mock_goal": 6,
    },
}

st.set_page_config(
    page_title=APP_NAME,
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="collapsed"
)
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
            --gold:#f4c15d;
            --shadow:0 18px 50px rgba(0,0,0,.34);
            --radius:22px;
        }

        html, body, [class*="css"]{
            font-family: "Inter", "Segoe UI", sans-serif;
        }

        .stApp{
            background:
                radial-gradient(circle at 12% 12%, rgba(90,178,255,0.12), transparent 26%),
                radial-gradient(circle at 88% 8%, rgba(139,92,246,0.14), transparent 24%),
                radial-gradient(circle at 50% 100%, rgba(244,193,93,0.05), transparent 28%),
                linear-gradient(180deg, #040b16 0%, #07111f 42%, #06101c 100%);
            color: var(--text);
        }

        .block-container{
            padding-top: 0.55rem !important;
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
            border: 1px solid rgba(255,255,255,.07);
            background: linear-gradient(135deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
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
                radial-gradient(circle at 15% 20%, rgba(90,178,255,.15), transparent 24%),
                radial-gradient(circle at 85% 10%, rgba(139,92,246,.12), transparent 22%);
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
            font-weight: 900;
            line-height: 1.05;
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
            font-weight:700;
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
            position: relative;
            overflow: hidden;
            border-radius: 32px;
            border: 1px solid rgba(255,255,255,.08);
            box-shadow: 0 28px 70px rgba(0,0,0,.38);
            min-height: 100%;
            backdrop-filter: blur(16px);
        }

        .hero-card{
            padding: 34px 30px;
            background:
                linear-gradient(145deg, rgba(255,255,255,.06), rgba(255,255,255,.025)),
                linear-gradient(180deg, rgba(8,18,33,.96), rgba(5,14,28,.98));
        }

        .hero-card::before{
            content:"";
            position:absolute;
            inset:-10% -10% auto auto;
            width:240px;
            height:240px;
            background: radial-gradient(circle, rgba(90,178,255,.20), transparent 62%);
            filter: blur(8px);
            pointer-events:none;
            animation: floatGlow 8s ease-in-out infinite;
        }

        .hero-card::after{
            content:"";
            position:absolute;
            left:-60px;
            bottom:-80px;
            width:260px;
            height:260px;
            background: radial-gradient(circle, rgba(139,92,246,.14), transparent 68%);
            filter: blur(12px);
            pointer-events:none;
            animation: floatGlow 10s ease-in-out infinite reverse;
        }

        .login-card{
            padding: 26px 24px;
            background:
                linear-gradient(180deg, rgba(10,19,34,.98) 0%, rgba(7,15,27,.99) 100%);
        }

        .login-card::before{
            content:"";
            position:absolute;
            inset:0;
            background:
                linear-gradient(90deg, transparent, rgba(255,255,255,.03), transparent);
            transform: translateX(-100%);
            animation: shineSweep 7s linear infinite;
            pointer-events:none;
        }

        .hero-pill{
            display:inline-flex;
            border-radius:999px;
            padding:8px 14px;
            background: rgba(255,255,255,.06);
            border: 1px solid rgba(255,255,255,.08);
            color:#dbe8fb;
            font-size:.82rem;
            font-weight:800;
            margin-bottom:18px;
            position: relative;
            z-index: 2;
        }

        .hero-logo-wrap{
            margin-bottom: 12px;
            position: relative;
            z-index: 2;
        }

        .hero-title{
            position: relative;
            z-index: 2;
            font-size: 2.9rem;
            line-height: 1.02;
            font-weight: 900;
            color: #ffffff;
            letter-spacing: -0.055em;
            margin-bottom: 14px;
            max-width: 760px;
        }

        .hero-title .grad{
            background: linear-gradient(135deg, #ffffff 0%, #9fd4ff 35%, #d7c2ff 70%, #f4c15d 100%);
            background-size: 220% 220%;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            animation: gradientShift 6s ease infinite;
        }

        .hero-dynamic-title{
            display:block;
            margin-top: 12px;
            font-size: 1.28rem;
            font-weight: 1000;
            letter-spacing: .16em;
            text-transform: uppercase;
            position: relative;
            width: fit-content;
            white-space: nowrap;
            overflow: visible;
            border-right: none;
            background: linear-gradient(
                90deg,
                #38bdf8 0%,
                #60a5fa 18%,
                #818cf8 36%,
                #a78bfa 54%,
                #f472b6 72%,
                #f59e0b 88%,
                #38bdf8 100%
            );
            background-size: 300% 100%;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            color: transparent;
            text-shadow: 0 0 18px rgba(96,165,250,.18);
            animation: nexusShift 4s linear infinite, nexusPulse 2.4s ease-in-out infinite;
        }

        .hero-subtext{
            position: relative;
            z-index: 2;
            color:#d3e0f3;
            font-size:1.02rem;
            line-height:1.8;
            max-width: 760px;
            margin-bottom: 22px;
        }

        .hero-metrics{
            position: relative;
            z-index: 2;
            display:grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap:14px;
            margin-top: 8px;
            margin-bottom: 14px;
        }

        .hero-metric{
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.035);
            border-radius:20px;
            padding:16px 16px 14px 16px;
            min-height: 110px;
        }

        .hero-metric-title{
            color:#ffffff;
            font-size:1rem;
            font-weight:800;
            margin-bottom:8px;
        }

        .hero-metric-sub{
            color:#9fb0c8;
            font-size:.9rem;
            line-height:1.55;
        }

        .hero-signature{
            position: relative;
            z-index: 2;
            color:#8ea6c6;
            font-size:.88rem;
            margin-top: 8px;
            font-weight:600;
        }

        .login-title{
            font-size: 1.85rem;
            font-weight: 900;
            color: #f7fbff;
            margin-bottom: 8px;
            letter-spacing: -0.03em;
        }

        .login-subtitle{
            color: var(--muted);
            font-size: .98rem;
            margin-bottom: 18px;
            line-height: 1.7;
        }

        .section-chip{
            display:inline-block;
            padding:7px 12px;
            border-radius:999px;
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.04);
            color:#dce8fb;
            font-size:.8rem;
            font-weight:800;
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

        .stTextInput > div > div > input:focus,
        .stNumberInput input:focus,
        .stTextArea textarea:focus{
            border-color: rgba(90,178,255,.45) !important;
            box-shadow: 0 0 0 1px rgba(90,178,255,.18) !important;
        }

        .stTextInput label,
        .stNumberInput label,
        .stDateInput label,
        .stTextArea label,
        .stSelectbox label,
        .stRadio label,
        .stCheckbox label {
            color:#dbe7f6 !important;
            font-weight:700 !important;
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
            font-weight: 800;
        }

        .stTabs [aria-selected="true"]{
            background: linear-gradient(135deg, rgba(90,178,255,.16), rgba(139,92,246,.16));
            border-color: rgba(126,164,255,.28);
        }

        .stButton > button, .stDownloadButton > button{
            width: 100%;
            border: 1px solid rgba(255,255,255,.08);
            border-radius: 16px;
            padding: 0.84rem 1rem;
            color: white;
            font-weight: 900;
            background: linear-gradient(135deg, #1196ff 0%, #6e61ff 100%);
            box-shadow: 0 14px 30px rgba(43,117,255,.28);
        }

        .stButton > button:hover, .stDownloadButton > button:hover{
            filter: brightness(1.06);
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

        @keyframes gradientShift{
            0%{background-position:0% 50%;}
            50%{background-position:100% 50%;}
            100%{background-position:0% 50%;}
        }

        @keyframes floatGlow{
            0%{transform: translateY(0px) translateX(0px);}
            50%{transform: translateY(-10px) translateX(6px);}
            100%{transform: translateY(0px) translateX(0px);}
        }

        @keyframes shineSweep{
            0%{transform: translateX(-100%);}
            100%{transform: translateX(160%);}
        }

        @keyframes typingTitle{
            0%{width:0;}
            35%{width:22ch;}
            65%{width:22ch;}
            100%{width:0;}
        }

        @keyframes blinkCaret{
            50%{border-color:transparent;}
        }

        @keyframes nexusShift{
            0%{background-position:0% 50%;}
            100%{background-position:300% 50%;}
        }

        @keyframes nexusPulse{
            0%{transform:scale(1);}
            50%{transform:scale(1.035);}
            100%{transform:scale(1);}
        }

        @media (max-width: 1100px){
            .hero-title{
                font-size:2.35rem;
            }

            .hero-metrics{
                grid-template-columns: 1fr;
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

# =========================================================
# HELPERS
# =========================================================
STUDY_STAGES = {
    "Iniciante": {
        "daily_questions_goal": 30,
        "daily_flashcard_goal": 50,
        "daily_minutes_goal": 120,
        "monthly_mock_goal": 2,
    },
    "Amador": {
        "daily_questions_goal": 50,
        "daily_flashcard_goal": 100,
        "daily_minutes_goal": 150,
        "monthly_mock_goal": 3,
    },
    "Profissional": {
        "daily_questions_goal": 70,
        "daily_flashcard_goal": 200,
        "daily_minutes_goal": 180,
        "monthly_mock_goal": 4,
    },
    "Lenda": {
        "daily_questions_goal": 100,
        "daily_flashcard_goal": 300,
        "daily_minutes_goal": 240,
        "monthly_mock_goal": 6,
    },
}


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

def log_flashcard_review(user_id: int, flashcard_id: int, response_time_seconds: float = 0):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT INTO flashcard_review_log (
            user_id,
            flashcard_id,
            reviewed_at,
            response_time_seconds
        )
        VALUES (?, ?, ?, ?)
    """, (
        int(user_id),
        int(flashcard_id),
        now_str,
        float(response_time_seconds or 0)
    ))

    cur.execute("""
        UPDATE flashcards
        SET last_reviewed = ?
        WHERE id = ?
    """, (now_str, int(flashcard_id)))

    conn.commit()
    conn.close()


def reset_flashcard_state():
    st.session_state.flashcard_fullscreen = False
    st.session_state.flashcard_index = 0
    st.session_state.flashcard_show_answer = False
    st.session_state.flashcard_show_note = False
    st.session_state.flashcard_queue_ids = []
    st.session_state["flashcard_timer_card_id"] = None


def reset_login_state():
    st.session_state.logged_in = False
    st.session_state.user_id = None
    st.session_state.username = None
    st.session_state.is_admin = False
    st.session_state.menu = "Visão Geral"
    st.session_state.admin_view_user_id = None
    st.session_state.admin_view_username = None
    st.session_state.admin_selected_student_label = None
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


def get_stage_goal_defaults(stage_name: str):
    stage_name = normalize_text(stage_name)
    if stage_name in STUDY_STAGES:
        return STUDY_STAGES[stage_name].copy()
    return STUDY_STAGES["Amador"].copy()


def get_stage_names():
    return list(STUDY_STAGES.keys())


def build_goal_payload_from_stage(stage_name: str):
    stage_name = normalize_text(stage_name)
    if stage_name not in STUDY_STAGES:
        stage_name = "Amador"

    defaults = get_stage_goal_defaults(stage_name)
    return {
        "phase_name": stage_name,
        "daily_questions_goal": defaults["daily_questions_goal"],
        "daily_flashcard_goal": defaults["daily_flashcard_goal"],
        "daily_minutes_goal": defaults["daily_minutes_goal"],
        "monthly_mock_goal": defaults["monthly_mock_goal"],
    }


def get_area_themes(area_name: str):
    area_name = normalize_text(area_name)
    if "AREA_STRUCTURE" in globals() and area_name in AREA_STRUCTURE:
        return list(AREA_STRUCTURE[area_name].keys())
    return []


def get_theme_subtopics(area_name: str, theme_name: str):
    area_name = normalize_text(area_name)
    theme_name = normalize_text(theme_name)
    if "AREA_STRUCTURE" in globals() and area_name in AREA_STRUCTURE and theme_name in AREA_STRUCTURE[area_name]:
        return AREA_STRUCTURE[area_name][theme_name]
    return []


def get_all_student_user_ids():
    users_df = fetch_non_admin_users_df()
    if users_df.empty:
        return []
    return sorted(users_df["id"].astype(int).tolist())


def clone_existing_flashcards_to_new_user_same_conn(conn, new_user_id: int):
    cur = conn.cursor()

    cur.execute("""
        SELECT
            deck, subject, topic, question, answer, note,
            due_date, last_reviewed, review_count, lapse_count,
            ease_factor, interval_days, card_state, card_type,
            cloze_text, cloze_answer, cloze_full_text
        FROM flashcards
        WHERE user_id IN (
            SELECT id
            FROM users
            WHERE is_admin = 0
              AND id <> ?
        )
        ORDER BY id ASC
    """, (int(new_user_id),))
    rows = cur.fetchall()

    if not rows:
        return

    existing_keys = set()
    cur.execute("""
        SELECT
            deck, subject, topic, question, answer, note,
            card_type, cloze_text, cloze_answer, cloze_full_text
        FROM flashcards
        WHERE user_id = ?
    """, (int(new_user_id),))
    existing_rows = cur.fetchall()

    for r in existing_rows:
        existing_keys.add((
            normalize_text(r["deck"]),
            normalize_text(r["subject"]),
            normalize_text(r["topic"]),
            normalize_text(r["question"]),
            normalize_text(r["answer"]),
            normalize_text(r["note"]),
            normalize_text(r["card_type"]),
            normalize_text(r["cloze_text"]),
            normalize_text(r["cloze_answer"]),
            normalize_text(r["cloze_full_text"]),
        ))

    inserted_keys = set()

    for row in rows:
        key = (
            normalize_text(row["deck"]),
            normalize_text(row["subject"]),
            normalize_text(row["topic"]),
            normalize_text(row["question"]),
            normalize_text(row["answer"]),
            normalize_text(row["note"]),
            normalize_text(row["card_type"]),
            normalize_text(row["cloze_text"]),
            normalize_text(row["cloze_answer"]),
            normalize_text(row["cloze_full_text"]),
        )

        if key in existing_keys or key in inserted_keys:
            continue

        cur.execute("""
            INSERT INTO flashcards (
                user_id, deck, subject, topic, question, answer, note, created_at,
                due_date, last_reviewed, review_count, lapse_count, ease_factor,
                interval_days, card_state, card_type, cloze_text, cloze_answer, cloze_full_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(new_user_id),
            normalize_text(row["deck"]),
            normalize_text(row["subject"]),
            normalize_text(row["topic"]),
            normalize_text(row["question"]),
            normalize_text(row["answer"]),
            normalize_text(row["note"]),
            datetime.now().isoformat(),
            normalize_text(row["due_date"]) or date.today().isoformat(),
            row["last_reviewed"],
            to_int(row["review_count"], 0),
            to_int(row["lapse_count"], 0),
            to_float(row["ease_factor"], 2.5),
            to_int(row["interval_days"], 0),
            normalize_text(row["card_state"]) or "new",
            normalize_text(row["card_type"]) or "basic",
            normalize_text(row["cloze_text"]),
            normalize_text(row["cloze_answer"]),
            normalize_text(row["cloze_full_text"]),
        ))

        inserted_keys.add(key)



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


def normalize_text(value):
    return str(value or "").strip()


def to_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def get_today_str():
    return date.today().isoformat()


def today_plus_days(days: int):
    return (date.today() + timedelta(days=days)).isoformat()


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


def hash_password(password: str) -> str:
    return hashlib.sha256((password or "").encode("utf-8")).hexdigest()


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


def get_stage_goal_defaults(stage_name: str):
    stage_name = normalize_text(stage_name)
    return STUDY_STAGES.get(stage_name, STUDY_STAGES["Amador"]).copy()


def get_stage_names():
    return list(STUDY_STAGES.keys())


def build_goal_payload_from_stage(stage_name: str):
    stage_name = normalize_text(stage_name)
    if stage_name not in STUDY_STAGES:
        stage_name = "Amador"

    defaults = get_stage_goal_defaults(stage_name)
    return {
        "phase_name": stage_name,
        "daily_questions_goal": defaults["daily_questions_goal"],
        "daily_flashcard_goal": defaults["daily_flashcard_goal"],
        "daily_minutes_goal": defaults["daily_minutes_goal"],
        "monthly_mock_goal": defaults["monthly_mock_goal"],
    }

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
        "flashcard_queue_ids": [],
        "flashcard_started_at": None,
        "flashcard_timer_card_id": None,
        "admin_view_user_id": None,
        "admin_view_username": None,
        "admin_selected_student_label": None,
        "fc_filter_deck_value": "Todos",
        "fc_filter_subject_value": "Todos",
        "fc_filter_topic_value": "Todos",
        "fc_filter_type_value": "Todos",
        "fc_due_only_value": True,
        "fc_search_term_value": "",
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def reset_flashcard_state():
    st.session_state.flashcard_fullscreen = False
    st.session_state.flashcard_index = 0
    st.session_state.flashcard_show_answer = False
    st.session_state.flashcard_show_note = False
    st.session_state.flashcard_queue_ids = []
    st.session_state["flashcard_timer_card_id"] = None
    st.session_state["flashcard_started_at"] = None


def reset_login_state():
    st.session_state.logged_in = False
    st.session_state.user_id = None
    st.session_state.username = None
    st.session_state.is_admin = False
    st.session_state.menu = "Visão Geral"
    st.session_state.admin_view_user_id = None
    st.session_state.admin_view_username = None
    st.session_state.admin_selected_student_label = None
    reset_flashcard_state()


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
            --gold:#f4c15d;
            --shadow:0 18px 50px rgba(0,0,0,.34);
            --radius:22px;
        }

        html, body, [class*="css"]{
            font-family: "Inter", "Segoe UI", sans-serif;
        }

        .stApp{
            background:
                radial-gradient(circle at 12% 12%, rgba(90,178,255,0.15), transparent 24%),
                radial-gradient(circle at 88% 8%, rgba(139,92,246,0.16), transparent 24%),
                radial-gradient(circle at 50% 100%, rgba(244,193,93,0.06), transparent 30%),
                linear-gradient(180deg, #030914 0%, #06101c 40%, #040d18 100%);
            color: var(--text);
        }

        .block-container{
            padding-top: 0.45rem !important;
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

        /* =========================
           HEADER ULTRA PREMIUM
        ========================= */
        .ultra-header-shell{
            position: relative;
            overflow: hidden;
            border-radius: 28px;
            padding: 18px 22px;
            margin-bottom: 12px;
            background:
                linear-gradient(135deg, rgba(255,255,255,.055), rgba(255,255,255,.018)),
                linear-gradient(180deg, rgba(8,18,33,.96), rgba(10,20,38,.98));
            border: 1px solid rgba(255,255,255,.09);
            box-shadow:
                0 26px 70px rgba(0,0,0,.35),
                inset 0 1px 0 rgba(255,255,255,.05),
                inset 0 0 0 1px rgba(255,255,255,.02),
                0 0 40px rgba(59,130,246,.08),
                0 0 70px rgba(124,58,237,.06);
            backdrop-filter: blur(14px);
        }

        .ultra-header-shell::before{
            content:"";
            position:absolute;
            inset:0;
            background:
                radial-gradient(circle at 12% 18%, rgba(56,189,248,.18), transparent 22%),
                radial-gradient(circle at 86% 12%, rgba(139,92,246,.16), transparent 24%),
                linear-gradient(90deg, transparent, rgba(255,255,255,.025), transparent);
            pointer-events:none;
        }

        .ultra-header-shell::after{
            content:"";
            position:absolute;
            inset:10px;
            border-radius:20px;
            border:1px solid rgba(255,255,255,.05);
            pointer-events:none;
        }

        .ultra-header-row{
            position: relative;
            z-index: 2;
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap:18px;
            flex-wrap:wrap;
        }

        .ultra-header-left{
            display:flex;
            align-items:center;
            gap:16px;
            min-width: 0;
            flex: 1 1 520px;
        }

        .ultra-logo-wrap{
            flex:0 0 auto;
            width:78px;
            height:78px;
            border-radius:22px;
            display:flex;
            align-items:center;
            justify-content:center;
            background:
              linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.028));
            border:1px solid rgba(255,255,255,.10);
            box-shadow:
              0 16px 34px rgba(0,0,0,.24),
              inset 0 1px 0 rgba(255,255,255,.06),
              0 0 24px rgba(59,130,246,.07);
         padding: 6px;
        }

       .ultra-logo-wrap img{
            max-height:58px;
            max-width:58px;
            object-fit:contain;
            filter: drop-shadow(0 10px 22px rgba(0,0,0,.24));
         }

        .ultra-title-stack{
            min-width:0;
        }

        .ultra-brand-title{
            margin:0;
            color:#f8fbff;
            font-size:2rem;
            line-height:1.02;
            font-weight:950;
            letter-spacing:-0.045em;
            text-shadow:
                0 2px 10px rgba(0,0,0,.22),
                0 0 22px rgba(255,255,255,.03);
        }

        .ultra-brand-title .grad{
            background: linear-gradient(135deg, #ffffff 0%, #bfe2ff 28%, #d6c7ff 58%, #f4c15d 100%);
            background-size: 220% 220%;
            -webkit-background-clip:text;
            -webkit-text-fill-color:transparent;
            background-clip:text;
            animation: headerGradientShift 7s ease infinite;
        }

        .ultra-brand-subtitle{
            margin-top:6px;
            color:#9eb0c9;
            font-size:.95rem;
            line-height:1.45;
            max-width: 760px;
        }

        .ultra-header-right{
            position: relative;
            z-index: 2;
            display:flex;
            align-items:center;
            gap:10px;
            flex-wrap:wrap;
            justify-content:flex-end;
            flex: 0 1 auto;
        }

        .ultra-chip{
            display:inline-flex;
            align-items:center;
            justify-content:center;
            padding:9px 14px;
            border-radius:999px;
            color:#edf4ff;
            font-size:.78rem;
            font-weight:850;
            border:1px solid rgba(255,255,255,.10);
            background:
                linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.028));
            box-shadow:
                0 10px 22px rgba(0,0,0,.18),
                inset 0 1px 0 rgba(255,255,255,.05);
            white-space:nowrap;
        }

        .ultra-chip.gold{
            color:#fff4d1;
            border-color: rgba(244,193,93,.20);
            box-shadow:
                0 10px 22px rgba(0,0,0,.18),
                inset 0 1px 0 rgba(255,255,255,.05),
                0 0 20px rgba(244,193,93,.08);
        }

        /* =========================
           NAV PREMIUM ULTRA
        ========================= */
        .ultra-nav-shell{
            position: relative;
            overflow:hidden;
            border-radius:24px;
            padding: 14px 18px 16px 18px;
            margin-bottom: 10px;
            background:
                linear-gradient(135deg, rgba(255,255,255,.04), rgba(255,255,255,.014)),
                linear-gradient(180deg, rgba(8,18,33,.94), rgba(8,17,31,.98));
            border:1px solid rgba(255,255,255,.08);
            box-shadow:
                0 20px 52px rgba(0,0,0,.28),
                inset 0 1px 0 rgba(255,255,255,.04),
                0 0 28px rgba(59,130,246,.05);
        }

        .ultra-nav-shell::before{
            content:"";
            position:absolute;
            inset:0;
            background:
                radial-gradient(circle at 14% 20%, rgba(56,189,248,.10), transparent 20%),
                radial-gradient(circle at 85% 10%, rgba(139,92,246,.10), transparent 24%);
            pointer-events:none;
        }

        .ultra-nav-title{
            position:relative;
            z-index:2;
            color:#f8fbff;
            font-size:1.08rem;
            font-weight:900;
            margin-bottom:2px;
            letter-spacing:-0.02em;
        }

        .ultra-nav-subtitle{
            position:relative;
            z-index:2;
            color:#93a8c4;
            font-size:.88rem;
            margin-bottom:12px;
        }

        .ultra-menu-active-label{
            position:relative;
            z-index:2;
            display:inline-flex;
            align-items:center;
            gap:8px;
            margin-bottom:12px;
            padding:8px 12px;
            border-radius:999px;
            border:1px solid rgba(255,255,255,.08);
            background:rgba(255,255,255,.035);
            color:#dce9fb;
            font-size:.78rem;
            font-weight:800;
        }

        .ultra-menu-active-label b{
            color:#ffffff;
        }

        .ultra-nav-shell div[data-testid="stHorizontalBlock"]{
            gap: 12px !important;
        }

        .ultra-nav-shell div[data-testid="stButton"] > button{
            width:100% !important;
            min-height: 52px !important;
            border-radius: 18px !important;
            padding: .72rem .82rem !important;
            font-size: .92rem !important;
            font-weight: 850 !important;
            letter-spacing: -.01em !important;
            color: #edf4ff !important;
            border: 1px solid rgba(255,255,255,.09) !important;
            background:
                linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03)),
                linear-gradient(135deg, rgba(39,116,255,.88), rgba(110,97,255,.88)) !important;
            box-shadow:
                0 14px 30px rgba(43,117,255,.18),
                inset 0 1px 0 rgba(255,255,255,.06) !important;
            transition: all .18s ease !important;
        }

        .ultra-nav-shell div[data-testid="stButton"] > button:hover{
            transform: translateY(-1px);
            filter: brightness(1.04);
            box-shadow:
                0 18px 34px rgba(43,117,255,.22),
                0 0 26px rgba(99,102,241,.12),
                inset 0 1px 0 rgba(255,255,255,.06) !important;
        }

        .ultra-nav-shell div[data-testid="stButton"] > button[data-active="true"]{
            border: 1px solid rgba(255,255,255,.16) !important;
            background:
                linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.035)),
                linear-gradient(135deg, #1ca2ff 0%, #5f6dfb 55%, #8c5cf6 100%) !important;
            box-shadow:
                0 18px 36px rgba(37,99,235,.26),
                0 0 28px rgba(99,102,241,.18),
                0 0 42px rgba(139,92,246,.10),
                inset 0 1px 0 rgba(255,255,255,.08) !important;
        }

        /* outros elementos já existentes */
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
            position: relative;
            overflow: hidden;
            border-radius: 32px;
            border: 1px solid rgba(255,255,255,.08);
            box-shadow: 0 28px 70px rgba(0,0,0,.38);
            min-height: 100%;
            backdrop-filter: blur(16px);
        }

        .hero-card{
            padding: 34px 30px;
            background:
                linear-gradient(145deg, rgba(255,255,255,.06), rgba(255,255,255,.025)),
                linear-gradient(180deg, rgba(8,18,33,.96), rgba(5,14,28,.98));
        }

        .hero-card::before{
            content:"";
            position:absolute;
            inset:-10% -10% auto auto;
            width:240px;
            height:240px;
            background: radial-gradient(circle, rgba(90,178,255,.20), transparent 62%);
            filter: blur(8px);
            pointer-events:none;
            animation: floatGlow 8s ease-in-out infinite;
        }

        .hero-card::after{
            content:"";
            position:absolute;
            left:-60px;
            bottom:-80px;
            width:260px;
            height:260px;
            background: radial-gradient(circle, rgba(139,92,246,.14), transparent 68%);
            filter: blur(12px);
            pointer-events:none;
            animation: floatGlow 10s ease-in-out infinite reverse;
        }

        .login-card{
            padding: 26px 24px;
            background:
                linear-gradient(180deg, rgba(10,19,34,.98) 0%, rgba(7,15,27,.99) 100%);
        }

        .login-card::before{
            content:"";
            position:absolute;
            inset:0;
            background:
                linear-gradient(90deg, transparent, rgba(255,255,255,.03), transparent);
            transform: translateX(-100%);
            animation: shineSweep 7s linear infinite;
            pointer-events:none;
        }

        .hero-pill{
            display:inline-flex;
            border-radius:999px;
            padding:8px 14px;
            background: rgba(255,255,255,.06);
            border: 1px solid rgba(255,255,255,.08);
            color:#dbe8fb;
            font-size:.82rem;
            font-weight:800;
            margin-bottom:18px;
            position: relative;
            z-index: 2;
        }

        .hero-logo-wrap{
            margin-bottom: 12px;
            position: relative;
            z-index: 2;
        }

        .hero-title{
            position: relative;
            z-index: 2;
            font-size: 2.9rem;
            line-height: 1.02;
            font-weight: 900;
            color: #ffffff;
            letter-spacing: -0.055em;
            margin-bottom: 14px;
            max-width: 760px;
        }

        .hero-title .grad{
            background: linear-gradient(135deg, #ffffff 0%, #9fd4ff 35%, #d7c2ff 70%, #f4c15d 100%);
            background-size: 220% 220%;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            animation: gradientShift 6s ease infinite;
        }

        .hero-dynamic-title{
            display:block;
            margin-top: 12px;
            font-size: 1.28rem;
            font-weight: 1000;
            letter-spacing: .16em;
            text-transform: uppercase;
            position: relative;
            width: fit-content;
            white-space: nowrap;
            overflow: visible;
            border-right: none;
            background: linear-gradient(
                90deg,
                #38bdf8 0%,
                #60a5fa 18%,
                #818cf8 36%,
                #a78bfa 54%,
                #f472b6 72%,
                #f59e0b 88%,
                #38bdf8 100%
            );
            background-size: 300% 100%;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            color: transparent;
            text-shadow: 0 0 18px rgba(96,165,250,.18);
            animation: nexusShift 4s linear infinite, nexusPulse 2.4s ease-in-out infinite;
        }

        .hero-subtext{
            position: relative;
            z-index: 2;
            color:#d3e0f3;
            font-size:1.02rem;
            line-height:1.8;
            max-width: 760px;
            margin-bottom: 22px;
        }

        .hero-metrics{
            position: relative;
            z-index: 2;
            display:grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap:14px;
            margin-top: 8px;
            margin-bottom: 14px;
        }

        .hero-metric{
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.035);
            border-radius:20px;
            padding:16px 16px 14px 16px;
            min-height: 110px;
        }

        .hero-metric-title{
            color:#ffffff;
            font-size:1rem;
            font-weight:800;
            margin-bottom:8px;
        }

        .hero-metric-sub{
            color:#9fb0c8;
            font-size:.9rem;
            line-height:1.55;
        }

        .hero-signature{
            position: relative;
            z-index: 2;
            color:#8ea6c6;
            font-size:.88rem;
            margin-top: 8px;
            font-weight:600;
        }

        .login-title{
            font-size: 1.85rem;
            font-weight: 900;
            color: #f7fbff;
            margin-bottom: 8px;
            letter-spacing: -0.03em;
        }

        .login-subtitle{
            color: var(--muted);
            font-size: .98rem;
            margin-bottom: 18px;
            line-height: 1.7;
        }

        .section-chip{
            display:inline-block;
            padding:7px 12px;
            border-radius:999px;
            border:1px solid rgba(255,255,255,.08);
            background: rgba(255,255,255,.04);
            color:#dce8fb;
            font-size:.8rem;
            font-weight:800;
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

        .stTextInput > div > div > input:focus,
        .stNumberInput input:focus,
        .stTextArea textarea:focus{
            border-color: rgba(90,178,255,.45) !important;
            box-shadow: 0 0 0 1px rgba(90,178,255,.18) !important;
        }

        .stTextInput label,
        .stNumberInput label,
        .stDateInput label,
        .stTextArea label,
        .stSelectbox label,
        .stRadio label,
        .stCheckbox label {
            color:#dbe7f6 !important;
            font-weight:700 !important;
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
            font-weight: 800;
        }

        .stTabs [aria-selected="true"]{
            background: linear-gradient(135deg, rgba(90,178,255,.16), rgba(139,92,246,.16));
            border-color: rgba(126,164,255,.28);
        }

        .stButton > button, .stDownloadButton > button{
            width: 100%;
            border: 1px solid rgba(255,255,255,.08);
            border-radius: 16px;
            padding: 0.84rem 1rem;
            color: white;
            font-weight: 900;
            background: linear-gradient(135deg, #1196ff 0%, #6e61ff 100%);
            box-shadow: 0 14px 30px rgba(43,117,255,.28);
        }

        .stButton > button:hover, .stDownloadButton > button:hover{
            filter: brightness(1.06);
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

        @keyframes gradientShift{
            0%{background-position:0% 50%;}
            50%{background-position:100% 50%;}
            100%{background-position:0% 50%;}
        }

        @keyframes headerGradientShift{
            0%{background-position:0% 50%;}
            50%{background-position:100% 50%;}
            100%{background-position:0% 50%;}
        }

        @keyframes floatGlow{
            0%{transform: translateY(0px) translateX(0px);}
            50%{transform: translateY(-10px) translateX(6px);}
            100%{transform: translateY(0px) translateX(0px);}
        }

        @keyframes shineSweep{
            0%{transform: translateX(-100%);}
            100%{transform: translateX(160%);}
        }

        @keyframes nexusShift{
            0%{background-position:0% 50%;}
            100%{background-position:300% 50%;}
        }

        @keyframes nexusPulse{
            0%{transform:scale(1);}
            50%{transform:scale(1.035);}
            100%{transform:scale(1);}
        }

        @media (max-width: 1100px){
            .hero-title{
                font-size:2.35rem;
            }

            .hero-metrics{
                grid-template-columns: 1fr;
            }

            .ultra-header-row{
                flex-direction:column;
                align-items:flex-start;
            }

            .ultra-header-right{
                justify-content:flex-start;
            }
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# =========================================================
# BANCO DE DADOS
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_dataframe(query: str, params=()):
    conn = get_conn()
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def init_db():

def zerar_logs_flashcards():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM flashcard_review_log")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # =========================================================
    # USERS
    # =========================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # =========================================================
    # GOALS
    # =========================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            daily_questions INTEGER DEFAULT 30,
            daily_minutes INTEGER DEFAULT 120,
            monthly_mocks INTEGER DEFAULT 2,
            daily_flashcards INTEGER DEFAULT 50,
            stage TEXT DEFAULT 'Iniciante',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # =========================================================
    # SESSIONS
    # =========================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS study_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            study_date TEXT NOT NULL,
            questions_solved INTEGER DEFAULT 0,
            study_minutes INTEGER DEFAULT 0,
            subject TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # =========================================================
    # MOCKS
    # =========================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            mock_date TEXT NOT NULL,
            score REAL DEFAULT 0,
            total_questions INTEGER DEFAULT 0,
            correct_answers INTEGER DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # =========================================================
    # FLASHCARDS
    # =========================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS flashcards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deck TEXT,
            subject TEXT,
            subtopic TEXT,
            front TEXT NOT NULL,
            back TEXT NOT NULL,
            card_type TEXT DEFAULT 'basic',
            tags TEXT,
            created_by_user_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_reviewed TEXT
        )
    """)

    # =========================================================
    # FLASHCARD REVIEW LOG
    # =========================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flashcard_review_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        flashcard_id INTEGER NOT NULL,
        reviewed_at TEXT NOT NULL,
        response_time_seconds REAL DEFAULT 0
    )
    """)

    cur.execute("PRAGMA table_info(flashcards)")
    cols = [row[1] for row in cur.fetchall()]
    if "last_reviewed" not in cols:
        cur.execute("ALTER TABLE flashcards ADD COLUMN last_reviewed TEXT")

    admin_hash = hashlib.sha256(DEFAULT_ADMIN_PASS.encode()).hexdigest()
    cur.execute("""
        INSERT OR IGNORE INTO users (username, password_hash, is_admin)
        VALUES (?, ?, 1)
    """, (DEFAULT_ADMIN_USER, admin_hash))

    conn.commit()
    conn.close()


# =========================================================
# USUÁRIOS / METAS
# =========================================================
def create_user(username: str, password: str, is_admin: int = 0):
    username = normalize_text(username)
    password = normalize_text(password)


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

        defaults = build_goal_payload_from_stage("Amador")
        cur.execute(
            """
            INSERT INTO goals (
                user_id,
                daily_questions_goal,
                daily_flashcard_goal,
                daily_minutes_goal,
                monthly_mock_goal,
                phase_name,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                defaults["daily_questions_goal"],
                defaults["daily_flashcard_goal"],
                defaults["daily_minutes_goal"],
                defaults["monthly_mock_goal"],
                defaults["phase_name"],
                datetime.now().isoformat(),
                datetime.now().isoformat()
            )
        )

        # aluno novo recebe automaticamente todos os flashcards já existentes
        if int(is_admin) == 0:
            cur.execute("""
                SELECT
                    deck, subject, topic, question, answer, note,
                    due_date, last_reviewed, review_count, lapse_count,
                    ease_factor, interval_days, card_state, card_type,
                    cloze_text, cloze_answer, cloze_full_text
                FROM flashcards
                ORDER BY id ASC
            """)
            rows = cur.fetchall()

            existing_keys = set()
            inserted_keys = set()

            for row in rows:
                key = (
                    normalize_text(row["deck"]),
                    normalize_text(row["subject"]),
                    normalize_text(row["topic"]),
                    normalize_text(row["question"]),
                    normalize_text(row["answer"]),
                    normalize_text(row["note"]),
                    normalize_text(row["card_type"]),
                    normalize_text(row["cloze_text"]),
                    normalize_text(row["cloze_answer"]),
                    normalize_text(row["cloze_full_text"]),
                )

                if key in existing_keys or key in inserted_keys:
                    continue

                cur.execute(
                    """
                    INSERT INTO flashcards (
                        user_id, deck, subject, topic, question, answer, note, created_at,
                        due_date, last_reviewed, review_count, lapse_count, ease_factor,
                        interval_days, card_state, card_type, cloze_text, cloze_answer, cloze_full_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(user_id),
                        normalize_text(row["deck"]),
                        normalize_text(row["subject"]),
                        normalize_text(row["topic"]),
                        normalize_text(row["question"]),
                        normalize_text(row["answer"]),
                        normalize_text(row["note"]),
                        datetime.now().isoformat(),
                        normalize_text(row["due_date"]) or date.today().isoformat(),
                        row["last_reviewed"],
                        to_int(row["review_count"], 0),
                        to_int(row["lapse_count"], 0),
                        to_float(row["ease_factor"], 2.5),
                        to_int(row["interval_days"], 0),
                        normalize_text(row["card_state"]) or "new",
                        normalize_text(row["card_type"]) or "basic",
                        normalize_text(row["cloze_text"]),
                        normalize_text(row["cloze_answer"]),
                        normalize_text(row["cloze_full_text"]),
                    )
                )

                inserted_keys.add(key)

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
    cur.execute(
        """
        SELECT *
        FROM goals
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (user_id,)
    )
    row = cur.fetchone()
    conn.close()

    if row:
        goal = dict(row)

        if "daily_flashcard_goal" not in goal:
            defaults = get_stage_goal_defaults(goal.get("phase_name", "Amador"))
            goal["daily_flashcard_goal"] = defaults["daily_flashcard_goal"]

        return goal

    return build_goal_payload_from_stage("Amador")


def update_goal_settings(user_id: int, phase_name: str):
    payload = build_goal_payload_from_stage(phase_name)

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM goals WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,)
        )
        row = cur.fetchone()

        if row:
            cur.execute(
                """
                UPDATE goals
                SET daily_questions_goal = ?,
                    daily_flashcard_goal = ?,
                    daily_minutes_goal = ?,
                    monthly_mock_goal = ?,
                    phase_name = ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (
                    payload["daily_questions_goal"],
                    payload["daily_flashcard_goal"],
                    payload["daily_minutes_goal"],
                    payload["monthly_mock_goal"],
                    payload["phase_name"],
                    datetime.now().isoformat(),
                    user_id
                )
            )
        else:
            cur.execute(
                """
                INSERT INTO goals (
                    user_id,
                    daily_questions_goal,
                    daily_flashcard_goal,
                    daily_minutes_goal,
                    monthly_mock_goal,
                    phase_name,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    payload["daily_questions_goal"],
                    payload["daily_flashcard_goal"],
                    payload["daily_minutes_goal"],
                    payload["monthly_mock_goal"],
                    payload["phase_name"],
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
    df = fetch_dataframe(
        """
        SELECT id, username, is_admin, created_at
        FROM users
        ORDER BY id ASC
        """
    )

    if df.empty:
        return pd.DataFrame(
            columns=["id", "username", "is_admin", "created_at", "is_admin_label"]
        )

    df["is_admin_label"] = df["is_admin"].apply(lambda x: "Sim" if int(x) == 1 else "Não")
    return df


def fetch_non_admin_users_df():
    df = fetch_users_df()
    if df.empty:
        return pd.DataFrame(columns=["id", "username"])

    return df[df["is_admin"] == 0][["id", "username"]].copy()


def get_target_user_options(include_current_user=True):
    users_df = fetch_non_admin_users_df()
    options = []

    if users_df.empty:
        if include_current_user and st.session_state.get("user_id"):
            options.append({
                "label": st.session_state.get("username", "Usuário atual"),
                "user_id": int(st.session_state.get("user_id")),
            })
        return options

    for _, row in users_df.iterrows():
        options.append({
            "label": str(row["username"]),
            "user_id": int(row["id"]),
        })

    if include_current_user and not st.session_state.get("is_admin", False):
        current_id = int(st.session_state.get("user_id"))
        if current_id not in [x["user_id"] for x in options]:
            options.append({
                "label": st.session_state.get("username", "Usuário atual"),
                "user_id": current_id,
            })

    return options


def resolve_selected_target_user_ids(selected_labels):
    options = get_target_user_options(include_current_user=True)
    label_map = {item["label"]: item["user_id"] for item in options}

    user_ids = [label_map[label] for label in selected_labels if label in label_map]
    user_ids = sorted(list(set(user_ids)))
    return user_ids


# =========================================================
# HEADER / LOGIN
# =========================================================
def render_app_header(username: Optional[str] = None, is_admin: bool = False):
    logo_html = render_logo_html(height=58, css_class="")
    today_str = datetime.now().strftime("%d/%m/%Y")

    if logo_html:
        logo_block = f'<div class="ultra-logo-wrap">{logo_html}</div>'
    else:
        logo_block = '<div class="ultra-logo-wrap"><span style="font-size:1.8rem;">🩺</span></div>'

    chips = [
        f'<div class="ultra-chip gold">Versão {html.escape(APP_VERSION)}</div>',
        f'<div class="ultra-chip">{today_str}</div>',
        f'<div class="ultra-chip">{"Administrador" if is_admin else "Aluno"}</div>',
    ]

    if username:
        chips.append(f'<div class="ultra-chip">Usuário: {html.escape(username)}</div>')

    chips_html = "".join(chips)

    header_html = f"""
    <div class="ultra-header-shell">
        <div class="ultra-header-row">
            <div class="ultra-header-left">
                {logo_block}
                <div class="ultra-title-stack">
                    <div class="ultra-brand-title">
                        <span class="grad">{html.escape(APP_NAME)}</span>
                    </div>
                    <div class="ultra-brand-subtitle">{html.escape(APP_SUBTITLE)}</div>
                </div>
            </div>
            <div class="ultra-header-right">
                {chips_html}
            </div>
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
        st.markdown('<div class="hero-logo-wrap">', unsafe_allow_html=True)
        st.image(logo_path, width=92)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown(
        """
        <div class="hero-title">
            Transforme sua rotina em uma
            <span class="grad">operação de aprovação</span>.
            <span class="hero-dynamic-title">Nexus Med</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="hero-subtext">
            Controle metas, questões, cronograma, desempenho, revisão e evolução diária
            em uma experiência premium, limpa e profissional.
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="hero-metrics">
            <div class="hero-metric">
                <div class="hero-metric-title">Gestão diária</div>
                <div class="hero-metric-sub">Questões + tempo com leitura rápida da execução diária.</div>
            </div>
            <div class="hero-metric">
                <div class="hero-metric-title">Acompanhamento</div>
                <div class="hero-metric-sub">Cronograma + metas para manter direção e constância.</div>
            </div>
            <div class="hero-metric">
                <div class="hero-metric-title">Performance</div>
                <div class="hero-metric-sub">Indicadores estratégicos para corrigir rota com precisão.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown('<div class="hero-signature">By Jhon Jason</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

def render_login_screen():
    st.markdown('<div class="top-spacer-sm"></div>', unsafe_allow_html=True)
    col_left, col_right = st.columns([1.08, 0.92], gap="large")

    with col_left:
        render_auth_hero()

    with col_right:
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-chip">ACESSO SEGURO</div>', unsafe_allow_html=True)
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

        st.markdown('</div>', unsafe_allow_html=True)


# =========================================================
# MENU
# =========================================================
def get_main_menu_options():
    """
    Edite aqui se quiser mudar a ordem.
    """
    return [
        "Visão Geral",
        "Cronograma",
        "Questões",
        "Flashcards",
        "Simulados",
        "Relatórios",
        "Ranking Simulados",
        "Configurações",
    ]


def render_main_menu():
    menu_options = get_main_menu_options()

    # Use este bloco se você já usa radio na sidebar
    selected = st.sidebar.radio(
        "Menu",
        menu_options,
        key="main_navigation_menu"
    )

    return selected
def render_top_menu():
    menus = [
        "Visão Geral",
        "Cronograma",
        "Questões",
        "Flashcards",
        "Simulados",
        "Relatórios",
        "Ranking Simulados",
        "Configurações",
        "Administração",
    ]

    allowed = menus if st.session_state.is_admin else [m for m in menus if m != "Administração"]
    current = st.session_state.get("menu", "Visão Geral")

    if current not in allowed:
        st.session_state.menu = allowed[0]
        current = allowed[0]

    st.markdown('<div class="ultra-nav-shell">', unsafe_allow_html=True)
    st.markdown('<div class="ultra-nav-title">Navegação</div>', unsafe_allow_html=True)
    st.markdown('<div class="ultra-nav-subtitle">Escolha a área que deseja abrir no painel.</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="ultra-menu-active-label">Área ativa: <b>{html.escape(current)}</b></div>',
        unsafe_allow_html=True
    )

    cols = st.columns(len(allowed))
    for i, item in enumerate(allowed):
        with cols[i]:
            is_active = item == current

            st.markdown(
                f"""
                <script>
                const btns = window.parent.document.querySelectorAll('button[kind="secondary"], button[kind="primary"]');
                </script>
                """,
                unsafe_allow_html=True
            )

            if st.button(item, key=f"menu_{item}", use_container_width=True):
                st.session_state.menu = item
                if item != "Flashcards":
                    reset_flashcard_state()
                safe_rerun()

            st.markdown(
                f"""
                <script>
                const btn = window.parent.document.querySelector('button[data-testid="baseButton-secondary"][key="menu_{item}"], button[key="menu_{item}"]');
                </script>
                """,
                unsafe_allow_html=True
            )

            st.markdown(
                f"""
                <style>
                div[data-testid="stButton"]:has(button[kind]) button#{""} {{
                }}
                </style>
                """,
                unsafe_allow_html=True
            )

    st.markdown('</div>', unsafe_allow_html=True)

    # marca visualmente o botão ativo depois de renderizar
    active_index = allowed.index(current)
    st.markdown(
        f"""
        <script>
        (function() {{
            const groups = window.parent.document.querySelectorAll('div[data-testid="stButton"] button');
            let count = 0;
            groups.forEach((btn) => {{
                const txt = (btn.innerText || "").trim();
                const valid = {allowed!r};
                if (valid.includes(txt)) {{
                    if (txt === {current!r}) {{
                        btn.setAttribute("data-active", "true");
                    }} else {{
                        btn.setAttribute("data-active", "false");
                    }}
                    count += 1;
                }}
            }});
        }})();
        </script>
        """,
        unsafe_allow_html=True
    )

# =========================================================
# VISÃO GERAL
# =========================================================
def get_performance_badge_html(acc: float):
    acc = to_float(acc, 0)

    if acc < 70:
        return f'<span style="color:#ff6b6b;font-weight:900;">🔴 {acc:.1f}%</span>'
    elif 70 <= acc <= 79:
        return f'<span style="color:#f4c15d;font-weight:900;">🟡 {acc:.1f}%</span>'
    else:
        return f'<span style="color:#5be38b;font-weight:900;">🏆 {acc:.1f}%</span>'

def get_review_urgency_chip(next_review_date: str):
    next_review_date = normalize_text(next_review_date)
    if not next_review_date:
        return '<span class="overview-chip">Sem data</span>'

    try:
        review_dt = datetime.strptime(next_review_date, "%Y-%m-%d").date()
        today_dt = date.today()
        delta = (review_dt - today_dt).days

        if delta < 0:
            return '<span class="overview-chip" style="background:rgba(255,107,107,.16);color:#ff8d8d;border-color:rgba(255,107,107,.28);">Atrasada</span>'
        elif delta == 0:
            return '<span class="overview-chip" style="background:rgba(255,107,107,.16);color:#ff8d8d;border-color:rgba(255,107,107,.28);">Hoje</span>'
        elif delta <= 3:
            return f'<span class="overview-chip" style="background:rgba(244,193,93,.14);color:#ffd978;border-color:rgba(244,193,93,.25);">Em {delta} dia(s)</span>'
        elif delta <= 7:
            return f'<span class="overview-chip" style="background:rgba(90,178,255,.14);color:#a9d6ff;border-color:rgba(90,178,255,.25);">Em {delta} dia(s)</span>'
        else:
            return f'<span class="overview-chip">Em {delta} dia(s)</span>'
    except Exception:
        return '<span class="overview-chip">Sem data</span>'


def get_dashboard_base_data(user_id: int):
    today = get_today_str()
    month_start, month_end = get_month_range()
    last30_start, last30_end = get_last_30_days_range()

    sessions_df = fetch_sessions_df(user_id)
    schedule_df = fetch_schedule_df(user_id)
    mocks_df = fetch_mocks_df(user_id)
    goal = get_user_goal(user_id)
    review_df = build_questions_review_df(sessions_df, user_id=user_id)

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
        "review_df": review_df,
    }
def get_flashcard_extra_metrics(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    reviewed_total = 0
    reviewed_today = 0
    total_seconds_today = 0
    avg_seconds_today = 0

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM flashcard_review_log
            WHERE user_id = ?
        """, (int(user_id),))
        reviewed_total = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COUNT(*)
            FROM flashcard_review_log
            WHERE user_id = ?
              AND substr(reviewed_at, 1, 10) = ?
        """, (int(user_id), date.today().isoformat()))
        reviewed_today = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COALESCE(SUM(response_time_seconds), 0)
            FROM flashcard_review_log
            WHERE user_id = ?
              AND substr(reviewed_at, 1, 10) = ?
              AND response_time_seconds > 0
        """, (int(user_id), date.today().isoformat()))
        total_seconds_today = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COALESCE(AVG(response_time_seconds), 0)
            FROM flashcard_review_log
            WHERE user_id = ?
              AND substr(reviewed_at, 1, 10) = ?
              AND response_time_seconds > 0
        """, (int(user_id), date.today().isoformat()))
        avg_seconds_today = cur.fetchone()[0] or 0

    except Exception:
        reviewed_total = 0
        reviewed_today = 0
        total_seconds_today = 0
        avg_seconds_today = 0
    finally:
        conn.close()

    cards_per_hour_today = 0.0
    if total_seconds_today and total_seconds_today > 0:
        cards_per_hour_today = reviewed_today / (total_seconds_today / 3600)

    return {
        "reviewed_total": int(reviewed_total),
        "reviewed_today": int(reviewed_today),
        "avg_seconds_today": float(avg_seconds_today),
        "cards_per_hour_today": float(cards_per_hour_today),
    }

def build_dashboard_metrics(user_id: int):
    data = get_dashboard_base_data(user_id)
    sessions_df = data["sessions_df"].copy()
    schedule_df = data["schedule_df"].copy()
    mocks_df = data["mocks_df"].copy()
    goal = data["goal"]
    review_df = data["review_df"].copy()

    flashcards_df = fetch_flashcards_df(user_id)
    flashcards_reviewed_today = 0
    if not flashcards_df.empty and "last_reviewed" in flashcards_df.columns:
        last_reviewed_series = flashcards_df["last_reviewed"].fillna("").astype(str)
        flashcards_reviewed_today = int(
            last_reviewed_series.str.startswith(data["today"]).sum()
        )

    if sessions_df.empty:
        sessions_df = pd.DataFrame(columns=[
            "session_date", "study_minutes", "questions_done",
            "correct_answers", "subject", "topic", "grande_area"
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
        if "grande_area" not in rank_base.columns:
            rank_base["grande_area"] = ""

        rank_base["topic_display"] = rank_base["topic"].fillna("").astype(str).str.strip()
        rank_base["subject_display"] = rank_base["subject"].fillna("").astype(str).str.strip()
        rank_base["grande_area"] = rank_base["grande_area"].fillna("").astype(str).str.strip()

        rank_base["topic_display"] = rank_base["topic_display"].where(rank_base["topic_display"] != "", rank_base["subject_display"])
        rank_base["topic_display"] = rank_base["topic_display"].where(rank_base["topic_display"] != "", "Sem subtópico")

        grouped_rank = rank_base.groupby(
            ["topic_display", "grande_area", "subject_display"],
            as_index=False
        )[["questions_done", "correct_answers"]].sum()

        grouped_rank = grouped_rank[grouped_rank["questions_done"] > 0].copy()

        if not grouped_rank.empty:
            grouped_rank["accuracy"] = (grouped_rank["correct_answers"] / grouped_rank["questions_done"]) * 100
            grouped_rank["accuracy"] = grouped_rank["accuracy"].round(1)
            grouped_rank = grouped_rank.rename(columns={"subject_display": "subject"})
            ranking_df = grouped_rank.sort_values(
                ["accuracy", "questions_done"],
                ascending=[False, False]
            ).reset_index(drop=True)

    best_topics = ranking_df.head(10).copy() if not ranking_df.empty else pd.DataFrame(columns=["topic_display", "accuracy", "questions_done", "grande_area", "subject"])
    worst_topics = ranking_df.sort_values(["accuracy", "questions_done"], ascending=[True, False]).head(10).copy() if not ranking_df.empty else pd.DataFrame(columns=["topic_display", "accuracy", "questions_done", "grande_area", "subject"])

    upcoming_reviews_df = pd.DataFrame(columns=review_df.columns.tolist()) if review_df.empty else review_df.copy()
    if not upcoming_reviews_df.empty:
        upcoming_reviews_df["days_to_review"] = upcoming_reviews_df["next_review_date"].apply(
            lambda x: (
                datetime.strptime(str(x), "%Y-%m-%d").date() - date.today()
            ).days if normalize_text(x) else 9999
        )
        upcoming_reviews_df = upcoming_reviews_df[
            upcoming_reviews_df["days_to_review"] <= 7
        ].sort_values(
            ["days_to_review", "priority_order", "accuracy", "questions_done"],
            ascending=[True, True, True, False]
        ).head(10).copy()

    daily_questions_goal = to_int(goal.get("daily_questions_goal", 50), 50)
    daily_flashcard_goal = to_int(goal.get("daily_flashcard_goal", 100), 100)
    daily_minutes_goal = to_int(goal.get("daily_minutes_goal", 150), 150)
    monthly_mock_goal = to_int(goal.get("monthly_mock_goal", 3), 3)
    phase_name = str(goal.get("phase_name", "Amador") or "Amador")

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
            "label": "Meta de flashcards",
            "meta": f"{flashcards_reviewed_today}/{daily_flashcard_goal}",
            "status": min((flashcards_reviewed_today / daily_flashcard_goal) * 100, 100) if daily_flashcard_goal > 0 else 0,
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
        "daily_flashcard_goal": daily_flashcard_goal,
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
        "review_df": review_df,
        "upcoming_reviews_df": upcoming_reviews_df,
    }

def render_kpi_cards(metrics: dict):
    flash_extra = get_flashcard_extra_metrics(st.session_state.user_id)

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
            "label": "Flashcards acumulados",
            "value": flash_extra["reviewed_total"],
            "sub": f'Velocidade: {flash_extra["cards_per_hour_today"]:.1f} fc/h',
        },
        {
            "label": "Tempo médio/card",
            "value": f'{flash_extra["avg_seconds_today"]:.1f}s',
            "sub": "Base de hoje",
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

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Prioridades do dia</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Leitura rápida das metas operacionais.</div>', unsafe_allow_html=True)

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


def render_ranking_10_panel(metrics: dict):
    best_df = metrics["best_topics"].copy()
    worst_df = metrics["worst_topics"].copy()

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">10 melhores e 10 piores subtópicos</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Semáforo: vermelho &lt; 70 • amarelo 70 a 79 • verde com troféu &ge; 80.</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown('<div class="rank-col">', unsafe_allow_html=True)
        st.markdown('<div class="rank-col-title">10 Melhores</div>', unsafe_allow_html=True)

        if best_df.empty:
            st.markdown('<div class="b3-empty">Ainda não há dados suficientes.</div>', unsafe_allow_html=True)
        else:
            for _, row in best_df.iterrows():
                name = str(row.get("topic_display", "Sem nome"))
                acc = round(float(row.get("accuracy", 0)), 1)
                qtd = int(row.get("questions_done", 0))
                area = normalize_text(row.get("grande_area", ""))
                subject = normalize_text(row.get("subject", ""))

                aux_parts = []
                if area:
                    aux_parts.append(area)
                if subject and subject != name:
                    aux_parts.append(subject)
                aux_parts.append(f"{qtd} questões")

                st.markdown(
                    (
                        '<div class="rank-item">'
                        '<div>'
                        f'<div class="rank-name">{html.escape(name)}</div>'
                        f'<div class="rank-aux">{html.escape(" • ".join(aux_parts))}</div>'
                        '</div>'
                        f'<div class="rank-score">{get_performance_badge_html(acc)}</div>'
                        '</div>'
                    ),
                    unsafe_allow_html=True
                )

        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="rank-col">', unsafe_allow_html=True)
        st.markdown('<div class="rank-col-title">10 Piores</div>', unsafe_allow_html=True)

        if worst_df.empty:
            st.markdown('<div class="b3-empty">Ainda não há dados suficientes.</div>', unsafe_allow_html=True)
        else:
            for _, row in worst_df.iterrows():
                name = str(row.get("topic_display", "Sem nome"))
                acc = round(float(row.get("accuracy", 0)), 1)
                qtd = int(row.get("questions_done", 0))
                area = normalize_text(row.get("grande_area", ""))
                subject = normalize_text(row.get("subject", ""))

                aux_parts = []
                if area:
                    aux_parts.append(area)
                if subject and subject != name:
                    aux_parts.append(subject)
                aux_parts.append(f"{qtd} questões")

                st.markdown(
                    (
                        '<div class="rank-item">'
                        '<div>'
                        f'<div class="rank-name">{html.escape(name)}</div>'
                        f'<div class="rank-aux">{html.escape(" • ".join(aux_parts))}</div>'
                        '</div>'
                        f'<div class="rank-score">{get_performance_badge_html(acc)}</div>'
                        '</div>'
                    ),
                    unsafe_allow_html=True
                )

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_strategy_panel(metrics: dict):
    flash_extra = get_flashcard_extra_metrics(st.session_state.user_id)

    stats = [
        ("Etapa atual", str(metrics["phase_name"])),
        ("Flashcards/dia", str(metrics["daily_flashcard_goal"])),
        ("Flashcards acumulados", str(flash_extra["reviewed_total"])),
        ("Velocidade", f'{flash_extra["cards_per_hour_today"]:.1f} fc/h'),
        ("Tempo médio", f'{flash_extra["avg_seconds_today"]:.1f}s'),
        ("Média de simulados no mês", f'{metrics["avg_mock_score"]:.1f}%'),
        ("Itens concluídos", str(metrics["completed_items"])),
        ("Itens pendentes", str(metrics["pending_items"])),
        ("Sequência atual", f'{metrics["streak_current"]} dia(s)'),
        ("Melhor sequência 30d", f'{metrics["streak_best"]} dia(s)'),
    ]

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Resumo estratégico</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Panorama central da rotina de estudo e metas atuais.</div>', unsafe_allow_html=True)

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

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Ritmo dos últimos 30 dias</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Leitura simples da produção diária de questões.</div>', unsafe_allow_html=True)

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


def render_upcoming_reviews_panel(metrics: dict):
    review_df = metrics["upcoming_reviews_df"].copy()

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Revisões perto de vencer</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Mostra revisões vencidas ou com vencimento nos próximos 7 dias.</div>', unsafe_allow_html=True)

    if review_df.empty:
        st.markdown('<div class="b3-empty">Nenhuma revisão próxima do vencimento.</div>', unsafe_allow_html=True)
    else:
        for _, row in review_df.iterrows():
            topic_display = normalize_text(row.get("topic_display", "Sem nome"))
            grande_area = normalize_text(row.get("grande_area", ""))
            subject = normalize_text(row.get("subject", ""))
            next_review_date = normalize_text(row.get("next_review_date", ""))
            review_status = normalize_text(row.get("review_status", ""))
            accuracy = round(float(row.get("accuracy", 0)), 1)
            chip_html = get_review_urgency_chip(next_review_date)

            meta = []
            if grande_area:
                meta.append(grande_area)
            if subject and subject != topic_display:
                meta.append(subject)
            meta.append(f"{accuracy}%")
            if next_review_date:
                meta.append(f"Revisão: {next_review_date}")

            st.markdown(
                (
                    '<div class="overview-schedule-item">'
                    '<div class="overview-schedule-top">'
                    '<div>'
                    f'<div class="overview-schedule-title">{html.escape(topic_display)}</div>'
                    f'<div class="overview-schedule-meta">{html.escape(" • ".join(meta))}</div>'
                    f'<div class="overview-schedule-meta" style="margin-top:6px;">{html.escape(review_status)}</div>'
                    '</div>'
                    f'<div>{chip_html}</div>'
                    '</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

    st.markdown("</div>", unsafe_allow_html=True)


def render_dashboard_schedule_actions(metrics: dict):
    schedule_df = metrics["schedule_df"].copy()

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Cronograma em execução</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Marque tarefas como concluídas diretamente pela Visão Geral.</div>', unsafe_allow_html=True)

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


def render_goal_settings_panel(user_id: int, title: str = "Minhas metas", subtitle: str = "Todos os usuários podem ajustar suas metas pelo estágio."):
    current_goal = get_user_goal(user_id)
    current_stage = normalize_text(current_goal.get("phase_name", "Amador"))
    if current_stage not in STUDY_STAGES:
        current_stage = "Amador"

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{html.escape(title)}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-subtitle">{html.escape(subtitle)}</div>', unsafe_allow_html=True)

    selected_stage = st.selectbox(
        "Etapa/Fase",
        get_stage_names(),
        index=get_stage_names().index(current_stage),
        key=f"goal_stage_selector_{user_id}"
    )

    defaults = get_stage_goal_defaults(selected_stage)

    c1, c2, c3, c4 = st.columns(4, gap="large")
    with c1:
        st.markdown(
            f'<div class="mini-stat"><div class="mini-stat-label">Questões/dia</div><div class="mini-stat-value">{defaults["daily_questions_goal"]}</div></div>',
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div class="mini-stat"><div class="mini-stat-label">Flashcards/dia</div><div class="mini-stat-value">{defaults["daily_flashcard_goal"]}</div></div>',
            unsafe_allow_html=True
        )
    with c3:
        st.markdown(
            f'<div class="mini-stat"><div class="mini-stat-label">Tempo/dia</div><div class="mini-stat-value">{defaults["daily_minutes_goal"]} min</div></div>',
            unsafe_allow_html=True
        )
    with c4:
        st.markdown(
            f'<div class="mini-stat"><div class="mini-stat-label">Simulados/mês</div><div class="mini-stat-value">{defaults["monthly_mock_goal"]}</div></div>',
            unsafe_allow_html=True
        )

    if st.button("Salvar metas do estágio", key=f"save_goal_stage_{user_id}", use_container_width=True):
        ok, msg = update_goal_settings(user_id=user_id, phase_name=selected_stage)
        if ok:
            st.success(msg)
            safe_rerun()
        else:
            st.error(msg)

    st.markdown("</div>", unsafe_allow_html=True)


def render_dashboard_content_for_user(user_id: int, allow_goal_edit: bool = False):
    metrics = build_dashboard_metrics(user_id)

    render_kpi_cards(metrics)

    col1, col2 = st.columns([1.1, 0.9], gap="large")
    with col1:
        render_priorities_panel(metrics)
    with col2:
        render_strategy_panel(metrics)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    col3, col4 = st.columns([1, 1], gap="large")
    with col3:
        render_ranking_10_panel(metrics)
    with col4:
        render_line_chart_panel(metrics)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    col5, col6 = st.columns([1, 1], gap="large")
    with col5:
        render_upcoming_reviews_panel(metrics)
    with col6:
        render_dashboard_schedule_actions(metrics)

    if allow_goal_edit:
        st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
        render_goal_settings_panel(
            user_id=user_id,
            title="Minhas metas",
            subtitle="A seleção do estágio atualiza automaticamente questões, flashcards, minutos e simulados."
        )


def render_visao_geral():
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Visão Geral Executiva</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Seu cockpit premium com foco em rotina, desempenho, metas, revisões e execução do cronograma.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_dashboard_content_for_user(
        user_id=st.session_state.user_id,
        allow_goal_edit=True
    )
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


def get_all_student_user_ids():
    users_df = fetch_non_admin_users_df()
    if users_df.empty:
        return []
    return sorted(users_df["id"].astype(int).tolist())


def resolve_schedule_target_user_ids(mode: str, selected_labels):
    mode = normalize_text(mode)

    if not st.session_state.get("is_admin", False):
        return [int(st.session_state.user_id)]

    if mode == "Todos":
        ids = get_all_student_user_ids()
        return ids if ids else []

    if mode == "Usuário específico":
        ids = resolve_selected_target_user_ids(selected_labels[:1] if selected_labels else [])
        return ids

    if mode == "Multiusuários":
        ids = resolve_selected_target_user_ids(selected_labels)
        return ids

    return []


def add_schedule_item_for_users(
    target_user_ids,
    week_no: int,
    area: str,
    subject: str,
    topic: str,
    item_type: str,
    title: str,
    planned_date
):
    title = normalize_text(title)
    if not title:
        return False, "Digite o nome da tarefa."

    if not target_user_ids:
        return False, "Selecione ao menos um usuário."

    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    try:
        for user_id in target_user_ids:
            cur.execute(
                """
                INSERT INTO schedule_items (
                    user_id, week_no, area, subject, topic, item_type,
                    title, planned_date, completed, completed_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)
                """,
                (
                    int(user_id),
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
            inserted += 1

        conn.commit()
        return True, f"Item adicionado para {inserted} usuário(s)."
    except Exception as e:
        return False, f"Erro ao adicionar item: {e}"
    finally:
        conn.close()


def import_schedule_from_csv_for_users(target_user_ids):
    csv_path = get_schedule_csv_path()
    if not csv_path:
        return False, "CSV do cronograma não encontrado."

    if not target_user_ids:
        return False, "Selecione ao menos um usuário."

    try:
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except Exception:
            try:
                df = pd.read_csv(csv_path, encoding="latin1")
            except Exception:
                df = pd.read_csv(csv_path)
    except Exception as e:
        return False, f"Erro ao ler CSV: {e}"

    if df.empty:
        return False, "O CSV está vazio."

    lower_map = {str(c).strip().lower(): c for c in df.columns}
    required_cols = {"semana", "nome_tarefa", "grande_area"}
    if not required_cols.issubset(set(lower_map.keys())):
        return False, "O CSV precisa ter colunas semana, nome_tarefa e grande_area."

    week_col = lower_map["semana"]
    title_col = lower_map["nome_tarefa"]
    area_col = lower_map["grande_area"]
    task_num_col = lower_map.get("tarefa_num")
    subject_col = lower_map.get("materia") or lower_map.get("subject")
    topic_col = lower_map.get("subtopico") or lower_map.get("topic")
    type_col = lower_map.get("tipo") or lower_map.get("item_type")
    date_col = lower_map.get("data_planejada") or lower_map.get("planned_date")

    conn = get_conn()
    cur = conn.cursor()
    try:
        inserted = 0

        for user_id in target_user_ids:
            for _, row in df.iterrows():
                week_no = to_int(row.get(week_col, 0), 0)
                title = normalize_text(row.get(title_col, ""))
                area = normalize_text(row.get(area_col, ""))
                task_num = normalize_text(row.get(task_num_col, "")) if task_num_col else ""
                subject = normalize_text(row.get(subject_col, "")) if subject_col else area
                topic = normalize_text(row.get(topic_col, "")) if topic_col else title
                item_type = normalize_text(row.get(type_col, "")) if type_col else "Teoria"
                planned_date = normalize_text(row.get(date_col, "")) if date_col else ""

                if not title:
                    continue

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
                        int(user_id),
                        week_no,
                        area,
                        subject,
                        topic,
                        item_type if item_type else "Teoria",
                        final_title,
                        planned_date if planned_date else None,
                        datetime.now().isoformat()
                    )
                )
                inserted += 1

        conn.commit()
        return True, f"Cronograma importado com sucesso. {inserted} item(ns) criado(s)."
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
            "upcoming_count": 0,
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
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{html.escape(title)}</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Leitura rápida para orientar sua execução.</div>', unsafe_allow_html=True)

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
    st.markdown('<div class="section-subtitle">Cadastre, distribua, acompanhe e finalize tarefas com leitura executiva.</div>', unsafe_allow_html=True)
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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Adicionar item</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Preencha os campos para registrar uma nova tarefa no plano.</div>', unsafe_allow_html=True)

        target_options = get_target_user_options(include_current_user=True)
        target_labels = [x["label"] for x in target_options]

        with st.form("form_schedule_add", clear_on_submit=True):
            week_no = st.number_input("Semana", min_value=0, step=1, value=0)
            area = st.selectbox("Grande área", GREAT_AREAS, key="schedule_area")
            subject = st.text_input("Matéria", placeholder="Ex.: Cardiologia")
            topic = st.text_input("Subtópico", placeholder="Ex.: ICC")
            item_type = st.selectbox("Tipo", ["Teoria", "Questões", "Revisão", "Simulado", "Flashcards", "Outro"])
            title = st.text_input("Tarefa", placeholder="Ex.: Resolver 40 questões de ICC")
            planned_date = st.date_input("Data planejada", value=date.today())

            admin_target_mode = None
            selected_targets = []

            if st.session_state.get("is_admin", False):
                admin_target_mode = st.radio(
                    "Destino do cronograma",
                    ["Todos", "Multiusuários", "Usuário específico"],
                    horizontal=True,
                    key="schedule_admin_target_mode"
                )

                if admin_target_mode == "Multiusuários":
                    selected_targets = st.multiselect(
                        "Selecione os usuários",
                        target_labels,
                        default=[],
                        key="schedule_targets_multi_admin"
                    )
                elif admin_target_mode == "Usuário específico":
                    selected_target_single = st.selectbox(
                        "Selecione o usuário",
                        ["Selecione"] + target_labels,
                        key="schedule_target_single_admin"
                    )
                    selected_targets = [] if selected_target_single == "Selecione" else [selected_target_single]
                else:
                    st.info("O item será enviado para todos os alunos.")
            else:
                selected_targets = [st.session_state.get("username", "")]

            submitted = st.form_submit_button("Adicionar ao cronograma")

        if submitted:
            if st.session_state.get("is_admin", False):
                target_user_ids = resolve_schedule_target_user_ids(admin_target_mode, selected_targets)
            else:
                target_user_ids = [int(st.session_state.user_id)]

            ok, msg = add_schedule_item_for_users(
                target_user_ids,
                week_no,
                area,
                subject,
                topic,
                item_type,
                title,
                planned_date
            )
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)

        csv_exists = get_schedule_csv_path() is not None

        if st.session_state.get("is_admin", False):
            csv_target_mode = st.radio(
                "Destino da importação do CSV",
                ["Todos", "Multiusuários", "Usuário específico"],
                horizontal=True,
                key="schedule_csv_admin_target_mode"
            )

            csv_selected_targets = []
            if csv_target_mode == "Multiusuários":
                csv_selected_targets = st.multiselect(
                    "Usuários para receber o CSV",
                    target_labels,
                    default=[],
                    key="schedule_csv_targets_multi_admin"
                )
            elif csv_target_mode == "Usuário específico":
                csv_target_single = st.selectbox(
                    "Usuário para receber o CSV",
                    ["Selecione"] + target_labels,
                    key="schedule_csv_target_single_admin"
                )
                csv_selected_targets = [] if csv_target_single == "Selecione" else [csv_target_single]
        else:
            csv_target_mode = None
            csv_selected_targets = [st.session_state.get("username", "")]

        if st.button("Importar CSV como cronograma executável", use_container_width=True, disabled=not csv_exists):
            if st.session_state.get("is_admin", False):
                target_user_ids = resolve_schedule_target_user_ids(csv_target_mode, csv_selected_targets)
            else:
                target_user_ids = [int(st.session_state.user_id)]

            ok, msg = import_schedule_from_csv_for_users(target_user_ids)
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

        if not csv_exists:
            st.info("CSV do cronograma não encontrado no ambiente.")

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Gestão rápida do cronograma</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Marque como concluído ou exclua itens sem sair do painel.</div>', unsafe_allow_html=True)

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
def ensure_questions_review_schema():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS question_review_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                grande_area TEXT NOT NULL DEFAULT '',
                subject TEXT NOT NULL DEFAULT '',
                topic TEXT NOT NULL DEFAULT '',
                review_days INTEGER NOT NULL DEFAULT 0,
                completed_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def build_question_review_key(grande_area: str, subject: str, topic: str) -> str:
    return "|||".join([
        normalize_text(grande_area).lower(),
        normalize_text(subject).lower(),
        normalize_text(topic).lower(),
    ])


def fetch_question_review_status_map(user_id: int):
    ensure_questions_review_schema()
    df = fetch_dataframe("""
        SELECT *
        FROM question_review_status
        WHERE user_id = ?
        ORDER BY completed_at DESC, id DESC
    """, (user_id,))

    out = {}
    if df.empty:
        return out

    for _, row in df.iterrows():
        key = build_question_review_key(
            row.get("grande_area", ""),
            row.get("subject", ""),
            row.get("topic", "")
        )
        if key not in out:
            out[key] = {
                "completed_at": normalize_text(row.get("completed_at", "")),
                "review_days": to_int(row.get("review_days", 0), 0),
            }
    return out


def mark_question_review_done(user_id: int, grande_area: str, subject: str, topic: str, review_days: int):
    ensure_questions_review_schema()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO question_review_status (
                user_id, grande_area, subject, topic, review_days, completed_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            int(user_id),
            normalize_text(grande_area),
            normalize_text(subject),
            normalize_text(topic),
            to_int(review_days, 0),
            date.today().isoformat(),
            datetime.now().isoformat()
        ))
        conn.commit()
        return True, "Revisão concluída com sucesso."
    except Exception as e:
        return False, f"Erro ao concluir revisão: {e}"
    finally:
        conn.close()


def get_questions_taxonomy_from_csv():
    csv_path = get_schedule_csv_path()

    fallback = {
        "Clínica Médica": {
            "Cardiologia": ["Arritmias", "ICC", "SCA"],
            "Pneumologia": ["Asma", "DPOC"],
        },
        "Cirurgia": {
            "Abdome Agudo": ["Apendicite", "Colecistite"],
        },
        "Pediatria": {
            "Neonatologia": ["Icterícia Neonatal"],
        },
        "Ginecologia e Obstetrícia": {
            "Pré-natal": ["Hipertensão na Gestação", "Diabetes Gestacional"],
        },
        "Preventiva": {
            "Epidemiologia": ["Sensibilidade e Especificidade", "VPP e VPN"],
        },
    }

    if not csv_path or not os.path.exists(csv_path):
        return fallback

    try:
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except Exception:
            try:
                df = pd.read_csv(csv_path, encoding="latin1")
            except Exception:
                df = pd.read_csv(csv_path)

        if df.empty:
            return fallback

        col_map = {str(c).strip().lower(): c for c in df.columns}
        nome_col = col_map.get("nome_tarefa")
        area_col = col_map.get("grande_area")

        if not nome_col or not area_col:
            return fallback

        hierarchy = {}

        for _, row in df.iterrows():
            grande_area = normalize_text(row.get(area_col, ""))
            nome_tarefa = normalize_text(row.get(nome_col, ""))

            if not grande_area or not nome_tarefa:
                continue

            if " - " in nome_tarefa:
                tema, subtopico = nome_tarefa.split(" - ", 1)
                tema = normalize_text(tema)
                subtopico = normalize_text(subtopico)
            else:
                tema = normalize_text(nome_tarefa)
                subtopico = "Geral"

            if not tema:
                continue
            if not subtopico:
                subtopico = "Geral"

            hierarchy.setdefault(grande_area, {})
            hierarchy[grande_area].setdefault(tema, [])
            if subtopico not in hierarchy[grande_area][tema]:
                hierarchy[grande_area][tema].append(subtopico)

        if not hierarchy:
            return fallback

        ordered = {}
        for ga in sorted(hierarchy.keys()):
            ordered[ga] = {}
            for tema in sorted(hierarchy[ga].keys()):
                ordered[ga][tema] = sorted(hierarchy[ga][tema])

        return ordered

    except Exception:
        return fallback


def add_study_session(
    user_id: int,
    session_date,
    study_minutes: int,
    questions_done: int,
    correct_answers: int,
    subject: str,
    topic: str,
    notes: str,
    grande_area: str
):
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
                int(user_id),
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
            "correct_answers", "subject", "topic", "notes", "created_at",
            "grande_area", "accuracy"
        ])

    for col in ["study_minutes", "questions_done", "correct_answers"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["subject", "topic", "notes", "session_date", "created_at"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = ""

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


def get_questions_review_rule(accuracy: float):
    accuracy = to_float(accuracy, 0)

    if accuracy < 70:
        return {
            "review_days": 15,
            "review_label": "Revisão em 15 dias",
            "priority_label": "Alta prioridade",
            "priority_order": 1,
        }
    elif accuracy <= 80:
        return {
            "review_days": 23,
            "review_label": "Revisão em 23 dias",
            "priority_label": "Média prioridade",
            "priority_order": 2,
        }
    else:
        return {
            "review_days": 30,
            "review_label": "Revisão em 30 dias",
            "priority_label": "Baixa prioridade",
            "priority_order": 3,
        }


def build_questions_review_df(sessions_df: pd.DataFrame, user_id: int):
    ensure_questions_review_schema()

    if sessions_df.empty:
        return pd.DataFrame(columns=[
            "grande_area", "subject", "topic", "topic_display",
            "questions_done", "correct_answers", "study_minutes",
            "accuracy", "last_session_date", "review_days",
            "review_label", "priority_label", "priority_order",
            "next_review_date", "review_status", "last_review_done_at"
        ])

    base = sessions_df.copy()
    base["grande_area"] = base["grande_area"].fillna("").astype(str).str.strip()
    base["subject"] = base["subject"].fillna("").astype(str).str.strip()
    base["topic"] = base["topic"].fillna("").astype(str).str.strip()
    base["session_date"] = base["session_date"].fillna("").astype(str).str.strip()

    base["subject"] = base["subject"].where(base["subject"] != "", "Sem tema")
    base["topic"] = base["topic"].where(base["topic"] != "", "")
    base["topic_display"] = base["topic"].where(base["topic"] != "", base["subject"])

    grouped = (
        base.groupby(["grande_area", "subject", "topic", "topic_display"], as_index=False)
        .agg({
            "questions_done": "sum",
            "correct_answers": "sum",
            "study_minutes": "sum",
            "session_date": "max",
        })
        .rename(columns={"session_date": "last_session_date"})
    )

    grouped = grouped[grouped["questions_done"] > 0].copy()
    if grouped.empty:
        return pd.DataFrame(columns=[
            "grande_area", "subject", "topic", "topic_display",
            "questions_done", "correct_answers", "study_minutes",
            "accuracy", "last_session_date", "review_days",
            "review_label", "priority_label", "priority_order",
            "next_review_date", "review_status", "last_review_done_at"
        ])

    grouped["accuracy"] = ((grouped["correct_answers"] / grouped["questions_done"]) * 100).round(1)

    status_map = fetch_question_review_status_map(user_id)

    review_days_list = []
    review_label_list = []
    priority_label_list = []
    priority_order_list = []
    next_review_date_list = []
    review_status_list = []
    last_review_done_list = []

    today_dt = date.today()

    for _, row in grouped.iterrows():
        rule = get_questions_review_rule(float(row["accuracy"]))
        review_days = int(rule["review_days"])
        last_session = normalize_text(row["last_session_date"])
        key = build_question_review_key(
            row.get("grande_area", ""),
            row.get("subject", ""),
            row.get("topic", "")
        )

        try:
            last_session_dt = datetime.strptime(last_session, "%Y-%m-%d").date() if last_session else today_dt
        except Exception:
            last_session_dt = today_dt

        status_info = status_map.get(key)
        last_review_done_at = ""
        base_date = last_session_dt

        if status_info:
            completed_at = normalize_text(status_info.get("completed_at", ""))
            last_review_done_at = completed_at
            try:
                completed_dt = datetime.strptime(completed_at, "%Y-%m-%d").date() if completed_at else None
            except Exception:
                completed_dt = None

            if completed_dt and completed_dt >= last_session_dt:
                base_date = completed_dt

        next_review = base_date + timedelta(days=review_days)

        if next_review < today_dt:
            review_status = "Revisar agora"
        elif next_review == today_dt:
            review_status = "Vence hoje"
        else:
            delta = (next_review - today_dt).days
            review_status = f"Faltam {delta} dia(s)"

        review_days_list.append(review_days)
        review_label_list.append(rule["review_label"])
        priority_label_list.append(rule["priority_label"])
        priority_order_list.append(rule["priority_order"])
        next_review_date_list.append(next_review.isoformat())
        review_status_list.append(review_status)
        last_review_done_list.append(last_review_done_at)

    grouped["review_days"] = review_days_list
    grouped["review_label"] = review_label_list
    grouped["priority_label"] = priority_label_list
    grouped["priority_order"] = priority_order_list
    grouped["next_review_date"] = next_review_date_list
    grouped["review_status"] = review_status_list
    grouped["last_review_done_at"] = last_review_done_list

    grouped = grouped.sort_values(
        ["priority_order", "next_review_date", "accuracy", "questions_done"],
        ascending=[True, True, True, False]
    ).reset_index(drop=True)

    return grouped


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


def render_questions_review_panel(review_df: pd.DataFrame, user_id: int):
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Revisão automática por rendimento</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Regra aplicada: menor que 70% = 15 dias • 70 a 80% = 23 dias • maior que 80% = 30 dias.</div>',
        unsafe_allow_html=True
    )

    if review_df.empty:
        st.markdown('<div class="b3-empty">Ainda não há dados suficientes para gerar revisão automática por assunto.</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    urgent_df = review_df[
        review_df["review_status"].isin(["Revisar agora", "Vence hoje"])
    ].copy()
    next_df = review_df.head(8).copy()

    c1, c2, c3 = st.columns(3, gap="large")

    with c1:
        due_now = int(len(urgent_df))
        st.markdown(
            (
                '<div class="mini-stat">'
                '<div class="mini-stat-label">Assuntos para revisar agora</div>'
                f'<div class="mini-stat-value">{due_now}</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

    with c2:
        high_priority = int((review_df["priority_order"] == 1).sum())
        st.markdown(
            (
                '<div class="mini-stat">'
                '<div class="mini-stat-label">Alta prioridade</div>'
                f'<div class="mini-stat-value">{high_priority}</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

    with c3:
        mean_acc = round(float(review_df["accuracy"].mean()), 1) if not review_df.empty else 0.0
        st.markdown(
            (
                '<div class="mini-stat">'
                '<div class="mini-stat-label">Média dos assuntos</div>'
                f'<div class="mini-stat-value">{mean_acc}%</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)

    for _, row in next_df.iterrows():
        topic_display = normalize_text(row.get("topic_display", "Sem nome"))
        grande_area = normalize_text(row.get("grande_area", ""))
        subject = normalize_text(row.get("subject", ""))
        topic = normalize_text(row.get("topic", ""))
        accuracy = round(float(row.get("accuracy", 0)), 1)
        questions_done = to_int(row.get("questions_done", 0), 0)
        last_session_date = normalize_text(row.get("last_session_date", ""))
        next_review_date = normalize_text(row.get("next_review_date", ""))
        review_label = normalize_text(row.get("review_label", ""))
        review_status = normalize_text(row.get("review_status", ""))
        priority_label = normalize_text(row.get("priority_label", ""))
        last_review_done_at = normalize_text(row.get("last_review_done_at", ""))

        meta_parts = []
        if grande_area:
            meta_parts.append(grande_area)
        if subject and subject != topic_display:
            meta_parts.append(subject)
        meta_parts.append(f"{accuracy}%")
        meta_parts.append(f"{questions_done} questões")
        if last_session_date:
            meta_parts.append(f"Último treino: {last_session_date}")
        if next_review_date:
            meta_parts.append(f"Próxima revisão: {next_review_date}")
        if last_review_done_at:
            meta_parts.append(f"Última revisão concluída: {last_review_done_at}")

        st.markdown(
            (
                '<div class="b3-item">'
                '<div class="b3-item-top">'
                '<div>'
                f'<div class="b3-item-title">{html.escape(topic_display)}</div>'
                f'<div class="b3-item-meta">{html.escape(" • ".join(meta_parts))}</div>'
                f'<div class="b3-item-meta" style="margin-top:6px;">{html.escape(review_label)} • {html.escape(review_status)}</div>'
                '</div>'
                f'<div class="b3-chip">{html.escape(priority_label)}</div>'
                '</div>'
                '</div>'
            ),
            unsafe_allow_html=True
        )

        if st.button("Marcar revisão concluída", key=f"question_review_done_{build_question_review_key(grande_area, subject, topic)}", use_container_width=True):
            ok, msg = mark_question_review_done(
                user_id=user_id,
                grande_area=grande_area,
                subject=subject,
                topic=topic,
                review_days=to_int(row.get("review_days", 0), 0)
            )
            if ok:
                st.success(msg)
                safe_rerun()
            else:
                st.error(msg)

    st.markdown("</div>", unsafe_allow_html=True)


def render_questions_subject_table(review_df: pd.DataFrame):
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Mapa de revisão por assunto</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Consolidado automático dos assuntos a partir do desempenho nas questões.</div>', unsafe_allow_html=True)

    if review_df.empty:
        st.markdown('<div class="b3-empty">Nenhum assunto disponível para o mapa de revisão.</div>', unsafe_allow_html=True)
    else:
        view_df = review_df.copy()
        view_df = view_df.rename(columns={
            "grande_area": "Grande área",
            "subject": "Tema",
            "topic_display": "Assunto para revisão",
            "questions_done": "Questões",
            "correct_answers": "Acertos",
            "study_minutes": "Tempo (min)",
            "accuracy": "Acurácia (%)",
            "last_session_date": "Último treino",
            "review_label": "Regra",
            "next_review_date": "Próxima revisão",
            "review_status": "Status",
            "priority_label": "Prioridade",
            "last_review_done_at": "Última revisão concluída",
        })

        keep_cols = [
            "Grande área", "Tema", "Assunto para revisão", "Questões", "Acertos",
            "Acurácia (%)", "Tempo (min)", "Último treino", "Regra",
            "Próxima revisão", "Status", "Prioridade", "Última revisão concluída"
        ]
        st.dataframe(view_df[keep_cols], use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_questions_manager():
    ensure_questions_review_schema()

    sessions_df = fetch_sessions_df(st.session_state.user_id)
    summary = build_questions_summary(sessions_df)
    review_df = build_questions_review_df(sessions_df, user_id=st.session_state.user_id)

    taxonomy = get_questions_taxonomy_from_csv()
    area_options = sorted(list(taxonomy.keys()))

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Gestão de Questões</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Registre sessões, acompanhe acurácia e gere revisão automática por assunto conforme o rendimento.</div>',
        unsafe_allow_html=True
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_questions_kpis(summary)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    c_top_1, c_top_2 = st.columns([0.95, 1.05], gap="large")

    with c_top_1:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Registrar sessão</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-subtitle">Insira uma sessão de questões. O sistema recalcula automaticamente a revisão do assunto.</div>',
            unsafe_allow_html=True
        )

        session_date = st.date_input("Data", value=date.today(), key="question_date")

        grande_area = ""
        modo_ga = st.radio(
            "Grande área",
            ["Selecionar cadastrado", "Digitar novo"],
            horizontal=True,
            key="question_ga_mode"
        )

        if modo_ga == "Selecionar cadastrado":
            if area_options:
                grande_area = st.selectbox(
                    "Grande área cadastrada",
                    area_options,
                    key="question_ga_select"
                )
            else:
                st.info("Nenhuma grande área encontrada no CSV.")
        else:
            grande_area = st.text_input(
                "Nova grande área",
                placeholder="Ex.: Dermatologia",
                key="question_ga_new"
            )

        grande_area = normalize_text(grande_area)

        subject_options = sorted(list(taxonomy.get(grande_area, {}).keys())) if grande_area else []
        subject = ""

        modo_tema = st.radio(
            "Tema",
            ["Selecionar cadastrado", "Digitar novo"],
            horizontal=True,
            key=f"question_tema_mode__{grande_area or 'vazio'}"
        )

        if modo_tema == "Selecionar cadastrado":
            if subject_options:
                subject = st.selectbox(
                    "Tema cadastrado",
                    subject_options,
                    key=f"question_tema_select__{grande_area or 'vazio'}"
                )
            else:
                st.info("Nenhum tema encontrado para esta grande área.")
        else:
            subject = st.text_input(
                "Novo tema",
                placeholder="Ex.: Psoríase",
                key=f"question_tema_new__{grande_area or 'vazio'}"
            )

        subject = normalize_text(subject)

        topic_options = sorted(taxonomy.get(grande_area, {}).get(subject, [])) if grande_area and subject else []
        topic = ""

        modo_sub = st.radio(
            "Subtópico",
            ["Selecionar cadastrado", "Digitar novo"],
            horizontal=True,
            key=f"question_sub_mode__{grande_area or 'vazio'}__{subject or 'vazio'}"
        )

        if modo_sub == "Selecionar cadastrado":
            if topic_options:
                topic = st.selectbox(
                    "Subtópico cadastrado",
                    topic_options,
                    key=f"question_sub_select__{grande_area or 'vazio'}__{subject or 'vazio'}"
                )
            else:
                st.info("Nenhum subtópico encontrado para este tema.")
        else:
            topic = st.text_input(
                "Novo subtópico",
                placeholder="Ex.: Diagnóstico",
                key=f"question_sub_new__{grande_area or 'vazio'}__{subject or 'vazio'}"
            )

        topic = normalize_text(topic)

        study_minutes = st.number_input("Tempo estudado (min)", min_value=0, step=5, value=60)
        questions_done = st.number_input("Questões realizadas", min_value=0, step=1, value=20)
        correct_answers = st.number_input("Acertos", min_value=0, step=1, value=15)
        notes = st.text_area("Observações", placeholder="Ex.: errei fisiopatologia e conduta inicial")

        if st.button("Registrar sessão", key="btn_registrar_sessao_questoes", use_container_width=True):
            if not grande_area:
                st.error("Informe a grande área.")
            elif not subject:
                st.error("Informe o tema.")
            elif not topic:
                st.error("Informe o subtópico.")
            else:
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

    with c_top_2:
        render_questions_review_panel(review_df, user_id=st.session_state.user_id)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_questions_subject_table(review_df)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Assuntos com menor rendimento</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Prioridades de reforço a partir da acurácia acumulada.</div>', unsafe_allow_html=True)

        if review_df.empty:
            st.markdown('<div class="b3-empty">Ainda não há dados suficientes.</div>', unsafe_allow_html=True)
        else:
            worst_df = review_df.sort_values(
                ["accuracy", "questions_done"],
                ascending=[True, False]
            ).head(10).copy()

            for _, row in worst_df.iterrows():
                topic_display = normalize_text(row.get("topic_display", "Sem nome"))
                grande_area_item = normalize_text(row.get("grande_area", ""))
                accuracy = round(float(row.get("accuracy", 0)), 1)
                questions_done_item = to_int(row.get("questions_done", 0), 0)
                next_review_date = normalize_text(row.get("next_review_date", ""))

                meta = []
                if grande_area_item:
                    meta.append(grande_area_item)
                meta.append(f"{questions_done_item} questões")
                meta.append(f"{accuracy}%")
                if next_review_date:
                    meta.append(f"Revisão: {next_review_date}")

                st.markdown(
                    (
                        '<div class="b3-item">'
                        f'<div class="b3-item-title">{html.escape(topic_display)}</div>'
                        f'<div class="b3-item-meta">{html.escape(" • ".join(meta))}</div>'
                        '</div>'
                    ),
                    unsafe_allow_html=True
                )

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Histórico recente</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Visual executivo das últimas sessões com opção de exclusão.</div>', unsafe_allow_html=True)

        if sessions_df.empty:
            st.markdown('<div class="b3-empty">Nenhuma sessão registrada ainda.</div>', unsafe_allow_html=True)
        else:
            search_term = st.text_input(
                "Pesquisar por grande área, tema ou subtópico",
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
                    session_date_item = normalize_text(row.get("session_date", ""))
                    grande_area_item = normalize_text(row.get("grande_area", ""))
                    subject_item = normalize_text(row.get("subject", ""))
                    topic_item = normalize_text(row.get("topic", ""))
                    questions_done_item = to_int(row.get("questions_done", 0), 0)
                    correct_answers_item = to_int(row.get("correct_answers", 0), 0)
                    study_minutes_item = to_int(row.get("study_minutes", 0), 0)
                    accuracy_item = float(row.get("accuracy", 0))
                    notes_item = normalize_text(row.get("notes", ""))

                    review_rule = get_questions_review_rule(accuracy_item)
                    next_review = ""
                    try:
                        next_review = (
                            datetime.strptime(session_date_item, "%Y-%m-%d").date()
                            + timedelta(days=int(review_rule["review_days"]))
                        ).isoformat()
                    except Exception:
                        next_review = ""

                    meta_parts = []
                    if grande_area_item:
                        meta_parts.append(grande_area_item)
                    if subject_item:
                        meta_parts.append(subject_item)
                    if topic_item:
                        meta_parts.append(topic_item)
                    meta_parts.append(f"{questions_done_item} questões")
                    meta_parts.append(f"{correct_answers_item} acertos")
                    meta_parts.append(f"{accuracy_item:.1f}%")
                    meta_parts.append(f"{study_minutes_item} min")
                    if next_review:
                        meta_parts.append(f"Revisão: {next_review}")

                    note_html = (
                        f'<div class="b3-item-meta" style="margin-top:6px;">{html.escape(notes_item)}</div>'
                        if notes_item else ""
                    )

                    st.markdown(
                        (
                            '<div class="b3-item">'
                            '<div class="b3-item-top">'
                            '<div>'
                            f'<div class="b3-item-title">{html.escape(session_date_item)}</div>'
                            f'<div class="b3-item-meta">{html.escape(" • ".join(meta_parts))}</div>'
                            f'{note_html}'
                            '</div>'
                            f'<div class="b3-chip">{html.escape(review_rule["review_label"])}</div>'
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
def ensure_flashcards_extended_schema():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(flashcards)")
        cols = [row[1] for row in cur.fetchall()]

        alters = {
            "card_type": "ALTER TABLE flashcards ADD COLUMN card_type TEXT NOT NULL DEFAULT 'basic'",
            "cloze_text": "ALTER TABLE flashcards ADD COLUMN cloze_text TEXT DEFAULT ''",
            "cloze_answer": "ALTER TABLE flashcards ADD COLUMN cloze_answer TEXT DEFAULT ''",
            "cloze_full_text": "ALTER TABLE flashcards ADD COLUMN cloze_full_text TEXT DEFAULT ''",
        }

        for col, ddl in alters.items():
            if col not in cols:
                cur.execute(ddl)

        conn.commit()
    finally:
        conn.close()

def start_flashcard_timer():
    st.session_state["flashcard_started_at"] = datetime.now().timestamp()
def finish_flashcard_and_log(user_id: int, flashcard_id: int):
    started_at = st.session_state.get("flashcard_started_at")
    response_time_seconds = 0.0

    if started_at:
        response_time_seconds = max(
            0.0,
            datetime.now().timestamp() - float(started_at)
        )

    log_flashcard_review(
        user_id=user_id,
        flashcard_id=flashcard_id,
        response_time_seconds=response_time_seconds
    )

    st.session_state["flashcard_started_at"] = datetime.now().timestamp()

def initialize_new_flashcard_defaults(card_id: int):
    ensure_flashcards_extended_schema()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE flashcards
            SET due_date = COALESCE(due_date, ?),
                ease_factor = COALESCE(ease_factor, 2.5),
                interval_days = COALESCE(interval_days, 0),
                review_count = COALESCE(review_count, 0),
                lapse_count = COALESCE(lapse_count, 0),
                card_state = COALESCE(card_state, 'new'),
                card_type = COALESCE(card_type, 'basic'),
                cloze_text = COALESCE(cloze_text, ''),
                cloze_answer = COALESCE(cloze_answer, ''),
                cloze_full_text = COALESCE(cloze_full_text, '')
            WHERE id = ?
            """,
            (date.today().isoformat(), card_id)
        )
        conn.commit()
    finally:
        conn.close()


def extract_first_cloze_data(text: str):
    text = normalize_text(text)
    if not text:
        return "", "", ""

    import re

    pattern = r"\{\{c\d+::(.*?)(?:::(.*?))?\}\}"
    matches = re.findall(pattern, text)

    if not matches:
        return "", "", ""

    answers = []

    def replace_with_blank(match):
        inner = match.group(1)
        answers.append(inner.strip())
        return "_____"

    masked_text = re.sub(pattern, replace_with_blank, text)
    answer_text = " | ".join([a for a in answers if a])
    full_text = re.sub(pattern, lambda m: m.group(1).strip(), text)

    return masked_text, answer_text, full_text


def preview_flashcard_interval(interval_days: int, ease_factor: float, rating: str) -> int:
    interval_days = to_int(interval_days, 0)
    ease_factor = to_float(ease_factor, 2.5)
    rating = normalize_text(rating).lower()

    if rating == "again":
        return 1
    if rating == "hard":
        return max(2, int(round(max(1, interval_days) * 1.2)))
    if rating == "good":
        if interval_days <= 0:
            return 3
        if interval_days == 1:
            return 6
        return int(round(interval_days * ease_factor))
    if rating == "easy":
        if interval_days <= 0:
            return 5
        if interval_days == 1:
            return 8
        return int(round(interval_days * (ease_factor + 0.25)))

    return max(1, interval_days)


def format_interval_label(days: int) -> str:
    days = to_int(days, 0)
    return "1 dia" if days <= 1 else f"{days} dias"


def review_flashcard(card_id: int, rating: str):
    ensure_flashcards_extended_schema()

    rating = normalize_text(rating).lower()
    valid = {"again", "hard", "good", "easy"}
    if rating not in valid:
        return False, "Avaliação inválida."

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM flashcards WHERE id = ?", (card_id,))
        row = cur.fetchone()
        if not row:
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
        return True, "Revisão registrada."
    except Exception as e:
        return False, f"Erro ao revisar flashcard: {e}"
    finally:
        conn.close()


def fetch_flashcards_df(user_id: int):
    ensure_flashcards_extended_schema()

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
            created_at ASC,
            id ASC
        """,
        (user_id,)
    )

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "deck", "subject", "topic", "question", "answer", "note",
            "created_at", "due_date", "last_reviewed", "review_count",
            "lapse_count", "ease_factor", "interval_days", "card_state",
            "card_type", "cloze_text", "cloze_answer", "cloze_full_text"
        ])

    text_cols = [
        "deck", "subject", "topic", "question", "answer", "note",
        "card_state", "card_type", "cloze_text", "cloze_answer", "cloze_full_text"
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = ""

    for col in ["review_count", "lapse_count", "interval_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    if "ease_factor" in df.columns:
        df["ease_factor"] = pd.to_numeric(df["ease_factor"], errors="coerce").fillna(2.5)
    else:
        df["ease_factor"] = 2.5

    if "due_date" not in df.columns:
        df["due_date"] = date.today().isoformat()
    else:
        df["due_date"] = df["due_date"].fillna(date.today().isoformat()).astype(str)

    df["card_type"] = df["card_type"].replace("", "basic")
    return df


def get_admin_and_student_user_ids(include_admin=False):
    users_df = fetch_users_df()
    if users_df.empty:
        if include_admin and st.session_state.get("user_id"):
            return [int(st.session_state.user_id)]
        return []

    ids = []
    student_ids = users_df[users_df["is_admin"] == 0]["id"].astype(int).tolist()
    ids.extend(student_ids)

    if include_admin and st.session_state.get("user_id"):
        ids.append(int(st.session_state.user_id))

    return sorted(list(set(ids)))


def add_flashcard_for_users(target_user_ids, deck: str, subject: str, topic: str, question: str, answer: str, note: str):
    ensure_flashcards_extended_schema()

    question = normalize_text(question)
    answer = normalize_text(answer)

    if not question:
        return False, "Digite a frente/pergunta do flashcard."
    if not answer:
        return False, "Digite a resposta do flashcard."
    if not target_user_ids:
        return False, "Nenhum usuário selecionado."

    conn = get_conn()
    cur = conn.cursor()
    try:
        inserted = 0
        for user_id in sorted(set([int(x) for x in target_user_ids])):
            cur.execute(
                """
                INSERT INTO flashcards (
                    user_id, deck, subject, topic, question, answer, note, created_at,
                    due_date, last_reviewed, review_count, lapse_count, ease_factor,
                    interval_days, card_state, card_type, cloze_text, cloze_answer, cloze_full_text
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
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
                    "basic",
                    "",
                    "",
                    "",
                )
            )
            inserted += 1

        conn.commit()
        return True, f"Flashcard adicionado para {inserted} usuário(s)."
    except Exception as e:
        return False, f"Erro ao adicionar flashcard: {e}"
    finally:
        conn.close()


def add_cloze_flashcard_for_users(target_user_ids, deck: str, subject: str, topic: str, cloze_source_text: str, note: str):
    ensure_flashcards_extended_schema()

    cloze_source_text = normalize_text(cloze_source_text)
    if not cloze_source_text:
        return False, "Digite o texto do cloze."
    if not target_user_ids:
        return False, "Nenhum usuário selecionado."

    cloze_text, cloze_answer, cloze_full_text = extract_first_cloze_data(cloze_source_text)
    if not cloze_text or not cloze_answer:
        return False, "O cloze precisa conter ao menos um padrão como {{c1::texto}}."

    conn = get_conn()
    cur = conn.cursor()
    try:
        inserted = 0
        for user_id in sorted(set([int(x) for x in target_user_ids])):
            cur.execute(
                """
                INSERT INTO flashcards (
                    user_id, deck, subject, topic, question, answer, note, created_at,
                    due_date, last_reviewed, review_count, lapse_count, ease_factor,
                    interval_days, card_state, card_type, cloze_text, cloze_answer, cloze_full_text
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    normalize_text(deck),
                    normalize_text(subject),
                    normalize_text(topic),
                    cloze_text,
                    cloze_answer,
                    normalize_text(note),
                    datetime.now().isoformat(),
                    date.today().isoformat(),
                    None,
                    0,
                    0,
                    2.5,
                    0,
                    "new",
                    "cloze",
                    cloze_text,
                    cloze_answer,
                    cloze_full_text,
                )
            )
            inserted += 1

        conn.commit()
        return True, f"Flashcard cloze adicionado para {inserted} usuário(s)."
    except Exception as e:
        return False, f"Erro ao adicionar flashcard cloze: {e}"
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


def delete_flashcards_by_scope_for_users(
    target_user_ids,
    delete_mode: str,
    deck_name: str = "",
    subject_name: str = "",
    topic_name: str = "",
    is_admin: bool = False
):
    if not is_admin:
        return False, "Apenas o administrador pode apagar deck, matérias ou subtópicos."

    if not target_user_ids:
        return False, "Nenhum usuário selecionado."

    delete_mode = normalize_text(delete_mode)
    deck_name = normalize_text(deck_name)
    subject_name = normalize_text(subject_name)
    topic_name = normalize_text(topic_name)

    if delete_mode == "Deck":
        if not deck_name:
            return False, "Selecione um deck."
        where_sql = "deck = ?"
        params_base = [deck_name]
        label = f"deck '{deck_name}'"

    elif delete_mode == "Matéria":
        if not subject_name:
            return False, "Selecione uma matéria."
        where_sql = "subject = ?"
        params_base = [subject_name]
        label = f"matéria '{subject_name}'"

    elif delete_mode == "Subtópico":
        if not topic_name:
            return False, "Selecione um subtópico."
        where_sql = "topic = ?"
        params_base = [topic_name]
        label = f"subtópico '{topic_name}'"

    else:
        return False, "Modo de exclusão inválido."

    conn = get_conn()
    cur = conn.cursor()
    try:
        deleted_total = 0

        for user_id in sorted(set([int(x) for x in target_user_ids])):
            sql = f"""
                DELETE FROM flashcards
                WHERE user_id = ?
                  AND {where_sql}
            """
            cur.execute(sql, [user_id] + params_base)
            deleted_total += max(cur.rowcount, 0)

        conn.commit()

        if deleted_total <= 0:
            return False, f"Nenhum card encontrado para {label}."

        return True, f"Exclusão concluída com sucesso: {deleted_total} card(s) removido(s) de {label}."
    except Exception as e:
        return False, f"Erro ao excluir flashcards: {e}"
    finally:
        conn.close()


def _read_csv_flexible(uploaded_file):
    df = None
    for sep in [",", ";", "\t"]:
        for header in ["infer", None]:
            try:
                uploaded_file.seek(0)
                tmp = pd.read_csv(uploaded_file, sep=sep, header=header, dtype=str).fillna("")
                if not tmp.empty and tmp.shape[1] >= 1:
                    df = tmp.copy()
                    break
            except Exception:
                pass
        if df is not None:
            break
    return df


def import_flashcards_csv_basic_for_users(target_user_ids, uploaded_file, deck_name: str, subject_name: str, topic_name: str):
    ensure_flashcards_extended_schema()

    if uploaded_file is None:
        return False, "Envie um arquivo CSV basic."
    if not target_user_ids:
        return False, "Nenhum usuário selecionado."

    df = _read_csv_flexible(uploaded_file)
    if df is None or df.empty:
        return False, "Não foi possível ler o CSV basic."

    cols = list(df.columns)
    if len(cols) < 2:
        return False, "O CSV basic precisa ter pelo menos 2 colunas."

    q_col = cols[0]
    a_col = cols[1]
    n_col = cols[2] if len(cols) >= 3 else None

    conn = get_conn()
    cur = conn.cursor()
    try:
        inserted = 0
        for user_id in sorted(set([int(x) for x in target_user_ids])):
            for _, row in df.iterrows():
                question = normalize_text(row.get(q_col, ""))
                answer = normalize_text(row.get(a_col, ""))
                note = normalize_text(row.get(n_col, "")) if n_col is not None else ""

                if not question or not answer:
                    continue

                cur.execute(
                    """
                    INSERT INTO flashcards (
                        user_id, deck, subject, topic, question, answer, note, created_at,
                        due_date, last_reviewed, review_count, lapse_count, ease_factor,
                        interval_days, card_state, card_type, cloze_text, cloze_answer, cloze_full_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(user_id),
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
                        "basic",
                        "",
                        "",
                        "",
                    )
                )
                inserted += 1

        conn.commit()
        return True, f"{inserted} flashcards basic importados com sucesso."
    except Exception as e:
        return False, f"Erro ao importar CSV basic: {e}"
    finally:
        conn.close()


def import_flashcards_csv_cloze_for_users(target_user_ids, uploaded_file, deck_name: str, subject_name: str, topic_name: str):
    ensure_flashcards_extended_schema()

    if uploaded_file is None:
        return False, "Envie um arquivo CSV cloze."
    if not target_user_ids:
        return False, "Nenhum usuário selecionado."

    df = _read_csv_flexible(uploaded_file)
    if df is None or df.empty:
        return False, "Não foi possível ler o CSV cloze."

    cols = list(df.columns)
    text_col = cols[0] if len(cols) >= 1 else None
    n_col = cols[2] if len(cols) >= 3 else None

    if text_col is None:
        return False, "O CSV cloze precisa ter ao menos a primeira coluna com o texto cloze."

    conn = get_conn()
    cur = conn.cursor()
    try:
        inserted = 0
        for user_id in sorted(set([int(x) for x in target_user_ids])):
            for _, row in df.iterrows():
                raw_text = normalize_text(row.get(text_col, ""))
                note = normalize_text(row.get(n_col, "")) if n_col is not None else ""

                if not raw_text:
                    continue

                cloze_text, cloze_answer, cloze_full_text = extract_first_cloze_data(raw_text)
                if not cloze_text or not cloze_answer:
                    continue

                cur.execute(
                    """
                    INSERT INTO flashcards (
                        user_id, deck, subject, topic, question, answer, note, created_at,
                        due_date, last_reviewed, review_count, lapse_count, ease_factor,
                        interval_days, card_state, card_type, cloze_text, cloze_answer, cloze_full_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(user_id),
                        normalize_text(deck_name),
                        normalize_text(subject_name),
                        normalize_text(topic_name),
                        cloze_text,
                        cloze_answer,
                        note,
                        datetime.now().isoformat(),
                        date.today().isoformat(),
                        None,
                        0,
                        0,
                        2.5,
                        0,
                        "new",
                        "cloze",
                        cloze_text,
                        cloze_answer,
                        cloze_full_text,
                    )
                )
                inserted += 1

        conn.commit()
        return True, f"{inserted} flashcards cloze importados com sucesso."
    except Exception as e:
        return False, f"Erro ao importar CSV cloze: {e}"
    finally:
        conn.close()


def build_flashcard_filters(df: pd.DataFrame, selected_deck="Todos", selected_subject="Todos", selected_topic="Todos"):
    if df.empty:
        return [], [], [], []

    base_df = df.copy()

    decks = sorted([
        x for x in base_df["deck"].dropna().astype(str).unique().tolist()
        if normalize_text(x)
    ])

    if selected_deck != "Todos":
        base_df = base_df[base_df["deck"] == selected_deck].copy()

    subjects = sorted([
        x for x in base_df["subject"].dropna().astype(str).unique().tolist()
        if normalize_text(x)
    ])

    if selected_subject != "Todos":
        base_df = base_df[base_df["subject"] == selected_subject].copy()

    topics = sorted([
        x for x in base_df["topic"].dropna().astype(str).unique().tolist()
        if normalize_text(x)
    ])

    if selected_topic != "Todos":
        base_df = base_df[base_df["topic"] == selected_topic].copy()

    types_ = sorted([
        x for x in base_df["card_type"].dropna().astype(str).unique().tolist()
        if normalize_text(x)
    ])

    return decks, subjects, topics, types_


def filter_flashcards_df(df: pd.DataFrame, deck_filter: str, subject_filter: str, topic_filter: str, type_filter: str, search_term: str, due_only: bool = False):
    if df.empty:
        return df.copy()

    out = df.copy()

    if deck_filter != "Todos":
        out = out[out["deck"] == deck_filter]
    if subject_filter != "Todos":
        out = out[out["subject"] == subject_filter]
    if topic_filter != "Todos":
        out = out[out["topic"] == topic_filter]
    if type_filter != "Todos":
        out = out[out["card_type"] == type_filter]

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
            out["deck"].str.lower().str.contains(needle, na=False) |
            out["card_type"].str.lower().str.contains(needle, na=False) |
            out["cloze_text"].str.lower().str.contains(needle, na=False) |
            out["cloze_answer"].str.lower().str.contains(needle, na=False) |
            out["cloze_full_text"].str.lower().str.contains(needle, na=False)
        ]

    return out.reset_index(drop=True)


def render_flashcard_kpis(df: pd.DataFrame):
    total_cards = int(len(df)) if not df.empty else 0
    due_today = int((df["due_date"].fillna(date.today().isoformat()).astype(str) <= date.today().isoformat()).sum()) if not df.empty else 0
    total_subjects = int(df["subject"].astype(str).replace("", pd.NA).dropna().nunique()) if not df.empty else 0
    total_decks = int(df["deck"].astype(str).replace("", pd.NA).dropna().nunique()) if not df.empty else 0

    cards = [
        ("Flashcards", total_cards, "Base cadastrada"),
        ("Revisões para hoje", due_today, "Fila do dia"),
        ("Matérias", total_subjects, "Cobertura"),
        ("Decks", total_decks, "Baralhos ativos"),
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


def prepare_flashcard_queue(filtered_df: pd.DataFrame):
    ids = filtered_df["id"].astype(int).tolist() if not filtered_df.empty else []
    saved_ids = st.session_state.get("flashcard_queue_ids", [])

    if not ids:
        st.session_state.flashcard_queue_ids = []
        st.session_state.flashcard_index = 0
        return []

    if not saved_ids or saved_ids != ids:
        st.session_state.flashcard_queue_ids = ids
        st.session_state.flashcard_index = 0
        return ids

    return saved_ids


def inject_flashcard_fullscreen_css():
    st.markdown(
        """
        <style>
        .block-container{
            padding-top: 0.12rem !important;
            padding-bottom: 0.20rem !important;
            max-width: 1400px !important;
        }

        .fc-stage{
            width:100%;
            display:block;
            min-height:auto !important;
            margin:0 !important;
            padding:0 !important;
        }

        .fc-screen{
            width:100%;
            min-height:auto !important;
            display:block;
            position:relative;
            overflow:hidden;
            background:
                radial-gradient(circle at 12% 18%, rgba(56,189,248,.10), transparent 22%),
                radial-gradient(circle at 88% 12%, rgba(139,92,246,.14), transparent 24%),
                linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.015)),
                linear-gradient(180deg, #202226 0%, #24262b 100%);
            border: 1.5px solid rgba(255,255,255,.14);
            border-radius: 26px;
            padding: 0 !important;
            margin: 0 !important;
            box-shadow:
                0 30px 80px rgba(0,0,0,.38),
                0 0 0 1px rgba(255,255,255,.03) inset,
                0 0 35px rgba(59,130,246,.10),
                0 0 60px rgba(124,58,237,.08);
        }

        .fc-screen::before{
            content:"";
            position:absolute;
            inset:0;
            border-radius:26px;
            padding:1px;
            background: linear-gradient(
                135deg,
                rgba(255,255,255,.22) 0%,
                rgba(96,165,250,.22) 22%,
                rgba(129,140,248,.18) 48%,
                rgba(168,85,247,.20) 72%,
                rgba(255,255,255,.10) 100%
            );
            -webkit-mask:
                linear-gradient(#000 0 0) content-box,
                linear-gradient(#000 0 0);
            -webkit-mask-composite: xor;
            mask-composite: exclude;
            pointer-events:none;
        }

        .fc-screen::after{
            content:"";
            position:absolute;
            inset:10px;
            border-radius:18px;
            border:1px solid rgba(255,255,255,.05);
            pointer-events:none;
            box-shadow:
                inset 0 1px 0 rgba(255,255,255,.03),
                inset 0 0 18px rgba(255,255,255,.015);
        }

        .fc-question-wrap{
            width:100%;
            text-align:center;
            padding: 0px 28px 0 28px !important;
            margin: 0 !important;
            position:relative;
            z-index:2;
        }

        .fc-question{
            color:#ffffff;
            font-size: 1.80rem;
            font-weight: 800;
            line-height: 1.45;
            text-align:center;
            max-width: 1100px;
            margin: 0 auto !important;
            word-break: break-word;
            text-shadow:
                0 2px 10px rgba(0,0,0,.28),
                0 0 18px rgba(255,255,255,.04);
        }

        .fc-answer-divider{
            width:100%;
            border-top: 1px solid rgba(255,255,255,.16);
            margin-top: 12px;
            box-shadow: 0 1px 0 rgba(255,255,255,.03) inset;
            position:relative;
            z-index:2;
        }

        .fc-answer-area{
            width:100%;
            text-align:center;
            padding: 14px 28px 12px 28px !important;
            margin: 0 !important;
            display:flex;
            justify-content:center;
            align-items:center;
            position:relative;
            z-index:2;
        }

        .fc-answer-box{
            width:100%;
            max-width: 1120px;
            text-align:center;
            margin:0 auto !important;
            padding: 16px 18px;
            border-radius: 20px;
            background: linear-gradient(180deg, rgba(255,255,255,.035), rgba(255,255,255,.02));
            border: 1px solid rgba(255,255,255,.07);
            box-shadow:
                0 14px 32px rgba(0,0,0,.18),
                inset 0 1px 0 rgba(255,255,255,.03);
        }

        .fc-answer-main{
            color:#ffffff;
            font-size: 1.55rem;
            font-weight: 800;
            line-height: 1.5;
            text-align:center;
            margin: 0 auto !important;
            word-break: break-word;
            text-shadow:
                0 2px 10px rgba(0,0,0,.26),
                0 0 16px rgba(255,255,255,.03);
        }

        .fc-answer-note{
            color:#dbe4f2;
            font-size: 1.18rem;
            font-weight: 500;
            line-height: 1.58;
            text-align:center;
            margin: 18px auto 0 auto !important;
            max-width: 1050px;
            word-break: break-word;
        }

        .fc-no-answer-gap{
            height: 10px !important;
            width:100%;
            margin:0 !important;
            padding:0 !important;
        }

        .fc-bottom{
            width:100%;
            margin-top: 10px !important;
            padding: 12px 20px 18px 20px !important;
            text-align:center;
            border-top: 1px solid rgba(255,255,255,.06);
            position:relative;
            z-index:2;
            background: linear-gradient(180deg, rgba(255,255,255,.015), rgba(255,255,255,.01));
        }

        .fc-show-answer-wrap{
            max-width: 320px;
            margin: 0 auto !important;
        }

        .fc-show-answer-wrap div[data-testid="stButton"] > button{
            border-radius: 14px !important;
            min-height: 44px !important;
            font-size: 1rem !important;
            font-weight: 900 !important;
            background: linear-gradient(90deg, #1f9bff 0%, #6a63ff 55%, #8b5cf6 100%) !important;
            color: #fff !important;
            border: 1px solid rgba(255,255,255,.10) !important;
            box-shadow:
                0 10px 24px rgba(37,99,235,.22),
                0 0 24px rgba(99,102,241,.12) !important;
        }

        .fc-show-answer-wrap div[data-testid="stButton"] > button:hover{
            filter: brightness(1.05);
            transform: translateY(-1px);
            transition: .18s ease;
        }

        .fc-rating-top{
            margin-bottom: 4px !important;
        }

        .fc-rating-label{
            text-align:center;
            color:#ffffff;
            font-size:1rem;
            font-weight:800;
            white-space:nowrap;
            margin-bottom:6px;
            text-shadow: 0 1px 8px rgba(0,0,0,.18);
        }

        .fc-rating-btns div[data-testid="stButton"] > button{
            border-radius: 14px !important;
            min-height: 44px !important;
            font-size: .96rem !important;
            font-weight: 900 !important;
            background: linear-gradient(180deg, #4f535a 0%, #3d4148 100%) !important;
            color: #fff !important;
            border: 1px solid rgba(255,255,255,.10) !important;
            box-shadow:
                0 10px 24px rgba(0,0,0,.16),
                inset 0 1px 0 rgba(255,255,255,.04) !important;
        }

        .fc-rating-btns div[data-testid="stButton"] > button:hover{
            filter: brightness(1.06);
            transform: translateY(-1px);
            transition: .18s ease;
        }

        .fc-back-wrap{
            max-width: 240px;
            margin: 12px auto 0 auto !important;
        }

        .fc-back-wrap div[data-testid="stButton"] > button{
            border-radius: 14px !important;
            min-height: 42px !important;
            font-size: .94rem !important;
            font-weight: 900 !important;
            background: linear-gradient(90deg, #2497ff 0%, #5f6dfb 55%, #7c5cff 100%) !important;
            color: #fff !important;
            border: 1px solid rgba(255,255,255,.10) !important;
            box-shadow:
                0 10px 24px rgba(37,99,235,.18),
                0 0 22px rgba(99,102,241,.10) !important;
        }

        .fc-back-wrap div[data-testid="stButton"] > button:hover{
            filter: brightness(1.05);
            transform: translateY(-1px);
            transition: .18s ease;
        }

        @media (max-width: 900px){
            .fc-screen{
                border-radius: 18px;
            }

            .fc-screen::before{
                border-radius: 18px;
            }

            .fc-screen::after{
                inset:8px;
                border-radius: 12px;
            }

            .fc-question{
                font-size: 1.45rem;
            }

            .fc-answer-main{
                font-size: 1.25rem;
            }

            .fc-answer-note{
                font-size: 1rem;
            }

            .fc-question-wrap{
                padding: 0px 14px 0 14px !important;
            }

            .fc-answer-area{
                padding: 10px 14px 10px 14px !important;
            }

            .fc-bottom{
                padding: 10px 10px 14px 10px !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True
    )


def render_flashcard_player(filtered_df: pd.DataFrame):
    inject_flashcard_fullscreen_css()
    queue_ids = prepare_flashcard_queue(filtered_df)

    if not queue_ids:
        st.markdown('<div class="b4-empty">Nenhum flashcard encontrado com esse filtro.</div>', unsafe_allow_html=True)
        if st.button("Voltar", key="fc_back_empty", use_container_width=True):
            st.session_state.flashcard_fullscreen = False
            st.session_state.flashcard_show_answer = False
            st.session_state.flashcard_show_note = False
            st.session_state.flashcard_queue_ids = []
            safe_rerun()
        return

    total = len(queue_ids)
    current_index = st.session_state.get("flashcard_index", 0)
    current_index = max(0, min(current_index, total - 1))
    st.session_state.flashcard_index = current_index

    current_id = int(queue_ids[current_index])
    row_df = filtered_df[filtered_df["id"].astype(int) == current_id].copy()

    if row_df.empty:
        st.session_state.flashcard_queue_ids = []
        st.session_state.flashcard_index = 0
        safe_rerun()
        return

    row = row_df.iloc[0]
    card_id = int(row["id"])
    card_type = normalize_text(row.get("card_type", "basic")) or "basic"

    current_timer_card_id = st.session_state.get("flashcard_timer_card_id")
    if current_timer_card_id != card_id:
        start_flashcard_timer()
        st.session_state["flashcard_timer_card_id"] = card_id

    if card_type == "cloze":
        question = normalize_text(row.get("cloze_text", "")) or normalize_text(row.get("question", ""))
        answer = normalize_text(row.get("cloze_answer", "")) or normalize_text(row.get("answer", ""))
        full_text = normalize_text(row.get("cloze_full_text", ""))
        note = normalize_text(row.get("note", ""))
    else:
        question = normalize_text(row.get("question", ""))
        answer = normalize_text(row.get("answer", ""))
        full_text = ""
        note = normalize_text(row.get("note", ""))

    interval_days = to_int(row.get("interval_days", 0), 0)
    ease_factor = to_float(row.get("ease_factor", 2.5), 2.5)

    again_days = preview_flashcard_interval(interval_days, ease_factor, "again")
    hard_days = preview_flashcard_interval(interval_days, ease_factor, "hard")
    good_days = preview_flashcard_interval(interval_days, ease_factor, "good")
    easy_days = preview_flashcard_interval(interval_days, ease_factor, "easy")

    show_answer = st.session_state.get("flashcard_show_answer", False)
    badge_label = "Cloze" if card_type == "cloze" else "Basic"

    st.markdown('<div class="fc-stage">', unsafe_allow_html=True)
    st.markdown('<div class="fc-screen">', unsafe_allow_html=True)

    header_html = (
        '<div style="'
        'display:flex;'
        'justify-content:space-between;'
        'align-items:center;'
        'gap:12px;'
        'padding:16px 18px 10px 18px;'
        'position:relative;'
        'z-index:2;'
        '">'
            '<div style="'
            'display:inline-flex;'
            'align-items:center;'
            'padding:8px 12px;'
            'border-radius:999px;'
            'border:1px solid rgba(255,255,255,.10);'
            'background:rgba(255,255,255,.04);'
            'color:#eef4ff;'
            'font-size:.82rem;'
            'font-weight:900;'
            'letter-spacing:.02em;'
            '">'
                + html.escape(badge_label) +
            '</div>'
            '<div style="'
            'display:inline-flex;'
            'align-items:center;'
            'padding:8px 12px;'
            'border-radius:999px;'
            'border:1px solid rgba(255,255,255,.10);'
            'background:rgba(255,255,255,.04);'
            'color:#dbe6f8;'
            'font-size:.82rem;'
            'font-weight:800;'
            '">'
                + f'Card {current_index + 1} de {total}' +
            '</div>'
        '</div>'
    )
    st.markdown(header_html, unsafe_allow_html=True)

    st.markdown('<div class="fc-question-wrap">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="fc-question">{html.escape(question)}</div>',
        unsafe_allow_html=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

    if show_answer:
        st.markdown('<div class="fc-answer-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="fc-answer-area">', unsafe_allow_html=True)

        extra_parts = []
        if card_type == "cloze" and full_text:
            extra_parts.append(f"Frase completa: {html.escape(full_text)}")
        if note:
            extra_parts.append(f"Nota: {html.escape(note)}")

        extra_html = ""
        if extra_parts:
            extra_html = f'<div class="fc-answer-note">{"<br><br>".join(extra_parts)}</div>'

        st.markdown(
            f'<div class="fc-answer-box"><div class="fc-answer-main">{html.escape(answer) if answer else "Sem resposta cadastrada."}</div>{extra_html}</div>',
            unsafe_allow_html=True
        )
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="fc-no-answer-gap"></div>', unsafe_allow_html=True)

    st.markdown('<div class="fc-bottom">', unsafe_allow_html=True)

    if not show_answer:
        st.markdown('<div class="fc-show-answer-wrap">', unsafe_allow_html=True)
        if st.button("Mostrar resposta", key=f"fc_show_answer_{card_id}", use_container_width=True):
            st.session_state.flashcard_show_answer = True
            safe_rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="fc-rating-top">', unsafe_allow_html=True)

        l1, l2, l3, l4 = st.columns(4, gap="large")
        with l1:
            st.markdown(f'<div class="fc-rating-label">{html.escape(format_interval_label(again_days))}</div>', unsafe_allow_html=True)
        with l2:
            st.markdown(f'<div class="fc-rating-label">{html.escape(format_interval_label(hard_days))}</div>', unsafe_allow_html=True)
        with l3:
            st.markdown(f'<div class="fc-rating-label">{html.escape(format_interval_label(good_days))}</div>', unsafe_allow_html=True)
        with l4:
            st.markdown(f'<div class="fc-rating-label">{html.escape(format_interval_label(easy_days))}</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('<div class="fc-rating-btns">', unsafe_allow_html=True)

        r1, r2, r3, r4 = st.columns(4, gap="large")

        with r1:
            if st.button("De novo", key=f"fc_rate_again_{card_id}", use_container_width=True):
                finish_flashcard_and_log(st.session_state.user_id, card_id)
                ok, _ = review_flashcard(card_id, "again")
                if ok:
                    st.session_state.flashcard_show_answer = False
                    st.session_state.flashcard_show_note = False
                    if current_index < total - 1:
                        st.session_state.flashcard_index = current_index + 1
                    safe_rerun()

        with r2:
            if st.button("Difícil", key=f"fc_rate_hard_{card_id}", use_container_width=True):
                finish_flashcard_and_log(st.session_state.user_id, card_id)
                ok, _ = review_flashcard(card_id, "hard")
                if ok:
                    st.session_state.flashcard_show_answer = False
                    st.session_state.flashcard_show_note = False
                    if current_index < total - 1:
                        st.session_state.flashcard_index = current_index + 1
                    safe_rerun()

        with r3:
            if st.button("Bom", key=f"fc_rate_good_{card_id}", use_container_width=True):
                finish_flashcard_and_log(st.session_state.user_id, card_id)
                ok, _ = review_flashcard(card_id, "good")
                if ok:
                    st.session_state.flashcard_show_answer = False
                    st.session_state.flashcard_show_note = False
                    if current_index < total - 1:
                        st.session_state.flashcard_index = current_index + 1
                    safe_rerun()

        with r4:
            if st.button("Fácil", key=f"fc_rate_easy_{card_id}", use_container_width=True):
                finish_flashcard_and_log(st.session_state.user_id, card_id)
                ok, _ = review_flashcard(card_id, "easy")
                if ok:
                    st.session_state.flashcard_show_answer = False
                    st.session_state.flashcard_show_note = False
                    if current_index < total - 1:
                        st.session_state.flashcard_index = current_index + 1
                    safe_rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="fc-back-wrap">', unsafe_allow_html=True)
    if st.button("Voltar", key=f"fc_back_{card_id}", use_container_width=True):
        st.session_state.flashcard_fullscreen = False
        st.session_state.flashcard_show_answer = False
        st.session_state.flashcard_show_note = False
        st.session_state.flashcard_queue_ids = []
        st.session_state["flashcard_timer_card_id"] = None
        safe_rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

def render_flashcards_page():
    ensure_flashcards_extended_schema()
    df = fetch_flashcards_df(st.session_state.user_id)

    if "flashcard_queue_ids" not in st.session_state:
        st.session_state.flashcard_queue_ids = []

    if st.session_state.get("flashcard_fullscreen", False):
        filtered_df = filter_flashcards_df(
            df=df,
            deck_filter=st.session_state.get("fc_filter_deck_value", "Todos"),
            subject_filter=st.session_state.get("fc_filter_subject_value", "Todos"),
            topic_filter=st.session_state.get("fc_filter_topic_value", "Todos"),
            type_filter=st.session_state.get("fc_filter_type_value", "Todos"),
            search_term=st.session_state.get("fc_search_term_value", ""),
            due_only=st.session_state.get("fc_due_only_value", True),
        )
        render_flashcard_player(filtered_df)
        return

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Flashcards</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Basic + Cloze no mesmo baralho, revisão em sequência e exclusão em massa apenas para administrador.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_flashcard_kpis(df)

    left, right = st.columns([0.98, 1.02], gap="large")

    with left:
        tab_manual_basic, tab_manual_cloze, tab_import = st.tabs(["Adicionar Basic", "Adicionar Cloze", "Importar CSVs"])

        with tab_manual_basic:
            st.markdown('<div class="b4-card">', unsafe_allow_html=True)
            st.markdown('<div class="b4-title">Adicionar flashcard basic</div>', unsafe_allow_html=True)
            st.markdown('<div class="b4-sub">Frente, resposta e nota.</div>', unsafe_allow_html=True)

            with st.form("form_add_flashcard_basic", clear_on_submit=True):
                deck = st.text_input("Deck", placeholder="Ex.: Clínica Médica", key="fc_basic_deck")
                subject = st.text_input("Matéria", placeholder="Ex.: Pneumologia", key="fc_basic_subject")
                topic = st.text_input("Subtópico", placeholder="Ex.: Asma", key="fc_basic_topic")
                question = st.text_area("Frente / Pergunta", placeholder="Digite a pergunta", key="fc_basic_question")
                answer = st.text_area("Resposta / Verso", placeholder="Digite a resposta", key="fc_basic_answer")
                note = st.text_area("Nota / Explicação", placeholder="Digite a explicação adicional", key="fc_basic_note")

                if st.session_state.get("is_admin", False):
                    st.info("Como administrador, este flashcard será enviado para todos os alunos e também ficará disponível para sua revisão.")
                else:
                    st.info("Este flashcard será salvo apenas para seu usuário.")

                submitted = st.form_submit_button("Adicionar flashcard basic")

            if submitted:
                if st.session_state.get("is_admin", False):
                    target_user_ids = get_admin_and_student_user_ids(include_admin=True)
                else:
                    target_user_ids = [int(st.session_state.user_id)]

                ok, msg = add_flashcard_for_users(
                    target_user_ids, deck, subject, topic, question, answer, note
                )
                if ok:
                    st.success(msg)
                    safe_rerun()
                else:
                    st.error(msg)

            st.markdown("</div>", unsafe_allow_html=True)

        with tab_manual_cloze:
            st.markdown('<div class="b4-card">', unsafe_allow_html=True)
            st.markdown('<div class="b4-title">Adicionar flashcard cloze</div>', unsafe_allow_html=True)
            st.markdown('<div class="b4-sub">Use o padrão {{c1::texto}} no trecho que será ocultado.</div>', unsafe_allow_html=True)

            with st.form("form_add_flashcard_cloze", clear_on_submit=True):
                deck = st.text_input("Deck", placeholder="Ex.: Endócrino", key="fc_cloze_deck")
                subject = st.text_input("Matéria", placeholder="Ex.: DM2", key="fc_cloze_subject")
                topic = st.text_input("Subtópico", placeholder="Ex.: Tratamento", key="fc_cloze_topic")
                cloze_source_text = st.text_area(
                    "Texto Cloze",
                    placeholder="Ex.: A droga de primeira linha no DM2 é {{c1::Metformina}}.",
                    key="fc_cloze_text"
                )
                note = st.text_area("Nota / Explicação", placeholder="Observação adicional", key="fc_cloze_note")

                if st.session_state.get("is_admin", False):
                    st.info("Como administrador, este cloze será enviado para todos os alunos e também ficará disponível para sua revisão.")
                else:
                    st.info("Este cloze será salvo apenas para seu usuário.")

                submitted_cloze = st.form_submit_button("Adicionar flashcard cloze")

            if submitted_cloze:
                if st.session_state.get("is_admin", False):
                    target_user_ids = get_admin_and_student_user_ids(include_admin=True)
                else:
                    target_user_ids = [int(st.session_state.user_id)]

                ok, msg = add_cloze_flashcard_for_users(
                    target_user_ids,
                    deck,
                    subject,
                    topic,
                    cloze_source_text,
                    note
                )
                if ok:
                    st.success(msg)
                    safe_rerun()
                else:
                    st.error(msg)

            st.markdown("</div>", unsafe_allow_html=True)

        with tab_import:
            st.markdown('<div class="b4-card">', unsafe_allow_html=True)
            st.markdown('<div class="b4-title">Importar arquivos CSV</div>', unsafe_allow_html=True)
            st.markdown('<div class="b4-sub">Você pode importar 1 CSV basic e 1 CSV cloze no mesmo deck, mantendo a ordem do arquivo.</div>', unsafe_allow_html=True)

            with st.form("form_import_flashcards_dual", clear_on_submit=False):
                csv_deck = st.text_input("Deck do lote", placeholder="Ex.: Revisão Endócrino")
                csv_subject = st.text_input("Matéria do lote", placeholder="Ex.: Endocrinologia")
                csv_topic = st.text_input("Subtópico do lote", placeholder="Ex.: Antidiabéticos")

                basic_file = st.file_uploader("Arquivo CSV Basic", type=["csv"], key="fc_csv_upload_basic")
                cloze_file = st.file_uploader("Arquivo CSV Cloze", type=["csv"], key="fc_csv_upload_cloze")

                if st.session_state.get("is_admin", False):
                    st.info("Como administrador, os arquivos importados serão enviados para todos os alunos e também para o próprio admin revisar.")
                else:
                    st.info("Os arquivos importados serão salvos apenas para seu usuário.")

                import_submitted = st.form_submit_button("Importar arquivos")

            if import_submitted:
                if st.session_state.get("is_admin", False):
                    target_user_ids = get_admin_and_student_user_ids(include_admin=True)
                else:
                    target_user_ids = [int(st.session_state.user_id)]

                imported_msgs = []
                error_msgs = []

                if basic_file is not None:
                    ok, msg = import_flashcards_csv_basic_for_users(
                        target_user_ids, basic_file, csv_deck, csv_subject, csv_topic
                    )
                    if ok:
                        imported_msgs.append(msg)
                    else:
                        error_msgs.append(msg)

                if cloze_file is not None:
                    ok, msg = import_flashcards_csv_cloze_for_users(
                        target_user_ids, cloze_file, csv_deck, csv_subject, csv_topic
                    )
                    if ok:
                        imported_msgs.append(msg)
                    else:
                        error_msgs.append(msg)

                if basic_file is None and cloze_file is None:
                    error_msgs.append("Envie ao menos um arquivo: basic ou cloze.")

                if imported_msgs:
                    st.success(" | ".join(imported_msgs))
                    safe_rerun()
                if error_msgs and not imported_msgs:
                    st.error(" | ".join(error_msgs))
                elif error_msgs:
                    st.warning(" | ".join(error_msgs))

            st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="b4-card">', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Revisão e gestão</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">Filtre, revise em sequência e, se for administrador, exclua em massa por deck, matéria ou subtópico.</div>', unsafe_allow_html=True)

        current_deck = st.session_state.get("fc_filter_deck", "Todos")
        deck_options, _, _, _ = build_flashcard_filters(df)
        deck_options = ["Todos"] + deck_options
        if current_deck not in deck_options:
            current_deck = "Todos"

        deck_filter = st.selectbox(
            "Filtrar deck",
            deck_options,
            index=deck_options.index(current_deck),
            key="fc_filter_deck"
        )

        current_subject = st.session_state.get("fc_filter_subject", "Todos")
        _, subject_options_raw, _, _ = build_flashcard_filters(df, selected_deck=deck_filter)
        subject_options = ["Todos"] + subject_options_raw
        if current_subject not in subject_options:
            current_subject = "Todos"
            st.session_state["fc_filter_subject"] = "Todos"

        subject_filter = st.selectbox(
            "Filtrar matéria",
            subject_options,
            index=subject_options.index(current_subject),
            key="fc_filter_subject"
        )

        current_topic = st.session_state.get("fc_filter_topic", "Todos")
        _, _, topic_options_raw, _ = build_flashcard_filters(
            df,
            selected_deck=deck_filter,
            selected_subject=subject_filter
        )
        topic_options = ["Todos"] + topic_options_raw
        if current_topic not in topic_options:
            current_topic = "Todos"
            st.session_state["fc_filter_topic"] = "Todos"

        topic_filter = st.selectbox(
            "Filtrar subtópico",
            topic_options,
            index=topic_options.index(current_topic),
            key="fc_filter_topic"
        )

        current_type = st.session_state.get("fc_filter_type", "Todos")
        _, _, _, type_options_raw = build_flashcard_filters(
            df,
            selected_deck=deck_filter,
            selected_subject=subject_filter,
            selected_topic=topic_filter
        )
        type_options = ["Todos"] + type_options_raw
        if current_type not in type_options:
            current_type = "Todos"
            st.session_state["fc_filter_type"] = "Todos"

        type_filter = st.selectbox(
            "Filtrar tipo",
            type_options,
            index=type_options.index(current_type),
            key="fc_filter_type"
        )

        due_only = st.checkbox("Mostrar apenas cards vencidos para hoje", value=True, key="fc_due_only")
        search_term = st.text_input("Pesquisar flashcards", placeholder="Ex.: asma, metformina, cloze", key="fc_search_term")

        st.session_state["fc_filter_deck_value"] = deck_filter
        st.session_state["fc_filter_subject_value"] = subject_filter
        st.session_state["fc_filter_topic_value"] = topic_filter
        st.session_state["fc_filter_type_value"] = type_filter
        st.session_state["fc_due_only_value"] = due_only
        st.session_state["fc_search_term_value"] = search_term

        filtered_df = filter_flashcards_df(
            df, deck_filter, subject_filter, topic_filter, type_filter, search_term, due_only=due_only
        )

        qtd_filtrada = len(filtered_df)
        due_count = int((filtered_df["due_date"].fillna(date.today().isoformat()).astype(str) <= date.today().isoformat()).sum()) if not filtered_df.empty else 0
        filtered_decks = int(filtered_df["deck"].astype(str).replace("", pd.NA).dropna().nunique()) if not filtered_df.empty else 0

        c1, c2, c3 = st.columns(3, gap="large")
        with c1:
            st.markdown(
                f'<div class="mini-stat"><div class="mini-stat-label">Cards no filtro</div><div class="mini-stat-value">{qtd_filtrada}</div></div>',
                unsafe_allow_html=True
            )
        with c2:
            st.markdown(
                f'<div class="mini-stat"><div class="mini-stat-label">Vencidos no filtro</div><div class="mini-stat-value">{due_count}</div></div>',
                unsafe_allow_html=True
            )
        with c3:
            st.markdown(
                f'<div class="mini-stat"><div class="mini-stat-label">Decks no filtro</div><div class="mini-stat-value">{filtered_decks}</div></div>',
                unsafe_allow_html=True
            )

        if st.button("Abrir revisão em sequência", key="fc_open_player", use_container_width=True):
            st.session_state.flashcard_fullscreen = True
            st.session_state.flashcard_index = 0
            st.session_state.flashcard_show_answer = False
            st.session_state.flashcard_show_note = False
            st.session_state.flashcard_queue_ids = filtered_df["id"].astype(int).tolist() if not filtered_df.empty else []
            safe_rerun()

        st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-title">Exclusão em massa</div>', unsafe_allow_html=True)
        st.markdown('<div class="b4-sub">O administrador pode excluir flashcards por deck, matéria ou subtópico.</div>', unsafe_allow_html=True)

        if st.session_state.get("is_admin", False):
            delete_mode = st.radio(
                "Tipo de exclusão",
                ["Deck", "Matéria", "Subtópico"],
                horizontal=True,
                key="fc_delete_mode"
            )

            all_decks_for_delete = sorted([
                x for x in df["deck"].dropna().astype(str).unique().tolist()
                if normalize_text(x)
            ]) if not df.empty else []

            all_subjects_for_delete = sorted([
                x for x in df["subject"].dropna().astype(str).unique().tolist()
                if normalize_text(x)
            ]) if not df.empty else []

            all_topics_for_delete = sorted([
                x for x in df["topic"].dropna().astype(str).unique().tolist()
                if normalize_text(x)
            ]) if not df.empty else []

            deck_to_delete = "Selecione"
            subject_to_delete = "Selecione"
            topic_to_delete = "Selecione"

            if delete_mode == "Deck":
                deck_to_delete = st.selectbox(
                    "Deck para excluir",
                    ["Selecione"] + all_decks_for_delete,
                    key="fc_delete_deck_name"
                )
            elif delete_mode == "Matéria":
                subject_to_delete = st.selectbox(
                    "Matéria para excluir",
                    ["Selecione"] + all_subjects_for_delete,
                    key="fc_delete_subject_name"
                )
            else:
                topic_to_delete = st.selectbox(
                    "Subtópico para excluir",
                    ["Selecione"] + all_topics_for_delete,
                    key="fc_delete_topic_name"
                )

            confirm_delete = st.checkbox(
                "Confirmo que desejo excluir em massa",
                key="fc_confirm_delete_scope"
            )

            st.info("Como administrador, a exclusão afetará todos os alunos e também os cards do próprio admin, se existirem.")

            if st.button("Executar exclusão", key="fc_delete_scope_btn", use_container_width=True):
                if not confirm_delete:
                    st.error("Marque a confirmação para excluir.")
                else:
                    if delete_mode == "Deck" and deck_to_delete == "Selecione":
                        st.error("Selecione um deck.")
                    elif delete_mode == "Matéria" and subject_to_delete == "Selecione":
                        st.error("Selecione uma matéria.")
                    elif delete_mode == "Subtópico" and topic_to_delete == "Selecione":
                        st.error("Selecione um subtópico.")
                    else:
                        target_user_ids = get_admin_and_student_user_ids(include_admin=True)

                        ok, msg = delete_flashcards_by_scope_for_users(
                            target_user_ids=target_user_ids,
                            delete_mode=delete_mode,
                            deck_name="" if deck_to_delete == "Selecione" else deck_to_delete,
                            subject_name="" if subject_to_delete == "Selecione" else subject_to_delete,
                            topic_name="" if topic_to_delete == "Selecione" else topic_to_delete,
                            is_admin=True
                        )

                        if ok:
                            st.success(msg)
                            st.session_state.flashcard_queue_ids = []
                            st.session_state.flashcard_index = 0
                            safe_rerun()
                        else:
                            st.error(msg)
        else:
            st.warning("A exclusão de deck, matéria ou subtópico é permitida apenas para o administrador.")

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

    for col in ["grande_area", "mock_title", "mock_date"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

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


def build_mock_ranking_df(mocks_df: pd.DataFrame):
    if mocks_df.empty:
        return pd.DataFrame(columns=[
            "rank_pos", "mock_date", "title", "score_percent", "questions_count"
        ])

    rank_df = mocks_df.copy()
    rank_df["score_percent"] = pd.to_numeric(rank_df["score_percent"], errors="coerce").fillna(0.0)
    rank_df["questions_count"] = pd.to_numeric(rank_df["questions_count"], errors="coerce").fillna(0).astype(int)
    rank_df["mock_date"] = rank_df["mock_date"].fillna("").astype(str)
    rank_df["title"] = rank_df["title"].fillna("").astype(str)

    rank_df = rank_df.sort_values(
        ["score_percent", "questions_count", "mock_date", "id"],
        ascending=[False, False, False, False]
    ).reset_index(drop=True)

    rank_df["rank_pos"] = range(1, len(rank_df) + 1)
    return rank_df[["rank_pos", "mock_date", "title", "score_percent", "questions_count"]].copy()


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
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Evolução dos simulados</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Leitura visual simples da progressão dos resultados.</div>', unsafe_allow_html=True)

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


def render_mock_ranking_panel(mocks_df: pd.DataFrame):
    ranking_df = build_mock_ranking_df(mocks_df)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Ranking por simulado</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Classificação do melhor para o pior resultado entre seus simulados.</div>', unsafe_allow_html=True)

    if ranking_df.empty:
        st.markdown('<div class="b4-empty">Ainda não há simulados suficientes para gerar ranking.</div>', unsafe_allow_html=True)
    else:
        top_preview = ranking_df.head(10).copy()

        for _, row in top_preview.iterrows():
            rank_pos = int(row["rank_pos"])
            mock_date = normalize_text(row.get("mock_date", ""))
            title = normalize_text(row.get("title", "Sem título"))
            score_percent = round(float(row.get("score_percent", 0)), 1)
            questions_count = to_int(row.get("questions_count", 0), 0)

            badge = f"#{rank_pos}"
            if rank_pos == 1:
                badge = "🥇 #1"
            elif rank_pos == 2:
                badge = "🥈 #2"
            elif rank_pos == 3:
                badge = "🥉 #3"

            st.markdown(
                (
                    '<div class="mock-item">'
                    '<div class="mock-top">'
                    '<div>'
                    f'<div class="mock-title">{html.escape(title)}</div>'
                    f'<div class="mock-meta">{html.escape(mock_date)} • {questions_count} questões</div>'
                    '</div>'
                    f'<div class="mock-badge">{html.escape(badge)} • {score_percent:.1f}%</div>'
                    '</div>'
                    '</div>'
                ),
                unsafe_allow_html=True
            )

        st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
        view_df = ranking_df.rename(columns={
            "rank_pos": "Posição",
            "mock_date": "Data",
            "title": "Simulado",
            "score_percent": "Percentual (%)",
            "questions_count": "Questões"
        })
        st.dataframe(view_df, use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_mocks_page():
    mocks_df = fetch_mocks_df(st.session_state.user_id)
    area_scores_df = fetch_mock_area_scores_df(st.session_state.user_id)
    summary = build_mock_summary(mocks_df)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Simulados</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Cadastre resultados, acompanhe histórico, ranking e distribuição de acertos por grande área.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_mock_kpis(summary)

    left, right = st.columns([0.95, 1.05], gap="large")

    with left:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Registrar simulado</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Cadastre a data, o nome, o percentual geral e os acertos por grande área.</div>', unsafe_allow_html=True)

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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Histórico de simulados</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Gerencie os lançamentos mais recentes.</div>', unsafe_allow_html=True)

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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Desempenho por grande área</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Resumo dos acertos por área ao longo dos simulados.</div>', unsafe_allow_html=True)

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

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_mock_ranking_panel(mocks_df)
# =========================================================
# SIMULADOS
# RANKING MULTIUSUÁRIO PREMIUM
# =========================================================

def render_mock_multiuser_ranking():
    st.markdown("## 🏆 Ranking Multiusuário de Simulados")

    mock_names = fetch_available_mock_names()

    if not mock_names:
        st.info("Nenhum simulado encontrado na base.")
        return

    selected_mock = st.selectbox(
        "Selecione o simulado",
        options=mock_names,
        key="ranking_selected_mock"
    )

    df = fetch_mock_ranking_df(selected_mock)

    if df.empty:
        st.warning("Não há resultados para este simulado.")
        return

    geral = build_mock_diagnostics(df)

    leader = df.iloc[0]
    last = df.iloc[-1]

    elite = len(df[df["score_percent"] >= 85])
    alta = len(df[(df["score_percent"] >= 80) & (df["score_percent"] < 85)])
    boa = len(df[(df["score_percent"] >= 70) & (df["score_percent"] < 80)])
    inter = len(df[(df["score_percent"] >= 60) & (df["score_percent"] < 70)])
    critica = len(df[df["score_percent"] < 60])

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Usuários", geral["n_usuarios"])
    with c2:
        st.metric("Média", f"{geral['media']}%")
    with c3:
        st.metric("Mediana", f"{geral['mediana']}%")
    with c4:
        st.metric("Melhor", f"{geral['melhor']}%")
    with c5:
        st.metric("Pior", f"{geral['pior']}%")
    with c6:
        st.metric("Desvio padrão", f"{geral['desvio']}")

    st.markdown("### Ranking geral")

    show_df = df[[
        "rank",
        "user_name",
        "correct_answers",
        "total_questions",
        "score_percent",
        "diff_to_leader"
    ]].copy()

    show_df["performance_band"] = show_df["score_percent"].apply(classify_performance)

    show_df.columns = [
        "Posição",
        "Usuário",
        "Acertos",
        "Total",
        "% Acerto",
        "Dif. Líder",
        "Faixa"
    ]

    st.dataframe(show_df, use_container_width=True, hide_index=True)

    st.markdown("### Leitura estratégica do ranking")
    st.info(
        f"O líder atual é {leader['user_name']} com {leader['score_percent']}% "
        f"({leader['correct_answers']}/{leader['total_questions']}). "
        f"A diferença para o último colocado ({last['user_name']}) é de "
        f"{round(float(leader['score_percent']) - float(last['score_percent']), 2)} ponto(s)."
    )

    st.write(
        f"Distribuição do grupo: Elite = {elite}, Alta performance = {alta}, "
        f"Boa performance = {boa}, Intermediária = {inter}, Crítica = {critica}."
    )

    if geral["desvio"] <= 5:
        st.success("O grupo está relativamente homogêneo, com pequena dispersão entre os participantes.")
    elif geral["desvio"] <= 10:
        st.warning("O grupo apresenta dispersão moderada, com diferenças perceptíveis entre os participantes.")
    else:
        st.error("O grupo apresenta alta dispersão, com distância relevante entre topo e base do ranking.")

    st.markdown("### Destaques")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Top 3**")
        for _, row in df.head(3).iterrows():
            st.write(
                f"#{row['rank']} • {row['user_name']} — "
                f"{row['score_percent']}% ({row['correct_answers']}/{row['total_questions']})"
            )

    with col2:
        st.markdown("**Últimos 3**")
        for _, row in df.tail(3).iterrows():
            st.write(
                f"#{row['rank']} • {row['user_name']} — "
                f"{row['score_percent']}% ({row['correct_answers']}/{row['total_questions']})"
            )

    st.markdown("### Diagnóstico individual detalhado")

    for _, row in df.iterrows():
        texto = build_user_diagnostic_text(row, geral)

        score = safe_float(row["score_percent"])
        if score >= 85:
            extra = "Leitura estratégica: perfil de excelência e manutenção de alto nível."
            st.success(texto + " " + extra)
        elif score >= 80:
            extra = "Leitura estratégica: desempenho competitivo com potencial de chegar à elite."
            st.success(texto + " " + extra)
        elif score >= 70:
            extra = "Leitura estratégica: boa base, mas ainda com margem clara de crescimento."
            st.warning(texto + " " + extra)
        elif score >= 60:
            extra = "Leitura estratégica: desempenho intermediário, exigindo revisão direcionada."
            st.warning(texto + " " + extra)
        else:
            extra = "Leitura estratégica: necessidade de reforço de base e recuperação intensiva."
            st.error(texto + " " + extra)
# =========================================================
# HELPERS PDF / RELATÓRIOS
# =========================================================
def draw_pdf_multiline(
    c,
    text,
    x,
    y,
    max_width=17.2 * cm,
    line_height=0.52 * cm,
    font_name="Helvetica",
    font_size=10,
):
    c.setFont(font_name, font_size)
    words = str(text or "").split()
    if not words:
        return y

    current_line = ""
    for word in words:
        test_line = f"{current_line} {word}".strip()
        if c.stringWidth(test_line, font_name, font_size) <= max_width:
            current_line = test_line
        else:
            if current_line:
                c.drawString(x, y, current_line)
                y -= line_height
            current_line = word

    if current_line:
        c.drawString(x, y, current_line)
        y -= line_height

    return y


def draw_pdf_background(c):
    width, height = A4

    c.saveState()
    c.setFillColorRGB(0.985, 0.989, 0.995)
    c.rect(0, 0, width, height, fill=1, stroke=0)
    c.restoreState()

    c.saveState()
    c.setFillColorRGB(0.10, 0.22, 0.42)
    c.rect(0, height - 2.0 * cm, width, 2.0 * cm, fill=1, stroke=0)
    c.restoreState()

    c.saveState()
    c.setStrokeColorRGB(0.78, 0.84, 0.92)
    c.setLineWidth(0.03 * cm)
    c.line(1.2 * cm, height - 2.1 * cm, width - 1.2 * cm, height - 2.1 * cm)
    c.restoreState()

    logo_path = get_logo_path()
    if logo_path:
        try:
            img = ImageReader(logo_path)
            wm_w = 9.0 * cm
            wm_h = 9.0 * cm
            x = (width - wm_w) / 2
            y = (height - wm_h) / 2 - 1.2 * cm

            c.saveState()
            try:
                c.setFillAlpha(0.06)
            except Exception:
                pass

            c.drawImage(
                img,
                x,
                y,
                width=wm_w,
                height=wm_h,
                preserveAspectRatio=True,
                mask="auto"
            )
            c.restoreState()
        except Exception:
            pass

    c.saveState()
    c.setStrokeColorRGB(0.85, 0.89, 0.94)
    c.setLineWidth(0.02 * cm)
    c.line(1.5 * cm, 1.7 * cm, width - 1.5 * cm, 1.7 * cm)
    c.restoreState()


def draw_pdf_header(c, subtitle="Relatório Estratégico"):
    width, height = A4
    logo_path = get_logo_path()

    if logo_path:
        try:
            img = ImageReader(logo_path)
            c.drawImage(
                img,
                1.5 * cm,
                height - 1.65 * cm,
                width=1.35 * cm,
                height=1.35 * cm,
                preserveAspectRatio=True,
                mask="auto"
            )
        except Exception:
            pass

    c.setFillColorRGB(1, 1, 1)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(3.1 * cm, height - 0.95 * cm, "Nexus Med")

    c.setFont("Helvetica", 9)
    c.drawString(3.1 * cm, height - 1.38 * cm, subtitle)

    c.setFillColorRGB(0.88, 0.92, 0.98)
    c.setFont("Helvetica", 8)
    c.drawRightString(width - 1.5 * cm, height - 1.15 * cm, datetime.now().strftime("%d/%m/%Y %H:%M"))


def draw_pdf_footer(c, username=""):
    footer_text = "Nexus Med • Diagnóstico situacional premium"
    if normalize_text(username):
        footer_text += f" • Usuário: {username}"

    c.setFillColorRGB(0.45, 0.50, 0.58)
    c.setFont("Helvetica", 8)
    c.drawString(1.6 * cm, 1.15 * cm, footer_text)


def draw_pdf_section_title(c, title, x, y):
    c.setFillColorRGB(0.08, 0.16, 0.30)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x, y, title)

    c.setStrokeColorRGB(0.80, 0.86, 0.93)
    c.setLineWidth(0.03 * cm)
    c.line(x, y - 0.14 * cm, 19.0 * cm, y - 0.14 * cm)

    return y - 0.65 * cm


def draw_pdf_highlight_box(c, x, y, w, h, title, lines):
    c.saveState()
    c.setFillColorRGB(1, 1, 1)
    c.setStrokeColorRGB(0.82, 0.87, 0.94)
    c.setLineWidth(0.03 * cm)
    c.roundRect(x, y - h, w, h, 0.28 * cm, fill=1, stroke=1)
    c.restoreState()

    c.setFillColorRGB(0.08, 0.16, 0.30)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(x + 0.35 * cm, y - 0.45 * cm, title)

    c.setFillColorRGB(0.22, 0.26, 0.33)
    text_y = y - 0.95 * cm
    for line in lines:
        text_y = draw_pdf_multiline(
            c,
            line,
            x + 0.35 * cm,
            text_y,
            max_width=w - 0.7 * cm,
            line_height=0.42 * cm,
            font_name="Helvetica",
            font_size=8.8,
        )
        text_y -= 0.03 * cm

    return y - h


def pdf_prepare_page(c, subtitle="Relatório Estratégico", username=""):
    draw_pdf_background(c)
    draw_pdf_header(c, subtitle=subtitle)
    draw_pdf_footer(c, username=username)
    return A4[1] - 2.8 * cm


def pdf_new_page(c, subtitle="Continuação do relatório", username=""):
    c.showPage()
    return pdf_prepare_page(c, subtitle=subtitle, username=username)


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
        base["subject"] = base["subject"].where(base["subject"] != "", "Sem tema")
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


def build_situational_diagnosis(report: dict):
    qs = report["questions_summary"]
    ss = report["schedule_summary"]
    ms = report["mock_summary"]
    goal = report["goal"]
    topic_ranking = report["topic_ranking"]
    flashcards_df = report["flashcards_df"]

    strengths = []
    weaknesses = []
    suggestions = []

    daily_questions_goal = to_int(goal.get("daily_questions_goal", 60), 60)
    daily_minutes_goal = to_int(goal.get("daily_minutes_goal", 180), 180)
    monthly_mock_goal = to_int(goal.get("monthly_mock_goal", 4), 4)
    phase_name = normalize_text(goal.get("phase_name", "Amador")) or "Amador"

    due_today = 0
    if not flashcards_df.empty and "due_date" in flashcards_df.columns:
        due_today = int((flashcards_df["due_date"].fillna(date.today().isoformat()).astype(str) <= date.today().isoformat()).sum())

    execution_rate = round((ss["done"] / ss["total"]) * 100, 1) if ss["total"] > 0 else 0.0

    if qs["overall_accuracy"] >= 75:
        strengths.append(f"Acurácia geral consistente em {qs['overall_accuracy']}%.")
    elif qs["overall_accuracy"] >= 65:
        strengths.append(f"Acurácia geral razoável em {qs['overall_accuracy']}%, com espaço para refinamento.")
        weaknesses.append(f"Acurácia geral ainda abaixo do ideal competitivo em {qs['overall_accuracy']}%.")
    else:
        weaknesses.append(f"Acurácia geral baixa em {qs['overall_accuracy']}%, sugerindo falhas de consolidação.")

    if qs["today_questions"] >= daily_questions_goal:
        strengths.append(f"Meta diária de questões atingida com {qs['today_questions']} questões.")
    else:
        weaknesses.append(f"Meta diária de questões não atingida: {qs['today_questions']}/{daily_questions_goal}.")

    if qs["today_minutes"] >= daily_minutes_goal:
        strengths.append(f"Tempo diário de estudo compatível com a meta, totalizando {qs['today_minutes']} min.")
    else:
        weaknesses.append(f"Tempo diário de estudo abaixo da meta: {qs['today_minutes']}/{daily_minutes_goal} min.")

    if ms["count"] >= monthly_mock_goal:
        strengths.append(f"Meta mensal de simulados atingida com {ms['count']} registro(s).")
    else:
        weaknesses.append(f"Meta mensal de simulados abaixo do alvo: {ms['count']}/{monthly_mock_goal}.")

    if execution_rate >= 70:
        strengths.append(f"Boa execução do cronograma, com {execution_rate}% dos itens concluídos.")
    elif ss["total"] > 0:
        weaknesses.append(f"Execução do cronograma abaixo do ideal, com {execution_rate}% de conclusão.")
    else:
        weaknesses.append("Ainda não há itens cadastrados no cronograma para avaliação estratégica.")

    if ms["count"] >= 2:
        strengths.append(f"Boa exposição a simulados, com média de {ms['avg_score']}%.")
    else:
        weaknesses.append("Baixa exposição a simulados no período analisado.")

    if due_today > 0:
        weaknesses.append(f"Há {due_today} flashcards vencidos aguardando revisão.")

    if not topic_ranking.empty:
        best_topic = topic_ranking.iloc[0]
        worst_topic = topic_ranking.sort_values(["accuracy", "questions_done"], ascending=[True, False]).iloc[0]
        strengths.append(f"Melhor subtópico atual: {best_topic['topic_display']} ({best_topic['accuracy']}%).")
        weaknesses.append(f"Subtópico com maior necessidade de reforço: {worst_topic['topic_display']} ({worst_topic['accuracy']}%).")

    suggestions.append(f"Manter a rotina compatível com a etapa atual: {phase_name}.")
    suggestions.append("Priorizar revisão dos subtópicos de pior acurácia antes de ampliar carga de conteúdo novo.")
    suggestions.append("Transformar erros recorrentes em flashcards com resposta curta e nota explicativa objetiva.")
    suggestions.append("Usar simulados seriados para recalibrar prioridades semanais.")
    suggestions.append("Reservar bloco diário para revisão de flashcards vencidos antes de novas adições.")

    return {
        "strengths": strengths if strengths else ["Ainda não há dados suficientes para identificar pontos fortes com segurança."],
        "weaknesses": weaknesses if weaknesses else ["Ainda não há dados suficientes para identificar pontos fracos com segurança."],
        "suggestions": suggestions,
    }


def generate_pdf_report(report: dict, diagnosis: dict, username: str = ""):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    draw_pdf_background(c)
    draw_pdf_header(c, subtitle="Diagnóstico situacional premium")
    draw_pdf_footer(c, username=username)

    y = height - 2.8 * cm

    qs = report["questions_summary"]
    ss = report["schedule_summary"]
    ms = report["mock_summary"]
    goal = report["goal"]
    topic_ranking = report["topic_ranking"]

    c.setFillColorRGB(0.08, 0.16, 0.30)
    c.setFont("Helvetica-Bold", 17)
    c.drawString(2.0 * cm, y, "Relatório Diagnóstico Situacional")
    y -= 0.70 * cm

    c.setFillColorRGB(0.36, 0.42, 0.52)
    c.setFont("Helvetica", 9.5)
    c.drawString(2.0 * cm, y, "Leitura estratégica consolidada da rotina, desempenho e retenção.")
    y -= 0.95 * cm

    box_y = y
    draw_pdf_highlight_box(
        c, 2.0 * cm, box_y, 5.4 * cm, 2.6 * cm, "Etapa",
        [
            f"{goal.get('phase_name', 'Amador')}",
            f"Questões/dia: {to_int(goal.get('daily_questions_goal', 60), 60)}",
        ]
    )
    draw_pdf_highlight_box(
        c, 7.9 * cm, box_y, 5.4 * cm, 2.6 * cm, "Tempo e rotina",
        [
            f"Tempo/dia: {to_int(goal.get('daily_minutes_goal', 180), 180)} min",
            f"Flashcards: {len(report['flashcards_df'])}",
        ]
    )
    draw_pdf_highlight_box(
        c, 13.8 * cm, box_y, 5.2 * cm, 2.6 * cm, "Simulados",
        [
            f"Quantidade: {ms['count']}",
            f"Média: {ms['avg_score']}%",
        ]
    )

    y -= 3.15 * cm

    y = draw_pdf_section_title(c, "Resumo executivo", 2.0 * cm, y)
    summary_lines = [
        f"Questões totais: {qs['total_questions']} • Acertos: {qs['total_correct']} • Acurácia geral: {qs['overall_accuracy']}%.",
        f"Tempo total estudado: {qs['total_minutes']} minutos • Hoje: {qs['today_minutes']} minutos.",
        f"Cronograma: {ss['done']} itens concluídos de {ss['total']} planejados • {ss['pending']} pendentes.",
        f"Simulados: {ms['count']} registros • Média global: {ms['avg_score']}% • Melhor resultado: {ms['best_score']}%.",
    ]

    c.setFillColorRGB(0.18, 0.22, 0.28)
    for line in summary_lines:
        y = draw_pdf_multiline(
            c,
            line,
            2.0 * cm,
            y,
            max_width=17.0 * cm,
            line_height=0.50 * cm,
            font_name="Helvetica",
            font_size=10
        )
        y -= 0.04 * cm

    if y < 7.5 * cm:
        y = pdf_new_page(c, "Continuação do relatório", username=username)

    y -= 0.30 * cm
    y = draw_pdf_section_title(c, "Pontos fortes", 2.0 * cm, y)
    c.setFillColorRGB(0.16, 0.22, 0.28)
    for item in diagnosis["strengths"]:
        y = draw_pdf_multiline(c, f"• {item}", 2.1 * cm, y, max_width=16.8 * cm, line_height=0.48 * cm, font_name="Helvetica", font_size=10)
        y -= 0.03 * cm
        if y < 4.5 * cm:
            y = pdf_new_page(c, "Continuação do relatório", username=username)

    y -= 0.20 * cm
    y = draw_pdf_section_title(c, "Pontos fracos", 2.0 * cm, y)
    c.setFillColorRGB(0.16, 0.22, 0.28)
    for item in diagnosis["weaknesses"]:
        y = draw_pdf_multiline(c, f"• {item}", 2.1 * cm, y, max_width=16.8 * cm, line_height=0.48 * cm, font_name="Helvetica", font_size=10)
        y -= 0.03 * cm
        if y < 4.5 * cm:
            y = pdf_new_page(c, "Continuação do relatório", username=username)

    y -= 0.20 * cm
    y = draw_pdf_section_title(c, "Sugestões de melhoria", 2.0 * cm, y)
    c.setFillColorRGB(0.16, 0.22, 0.28)
    for item in diagnosis["suggestions"]:
        y = draw_pdf_multiline(c, f"• {item}", 2.1 * cm, y, max_width=16.8 * cm, line_height=0.48 * cm, font_name="Helvetica", font_size=10)
        y -= 0.03 * cm
        if y < 4.5 * cm:
            y = pdf_new_page(c, "Continuação do relatório", username=username)

    if not topic_ranking.empty:
        if y < 7.0 * cm:
            y = pdf_new_page(c, "Continuação do relatório", username=username)

        y -= 0.10 * cm
        y = draw_pdf_section_title(c, "Leitura dos subtópicos", 2.0 * cm, y)

        best_topic = topic_ranking.iloc[0]
        worst_topic = topic_ranking.sort_values(["accuracy", "questions_done"], ascending=[True, False]).iloc[0]

        topic_lines = [
            f"Melhor subtópico atual: {best_topic['topic_display']} com {best_topic['accuracy']}% de acurácia em {int(best_topic['questions_done'])} questões.",
            f"Subtópico de maior atenção: {worst_topic['topic_display']} com {worst_topic['accuracy']}% de acurácia em {int(worst_topic['questions_done'])} questões.",
        ]

        for line in topic_lines:
            y = draw_pdf_multiline(
                c,
                line,
                2.0 * cm,
                y,
                max_width=17.0 * cm,
                line_height=0.50 * cm,
                font_name="Helvetica",
                font_size=10
            )
            y -= 0.04 * cm

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


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


def render_reports_content(user_id: int, username_for_pdf: str = ""):
    report = build_report_data(user_id)
    diagnosis = build_situational_diagnosis(report)
    pdf_bytes = generate_pdf_report(report, diagnosis, username=username_for_pdf)

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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Resumo executivo</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Panorama consolidado da operação de estudo.</div>', unsafe_allow_html=True)

        for name, text in [
            ("Etapa atual", f"{goal.get('phase_name', 'Amador')}"),
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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Diagnóstico situacional</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Pontos fortes, fracos e sugestões automáticas.</div>', unsafe_allow_html=True)

        st.markdown("**Pontos fortes**")
        for item in diagnosis["strengths"]:
            st.markdown(f"- {item}")

        st.markdown("**Pontos fracos**")
        for item in diagnosis["weaknesses"]:
            st.markdown(f"- {item}")

        st.markdown("**Sugestões**")
        for item in diagnosis["suggestions"]:
            st.markdown(f"- {item}")

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    col3, col4 = st.columns([1, 1], gap="large")

    with col3:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Ranking por subtópico</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Ordenado por acurácia e volume de questões.</div>', unsafe_allow_html=True)

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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Resumo por tema</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Volume, acurácia e tempo estudado por tema.</div>', unsafe_allow_html=True)

        if subject_summary.empty:
            st.markdown('<div class="b5-empty">Ainda não há dados suficientes para resumo por tema.</div>', unsafe_allow_html=True)
        else:
            view_df = subject_summary.rename(columns={
                "subject": "Tema",
                "questions_done": "Questões",
                "correct_answers": "Acertos",
                "accuracy": "Acurácia (%)",
                "study_minutes": "Tempo (min)"
            })
            st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Simulados por grande área</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Acurácia consolidada por área.</div>', unsafe_allow_html=True)

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
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Exportações</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Baixe suas bases e o relatório situacional em PDF.</div>', unsafe_allow_html=True)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
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
    with c6:
        st.download_button(
            "Relatório PDF",
            data=pdf_bytes,
            file_name="relatorio_diagnostico_situacional.pdf",
            mime="application/pdf",
            use_container_width=True
        )

    st.markdown("</div>", unsafe_allow_html=True)


def render_reports_page():
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Relatórios</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Leitura gerencial com diagnóstico situacional premium e exportações.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_reports_content(
        user_id=st.session_state.user_id,
        username_for_pdf=st.session_state.get("username", "")
    )

def render_mock_premium_report_section():
    st.markdown("## 📄 Relatório Premium de Simulado")

    mock_names = fetch_available_mock_names()

    if not mock_names:
        st.info("Nenhum simulado disponível para gerar relatório.")
        return

    selected_mock = st.selectbox(
        "Selecione o simulado para gerar o PDF",
        options=mock_names,
        key="report_selected_mock"
    )

    df = fetch_mock_ranking_df(selected_mock)

    if df.empty:
        st.warning("Este simulado não possui dados para relatório.")
        return

    geral = build_mock_diagnostics(df)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Usuários analisados", geral["n_usuarios"])
    with c2:
        st.metric("Média", f"{geral['media']}%")
    with c3:
        st.metric("Melhor desempenho", f"{geral['melhor']}%")

    st.markdown("### Prévia do relatório")
    st.write(
        f"O relatório premium do simulado **{selected_mock}** incluirá ranking geral, "
        f"comparação entre usuários, distribuição por performance e diagnóstico individual."
    )

    preview_df = df[[
        "rank",
        "user_name",
        "correct_answers",
        "total_questions",
        "score_percent"
    ]].copy()

    preview_df.columns = [
        "Posição",
        "Usuário",
        "Acertos",
        "Total",
        "% Acerto"
    ]

    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    pdf_bytes = generate_mock_ranking_pdf(selected_mock, df)

    st.download_button(
        label="📥 Baixar PDF Premium",
        data=pdf_bytes,
        file_name=f"relatorio_premium_{selected_mock.replace(' ', '_').lower()}.pdf",
        mime="application/pdf",
        key=f"download_premium_pdf_{selected_mock}"
    )


def render_relatorios_page():
    st.markdown("# Relatórios")

    tab1, tab2 = st.tabs(["Relatórios Gerais", "PDF Premium Simulados"])

    with tab1:
        render_reports_page()

    with tab2:
        render_mock_premium_report_section()

# =========================================================
# ADMINISTRAÇÃO
# =========================================================
def build_admin_student_options(users_df: pd.DataFrame):
    if users_df.empty:
        return {}

    base_df = users_df.copy()
    base_df = base_df[base_df["is_admin"] == 0].copy()

    if base_df.empty:
        return {}

    option_map = {}
    for _, row in base_df.iterrows():
        label = f'{row["username"]} (ID {row["id"]})'
        option_map[label] = {
            "user_id": int(row["id"]),
            "username": str(row["username"]),
        }
    return option_map


def render_admin_selected_student_panel(selected_user_id: int, selected_username: str):
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="section-title">Painel do aluno: {html.escape(selected_username)}</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<div class="section-subtitle">Visão consolidada do aluno selecionado pelo administrador.</div>',
        unsafe_allow_html=True
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_dashboard_content_for_user(
        user_id=selected_user_id,
        allow_goal_edit=False
    )

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Relatórios do aluno</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Resumo executivo, diagnóstico e exportações do aluno selecionado.</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
    render_reports_content(
        user_id=selected_user_id,
        username_for_pdf=selected_username
    )


def render_admin_page():
    if not st.session_state.get("is_admin", False):
        st.warning("Você não tem permissão para acessar esta área.")
        return

    users_df = fetch_users_df()

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Administração</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Controle de usuários e visualização estratégica dos alunos.</div>', unsafe_allow_html=True)
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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Criar usuário</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Cadastre novos acessos para a plataforma.</div>', unsafe_allow_html=True)

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
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Usuários cadastrados</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Leitura rápida da base de acessos.</div>', unsafe_allow_html=True)

        if users_df.empty:
            st.markdown('<div class="b5-empty">Nenhum usuário cadastrado.</div>', unsafe_allow_html=True)
        else:
            view_df = users_df[["id", "username", "is_admin_label", "created_at"]].copy()
            view_df.columns = ["ID", "Usuário", "Administrador", "Criado em"]
            st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)

    option_map = build_admin_student_options(users_df)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Visualizar estatísticas de aluno</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Selecione um aluno para abrir visão geral, relatório e diagnóstico completos.</div>', unsafe_allow_html=True)

    if not option_map:
        st.markdown('<div class="b5-empty">Nenhum aluno disponível para visualização.</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    labels = list(option_map.keys())
    default_label = labels[0]

    if "admin_selected_student_label" not in st.session_state or st.session_state.admin_selected_student_label not in labels:
        st.session_state.admin_selected_student_label = default_label

    selected_label = st.selectbox(
        "Selecionar aluno",
        labels,
        index=labels.index(st.session_state.admin_selected_student_label),
        key="admin_selected_student_selector"
    )
    st.session_state.admin_selected_student_label = selected_label

    selected_info = option_map[selected_label]
    selected_user_id = selected_info["user_id"]
    selected_username = selected_info["username"]

    c1, c2 = st.columns([1, 1], gap="large")
    with c1:
        if st.button("Ver estatísticas do aluno", use_container_width=True, key="admin_open_student_stats"):
            st.session_state.admin_view_user_id = selected_user_id
            st.session_state.admin_view_username = selected_username
            safe_rerun()
    with c2:
        if st.button("Fechar visualização do aluno", use_container_width=True, key="admin_close_student_stats"):
            st.session_state.admin_view_user_id = None
            st.session_state.admin_view_username = None
            safe_rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    selected_view_user_id = st.session_state.get("admin_view_user_id")
    selected_view_username = st.session_state.get("admin_view_username")

    if selected_view_user_id:
        st.markdown('<div class="top-spacer-md"></div>', unsafe_allow_html=True)
        render_admin_selected_student_panel(
            selected_user_id=selected_view_user_id,
            selected_username=selected_view_username or "Aluno"
        )

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
        🩺 <b>Nexus Med</b> • Plataforma premium de acompanhamento • Mentoria do Jhon - Whatsapp (63)99937-2522
    </div>
    """)

# =========================================================
# COMPATIBILIDADE / UPGRADES DE SCHEMA
# =========================================================
def ensure_schema_upgrades():
    conn = get_conn()
    cur = conn.cursor()

    # -----------------------------------------------------
    # study_sessions
    # -----------------------------------------------------
    try:
        cur.execute("PRAGMA table_info(study_sessions)")
        session_cols = [row[1] for row in cur.fetchall()]
    except Exception:
        session_cols = []

    if "grande_area" not in session_cols:
        cur.execute("ALTER TABLE study_sessions ADD COLUMN grande_area TEXT DEFAULT ''")

    # -----------------------------------------------------
    # goals
    # -----------------------------------------------------
    try:
        cur.execute("PRAGMA table_info(goals)")
        goal_cols = [row[1] for row in cur.fetchall()]
    except Exception:
        goal_cols = []

    if "daily_flashcard_goal" not in goal_cols:
        cur.execute("ALTER TABLE goals ADD COLUMN daily_flashcard_goal INTEGER NOT NULL DEFAULT 100")

    if "phase_name" not in goal_cols:
        cur.execute("ALTER TABLE goals ADD COLUMN phase_name TEXT DEFAULT 'Amador'")

    if "created_at" not in goal_cols:
        cur.execute("ALTER TABLE goals ADD COLUMN created_at TEXT")

    if "updated_at" not in goal_cols:
        cur.execute("ALTER TABLE goals ADD COLUMN updated_at TEXT")

    # -----------------------------------------------------
    # flashcards
    # -----------------------------------------------------
    try:
        cur.execute("PRAGMA table_info(flashcards)")
        flashcard_cols = [row[1] for row in cur.fetchall()]
    except Exception:
        flashcard_cols = []

    flashcard_alters = {
        "due_date": "ALTER TABLE flashcards ADD COLUMN due_date TEXT",
        "last_reviewed": "ALTER TABLE flashcards ADD COLUMN last_reviewed TEXT",
        "review_count": "ALTER TABLE flashcards ADD COLUMN review_count INTEGER NOT NULL DEFAULT 0",
        "lapse_count": "ALTER TABLE flashcards ADD COLUMN lapse_count INTEGER NOT NULL DEFAULT 0",
        "ease_factor": "ALTER TABLE flashcards ADD COLUMN ease_factor REAL NOT NULL DEFAULT 2.5",
        "interval_days": "ALTER TABLE flashcards ADD COLUMN interval_days INTEGER NOT NULL DEFAULT 0",
        "card_state": "ALTER TABLE flashcards ADD COLUMN card_state TEXT NOT NULL DEFAULT 'new'",
        "card_type": "ALTER TABLE flashcards ADD COLUMN card_type TEXT NOT NULL DEFAULT 'basic'",
        "cloze_text": "ALTER TABLE flashcards ADD COLUMN cloze_text TEXT DEFAULT ''",
        "cloze_answer": "ALTER TABLE flashcards ADD COLUMN cloze_answer TEXT DEFAULT ''",
        "cloze_full_text": "ALTER TABLE flashcards ADD COLUMN cloze_full_text TEXT DEFAULT ''",
    }

    for col, ddl in flashcard_alters.items():
        if col not in flashcard_cols:
            cur.execute(ddl)

    # -----------------------------------------------------
    # mock_area_scores
    # -----------------------------------------------------
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS flashcard_review_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        flashcard_id INTEGER NOT NULL,
        reviewed_at TEXT NOT NULL,
        response_time_seconds REAL DEFAULT 0
    )
    """)

    # -----------------------------------------------------
    # question_review_status
    # -----------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS question_review_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        grande_area TEXT NOT NULL DEFAULT '',
        subject TEXT NOT NULL DEFAULT '',
        topic TEXT NOT NULL DEFAULT '',
        review_days INTEGER NOT NULL DEFAULT 0,
        completed_at TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()

# =========================================================
# CSS DASHBOARD
# =========================================================
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
            margin-bottom:10px;
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
# MAIN
# =========================================================
def render_simulados_page():
    st.markdown("# Simulados")

    tab1, tab2 = st.tabs(["Lançamentos / Histórico", "Ranking Multiusuário"])

    with tab1:
        render_mocks_page()

    with tab2:
        render_mock_multiuser_ranking()
def main():
    zerar_logs_flashcards()
    ensure_session_defaults()
    init_db()
    ensure_schema_upgrades()
    ensure_questions_review_schema()
    ensure_flashcards_extended_schema()

    inject_global_css()
    inject_dashboard_css()

    if not st.session_state.logged_in:
        render_login_screen()
        return

    current_menu = st.session_state.get("menu", "Visão Geral")
    is_flashcard_fullscreen = (
        current_menu == "Flashcards"
        and st.session_state.get("flashcard_fullscreen", False)
    )

    if not is_flashcard_fullscreen:
        render_app_header(
            username=st.session_state.username,
            is_admin=st.session_state.is_admin
        )
        render_top_menu()

    if current_menu == "Visão Geral":
       render_visao_geral()
    elif current_menu == "Cronograma":
       render_schedule_manager()
    elif current_menu == "Questões":
       render_questions_manager()
    elif current_menu == "Flashcards":
       render_flashcards_page()
    elif current_menu == "Simulados":
       render_simulados_page()
    elif current_menu == "Relatórios":
       render_relatorios_page()
    elif current_menu == "Ranking Simulados":
       render_mock_multiuser_ranking()
    elif current_menu == "Configurações":
       render_goal_settings_panel(
        user_id=st.session_state.user_id,
        title="Configurações de metas",
        subtitle="Ajuste sua etapa para atualizar automaticamente questões, flashcards, minutos e simulados."
    )
    elif current_menu == "Administração":
       render_admin_page()
    else:
       render_visao_geral()

    if not is_flashcard_fullscreen:
        render_footer_premium()

        st.markdown('<div class="top-spacer-lg"></div>', unsafe_allow_html=True)
        _, c2, _ = st.columns([1, 1, 1])
        with c2:
            if st.button("Encerrar sessão", use_container_width=True, key="main_logout_button"):
                reset_login_state()
                safe_rerun()


if __name__ == "__main__":
    main()
