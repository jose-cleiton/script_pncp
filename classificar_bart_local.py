#!/usr/bin/env python3
"""
Script para classificar licitações usando o modelo BART local treinado.

Uso:
    python classificar_bart_local.py --db <origem.db> --out <saida.db> [--model-dir DIR] [--batch-size N] [--limit M]

Observação: este arquivo foi exportado do notebook `classificar_bart_local.ipynb`.
"""
from __future__ import annotations

import argparse
import sqlite3
import json
from pathlib import Path
from typing import List

# importe componentes do seu pacote de modelo
import sys
import os
from pathlib import Path as _Path

# Garantir que o diretório do workspace esteja no sys.path para importar o pacote
# quando o script for executado de outro cwd. Espera-se que a estrutura seja:
# /Users/jose-cleiton/dev/<folders...>
_repo_root = _Path(__file__).resolve().parents[1]
# Add repo root so 'modelo_classificacao_relevancia' package can be imported
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
# Also add the package folder itself so absolute imports like 'app.xxx' inside
# the model package resolve (app/ is a top-level package inside that folder).
_model_pkg = _repo_root / "modelo_classificacao_relevancia"
if _model_pkg.exists() and str(_model_pkg) not in sys.path:
    sys.path.insert(0, str(_model_pkg))

from modelo_classificacao_relevancia.app.config.settings import AppSettings
from modelo_classificacao_relevancia.app.services.bart_model_service import (
    BartModelService,
)
from modelo_classificacao_relevancia.app.services.text_cleaner import TextCleaner
from modelo_classificacao_relevancia.app.services.rule_engine import RuleEngine
from modelo_classificacao_relevancia.app.services.decision_service import (
    DecisionService,
)
from modelo_classificacao_relevancia.app.services.classification_service import (
    ClassificationService,
)


def obter_objeto_compra(row: dict) -> str:
    # mesma prioridade: coluna direta -> dados_json.objetoCompra (ou variantes)
    if row.get("objeto_compra"):
        return row["objeto_compra"]
    # tente diferentes chaves no dados_json
    raw = row.get("dados_json") or row.get("dados") or ""
    if raw:
        try:
            d = json.loads(raw) if isinstance(raw, str) else raw
            return (
                d.get("objetoCompra")
                or d.get("objeto_compra")
                or d.get("Objeto")
                or d.get("objeto")
                or ""
            )
        except Exception:
            return ""
    return ""


def criar_tabela_saida(conn: sqlite3.Connection):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS contratacoes_filtradas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pncp_id TEXT UNIQUE,
            numero_controle TEXT,
            objeto_compra TEXT,
            prob_nao_relevante REAL,
            prob_relevante REAL,
            categoria TEXT,
            motivo TEXT,
            origem TEXT,
            criado_em TEXT DEFAULT (datetime('now'))
        )"""
    )
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="DB de origem")
    parser.add_argument("--out", required=True, help="DB de saída")
    parser.add_argument("--model-dir", default=None, help="Dir do modelo BART")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    # Configurações do app de modelo
    settings = AppSettings()
    if args.model_dir:
        # override simples
        settings = AppSettings(
            model=type(settings.model)(
                model_dir=args.model_dir, max_length=settings.model.max_length
            )
        )

    model_service = BartModelService(settings=settings.model)
    cleaner = TextCleaner()
    rule_engine = RuleEngine(cleaner=cleaner, settings=settings.rules)
    decision_service = DecisionService(thresholds=settings.thresholds)
    classifier = ClassificationService(
        cleaner=cleaner,
        rule_engine=rule_engine,
        model_service=model_service,
        decision_service=decision_service,
    )

    # Conexões DB
    conn_src = sqlite3.connect(args.db)
    conn_src.row_factory = sqlite3.Row
    conn_out = sqlite3.connect(args.out)
    criar_tabela_saida(conn_out)

    # Leitura: adapte o SELECT à sua tabela (contratacoes / contratacoes_filtradas)
    # Algumas bases podem não ter a coluna 'id' — evite ordenar por ela.
    sql = "SELECT * FROM contratacoes"
    if args.limit:
        sql = f"SELECT * FROM contratacoes LIMIT {int(args.limit)}"

    rows = list(conn_src.execute(sql).fetchall())
    texts: List[str] = []
    mapping: List[tuple] = []  # guarda (pncp_id, numero_controle, objeto_text)
    for r in rows:
        row = dict(r)
        objeto = obter_objeto_compra(row) or ""
        pncp = row.get("pncp_id") or row.get("numeroControlePNCP") or ""
        numero_controle = row.get("pncp_id") or pncp
        texts.append(objeto)
        mapping.append((pncp, numero_controle, objeto))

    if not texts:
        print("Nenhum registro encontrado para classificar.")
        return

    # Processa em batches
    batch_size = int(args.batch_size)
    total = len(texts)
    for i in range(0, total, batch_size):
        batch_texts = texts[i : i + batch_size]
        # classifier.classify aplica limpeza, regras e decision_service
        df = classifier.classify(batch_texts)
        # salvar resultados
        for j, row_res in df.iterrows():
            pncp_id = mapping[i + j][0]
            numero_controle = mapping[i + j][1]
            objeto_text = mapping[i + j][2]
            prob_nao = float(row_res.get("prob_nao_relevante", 0.0))
            prob_relev = float(row_res.get("prob_relevante", 0.0))
            categoria = row_res.get("classificacao_final", "")
            motivo = row_res.get("motivo_regra", "") or row_res.get("status_final", "")
            origem = row_res.get("origem_decisao", "")
            conn_out.execute(
                """INSERT OR REPLACE INTO contratacoes_filtradas
                (pncp_id, numero_controle, objeto_compra, prob_nao_relevante, prob_relevante, categoria, motivo, origem)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    pncp_id,
                    numero_controle,
                    objeto_text,
                    prob_nao,
                    prob_relev,
                    categoria,
                    motivo,
                    origem,
                ),
            )
        conn_out.commit()
        print(
            f"Batch {i // batch_size + 1} / {(total-1)//batch_size + 1} salvo ({min(i+batch_size,total)}/{total})."
        )

    conn_src.close()
    conn_out.close()
    print("Classificação concluída. DB salvo em:", args.out)


if __name__ == "__main__":
    main()
