#!/usr/bin/env python3
"""
scripts/extract_arquivos_links.py
Extrai links de arquivos (edital/anexo) do campo `dados_json` na tabela `contratacoes`
para um CSV com colunas: rowid,pncp_id,titulo,tipo,data,link

Uso:
    python3 scripts/extract_arquivos_links.py --db data/coleta/2026-03-14/pncp_data.db --out arquivos_links.csv --limit 0

--limit 0 significa sem limite (processa todos).
"""

import argparse
import csv
import json
import os
import sqlite3
import sys


def extract(db_path: str, out_path: str, limit: int = 0):
    if not os.path.exists(db_path):
        print(f"ERRO: DB não encontrado: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    sql = "SELECT rowid, dados_json FROM contratacoes"
    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"

    rows = cur.execute(sql).fetchall()
    print(f"Registros lidos: {len(rows)}")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rowid", "pncp_id", "titulo", "tipo", "data", "link"])

        total = 0
        for row in rows:
            rowid = row[0]
            try:
                data = json.loads(row[1])
            except Exception as e:
                # pular registro mal-formado
                continue

            pncp_id = data.get("numeroControlePNCP") or ""
            arquivos = data.get("Arquivos") or []
            # Alguns registros podem armazenar arquivos em outro campo; tentar detectar
            if not arquivos and isinstance(data.get("arquivos"), list):
                arquivos = data.get("arquivos")

            for a in arquivos:
                titulo = a.get("titulo") or a.get("nome") or ""
                tipo = (
                    a.get("tipo")
                    or a.get("tipoDocumentoNome")
                    or a.get("tipoDocumentoDescricao")
                    or ""
                )
                data_pub = a.get("data") or a.get("dataPublicacaoPncp") or ""
                link = a.get("link") or a.get("url") or a.get("uri") or ""

                writer.writerow([rowid, pncp_id, titulo, tipo, data_pub, link])
                total += 1

    conn.close()
    print(f"Exportado: {total} links para {out_path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--db",
        default="data/coleta/2026-03-14/pncp_data.db",
        help="Caminho para o arquivo SQLite",
    )
    p.add_argument("--out", default="arquivos_links.csv", help="Arquivo CSV de saída")
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Número máximo de registros a ler (0 = todos)",
    )
    args = p.parse_args()

    extract(args.db, args.out, args.limit)
