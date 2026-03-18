#!/usr/bin/env python3
"""
scripts/listar_arquivos_edital.py
==================================
Lista e opcionalmente baixa TODOS os arquivos de um edital do PNCP.

Dado um pncp_id (ex: "46523049000120-1-000021/2026"), consulta a API e
exibe a tabela de arquivos igual à aba "Arquivos" da página pública:
  Nome | Data/Hora de Inclusão | Tipo | Link de Download

Uso:
    # Listar arquivos de um pncp_id específico
    python3 scripts/listar_arquivos_edital.py --id "46523049000120-1-000021/2026"

    # Listar arquivos de todos os registros do banco (com Arquivos enriquecidos)
    python3 scripts/listar_arquivos_edital.py --db data/coleta/2026-03-14/pncp_data.db

    # Baixar os arquivos de um edital
    python3 scripts/listar_arquivos_edital.py --id "46523049000120-1-000021/2026" --baixar

    # Baixar arquivos de todos os editais do banco
    python3 scripts/listar_arquivos_edital.py --db data/coleta/2026-03-14/pncp_data.db --baixar --destino downloads/
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Parse do numeroControlePNCP
# ---------------------------------------------------------------------------


def _parse_id(pncp_id: str) -> tuple[str, str, str] | None:
    """
    Extrai (cnpj, seq, ano) de um numeroControlePNCP.
    Aceita:  "46523049000120-1-000021/2026"  ou  "46523049000120-1-21/2026"
    Retorna: ("46523049000120", "21", "2026")
    """
    m = re.match(r"(\d+)-1-(\d+)/(\d+)", pncp_id.strip())
    if not m:
        return None
    cnpj, seq_pad, ano = m.groups()
    return cnpj, str(int(seq_pad)), ano  # remove zero-padding do seq


# ---------------------------------------------------------------------------
# Busca via API
# ---------------------------------------------------------------------------


def buscar_arquivos_api(cnpj: str, ano: str, seq: str) -> list[dict]:
    """
    Consulta GET /pncp-api/v1/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos
    com paginação automática.
    Retorna lista de dicionários com os metadados de cada arquivo.
    """
    base = "https://pncp.gov.br/pncp-api/v1"
    todos: list[dict] = []
    pagina = 1
    tamanho = 100

    while True:
        url = (
            f"{base}/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos"
            f"?pagina={pagina}&tamanhoPagina={tamanho}"
        )
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
                if not raw.strip():
                    break
                data = json.loads(raw)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                break
            print(f"  ⚠️  HTTP {e.code} ao buscar arquivos de {cnpj}/{ano}/{seq}")
            break
        except Exception as e:
            print(f"  ⚠️  Erro: {e}")
            break

        # A API pode retornar lista direta ou dict com 'data'/'content'
        if isinstance(data, list):
            itens = data
        elif isinstance(data, dict):
            itens = data.get("data") or data.get("content") or data.get("items") or []
        else:
            itens = []

        if not itens:
            break

        todos.extend(itens)

        # Se retornou menos que o tamanho da página → última página
        if len(itens) < tamanho:
            break
        pagina += 1

    return todos


# ---------------------------------------------------------------------------
# Exibição em tabela
# ---------------------------------------------------------------------------


def _fmt_data(iso: str) -> str:
    """Converte '2026-03-03T11:10:35' → '03/03/2026 - 11:10:35'."""
    if not iso:
        return "—"
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(iso, fmt)
            if "T" in iso:
                return dt.strftime("%d/%m/%Y - %H:%M:%S")
            return dt.strftime("%d/%m/%Y")
        except ValueError:
            continue
    return iso


def exibir_tabela(pncp_id: str, arquivos: list[dict]) -> None:
    """Imprime tabela de arquivos no terminal."""
    print(f"\n{'─'*90}")
    print(f"  Edital: {pncp_id}")
    print(f"  Total de arquivos: {len(arquivos)}")
    print(f"{'─'*90}")
    print(f"  {'#':<4} {'Nome':<40} {'Data/Hora Inclusão':<22} {'Tipo':<28}")
    print(f"  {'─'*4} {'─'*40} {'─'*22} {'─'*28}")
    for i, a in enumerate(arquivos, 1):
        titulo = (a.get("titulo") or "—")[:39]
        data = _fmt_data(a.get("dataPublicacaoPncp") or a.get("data") or "")
        tipo = (
            a.get("tipoDocumentoNome")
            or a.get("tipoDocumentoDescricao")
            or a.get("tipo")
            or "—"
        )[:27]
        seq_doc = a.get("sequencialDocumento") or i
        link = a.get("url") or a.get("uri") or a.get("link") or ""
        print(f"  {seq_doc:<4} {titulo:<40} {data:<22} {tipo:<28}")
        if link:
            print(f"       🔗 {link}")
    print(f"{'─'*90}\n")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _sanitizar_nome(nome: str) -> str:
    """Remove caracteres inválidos para nome de arquivo."""
    return re.sub(r'[\\/:*?"<>|]', "_", nome).strip()


def baixar_arquivos(
    pncp_id: str,
    arquivos: list[dict],
    destino: str,
    delay: float = 0.5,
) -> None:
    """
    Baixa todos os arquivos do edital para a pasta:
        {destino}/{pncp_id_sanitizado}/

    O nome do arquivo é derivado de 'titulo' + extensão detectada pelo
    Content-Type ou Content-Disposition do servidor.
    """
    pasta_id = _sanitizar_nome(pncp_id)
    pasta = Path(destino) / pasta_id
    pasta.mkdir(parents=True, exist_ok=True)

    print(f"\n📥  Baixando {len(arquivos)} arquivo(s) para: {pasta}/")

    for i, a in enumerate(arquivos, 1):
        seq_doc = a.get("sequencialDocumento") or i
        titulo = _sanitizar_nome(a.get("titulo") or f"arquivo_{seq_doc}")
        tipo = a.get("tipoDocumentoNome") or a.get("tipoDocumentoDescricao") or ""
        link = a.get("url") or a.get("uri") or a.get("link") or ""

        if not link:
            print(f"  [{i}/{len(arquivos)}] ⚠️  Sem link para '{titulo}' — pulando")
            continue

        print(f"  [{i}/{len(arquivos)}] {titulo} ({tipo})")

        req = urllib.request.Request(link, headers={"Accept": "*/*"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                # Detectar extensão pelo Content-Type
                ct = resp.headers.get("Content-Type", "")
                ext = _extensao_por_content_type(ct)

                # Tentar Content-Disposition para nome/extensão real
                cd = resp.headers.get("Content-Disposition", "")
                nome_cd = _nome_do_content_disposition(cd)
                if nome_cd:
                    nome_arquivo = pasta / nome_cd
                else:
                    nome_arquivo = pasta / f"{titulo}{ext}"

                conteudo = resp.read()

            nome_arquivo.write_bytes(conteudo)
            tamanho_kb = len(conteudo) / 1024
            print(f"       ✅  {nome_arquivo.name}  ({tamanho_kb:.1f} KB)")

        except urllib.error.HTTPError as e:
            print(f"       ❌  HTTP {e.code}: {link}")
        except Exception as e:
            print(f"       ❌  Erro: {e}")

        if delay > 0:
            time.sleep(delay)

    print(f"\n✅  Download concluído: {pasta}/\n")


def _extensao_por_content_type(ct: str) -> str:
    ct = ct.lower().split(";")[0].strip()
    mapa = {
        "application/pdf": ".pdf",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/zip": ".zip",
        "text/plain": ".txt",
        "text/html": ".html",
        "image/jpeg": ".jpg",
        "image/png": ".png",
    }
    return mapa.get(ct, ".bin")


def _nome_do_content_disposition(cd: str) -> str | None:
    """Extrai nome do arquivo do header Content-Disposition, se presente."""
    if not cd:
        return None
    # filename="nome.pdf" ou filename*=UTF-8''nome.pdf
    m = re.search(r'filename\*?=["\']?(?:UTF-8\'\')?([^"\';\r\n]+)', cd, re.IGNORECASE)
    if m:
        nome = m.group(1).strip().strip("\"'")
        if nome:
            return _sanitizar_nome(nome)
    return None


# ---------------------------------------------------------------------------
# Processamento a partir do banco SQLite
# ---------------------------------------------------------------------------


def processar_banco(
    db_path: str,
    baixar: bool,
    destino: str,
    limite: int,
    delay: float,
) -> None:
    """Lê pncp_ids do banco e busca arquivos via API para cada um."""
    conn = sqlite3.connect(db_path)
    sql = "SELECT rowid, dados_json FROM contratacoes ORDER BY rowid"
    if limite > 0:
        sql += f" LIMIT {limite}"
    rows = conn.execute(sql).fetchall()
    conn.close()

    print(f"📂  Banco: {db_path}  ({len(rows)} registros)")

    for rowid, dados_json_str in rows:
        try:
            dados = json.loads(dados_json_str)
        except Exception:
            continue

        pncp_id = dados.get("numeroControlePNCP") or ""
        if not pncp_id:
            continue

        parsed = _parse_id(pncp_id)
        if not parsed:
            print(f"  ⚠️  [{rowid}] ID inválido: {pncp_id}")
            continue

        cnpj, seq, ano = parsed
        arquivos = buscar_arquivos_api(cnpj, ano, seq)
        exibir_tabela(pncp_id, arquivos)

        if baixar and arquivos:
            baixar_arquivos(pncp_id, arquivos, destino, delay)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser(
        description="Lista e baixa arquivos de editais do PNCP.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--id",
        metavar="PNCP_ID",
        help="numeroControlePNCP de um edital específico. Ex: '46523049000120-1-000021/2026'",
    )
    group.add_argument(
        "--db",
        metavar="CAMINHO.db",
        help="Banco SQLite — processa todos os registros.",
    )
    p.add_argument(
        "--baixar",
        action="store_true",
        default=False,
        help="Baixar os arquivos após listar.",
    )
    p.add_argument(
        "--destino",
        default="downloads/arquivos_editais",
        metavar="PASTA",
        help="Pasta de destino para os downloads. Padrão: downloads/arquivos_editais",
    )
    p.add_argument(
        "--limite",
        type=int,
        default=0,
        metavar="N",
        help="Processar apenas os N primeiros registros do banco (0 = todos).",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.5,
        metavar="SEG",
        help="Intervalo entre downloads em segundos. Padrão: 0.5",
    )
    args = p.parse_args()

    if args.id:
        parsed = _parse_id(args.id)
        if not parsed:
            print(f"ERRO: ID inválido: {args.id}")
            sys.exit(1)
        cnpj, seq, ano = parsed
        print(f"🔍  Buscando arquivos: CNPJ={cnpj}  ANO={ano}  SEQ={seq}")
        arquivos = buscar_arquivos_api(cnpj, ano, seq)
        exibir_tabela(args.id, arquivos)
        if args.baixar and arquivos:
            baixar_arquivos(args.id, arquivos, args.destino, args.delay)
        elif args.baixar and not arquivos:
            print("⚠️  Nenhum arquivo encontrado para baixar.")
    else:
        processar_banco(args.db, args.baixar, args.destino, args.limite, args.delay)


if __name__ == "__main__":
    main()
