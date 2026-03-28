"""
gerar_relatorio_pdf.py
======================
Gera um relatório PDF profissional em formato paisagem (A4) a partir do banco
SQLite de licitações filtradas pelo pipeline PNCP.

Dependências:
    pip install reportlab

Uso:
    python gerar_relatorio_pdf.py
    python gerar_relatorio_pdf.py --db data/resultado/2026-03-14/pncp_filtrado_gpt.db
    python gerar_relatorio_pdf.py --db <caminho.db> --saida relatorio.pdf --limite 50

Parâmetros:
    --db      Caminho do banco SQLite  (padrão: data/resultado/2026-03-14/pncp_filtrado_gpt.db)
    --saida   Nome do arquivo PDF de saída  (padrão: relatorio_licitacoes_pncp_paisagem.pdf)
    --limite  Número máximo de registros  (padrão: sem limite)
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
from typing import Any

# ---------------------------------------------------------------------------
# Dependência externa: reportlab
# ---------------------------------------------------------------------------
try:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm, mm
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import (
        HRFlowable,
        KeepTogether,
        PageBreak,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )
    from reportlab.platypus.flowables import BalancedColumns
except ImportError:
    print(
        "ERRO: reportlab não está instalado.\n"
        "Execute:  pip install reportlab\n"
        "ou:       pip install -r requirements.txt"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constantes visuais
# ---------------------------------------------------------------------------

# Paleta de cores
COR_PRIMARIA = colors.HexColor("#1A3A5C")  # azul escuro — cabeçalho
COR_SECUNDARIA = colors.HexColor("#2E6DA4")  # azul médio — subtítulo
COR_ACENTO = colors.HexColor("#E8F0F8")  # azul muito claro — fundo de célula
COR_SEPARADOR = colors.HexColor("#B0C4DE")  # azul aço — linhas
COR_LABEL = colors.HexColor("#4A6B8A")  # azul acinzentado — rótulos
COR_TEXTO = colors.HexColor("#1C1C1C")  # quase preto — texto normal
COR_LINK = colors.HexColor("#1155CC")  # azul link
COR_CATEGORIA = {
    "cracha": colors.HexColor("#2E7D32"),  # verde
    "ponto_eletronico": colors.HexColor("#1565C0"),  # azul
    "controle_acesso": colors.HexColor("#6A1B9A"),  # roxo
    "cftv": colors.HexColor("#E65100"),  # laranja
}
COR_CATEGORIA_BG = {
    "cracha": colors.HexColor("#E8F5E9"),
    "ponto_eletronico": colors.HexColor("#E3F2FD"),
    "controle_acesso": colors.HexColor("#F3E5F5"),
    "cftv": colors.HexColor("#FFF3E0"),
}

MARGEM = 1.5 * cm
PAGE_W, PAGE_H = landscape(A4)
LARGURA_UTIL = PAGE_W - 2 * MARGEM


# ---------------------------------------------------------------------------
# 1. Busca de arquivos via API PNCP (em tempo de geração do PDF)
# ---------------------------------------------------------------------------

_BASE_API = "https://pncp.gov.br/pncp-api/v1"
_RE_ID = re.compile(r"(\d+)-1-(\d+)/(\d+)")


def _parse_pncp_id(pncp_id: str) -> tuple[str, str, str] | None:
    """Extrai (cnpj, seq, ano) de um numeroControlePNCP."""
    m = _RE_ID.match(pncp_id.strip())
    if not m:
        return None
    cnpj, seq_pad, ano = m.groups()
    return cnpj, str(int(seq_pad)), ano


def _buscar_arquivos_api(pncp_id: str) -> list[dict]:
    """
    Consulta GET /pncp-api/v1/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos
    e retorna a lista de arquivos com paginação automática.
    Retorna lista vazia em caso de erro ou ausência de arquivos.
    """
    parsed = _parse_pncp_id(pncp_id)
    if not parsed:
        return []
    cnpj, seq, ano = parsed
    todos: list[dict] = []
    pagina = 1
    while True:
        url = (
            f"{_BASE_API}/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos"
            f"?pagina={pagina}&tamanhoPagina=100"
        )
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                if not raw.strip():
                    break
                data = json.loads(raw)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                print(f"  ⚠  HTTP {e.code} — arquivos de {pncp_id}")
            break
        except Exception:  # noqa: BLE001
            break

        itens: list[dict] = (
            data
            if isinstance(data, list)
            else data.get("data") or data.get("content") or []
        )
        if not itens:
            break
        todos.extend(itens)
        if len(itens) < 100:
            break
        pagina += 1
        time.sleep(0.2)
    return todos


def _buscar_itens_api(pncp_id: str) -> list[dict]:
    """
    Consulta GET /pncp-api/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens
    e retorna a lista de itens com paginação automática.
    Retorna lista vazia em caso de erro ou ausência de itens.
    """
    parsed = _parse_pncp_id(pncp_id)
    if not parsed:
        return []
    cnpj, seq, ano = parsed
    todos: list[dict] = []
    pagina = 1
    while True:
        url = (
            f"{_BASE_API}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
            f"?pagina={pagina}&tamanhoPagina=100"
        )
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                if not raw.strip():
                    break
                data = json.loads(raw)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                print(f"  ⚠  HTTP {e.code} — itens de {pncp_id}")
            break
        except Exception:  # noqa: BLE001
            break

        itens: list[dict] = (
            data
            if isinstance(data, list)
            else data.get("data") or data.get("content") or []
        )
        if not itens:
            break
        todos.extend(itens)
        if len(itens) < 100:
            break
        pagina += 1
        time.sleep(0.2)
    return todos


# ---------------------------------------------------------------------------
# 2. Conexão com o banco
# ---------------------------------------------------------------------------


def conectar(caminho_db: str) -> sqlite3.Connection:
    """Abre a conexão com o banco SQLite e retorna um objeto Connection."""
    if not os.path.exists(caminho_db):
        print(f"ERRO: Banco não encontrado: {caminho_db}")
        sys.exit(1)
    conn = sqlite3.connect(caminho_db)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# 2. Inspeção de schema
# ---------------------------------------------------------------------------


def inspecionar_schema(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """
    Retorna um dicionário {nome_tabela: [lista_de_colunas]} para todas as
    tabelas do banco (excluindo tabelas internas do SQLite).
    """
    tabelas = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()

    schema: dict[str, list[str]] = {}
    for t in tabelas:
        nome = t["name"]
        cols = conn.execute(f"PRAGMA table_info({nome})").fetchall()
        schema[nome] = [c["name"] for c in cols]

    return schema


def localizar_tabela_principal(schema: dict[str, list[str]]) -> tuple[str, str | None]:
    """
    Identifica a tabela que contém as licitações e a coluna JSON bruta.

    Estratégia:
      1. Procura tabela com 'contratac' ou 'licitac' no nome.
      2. Dentro da tabela encontrada, procura coluna com 'json' no nome.

    Retorna:
        (nome_tabela, nome_coluna_json | None)
    """
    candidatas = [
        t
        for t in schema
        if any(kw in t.lower() for kw in ("contratac", "licitac", "filtrad"))
    ]
    if not candidatas:
        # Fallback: usa a primeira tabela disponível
        candidatas = list(schema.keys())

    tabela = candidatas[0]
    colunas = schema[tabela]

    # Procura coluna JSON
    col_json = next(
        (c for c in colunas if "json" in c.lower()),
        None,
    )
    return tabela, col_json


# ---------------------------------------------------------------------------
# 3. Extração e normalização de dados
# ---------------------------------------------------------------------------


def _safe(obj: Any, *keys: str, default: str = "") -> str:
    """
    Navega por dicionários aninhados com segurança.

    Exemplo:
        _safe(d, "unidadeOrgao", "municipioNome")
    """
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    if cur is None or str(cur).strip().lower() in ("none", "null", ""):
        return default
    return str(cur).strip()


def _link_valido(url: str | None) -> str | None:
    """
    Retorna a URL se for válida (começa com http), caso contrário None.
    Rejeita textos como 'banco de dados', 'none', strings em branco.
    """
    if not url:
        return None
    url = str(url).strip()
    if url.lower() in ("none", "null", "", "banco de dados", "n/a"):
        return None
    if not url.lower().startswith("http"):
        return None
    return url


def montar_link_pncp(numero_controle: str) -> str | None:
    """
    Constrói a URL pública do edital no PNCP a partir do
    ``numeroControlePNCP``.

    Formato esperado: ``{CNPJ}-1-{SEQ}/{ANO}``
    Exemplo: ``46523049000120-1-000021/2026``
    → ``https://pncp.gov.br/editais/46523049000120/2026/21``
    """
    if not numero_controle:
        return None
    m = re.match(r"(\d+)-1-(\d+)/(\d+)", numero_controle.strip())
    if not m:
        return None
    cnpj, seq_pad, ano = m.groups()
    seq = str(int(seq_pad))  # remove zeros à esquerda
    return f"https://pncp.gov.br/editais/{cnpj}/{ano}/{seq}"


def extrair_registros(
    conn: sqlite3.Connection,
    tabela: str,
    col_json: str | None,
    limite: int | None,
    buscar_arquivos: bool = True,
    buscar_itens: bool = True,
) -> list[dict]:
    """
    Lê os registros da tabela, faz parse do JSON e normaliza os campos
    relevantes para o relatório.

    Por padrão consulta a API PNCP para obter arquivos e itens de cada
    edital. Passe ``buscar_arquivos=False`` ou ``buscar_itens=False``
    para desativar individualmente.

    Retorna uma lista de dicionários com as chaves padronizadas.
    """
    colunas_tabela = [c["name"] for c in conn.execute(f"PRAGMA table_info({tabela})").fetchall()]
    ordem = "id" if "id" in colunas_tabela else "numero_controle_pncp" if "numero_controle_pncp" in colunas_tabela else "rowid"
    sql = f"SELECT * FROM {tabela} ORDER BY {ordem}"
    if limite:
        sql += f" LIMIT {limite}"

    rows = conn.execute(sql).fetchall()
    total_rows = len(rows)
    registros: list[dict] = []

    for idx_row, row in enumerate(rows, start=1):
        # Converte row → dict simples
        base = dict(row)

        # Parse do JSON se disponível
        dados: dict = {}
        if col_json and base.get(col_json):
            try:
                dados = json.loads(base[col_json])
            except (json.JSONDecodeError, TypeError):
                dados = {}

        _pid = (
            _safe(dados, "numeroControlePNCP")
            or base.get("pncp_id", "")
            or f"#{idx_row}"
        )
        print(
            f"  [{idx_row}/{len(rows)}] {_pid}",
            end="",
            flush=True,
        )

        # -----------------------------------------------------------------
        # Normalização dos campos — prioridade: JSON > colunas do banco
        # -----------------------------------------------------------------
        orgao_nome = (
            _safe(dados, "orgaoEntidade", "razaoSocial")
            or base.get("orgao_nome", "")
            or _safe(dados, "unidadeOrgao", "nomeUnidade")
        )

        registro = {
            # Identificação
            "pncp_id": base.get("pncp_id") or base.get("numero_controle_pncp") or _safe(dados, "numeroControlePNCP"),
            "numero_controle": _safe(dados, "numeroControlePNCP")
            or base.get("pncp_id", "") or base.get("numero_controle_pncp", ""),
            "ano_compra": _safe(dados, "anoCompra"),
            "sequencial_compra": _safe(dados, "sequencialCompra"),
            "numero_compra": _safe(dados, "numeroCompra"),
            "processo": _safe(dados, "processo"),
            # Órgão
            "orgao_nome": orgao_nome,
            "cnpj": _safe(dados, "orgaoEntidade", "cnpj"),
            "uf_sigla": base.get("uf_sigla") or _safe(dados, "unidadeOrgao", "ufSigla"),
            "uf_nome": _safe(dados, "unidadeOrgao", "ufNome"),
            "municipio": _safe(dados, "unidadeOrgao", "municipioNome"),
            "unidade": _safe(dados, "unidadeOrgao", "nomeUnidade"),
            # Objeto
            "objeto_compra": (
                base.get("objeto_compra") or _safe(dados, "objetoCompra")
            ),
            # Modalidade / situação
            "modalidade": _safe(dados, "modalidadeNome"),
            "modo_disputa": _safe(dados, "modoDisputaNome"),
            "situacao": _safe(dados, "situacaoCompraNome") or base.get("situacao_nome"),
            "instrumento": _safe(dados, "tipoInstrumentoConvocatorioNome"),
            # Valores
            "valor_estimado": _safe(dados, "valorTotalEstimado") or base.get("valor_estimado"),
            "valor_homologado": _safe(dados, "valorTotalHomologado"),
            # Datas
            "data_abertura": _safe(dados, "dataAberturaProposta"),
            "data_encerramento": _safe(dados, "dataEncerramentoProposta"),
            "data_publicacao": _safe(dados, "dataPublicacaoPncp"),
            "data_atualizacao": _safe(dados, "dataAtualizacao"),
            "data_inclusao": _safe(dados, "dataInclusao"),
            # Links
            "link_sistema": _link_valido(_safe(dados, "linkSistemaOrigem")),
            "link_processo": _link_valido(_safe(dados, "linkProcessoEletronico")),
            "link_pncp": montar_link_pncp(
                _safe(dados, "numeroControlePNCP") or base.get("pncp_id", "")
            ),
            # Amparo legal
            "amparo_legal": _safe(dados, "amparoLegal", "nome"),
            # Usuário
            "usuario": _safe(dados, "usuarioNome"),
            # Classificação (campos do banco)
            "categoria": base.get("categoria") or "",
            "motivo": base.get("motivo") or "",
        }

        # ------------------------------------------------------------------
        # Arquivos — busca na API se solicitado
        # ------------------------------------------------------------------
        _pid_api = _safe(dados, "numeroControlePNCP") or base.get("pncp_id", "")
        if buscar_arquivos:
            print("  📎 arquivos...", end="", flush=True)
            registro["arquivos"] = dados.get("Arquivos") or _buscar_arquivos_api(
                _pid_api
            )
            n_arq = len(registro["arquivos"])
            print(f" {n_arq} arquivo(s)", end="", flush=True)
        else:
            registro["arquivos"] = dados.get("Arquivos") or []

        # ------------------------------------------------------------------
        # Itens — busca na API se solicitado
        # ------------------------------------------------------------------
        if buscar_itens:
            print("  📦 itens...", end="", flush=True)
            registro["itens"] = dados.get("Itens") or _buscar_itens_api(_pid_api)
            n_it = len(registro["itens"])
            print(f" {n_it} item(ns)", end="", flush=True)
        else:
            registro["itens"] = dados.get("Itens") or []

        print()  # quebra de linha após cada registro
        registros.append(registro)

    return registros


# ---------------------------------------------------------------------------
# 4. Formatadores
# ---------------------------------------------------------------------------


def fmt_data(iso: str) -> str:
    """
    Converte ISO 8601 (2026-03-02T08:00:00) para DD/MM/AAAA HH:MM.
    Retorna 'Não informado' se vazio ou inválido.
    """
    if not iso or iso.strip() in ("", "None", "null"):
        return "Não informado"
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(iso.strip(), fmt)
            if "T" in iso or fmt == "%Y-%m-%dT%H:%M:%S":
                return dt.strftime("%d/%m/%Y %H:%M")
            return dt.strftime("%d/%m/%Y")
        except ValueError:
            continue
    return iso  # retorna original se não conseguir parsear


def fmt_moeda(valor: str) -> str:
    """
    Converte '50142.0' → 'R$ 50.142,00'.
    Retorna 'Não informado' se vazio ou inválido.
    """
    if not valor or valor.strip() in ("", "None", "null"):
        return "Não informado"
    try:
        numero = float(valor)
        inteiro, decimal = f"{numero:,.2f}".split(".")
        inteiro_br = inteiro.replace(",", ".")
        return f"R$ {inteiro_br},{decimal}"
    except (ValueError, TypeError):
        return valor


def fmt_cnpj(cnpj: str) -> str:
    """Formata CNPJ: 00.000.000/0000-00."""
    if not cnpj:
        return "Não informado"
    c = "".join(filter(str.isdigit, cnpj))
    if len(c) == 14:
        return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"
    return cnpj


def nome_categoria(cat: str) -> str:
    """Converte o slug da categoria para nome legível."""
    mapa = {
        "cracha": "Crachá / Identificação",
        "ponto_eletronico": "Ponto Eletrônico",
        "controle_acesso": "Controle de Acesso",
        "cftv": "CFTV / Câmeras",
    }
    return mapa.get(cat.lower(), cat.title()) if cat else "—"


# ---------------------------------------------------------------------------
# 5. Estilos de parágrafo
# ---------------------------------------------------------------------------


def _criar_estilos() -> dict[str, ParagraphStyle]:
    """Retorna um dicionário com todos os estilos de parágrafo usados no PDF."""
    base = getSampleStyleSheet()["Normal"]

    def s(**kw) -> ParagraphStyle:
        return ParagraphStyle("_", parent=base, **kw)

    return {
        "titulo": ParagraphStyle(
            "titulo",
            fontName="Helvetica-Bold",
            fontSize=20,
            textColor=COR_PRIMARIA,
            alignment=TA_CENTER,
            spaceAfter=4,
        ),
        "subtitulo": ParagraphStyle(
            "subtitulo",
            fontName="Helvetica",
            fontSize=11,
            textColor=COR_SECUNDARIA,
            alignment=TA_CENTER,
            spaceAfter=2,
        ),
        "geracao": ParagraphStyle(
            "geracao",
            fontName="Helvetica",
            fontSize=8,
            textColor=colors.grey,
            alignment=TA_CENTER,
            spaceAfter=8,
        ),
        "indice": ParagraphStyle(
            "indice",
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=colors.white,
        ),
        "num_controle": ParagraphStyle(
            "num_controle",
            fontName="Helvetica",
            fontSize=9,
            textColor=colors.white,
        ),
        "label": ParagraphStyle(
            "label",
            fontName="Helvetica-Bold",
            fontSize=7.5,
            textColor=COR_LABEL,
            spaceAfter=1,
        ),
        "valor": ParagraphStyle(
            "valor",
            fontName="Helvetica",
            fontSize=8.5,
            textColor=COR_TEXTO,
            spaceAfter=2,
            leading=11,
        ),
        "objeto": ParagraphStyle(
            "objeto",
            fontName="Helvetica",
            fontSize=9,
            textColor=COR_TEXTO,
            leading=13,
            spaceAfter=4,
        ),
        "motivo": ParagraphStyle(
            "motivo",
            fontName="Helvetica-Oblique",
            fontSize=8,
            textColor=COR_LABEL,
            leading=11,
        ),
        "link": ParagraphStyle(
            "link",
            fontName="Helvetica",
            fontSize=8,
            textColor=COR_LINK,
        ),
        "arquivo_nome": ParagraphStyle(
            "arquivo_nome",
            fontName="Helvetica",
            fontSize=8,
            textColor=COR_LINK,
            leading=11,
        ),
        "arquivo_tipo": ParagraphStyle(
            "arquivo_tipo",
            fontName="Helvetica",
            fontSize=7.5,
            textColor=COR_TEXTO,
            leading=10,
        ),
        "arquivo_data": ParagraphStyle(
            "arquivo_data",
            fontName="Helvetica",
            fontSize=7.5,
            textColor=COR_LABEL,
            leading=10,
        ),
        "arquivo_cab": ParagraphStyle(
            "arquivo_cab",
            fontName="Helvetica-Bold",
            fontSize=7,
            textColor=colors.white,
        ),
        "nao_inf": ParagraphStyle(
            "nao_inf",
            fontName="Helvetica-Oblique",
            fontSize=8,
            textColor=colors.grey,
        ),
        "rodape": ParagraphStyle(
            "rodape",
            fontName="Helvetica",
            fontSize=7,
            textColor=colors.grey,
            alignment=TA_CENTER,
        ),
    }


# ---------------------------------------------------------------------------
# 6. Construção dos blocos de cada licitação
# ---------------------------------------------------------------------------


def _par(texto: str, estilo: ParagraphStyle) -> Paragraph:
    """Cria um Paragraph escapando caracteres XML problemáticos."""
    # Escapa apenas os caracteres que quebram o XML do ReportLab,
    # mas preserva as tags <a href=...> que inserimos explicitamente.
    seguro = (
        texto.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        # ReportLab aceita aspas duplas sem escape, mas por segurança:
        .replace('"', "&quot;")
    )
    return Paragraph(seguro, estilo)


def _par_link(url: str, texto: str, estilo: ParagraphStyle) -> Paragraph:
    """Cria um Paragraph com link clicável."""
    markup = f'<a href="{url}" color="#{COR_LINK.hexval()[2:]}">{texto[:80]}{"…" if len(texto) > 80 else ""}</a>'
    return Paragraph(markup, estilo)


def _celula(label: str, valor: str, estilos: dict) -> list:
    """Retorna [Paragraph(label), Paragraph(valor)] para uso em tabelas."""
    return [
        Paragraph(label, estilos["label"]),
        Paragraph(valor or "Não informado", estilos["valor"]),
    ]


def _linha_divisoria() -> HRFlowable:
    return HRFlowable(
        width="100%",
        thickness=0.5,
        color=COR_SEPARADOR,
        spaceAfter=4,
        spaceBefore=4,
    )


def _status_prazo(data_encerramento: str) -> tuple[str, object, object]:
    """
    Calcula o texto, cor de fundo e cor de texto do badge de prazo
    com base na data de encerramento da proposta.

    Retorna: (texto, cor_fundo, cor_texto)
    """
    if not data_encerramento or data_encerramento.strip() in ("", "None", "null"):
        return "", None, None

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(data_encerramento.strip(), fmt)
            break
        except ValueError:
            continue
    else:
        return "", None, None

    hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    enc = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    diff = (enc - hoje).days  # positivo = futuro, negativo = passado

    # Ainda aberto (aceita propostas)
    if diff > 3:
        txt = f"Vence em {diff} dias"
        bg = colors.HexColor("#1B5E20")  # verde escuro
        fg = colors.white
    elif diff == 3:
        txt = f"Vence em {diff} dias"
        bg = colors.HexColor("#2E7D32")
        fg = colors.white
    elif diff == 2:
        txt = "Vence depois de amanhã"
        bg = colors.HexColor("#F57F17")  # amarelo escuro
        fg = colors.white
    elif diff == 1:
        txt = "Vence amanhã"
        bg = colors.HexColor("#E65100")  # laranja
        fg = colors.white
    elif diff == 0:
        txt = "Vence hoje"
        bg = colors.HexColor("#B71C1C")  # vermelho
        fg = colors.white
    # Encerrado (não aceita mais propostas)
    elif diff == -1:
        txt = "Venceu ontem"
        bg = colors.HexColor("#4A148C")  # roxo escuro
        fg = colors.white
    elif diff == -2:
        txt = "Venceu antes de ontem"
        bg = colors.HexColor("#37474F")  # cinza azulado
        fg = colors.white
    else:
        txt = f"Vencido há {abs(diff)} dias"
        bg = colors.HexColor("#212121")  # quase preto
        fg = colors.HexColor("#BDBDBD")  # cinza claro

    return txt, bg, fg


def construir_bloco_licitacao(
    reg: dict,
    indice: int,
    estilos: dict,
) -> list:
    """
    Constrói a lista de flowables (blocos) que representam uma licitação no PDF.
    Cada licitação é envolta em KeepTogether quando possível.
    """
    largura = LARGURA_UTIL
    cat = reg["categoria"].lower() if reg.get("categoria") else ""
    cor_cat = COR_CATEGORIA.get(cat, COR_PRIMARIA)
    cor_cat_bg = COR_CATEGORIA_BG.get(cat, COR_ACENTO)

    # -----------------------------------------------------------------------
    # Faixa de cabeçalho do card (índice + número de controle + categoria)
    # -----------------------------------------------------------------------

    # Badge de prazo — calculado a partir da data de encerramento
    prazo_txt, prazo_bg, prazo_fg = _status_prazo(reg.get("data_encerramento", ""))

    cab_esq = [
        Paragraph(f"#{indice}", estilos["indice"]),
        Paragraph(reg["numero_controle"] or reg["pncp_id"], estilos["num_controle"]),
    ]

    # Coluna direita: categoria (topo) + badge de prazo (baixo)
    _estilo_cat = ParagraphStyle(
        "_cat",
        fontName="Helvetica-Bold",
        fontSize=8,
        textColor=colors.white,
        alignment=TA_RIGHT,
    )
    cab_dir = [Paragraph(nome_categoria(cat), _estilo_cat)]
    if prazo_txt:
        cab_dir.append(
            Paragraph(
                prazo_txt,
                ParagraphStyle(
                    "_prazo",
                    fontName="Helvetica-Bold",
                    fontSize=8,
                    textColor=prazo_fg,
                    alignment=TA_RIGHT,
                    spaceBefore=3,
                ),
            )
        )

    tabela_cab = Table(
        [[cab_esq, cab_dir]],
        colWidths=[largura * 0.65, largura * 0.35],
    )

    # Estilo base do cabeçalho; se houver badge de prazo, a célula direita
    # recebe cor de fundo diferenciada para chamar atenção.
    cab_style = [
        ("BACKGROUND", (0, 0), (0, -1), cor_cat),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (0, -1), 8),
        ("RIGHTPADDING", (-1, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("ROUNDEDCORNERS", [4, 4, 0, 0]),
    ]
    if prazo_bg:
        cab_style.append(("BACKGROUND", (1, 0), (1, -1), prazo_bg))
    else:
        cab_style.append(("BACKGROUND", (1, 0), (1, -1), cor_cat))

    tabela_cab.setStyle(TableStyle(cab_style))

    # -----------------------------------------------------------------------
    # Linha 1: Órgão | CNPJ | UF / Município | Processo | Número da Compra
    # -----------------------------------------------------------------------
    w = largura / 5
    linha1 = Table(
        [
            [
                _celula(
                    "Órgão / Entidade", reg["orgao_nome"] or reg["unidade"], estilos
                ),
                _celula("CNPJ", fmt_cnpj(reg["cnpj"]), estilos),
                _celula(
                    "UF / Município", f"{reg['uf_sigla']} — {reg['municipio']}", estilos
                ),
                _celula("Processo", reg["processo"], estilos),
                _celula("Nº da Compra", reg["numero_compra"], estilos),
            ]
        ],
        colWidths=[w * 1.8, w * 1.0, w * 0.9, w * 0.7, w * 0.6],
    )
    linha1.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), cor_cat_bg),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LINEAFTER", (0, 0), (-2, -1), 0.5, COR_SEPARADOR),
            ]
        )
    )

    # -----------------------------------------------------------------------
    # Linha 2: Modalidade | Modo de disputa | Situação | Instrumento | Amparo
    # -----------------------------------------------------------------------
    linha2 = Table(
        [
            [
                _celula("Modalidade", reg["modalidade"], estilos),
                _celula("Modo de Disputa", reg["modo_disputa"], estilos),
                _celula("Situação", reg["situacao"], estilos),
                _celula("Instrumento Convocatório", reg["instrumento"], estilos),
                _celula("Amparo Legal", reg["amparo_legal"], estilos),
            ]
        ],
        colWidths=[w * 1.0, w * 0.8, w * 1.0, w * 1.0, w * 1.2],
    )
    linha2.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LINEAFTER", (0, 0), (-2, -1), 0.5, COR_SEPARADOR),
            ]
        )
    )

    # -----------------------------------------------------------------------
    # Linha 3: Valores | Período de proposta | Publicação | Usuário
    # -----------------------------------------------------------------------
    periodo = (
        f"de {fmt_data(reg['data_abertura'])} "
        f"até {fmt_data(reg['data_encerramento'])}"
    )
    linha3 = Table(
        [
            [
                _celula(
                    "Valor Total Estimado", fmt_moeda(reg["valor_estimado"]), estilos
                ),
                _celula(
                    "Valor Total Homologado",
                    fmt_moeda(reg["valor_homologado"]),
                    estilos,
                ),
                _celula("Período da Proposta", periodo, estilos),
                _celula(
                    "Publicação no PNCP", fmt_data(reg["data_publicacao"]), estilos
                ),
                _celula("Cadastrado por", reg["usuario"], estilos),
            ]
        ],
        colWidths=[w * 0.9, w * 0.9, w * 1.4, w * 0.9, w * 0.9],
    )
    linha3.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), cor_cat_bg),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LINEAFTER", (0, 0), (-2, -1), 0.5, COR_SEPARADOR),
            ]
        )
    )

    # -----------------------------------------------------------------------
    # Descrição do objeto (caixa larga, text-wrap)
    # -----------------------------------------------------------------------
    objeto_texto = reg.get("objeto_compra") or "Não informado"
    tabela_objeto = Table(
        [
            [
                Paragraph("DESCRIÇÃO DO OBJETO", estilos["label"]),
                Paragraph(objeto_texto, estilos["objeto"]),
            ]
        ],
        colWidths=[largura * 0.13, largura * 0.87],
    )
    tabela_objeto.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LINEAFTER", (0, 0), (0, -1), 0.5, COR_SEPARADOR),
            ]
        )
    )

    # -----------------------------------------------------------------------
    # Linha de links
    # -----------------------------------------------------------------------
    def _bloco_link(label: str, url: str | None) -> list:
        itens = [Paragraph(label, estilos["label"])]
        if url:
            itens.append(_par_link(url, url, estilos["link"]))
        else:
            itens.append(Paragraph("Não informado", estilos["nao_inf"]))
        return itens

    def _bloco_link_rotulo(label: str, url: str | None, rotulo: str) -> list:
        """Bloco com label + link clicável exibindo ``rotulo`` em vez da URL."""
        itens = [Paragraph(label, estilos["label"])]
        if url:
            itens.append(_par_link(url, rotulo, estilos["link"]))
        else:
            itens.append(Paragraph("Não informado", estilos["nao_inf"]))
        return itens

    tabela_links = Table(
        [
            [
                _bloco_link("Link do Sistema de Origem", reg["link_sistema"]),
                _bloco_link("Link do Processo Eletrônico", reg["link_processo"]),
                _bloco_link_rotulo(
                    "Ver Edital no PNCP",
                    reg.get("link_pncp"),
                    "🔗 Abrir no Portal PNCP",
                ),
                _celula("Motivo da Classificação", reg.get("motivo", ""), estilos),
            ]
        ],
        colWidths=[largura * 0.27, largura * 0.27, largura * 0.22, largura * 0.24],
    )
    tabela_links.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LINEAFTER", (0, 0), (-2, -1), 0.5, COR_SEPARADOR),
                # Borda inferior arredondada do card
                ("LINEBELOW", (0, -1), (-1, -1), 1.5, cor_cat),
            ]
        )
    )

    # -----------------------------------------------------------------------
    # Tabela de arquivos do edital
    # -----------------------------------------------------------------------
    arquivos = reg.get("arquivos") or []

    if arquivos:
        # Cabeçalho da seção
        cab_arq = Table(
            [
                [
                    Paragraph("#", estilos["arquivo_cab"]),
                    Paragraph("Tipo / Download", estilos["arquivo_cab"]),
                    Paragraph("Data/Hora de Inclusão", estilos["arquivo_cab"]),
                ]
            ],
            colWidths=[
                largura * 0.04,
                largura * 0.60,
                largura * 0.36,
            ],
        )
        cab_arq.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), COR_PRIMARIA),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )

        # Linhas de cada arquivo
        linhas_arq = []
        for idx, arq in enumerate(arquivos):
            seq = arq.get("sequencialDocumento") or (idx + 1)
            link_arq = arq.get("link") or arq.get("url") or arq.get("uri") or ""
            tipo_arq = (
                arq.get("tipoDocumentoNome")
                or arq.get("tipoDocumentoDescricao")
                or arq.get("tipo")
                or "—"
            )
            data_arq = arq.get("dataPublicacaoPncp") or arq.get("data") or ""
            data_fmt = fmt_data(data_arq).replace(" ", " - ", 1) if data_arq else "—"

            # Tipo como link clicável
            if link_arq:
                tipo_cell = Paragraph(
                    f'<a href="{link_arq}" '
                    f'color="#{COR_LINK.hexval()[2:]}">'
                    f"{tipo_arq}</a>",
                    estilos["arquivo_nome"],
                )
            else:
                tipo_cell = Paragraph(tipo_arq, estilos["arquivo_tipo"])

            bg = COR_ACENTO if idx % 2 == 0 else colors.white
            linhas_arq.append(
                (
                    [Paragraph(str(seq), estilos["arquivo_data"])],
                    [tipo_cell],
                    [Paragraph(data_fmt, estilos["arquivo_data"])],
                    bg,
                )
            )

        dados_arq = [[r[0], r[1], r[2]] for r in linhas_arq]
        corpo_arq = Table(
            dados_arq,
            colWidths=[
                largura * 0.04,
                largura * 0.60,
                largura * 0.36,
            ],
        )
        style_arq = [
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LINEBELOW", (0, 0), (-1, -1), 0.3, COR_SEPARADOR),
        ]
        for idx, row in enumerate(linhas_arq):
            style_arq.append(("BACKGROUND", (0, idx), (-1, idx), row[3]))
        corpo_arq.setStyle(TableStyle(style_arq))

        bloco_arquivos = KeepTogether(
            [
                Spacer(1, 4),
                cab_arq,
                corpo_arq,
            ]
        )
    else:
        bloco_arquivos = None

    # -----------------------------------------------------------------------
    # Agrupamento final do card
    # -----------------------------------------------------------------------
    from reportlab.platypus import Flowable  # noqa: PLC0415

    elementos: list[Flowable] = [
        tabela_cab,
        linha1,
        linha2,
        linha3,
        tabela_objeto,
        tabela_links,
    ]
    if bloco_arquivos:
        elementos.append(bloco_arquivos)
    else:
        bloco_arquivos = None

    # -----------------------------------------------------------------------
    # Tabela de ITENS do edital
    itens = reg.get("itens") or []

    if itens:
        cab_it = Table(
            [
                [
                    Paragraph("#", estilos["arquivo_cab"]),
                    Paragraph("Descrição do Item", estilos["arquivo_cab"]),
                    Paragraph("Quantidade", estilos["arquivo_cab"]),
                    Paragraph("Unidade", estilos["arquivo_cab"]),
                    Paragraph("Valor Unit.", estilos["arquivo_cab"]),
                    Paragraph("Valor Total", estilos["arquivo_cab"]),
                ]
            ],
            colWidths=[
                largura * 0.06,
                largura * 0.54,
                largura * 0.12,
                largura * 0.08,
                largura * 0.10,
                largura * 0.10,
            ],
        )
        cab_it.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), COR_PRIMARIA),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )

        linhas_it = []
        for idx, it in enumerate(itens):
            seq = it.get("sequencialItem") or it.get("sequencial") or (idx + 1)
            descricao = (
                it.get("descricao")
                or it.get("descricaoItem")
                or it.get("descricaoObjeto")
                or "—"
            )
            quantidade = it.get("quantidade") or it.get("qtde") or ""
            unidade = it.get("unidade") or it.get("unidadeMedida") or ""
            valor_unit = (
                it.get("valorUnitario")
                or it.get("valor_unitario")
                or it.get("valor")
                or ""
            )
            valor_total = it.get("valorTotal") or it.get("valor_total") or ""

            val_unit_fmt = fmt_moeda(str(valor_unit)) if valor_unit else "—"
            val_tot_fmt = fmt_moeda(str(valor_total)) if valor_total else "—"

            bg = COR_ACENTO if idx % 2 == 0 else colors.white
            linhas_it.append(
                [
                    Paragraph(str(seq), estilos["arquivo_data"]),
                    Paragraph(descricao, estilos["arquivo_tipo"]),
                    Paragraph(str(quantidade) or "—", estilos["arquivo_data"]),
                    Paragraph(unidade or "—", estilos["arquivo_data"]),
                    Paragraph(val_unit_fmt, estilos["arquivo_data"]),
                    Paragraph(val_tot_fmt, estilos["arquivo_data"]),
                    bg,
                ]
            )

        dados_it = [[r[0], r[1], r[2], r[3], r[4], r[5]] for r in linhas_it]
        corpo_it = Table(
            dados_it,
            colWidths=[
                largura * 0.06,
                largura * 0.54,
                largura * 0.12,
                largura * 0.08,
                largura * 0.10,
                largura * 0.10,
            ],
        )
        style_it = [
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LINEBELOW", (0, 0), (-1, -1), 0.3, COR_SEPARADOR),
        ]
        for idx, row in enumerate(linhas_it):
            style_it.append(("BACKGROUND", (0, idx), (-1, idx), row[6]))
        corpo_it.setStyle(TableStyle(style_it))

        bloco_itens = KeepTogether(
            [
                Spacer(1, 4),
                cab_it,
                corpo_it,
            ]
        )
    else:
        bloco_itens = None
    if bloco_itens:
        elementos.append(bloco_itens)
    elementos.append(Spacer(1, 12))

    return [KeepTogether(elementos)]


# ---------------------------------------------------------------------------
# 7. Cabeçalho e rodapé de cada página
# ---------------------------------------------------------------------------


class _NumeracaoPaginas:
    """Callback do ReportLab para desenhar cabeçalho e rodapé em cada página."""

    def __init__(self, total_registros: int, data_geracao: str) -> None:
        self._total = total_registros
        self._data = data_geracao

    def __call__(self, canvas, doc) -> None:
        canvas.saveState()

        page_w = doc.pagesize[0]
        page_h = doc.pagesize[1]

        # ----- Cabeçalho ---------------------------------------------------
        canvas.setFillColor(COR_PRIMARIA)
        canvas.rect(0, page_h - 22, page_w, 22, fill=True, stroke=False)

        canvas.setFont("Helvetica-Bold", 10)
        canvas.setFillColor(colors.white)
        canvas.drawString(MARGEM, page_h - 15, "Relatório de Licitações — PNCP")

        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            page_w - MARGEM,
            page_h - 15,
            f"Total de registros: {self._total}   |   Gerado em: {self._data}",
        )

        # ----- Rodapé -------------------------------------------------------
        canvas.setFillColor(COR_SEPARADOR)
        canvas.rect(0, 0, page_w, 14, fill=True, stroke=False)

        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(COR_PRIMARIA)
        canvas.drawCentredString(
            page_w / 2,
            4,
            f"Página {doc.page}  •  Pipeline PNCP  •  Dados obtidos via API PNCP / ComprasGov",
        )

        canvas.restoreState()


# ---------------------------------------------------------------------------
# 8. Geração do PDF
# ---------------------------------------------------------------------------


def gerar_pdf(
    registros: list[dict],
    caminho_saida: str,
) -> None:
    """
    Monta e salva o PDF com todos os registros.

    Args:
        registros:     Lista de dicionários normalizados.
        caminho_saida: Caminho do arquivo .pdf a ser criado.
    """
    os.makedirs(os.path.dirname(os.path.abspath(caminho_saida)), exist_ok=True)

    estilos = _criar_estilos()
    data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
    total = len(registros)

    numeracao = _NumeracaoPaginas(total, data_geracao)

    doc = SimpleDocTemplate(
        caminho_saida,
        pagesize=landscape(A4),
        leftMargin=MARGEM,
        rightMargin=MARGEM,
        topMargin=MARGEM + 22,  # espaço para o cabeçalho fixo
        bottomMargin=MARGEM + 14,  # espaço para o rodapé fixo
        title="Relatório de Licitações PNCP",
        author="Pipeline PNCP",
        subject="Licitações filtradas por relevância",
    )

    # ------------------------------------------------------------------
    # Página de capa / sumário
    # ------------------------------------------------------------------
    story: list = []

    story.append(Spacer(1, 1.2 * cm))
    story.append(Paragraph("Relatório de Licitações", estilos["titulo"]))
    story.append(
        Paragraph(
            "Portal Nacional de Contratações Públicas — PNCP", estilos["subtitulo"]
        )
    )
    story.append(
        Paragraph(
            f"Total de licitações filtradas: <b>{total}</b>   •   Gerado em: {data_geracao}",
            estilos["geracao"],
        )
    )
    story.append(_linha_divisoria())
    story.append(Spacer(1, 0.4 * cm))

    # Legenda de categorias
    legenda_dados = [
        [
            Paragraph(
                "Categorias identificadas",
                ParagraphStyle(
                    "_leg_tit",
                    fontName="Helvetica-Bold",
                    fontSize=8.5,
                    textColor=COR_PRIMARIA,
                ),
            ),
        ],
    ]
    for slug, nome in [
        ("ponto_eletronico", "Ponto Eletrônico"),
        ("controle_acesso", "Controle de Acesso"),
        ("cftv", "CFTV / Câmeras"),
        ("cracha", "Crachá / Identificação"),
    ]:
        legenda_dados.append(
            [
                Paragraph(
                    f"<font color='#{COR_CATEGORIA[slug].hexval()[2:]}'>■</font>  {nome}",
                    ParagraphStyle(
                        "_leg", fontName="Helvetica", fontSize=8, textColor=COR_TEXTO
                    ),
                )
            ]
        )

    tabela_legenda = Table(legenda_dados, colWidths=[LARGURA_UTIL / 4])
    tabela_legenda.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), COR_ACENTO),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("BOX", (0, 0), (-1, -1), 0.5, COR_SEPARADOR),
            ]
        )
    )
    story.append(tabela_legenda)
    story.append(Spacer(1, 0.6 * cm))
    story.append(_linha_divisoria())
    story.append(Spacer(1, 0.4 * cm))

    # ------------------------------------------------------------------
    # Blocos de licitações
    # ------------------------------------------------------------------
    for i, reg in enumerate(registros, start=1):
        story.extend(construir_bloco_licitacao(reg, i, estilos))

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------
    doc.build(
        story,
        onFirstPage=numeracao,
        onLaterPages=numeracao,
    )

    print(f"\n✅  PDF gerado com sucesso!")
    print(f"   Arquivo : {os.path.abspath(caminho_saida)}")
    print(f"   Registros: {total}")


# ---------------------------------------------------------------------------
# 9. CLI + main
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera relatório PDF de licitações do banco PNCP filtrado.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db",
        default="data/resultado/2026-03-14/pncp_filtrado_gpt.db",
        metavar="CAMINHO",
        help="Caminho do banco SQLite. Padrão: data/resultado/2026-03-14/pncp_filtrado_gpt.db",
    )
    parser.add_argument(
        "--saida",
        default="relatorio_licitacoes_pncp_paisagem.pdf",
        metavar="ARQUIVO.pdf",
        help="Nome do arquivo PDF de saída. Padrão: relatorio_licitacoes_pncp_paisagem.pdf",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=None,
        metavar="N",
        help="Número máximo de registros a incluir no PDF. Padrão: todos.",
    )
    parser.add_argument(
        "--sem-arquivos",
        dest="arquivos",
        action="store_false",
        default=True,
        help="Não buscar arquivos dos editais na API PNCP.",
    )
    parser.add_argument(
        "--sem-itens",
        dest="itens",
        action="store_false",
        default=True,
        help="Não buscar itens dos editais na API PNCP.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    print(f"📂  Banco     : {args.db}")
    print(f"📄  Saída     : {args.saida}")
    print(f"🔢  Limite    : {args.limite or 'todos os registros'}")
    _arq_label = "sim" if args.arquivos else "não (desativado)"
    _it_label = "sim" if args.itens else "não (desativado)"
    print(f"📎  Arquivos  : {_arq_label}")
    print(f"📦  Itens     : {_it_label}")
    print()

    # 1. Conectar
    conn = conectar(args.db)

    # 2. Inspecionar schema
    schema = inspecionar_schema(conn)
    print("🗃️   Tabelas encontradas:", list(schema.keys()))

    # 3. Localizar tabela principal e coluna JSON
    tabela, col_json = localizar_tabela_principal(schema)
    print(f"✅  Tabela principal : {tabela}")
    print(f"✅  Coluna JSON      : {col_json or '(não encontrada)'}")

    # 4. Extrair e normalizar registros
    print("🌐  Consultando API PNCP por registro (pode demorar)...")
    registros = extrair_registros(
        conn,
        tabela,
        col_json,
        args.limite,
        buscar_arquivos=args.arquivos,
        buscar_itens=args.itens,
    )
    conn.close()
    print(f"📊  Registros lidos  : {len(registros)}")

    if not registros:
        print("⚠️   Nenhum registro encontrado. PDF não gerado.")
        return

    # 5. Gerar PDF
    print("📝  Gerando PDF...")
    gerar_pdf(registros, args.saida)


if __name__ == "__main__":
    main()
