"""
classificar_gpt.py
Etapa 5: classificação de contratações via LLM (OpenAI ou Gemini).

Responsabilidade deste módulo: ler a tabela `contratacoes` do banco SQLite
produzido pela Etapa 1, enviar cada `objeto_compra` para um LLM configurável,
receber uma classificação estruturada e persistir apenas os registros
relevantes num novo banco SQLite.

Arquitetura:
    ClassificadorBase        → interface abstrata (ABC) que todo classificador
                               deve implementar
    GptClassificador         → implementação via OpenAI Responses API
    GeminiClassificador      → implementação via Google Gemini API
    criar_classificador()    → factory que instancia o provedor correto
    BancoPncpReader          → lê registros do banco de origem
    BancoPncpFiltradoWriter  → cria o banco de destino e persiste os relevantes
    GptClassificacaoStage    → orquestra tudo com retomada de progresso e
                               tratamento de erros

Uso direto (sem CLI):
    from classificar_gpt import GptClassificacaoStage

    # Com OpenAI (padrão)
    stage = GptClassificacaoStage(
        banco_origem="data/coleta/2026-03-14/pncp_data.db",
        banco_destino="data/resultado/2026-03-14/pncp_filtrado_gpt.db",
        provedor="openai",          # ou "gemini"
    )
    stage.executar()

Via CLI integrada:
    python main.py --etapa classificar_gpt --data 2026-03-14
    python main.py --etapa classificar_gpt --data 2026-03-14 --provedor gemini
    python main.py --etapa classificar_gpt --data 2026-03-14 --provedor openai --modelo gpt-4o
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator

# Tornar import opcional para que quem use apenas Gemini não precise instalar
# o pacote `openai`. Se o import falhar, definimos um flag e valores
# substitutos; o erro real será lançado quando alguém tentar instanciar o
# classificador OpenAI.
try:
    from openai import OpenAI, APIError, RateLimitError, APITimeoutError

    _HAS_OPENAI = True
except Exception:  # pragma: no cover - ambiente sem openai
    OpenAI = None
    APIError = Exception
    RateLimitError = Exception
    APITimeoutError = Exception
    _HAS_OPENAI = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
Você é um classificador de licitações públicas brasileiras.

Dado o campo "objeto_compra" de uma contratação, determine se ela é RELEVANTE
para uma empresa que comercializa exclusivamente produtos das categorias abaixo:

CATEGORIAS RELEVANTES (relevante = 1):
  - cracha: crachá, porta-crachá, cordão de crachá, fita de crachá
  - ponto_eletronico: relógio de ponto, REP, bobina de ponto, software de ponto
  - controle_acesso: controle de acesso, catraca, biometria, leitor facial, reconhecimento facial
  - cftv: CFTV, videomonitoramento, câmera de segurança, DVR, NVR, alarme eletrônico

REGRA CRÍTICA DE EXCLUSÃO (relevante = 0):
  Palavras como "controle", "monitoramento", "câmera", "segurança", "digital",
  "eletrônico", "sistema" ou "software" sozinhas NÃO classificam como relevante.
  O objeto precisa mencionar especificamente um dos produtos acima.
  Em caso de dúvida, classifique como 0.

Retorne EXCLUSIVAMENTE um objeto JSON no seguinte formato (sem markdown, sem explicação):
{
  "relevante": 0,
  "categoria": "<uma das chaves acima ou null>",
  "motivo": "<frase curta explicando a decisão>"
}
"""

_CATEGORIAS_VALIDAS = frozenset(
    {"cracha", "ponto_eletronico", "controle_acesso", "cftv"}
)

_MODELO_OPENAI = (
    "gpt-4.1"  # equivalente production-ready; troque para "gpt-4o" se necessário
)
_MODELO_GEMINI = "gemini-2.0-flash"

_PROVEDORES_DISPONIVEIS = ("openai", "gemini")

_MAX_TENTATIVAS = 4
_PAUSA_RATE_LIMIT = 20.0  # segundos de espera ao receber 429


# ---------------------------------------------------------------------------
# Modelos de dados
# ---------------------------------------------------------------------------


@dataclass
class ContratacaoRow:
    """Representa um registro lido da tabela `contratacoes`."""

    pncp_id: str
    orgao_nome: str
    uf_sigla: str
    objeto_compra: str
    dados_json: str


@dataclass
class ClassificacaoResult:
    """Resultado produzido pelo classificador para um registro."""

    pncp_id: str
    orgao_nome: str
    uf_sigla: str
    objeto_compra: str
    dados_json: str
    relevante: int
    categoria: str | None
    motivo: str


# ---------------------------------------------------------------------------
# Interface abstrata — ClassificadorBase
# ---------------------------------------------------------------------------


class ClassificadorBase(ABC):
    """
    Contrato que todo classificador de LLM deve implementar.

    Ao adicionar um novo provedor, basta herdar desta classe e
    implementar `classificar()` — o restante do pipeline não muda.
    """

    @abstractmethod
    def classificar(self, registro: ContratacaoRow) -> ClassificacaoResult:
        """Classifica um registro e retorna o resultado estruturado."""
        ...

    # ------------------------------------------------------------------
    # Método utilitário compartilhado por todas as implementações
    # ------------------------------------------------------------------

    def _parsear_resposta(self, texto: str, pncp_id: str) -> dict:
        """
        Converte o texto retornado pelo modelo em dict validado.

        Se o JSON for inválido ou os campos estiverem ausentes, retorna
        um dict com relevante=0 para não bloquear o pipeline.
        """
        try:
            texto_limpo = texto.strip()
            if texto_limpo.startswith("```"):
                linhas = texto_limpo.splitlines()
                texto_limpo = "\n".join(
                    ln for ln in linhas if not ln.startswith("```")
                ).strip()

            dados = json.loads(texto_limpo)

            relevante = int(dados.get("relevante", 0))
            categoria = dados.get("categoria") or None
            if categoria not in _CATEGORIAS_VALIDAS:
                categoria = None

            return {
                "relevante": relevante,
                "categoria": categoria,
                "motivo": str(dados.get("motivo", "")),
            }

        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.error(
                "Falha ao parsear resposta pncp_id=%s: %s | %.200s",
                pncp_id,
                exc,
                texto,
            )
            return {
                "relevante": 0,
                "categoria": None,
                "motivo": f"Erro ao parsear resposta da API: {exc}",
            }


# ---------------------------------------------------------------------------
# Classe 1 — Leitura do banco de origem
# ---------------------------------------------------------------------------


class BancoPncpReader:
    """
    Lê registros da tabela `contratacoes` do banco SQLite de origem.

    Expõe um iterador paginado para processar volumes grandes sem carregar
    tudo na memória.
    """

    _SQL_TOTAL = "SELECT COUNT(*) FROM contratacoes"
    _SQL_PAGINA = """
        SELECT
            pncp_id,
            orgao_nome,
            uf_sigla,
            objeto_compra,
            dados_json
        FROM contratacoes
        ORDER BY rowid
        LIMIT ? OFFSET ?
    """

    def __init__(self, caminho_db: str, tamanho_pagina: int = 200) -> None:
        """
        Args:
            caminho_db:     Caminho para o arquivo .db de origem.
            tamanho_pagina: Quantos registros buscar por página.

        Raises:
            FileNotFoundError: se o banco de dados não existir.
        """
        if not os.path.exists(caminho_db):
            raise FileNotFoundError(
                f"Banco de origem não encontrado: {caminho_db}\n"
                "Execute primeiro: python main.py --etapa coletar"
            )
        self._caminho_db = caminho_db
        self._tamanho_pagina = tamanho_pagina
        # Detecta esquema da tabela `contratacoes` para suportar variantes
        # (alguns DBs armazenam colunas planas; outros apenas `dados_json`).
        self._usar_dados_json = False
        try:
            with sqlite3.connect(self._caminho_db) as conn:
                cols = [
                    r[1]
                    for r in conn.execute("PRAGMA table_info(contratacoes);").fetchall()
                ]
            # Se não existir coluna pncp_id ou objeto_compra, usaremos dados_json
            if not ("pncp_id" in cols and "objeto_compra" in cols):
                self._usar_dados_json = True
        except Exception:
            # Em caso de erro ao inspecionar (tabela inexistente etc.), deixamos
            # o comportamento padrão — o erro real será lançado mais adiante
            # ao tentar consultar a tabela.
            self._usar_dados_json = False

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def contar(self) -> int:
        """Retorna o total de registros na tabela."""
        with sqlite3.connect(self._caminho_db) as conn:
            return conn.execute(self._SQL_TOTAL).fetchone()[0]

    def iterar(self) -> Iterator[ContratacaoRow]:
        """Itera sobre todos os registros em páginas."""
        yield from self.iterar_faixa(offset=0, limite=None)

    def iterar_faixa(
        self, offset: int = 0, limite: int | None = None
    ) -> Iterator[ContratacaoRow]:
        """
        Itera sobre uma faixa específica de registros.

        Args:
            offset: Posição inicial (rowid order).
            limite: Quantidade máxima de registros. None = sem limite.
        """
        lidos = 0
        pos = offset
        with sqlite3.connect(self._caminho_db) as conn:
            conn.row_factory = sqlite3.Row
            while True:
                buscar = self._tamanho_pagina
                if limite is not None:
                    restam = limite - lidos
                    if restam <= 0:
                        break
                    buscar = min(buscar, restam)
                if not self._usar_dados_json:
                    # esquema esperado: colunas planas (pncp_id, objeto_compra, ...)
                    # Algumas bases podem não ter essas colunas (erro SQLite
                    # "no such column: pncp_id"). Fazemos uma tentativa e,
                    # em caso de OperationalError, ativamos o fallback para
                    # `dados_json` de forma resiliente.
                    try:
                        rows = conn.execute(self._SQL_PAGINA, (buscar, pos)).fetchall()
                    except sqlite3.OperationalError as exc:
                        # Detectamos esquema diferente em tempo de execução;
                        # ligamos o modo `dados_json` e reiniciamos a iteração
                        # nesta mesma posição.
                        logger.warning(
                            "Banco com esquema diferente: %s — usando dados_json fallback",
                            exc,
                        )
                        self._usar_dados_json = True
                        continue

                    if not rows:
                        break
                    for row in rows:
                        yield ContratacaoRow(
                            pncp_id=row["pncp_id"] or "",
                            orgao_nome=row["orgao_nome"] or "",
                            uf_sigla=row["uf_sigla"] or "",
                            objeto_compra=row["objeto_compra"] or "",
                            dados_json=row["dados_json"] or "{}",
                        )
                        lidos += 1
                else:
                    # esquema alternativo: apenas `dados_json` contém o objeto
                    rows = conn.execute(
                        "SELECT dados_json FROM contratacoes ORDER BY rowid LIMIT ? OFFSET ?",
                        (buscar, pos),
                    ).fetchall()
                    if not rows:
                        break
                    for row in rows:
                        dados_text = row[0] or "{}"
                        try:
                            obj = json.loads(dados_text)
                        except Exception:
                            obj = {}

                        # Mapeamento defensivo: nomes comuns no JSON
                        pncp_id = (
                            obj.get("numeroControlePNCP")
                            or obj.get("numero_controle_pncp")
                            or ""
                        )
                        orgao_nome = (
                            obj.get("orgaoNome")
                            or (obj.get("orgaoEntidade") or {}).get("razaoSocial")
                            or (obj.get("unidadeOrgao") or {}).get("nomeUnidade")
                            or obj.get("orgao_nome")
                            or ""
                        )
                        uf_sigla = (
                            obj.get("uf")
                            or (obj.get("unidadeOrgao") or {}).get("ufSigla")
                            or obj.get("ufSigla")
                            or obj.get("uf_sigla")
                            or ""
                        )
                        objeto_compra = (
                            obj.get("objetoCompra") or obj.get("objeto_compra") or ""
                        )

                        yield ContratacaoRow(
                            pncp_id=str(pncp_id),
                            orgao_nome=str(orgao_nome),
                            uf_sigla=str(uf_sigla),
                            objeto_compra=str(objeto_compra),
                            dados_json=dados_text,
                        )
                        lidos += 1
                pos += len(rows)


# ---------------------------------------------------------------------------
# Classe 2a — Classificação via OpenAI Responses API
# ---------------------------------------------------------------------------


class GptClassificador(ClassificadorBase):
    """
    Implementação de ClassificadorBase usando a OpenAI Responses API.

    Lê a chave da variável OPENAI_API_KEY se `api_key` não for informada.
    Implementa retentativas com back-off exponencial para RateLimitError,
    APITimeoutError e APIError genérico.
    """

    def __init__(
        self,
        api_key: str | None = None,
        modelo: str = _MODELO_OPENAI,
        max_tentativas: int = _MAX_TENTATIVAS,
        pausa_rate_limit: float = _PAUSA_RATE_LIMIT,
    ) -> None:
        chave = api_key or os.environ.get("OPENAI_API_KEY")
        if not chave:
            raise EnvironmentError(
                "OPENAI_API_KEY não definida.\n" "Defina: export OPENAI_API_KEY=sk-..."
            )
        if not _HAS_OPENAI:
            raise ImportError(
                "Pacote 'openai' não encontrado. Instale com: "
                "python -m pip install openai"
            )
        self._client = OpenAI(api_key=chave)
        self._modelo = modelo
        self._max_tentativas = max_tentativas
        self._pausa_rate_limit = pausa_rate_limit

    def classificar(self, registro: ContratacaoRow) -> ClassificacaoResult:
        resposta = self._chamar_api(registro.objeto_compra)
        dados = self._parsear_resposta(resposta, registro.pncp_id)
        return ClassificacaoResult(
            pncp_id=registro.pncp_id,
            orgao_nome=registro.orgao_nome,
            uf_sigla=registro.uf_sigla,
            objeto_compra=registro.objeto_compra,
            dados_json=registro.dados_json,
            relevante=dados["relevante"],
            categoria=dados.get("categoria"),
            motivo=dados.get("motivo", ""),
        )

    def _chamar_api(self, objeto_compra: str) -> str:
        for tentativa in range(1, self._max_tentativas + 1):
            try:
                response = self._client.responses.create(
                    model=self._modelo,
                    instructions=_SYSTEM_PROMPT,
                    input=objeto_compra,
                )
                return response.output_text
            except RateLimitError:
                logger.warning(
                    "[OpenAI] Rate limit. Aguardando %.0fs " "(tentativa %d/%d).",
                    self._pausa_rate_limit,
                    tentativa,
                    self._max_tentativas,
                )
                time.sleep(self._pausa_rate_limit)
            except APITimeoutError:
                espera = 2**tentativa
                logger.warning(
                    "[OpenAI] Timeout. Aguardando %ds (tentativa %d/%d).",
                    espera,
                    tentativa,
                    self._max_tentativas,
                )
                time.sleep(espera)
            except APIError as exc:
                espera = 2**tentativa
                logger.warning(
                    "[OpenAI] Erro: %s. Aguardando %ds (tentativa %d/%d).",
                    exc,
                    espera,
                    tentativa,
                    self._max_tentativas,
                )
                time.sleep(espera)

        raise RuntimeError(f"OpenAI API falhou após {self._max_tentativas} tentativas.")


# ---------------------------------------------------------------------------
# Classe 2b — Classificação via Google Gemini API
# ---------------------------------------------------------------------------


class GeminiClassificador(ClassificadorBase):
    """
    Implementação de ClassificadorBase usando o Google Gemini (google-genai).

    Lê a chave da variável GEMINI_API_KEY se `api_key` não for informada.
    O prompt de sistema é passado como `system_instruction` na criação do
    cliente, e cada chamada usa `client.models.generate_content()`.
    """

    def __init__(
        self,
        api_key: str | None = None,
        modelo: str = _MODELO_GEMINI,
        max_tentativas: int = _MAX_TENTATIVAS,
        pausa_rate_limit: float = _PAUSA_RATE_LIMIT,
    ) -> None:
        chave = api_key or os.environ.get("GEMINI_API_KEY")
        if not chave:
            raise EnvironmentError(
                "GEMINI_API_KEY não definida.\n" "Defina: export GEMINI_API_KEY=AIza..."
            )
        from google import genai
        from google.genai import types as genai_types

        self._genai = genai
        self._types = genai_types
        self._client = genai.Client(api_key=chave)
        self._modelo = modelo
        self._max_tentativas = max_tentativas
        self._pausa_rate_limit = pausa_rate_limit

    def classificar(self, registro: ContratacaoRow) -> ClassificacaoResult:
        resposta = self._chamar_api(registro.objeto_compra)
        dados = self._parsear_resposta(resposta, registro.pncp_id)
        return ClassificacaoResult(
            pncp_id=registro.pncp_id,
            orgao_nome=registro.orgao_nome,
            uf_sigla=registro.uf_sigla,
            objeto_compra=registro.objeto_compra,
            dados_json=registro.dados_json,
            relevante=dados["relevante"],
            categoria=dados.get("categoria"),
            motivo=dados.get("motivo", ""),
        )

    def _chamar_api(self, objeto_compra: str) -> str:
        config = self._types.GenerateContentConfig(
            system_instruction=_SYSTEM_PROMPT,
            temperature=0.0,
        )
        for tentativa in range(1, self._max_tentativas + 1):
            try:
                response = self._client.models.generate_content(
                    model=self._modelo,
                    contents=objeto_compra,
                    config=config,
                )
                return response.text or ""
            except Exception as exc:  # noqa: BLE001
                # O SDK do Gemini usa exceções do google-api-core; tratamos
                # genericamente para não criar dependência direta aqui.
                nome = type(exc).__name__
                espera = (
                    self._pausa_rate_limit
                    if "ResourceExhausted" in nome or "429" in str(exc)
                    else 2**tentativa
                )
                logger.warning(
                    "[Gemini] %s: %s. Aguardando %.0fs (tentativa %d/%d).",
                    nome,
                    exc,
                    espera,
                    tentativa,
                    self._max_tentativas,
                )
                time.sleep(espera)

        raise RuntimeError(f"Gemini API falhou após {self._max_tentativas} tentativas.")


# ---------------------------------------------------------------------------
# Factory — criar_classificador()
# ---------------------------------------------------------------------------


def criar_classificador(
    provedor: str = "openai",
    modelo: str | None = None,
    api_key: str | None = None,
) -> ClassificadorBase:
    """
    Instancia o classificador correto conforme o provedor solicitado.

    Args:
        provedor: ``"openai"`` ou ``"gemini"``.
        modelo:   Nome do modelo. Se None, usa o padrão do provedor.
        api_key:  Chave de API. Se None, lê da variável de ambiente
                  correspondente (OPENAI_API_KEY ou GEMINI_API_KEY).

    Returns:
        Instância de :class:`ClassificadorBase` pronta para uso.

    Raises:
        ValueError: se o provedor não for reconhecido.
    """
    provedor = provedor.lower().strip()
    if provedor == "openai":
        return GptClassificador(
            api_key=api_key,
            modelo=modelo or _MODELO_OPENAI,
        )
    if provedor == "gemini":
        return GeminiClassificador(
            api_key=api_key,
            modelo=modelo or _MODELO_GEMINI,
        )
    raise ValueError(
        f"Provedor desconhecido: '{provedor}'. " f"Use um de: {_PROVEDORES_DISPONIVEIS}"
    )


# ---------------------------------------------------------------------------
# Classe 3 — Escrita do banco de destino
# ---------------------------------------------------------------------------


class BancoPncpFiltradoWriter:
    """
    Cria e popula o banco SQLite de destino com os registros relevantes.

    A tabela `contratacoes_filtradas` é criada na primeira vez. Nas execuções
    seguintes, a tabela já existe e novos registros são inseridos (ou ignorados
    via INSERT OR IGNORE para idempotência).
    """

    _SQL_CRIAR_TABELA = """
        CREATE TABLE IF NOT EXISTS contratacoes_filtradas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            pncp_id     TEXT    NOT NULL UNIQUE,
            orgao_nome  TEXT,
            uf_sigla    TEXT,
            objeto_compra TEXT,
            dados_json  TEXT,
            relevante   INTEGER NOT NULL DEFAULT 1,
            categoria   TEXT,
            motivo      TEXT
        )
    """

    _SQL_CRIAR_INDICE = """
        CREATE INDEX IF NOT EXISTS idx_filtradas_pncp_id
        ON contratacoes_filtradas (pncp_id)
    """

    _SQL_INSERIR = """
        INSERT OR IGNORE INTO contratacoes_filtradas
            (pncp_id, orgao_nome, uf_sigla, objeto_compra,
             dados_json, relevante, categoria, motivo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SQL_JA_PROCESSADOS = """
        SELECT pncp_id FROM contratacoes_filtradas
    """

    def __init__(self, caminho_db: str) -> None:
        """
        Args:
            caminho_db: Caminho para o arquivo .db de destino (será criado
                        se não existir).
        """
        os.makedirs(os.path.dirname(os.path.abspath(caminho_db)), exist_ok=True)
        self._caminho_db = caminho_db
        self._inicializar()

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def salvar(self, resultado: ClassificacaoResult) -> None:
        """
        Persiste um resultado no banco de destino.

        Usa INSERT OR IGNORE para garantir idempotência: se o pncp_id
        já existir, a linha é simplesmente ignorada.
        """
        with sqlite3.connect(self._caminho_db) as conn:
            conn.execute(
                self._SQL_INSERIR,
                (
                    resultado.pncp_id,
                    resultado.orgao_nome,
                    resultado.uf_sigla,
                    resultado.objeto_compra,
                    resultado.dados_json,
                    resultado.relevante,
                    resultado.categoria,
                    resultado.motivo,
                ),
            )

    def carregar_ja_processados(self) -> set[str]:
        """
        Retorna o conjunto de pncp_ids já presentes no banco de destino.

        Usado pelo orquestrador para retomar o progresso sem reprocessar
        registros já classificados como relevantes.
        """
        with sqlite3.connect(self._caminho_db) as conn:
            rows = conn.execute(self._SQL_JA_PROCESSADOS).fetchall()
        return {row[0] for row in rows}

    def contar(self) -> int:
        """Retorna quantos registros relevantes foram salvos."""
        with sqlite3.connect(self._caminho_db) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM contratacoes_filtradas"
            ).fetchone()[0]

    # ------------------------------------------------------------------
    # Implementação interna
    # ------------------------------------------------------------------

    def _inicializar(self) -> None:
        """Cria a tabela e o índice se ainda não existirem.

        Ativa WAL (Write-Ahead Logging) para permitir leituras
        concorrentes sem bloquear escritas — necessário no modo paralelo.
        """
        with sqlite3.connect(self._caminho_db) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(self._SQL_CRIAR_TABELA)
            conn.execute(self._SQL_CRIAR_INDICE)


# ---------------------------------------------------------------------------
# Registro de progresso (pncp_ids já enviados à API, relevantes ou não)
# ---------------------------------------------------------------------------


class _ProgressoTracker:
    """
    Rastreia quais pncp_ids já foram enviados à API (independente do resultado).

    Usa um arquivo de texto simples (.progresso) ao lado do banco de destino
    para que o pipeline possa ser interrompido e retomado sem reprocessar
    registros já avaliados.
    """

    def __init__(self, caminho_db_destino: str) -> None:
        base = os.path.splitext(caminho_db_destino)[0]
        self._arquivo = base + ".progresso"
        self._processados: set[str] = self._carregar()

    def ja_processado(self, pncp_id: str) -> bool:
        return pncp_id in self._processados

    def marcar(self, pncp_id: str) -> None:
        self._processados.add(pncp_id)
        with open(self._arquivo, "a", encoding="utf-8") as fh:
            fh.write(pncp_id + "\n")

    def total(self) -> int:
        return len(self._processados)

    def _carregar(self) -> set[str]:
        if not os.path.exists(self._arquivo):
            return set()
        with open(self._arquivo, encoding="utf-8") as fh:
            return {linha.strip() for linha in fh if linha.strip()}


# ---------------------------------------------------------------------------
# Orquestrador — GptClassificacaoStage
# ---------------------------------------------------------------------------


class GptClassificacaoStage:
    """
    Etapa 5: orquestra leitura, classificação LLM e escrita dos relevantes.

    Suporta OpenAI e Gemini via o parâmetro `provedor`.

    Args:
        banco_origem:  Banco SQLite com a tabela `contratacoes`.
        banco_destino: Banco SQLite de saída (criado se não existir).
        provedor:      ``"openai"`` (padrão) ou ``"gemini"``.
        modelo:        Nome do modelo. Se None, usa o padrão do provedor.
        api_key:       Chave de API. Se None, lê da variável de ambiente
                       (OPENAI_API_KEY ou GEMINI_API_KEY).
        intervalo_log: A cada quantos registros exibir progresso.
    """

    def __init__(
        self,
        banco_origem: str,
        banco_destino: str,
        provedor: str = "openai",
        modelo: str | None = None,
        api_key: str | None = None,
        intervalo_log: int = 10,
    ) -> None:
        self._leitor = BancoPncpReader(banco_origem)
        self._classificador = criar_classificador(
            provedor=provedor,
            modelo=modelo,
            api_key=api_key,
        )
        self._escritor = BancoPncpFiltradoWriter(banco_destino)
        self._progresso = _ProgressoTracker(banco_destino)
        self._intervalo_log = intervalo_log
        self._provedor = provedor

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def executar(self) -> None:
        """
        Executa a classificação completa com retomada de progresso.

        Fluxo por registro:
          1. Verifica se já foi processado (pula se sim)
          2. Chama a API do LLM configurado
          3. Se relevante=1, salva no banco de destino
          4. Marca como processado no tracker
        """
        print("=" * 60)
        print(f"ETAPA 5 — CLASSIFICAÇÃO LLM (provedor: {self._provedor})")
        print("=" * 60)

        total_origem = self._leitor.contar()
        ja_processados_antes = self._progresso.total()

        print(f"  Banco de origem : {total_origem} registros")
        print(f"  Já processados  : {ja_processados_antes}" " (retomando progresso)")
        print(f"  A processar     : {total_origem - ja_processados_antes}")
        print()

        contadores = {
            "processados": 0,
            "relevantes": 0,
            "erros": 0,
            "pulados": 0,
        }

        for registro in self._leitor.iterar():
            if self._progresso.ja_processado(registro.pncp_id):
                contadores["pulados"] += 1
                continue

            resultado = self._classificar_com_segurança(registro, contadores)
            if resultado is None:
                continue

            if resultado.relevante == 1:
                self._escritor.salvar(resultado)
                contadores["relevantes"] += 1

            self._progresso.marcar(registro.pncp_id)
            contadores["processados"] += 1

            if contadores["processados"] % self._intervalo_log == 0:
                self._log_progresso(contadores, total_origem, ja_processados_antes)

        self._relatorio_final(contadores, total_origem)

    # ------------------------------------------------------------------
    # Implementação interna
    # ------------------------------------------------------------------

    def _classificar_com_segurança(
        self,
        registro: ContratacaoRow,
        contadores: dict,
    ) -> ClassificacaoResult | None:
        """
        Tenta classificar o registro. Em caso de exceção irrecuperável,
        incrementa o contador de erros, marca como processado e retorna None.
        """
        try:
            return self._classificador.classificar(registro)
        except RuntimeError as exc:
            logger.error(
                "Erro irrecuperável para pncp_id=%s: %s — pulando registro.",
                registro.pncp_id,
                exc,
            )
            self._progresso.marcar(registro.pncp_id)
            contadores["erros"] += 1
            return None

    def _log_progresso(
        self, contadores: dict, total_origem: int, ja_antes: int
    ) -> None:
        enviados = contadores["processados"] + ja_antes
        pct = (enviados / total_origem * 100) if total_origem else 0
        total_banco = self._escritor.contar()
        faltam = max(0, total_origem - enviados)
        # contadores['relevantes'] conta quantos foram salvos nesta execução
        salvos_exec = contadores.get("relevantes", 0)
        logger.info(
            "Prog: %d/%d (%.1f%%) | faltam=%d | proc_run=%d | rel_run=%d | "
            "bank_total=%d | errs=%d | pul=%d",
            enviados,
            total_origem,
            pct,
            faltam,
            contadores.get("processados", 0),
            salvos_exec,
            total_banco,
            contadores.get("erros", 0),
            contadores.get("pulados", 0),
        )

    def _relatorio_final(self, contadores: dict, total_origem: int) -> None:
        print()
        print("=" * 60)
        print("RELATÓRIO FINAL — CLASSIFICAÇÃO GPT")
        print("=" * 60)
        print(f"  Total no banco de origem  : {total_origem}")
        ja = self._progresso.total()
        enviados = contadores["processados"] + ja
        faltam = max(0, total_origem - enviados)
        print(f"  Pulados (já processados)  : {contadores['pulados']}")
        print(f"  Processados nesta execução: {contadores['processados']}")
        print(f"  Enviados (total): {enviados}")
        print(f"  Faltam processar           : {faltam}")
        print(f"  Erros de API              : {contadores['erros']}")
        print(f"  Relev. nesta exec         : {contadores.get('relevantes', 0)}")
        print(f"  Relevantes salvos (total) : {self._escritor.contar()}")
        print("=" * 60)
