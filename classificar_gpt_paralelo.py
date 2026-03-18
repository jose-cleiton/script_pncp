"""
classificar_gpt_paralelo.py
Versão paralela da Etapa 5 usando ThreadPoolExecutor.

Divide os registros do banco de origem em N fatias e processa cada fatia
em uma thread independente, cada uma com seu próprio cliente de API.
O banco de destino é compartilhado com proteção por Lock.

Uso via CLI:
    python main.py --etapa classificar_gpt --data 2026-03-14 \\
                   --provedor gemini --workers 5

Uso direto:
    from classificar_gpt_paralelo import GptClassificacaoParalelaStage
    stage = GptClassificacaoParalelaStage(
        banco_origem="data/coleta/2026-03-14/pncp_data.db",
        banco_destino="data/resultado/2026-03-14/pncp_filtrado_gpt.db",
        provedor="gemini",
        workers=5,
    )
    stage.executar()

Considerações de rate limit:
    Gemini 2.0 Flash pago: 2.000 RPM (~33 req/s)
    Com 5 workers cada um fazendo ~1 req/s → ~5 req/s, bem dentro do limite.
    Aumente workers com cuidado se receber erros 429.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)

from classificar_gpt import (
    BancoPncpFiltradoWriter,
    BancoPncpReader,
    ClassificacaoResult,
    ContratacaoRow,
    _ProgressoTracker,
    criar_classificador,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Worker — processa uma fatia de registros
# ---------------------------------------------------------------------------


class _Worker:
    """
    Processa uma fatia contígua de registros do banco de origem.

    Cada worker tem seu próprio classificador (cliente de API independente).
    O escritor e o tracker são compartilhados e protegidos por locks.
    """

    def __init__(
        self,
        worker_id: int,
        banco_origem: str,
        escritor: BancoPncpFiltradoWriter,
        progresso: _ProgressoTracker,
        lock_escritor: threading.Lock,
        lock_progresso: threading.Lock,
        lock_print: threading.Lock,
        provedor: str,
        modelo: str | None,
        offset: int,
        limite: int,
        contadores_globais: dict,
    ) -> None:
        self._id = worker_id
        self._leitor = BancoPncpReader(banco_origem)
        self._classificador = criar_classificador(provedor=provedor, modelo=modelo)
        self._escritor = escritor
        self._progresso = progresso
        self._lock_escritor = lock_escritor
        self._lock_progresso = lock_progresso
        self._lock_print = lock_print
        self._offset = offset
        self._limite = limite
        self._contadores = contadores_globais

    def executar(self) -> None:
        """Processa a fatia atribuída a este worker."""
        for registro in self._leitor.iterar_faixa(self._offset, self._limite):
            # Checa progresso com lock para evitar condição de corrida
            with self._lock_progresso:
                if self._progresso.ja_processado(registro.pncp_id):
                    self._contadores["pulados"] += 1
                    continue

            resultado = self._classificar(registro)
            if resultado is None:
                continue

            # Salva no banco e marca progresso atomicamente
            with self._lock_escritor:
                if resultado.relevante == 1:
                    self._escritor.salvar(resultado)
                    self._contadores["relevantes"] += 1

            with self._lock_progresso:
                self._progresso.marcar(registro.pncp_id)

            self._contadores["processados"] += 1

            # Log a cada 25 registros deste worker
            if self._contadores["processados"] % 25 == 0:
                with self._lock_print:
                    logger.info(
                        "[Worker %d] processados=%d | " "relevantes=%d | erros=%d",
                        self._id,
                        self._contadores["processados"],
                        self._contadores["relevantes"],
                        self._contadores["erros"],
                    )

    def _classificar(self, registro: ContratacaoRow) -> ClassificacaoResult | None:
        try:
            return self._classificador.classificar(registro)
        except RuntimeError as exc:
            logger.error(
                "[Worker %d] Erro irrecuperável pncp_id=%s: %s",
                self._id,
                registro.pncp_id,
                exc,
            )
            with self._lock_progresso:
                self._progresso.marcar(registro.pncp_id)
            self._contadores["erros"] += 1
            return None


# ---------------------------------------------------------------------------
# Orquestrador paralelo
# ---------------------------------------------------------------------------


class GptClassificacaoParalelaStage:
    """
    Etapa 5 (paralela): divide os registros em fatias e processa com
    múltiplos workers simultâneos via ThreadPoolExecutor.

    Args:
        banco_origem:  Banco SQLite com a tabela `contratacoes`.
        banco_destino: Banco SQLite de saída.
        provedor:      ``"openai"`` ou ``"gemini"``.
        modelo:        Nome do modelo. None = padrão do provedor.
        workers:       Número de threads paralelas. Padrão: 5.
    """

    def __init__(
        self,
        banco_origem: str,
        banco_destino: str,
        provedor: str = "openai",
        modelo: str | None = None,
        workers: int = 5,
    ) -> None:
        self._banco_origem = banco_origem
        self._banco_destino = banco_destino
        self._provedor = provedor
        self._modelo = modelo
        self._workers = workers

        # Recursos compartilhados entre workers
        self._escritor = BancoPncpFiltradoWriter(banco_destino)
        self._progresso = _ProgressoTracker(banco_destino)
        self._lock_escritor = threading.Lock()
        self._lock_progresso = threading.Lock()
        self._lock_print = threading.Lock()

    def executar(self) -> None:
        """Divide os registros em fatias e processa em paralelo."""
        print("=" * 60)
        print(
            f"ETAPA 5 — CLASSIFICAÇÃO LLM PARALELA"
            f" ({self._workers} workers | {self._provedor})"
        )
        print("=" * 60)

        leitor = BancoPncpReader(self._banco_origem)
        total = leitor.contar()
        ja_antes = self._progresso.total()

        print(f"  Total no banco de origem : {total}")
        print(f"  Já processados (progresso): {ja_antes}")
        print(f"  A processar              : {total - ja_antes}")
        print(f"  Workers                  : {self._workers}")
        print()

        # Contador compartilhado (thread-safe via GIL para int no CPython)
        contadores = {
            "processados": 0,
            "relevantes": 0,
            "erros": 0,
            "pulados": 0,
        }

        # Divide os registros em N fatias contíguas
        fatias = self._calcular_fatias(total, self._workers)

        workers = [
            _Worker(
                worker_id=i,
                banco_origem=self._banco_origem,
                escritor=self._escritor,
                progresso=self._progresso,
                lock_escritor=self._lock_escritor,
                lock_progresso=self._lock_progresso,
                lock_print=self._lock_print,
                provedor=self._provedor,
                modelo=self._modelo,
                offset=offset,
                limite=limite,
                contadores_globais=contadores,
            )
            for i, (offset, limite) in enumerate(fatias)
        ]

        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = {pool.submit(w.executar): i for i, w in enumerate(workers)}
            for future in as_completed(futures):
                wid = futures[future]
                exc = future.exception()
                if exc:
                    logger.error("Worker %d encerrou com erro: %s", wid, exc)
                else:
                    logger.info("Worker %d concluído.", wid)

        self._relatorio_final(contadores, total)

    # ------------------------------------------------------------------
    # Implementação interna
    # ------------------------------------------------------------------

    @staticmethod
    def _calcular_fatias(total: int, n_workers: int) -> list[tuple[int, int]]:
        """
        Divide [0, total) em n_workers fatias o mais iguais possível.

        Returns:
            Lista de (offset, limite) para cada worker.
        """
        tamanho_base = total // n_workers
        resto = total % n_workers
        fatias = []
        offset = 0
        for i in range(n_workers):
            limite = tamanho_base + (1 if i < resto else 0)
            if limite > 0:
                fatias.append((offset, limite))
            offset += limite
        return fatias

    def _relatorio_final(self, contadores: dict, total: int) -> None:
        print()
        print("=" * 60)
        print("RELATÓRIO FINAL — CLASSIFICAÇÃO PARALELA")
        print("=" * 60)
        print(f"  Total no banco de origem  : {total}")
        print(f"  Processados nesta execução: {contadores['processados']}")
        print(f"  Pulados (já processados)  : {contadores['pulados']}")
        print(f"  Erros de API              : {contadores['erros']}")
        print(f"  Relevantes salvos (total) : {self._escritor.contar()}")
        print("=" * 60)
