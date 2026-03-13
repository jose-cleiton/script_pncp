"""
pipeline.py
Orquestrador completo do pipeline PNCP.

Responsabilidade deste arquivo: expor um ponto de entrada único (PncpPipeline)
que coordena todas as etapas do processo, do zero até o CSV de licitações filtradas.

Arquitetura:
    PipelineConfig      → centraliza todos os parâmetros e caminhos de arquivo
    ColetaStage         → etapa 1: consulta a API e persiste no SQLite
    RotulacaoStage      → etapa 2: exporta exemplos do DB para rotulação manual
    TreinamentoStage    → etapa 3: treina o modelo SGD a partir do dataset rotulado
    ClassificacaoStage  → etapa 4: aplica o modelo no banco e gera o CSV filtrado
    PncpPipeline        → orquestrador: monta as stages e executa o fluxo completo

Uso típico (equivalente ao notebook):
    from pipeline import PncpPipeline, PipelineConfig

    config = PipelineConfig(data="2026-03-13")
    pipeline = PncpPipeline(config)

    pipeline.coletar()               # Etapa 1
    pipeline.exportar_para_rotular() # Etapa 2 (antes de treinar)
    pipeline.treinar()               # Etapa 3
    pipeline.classificar()           # Etapa 4

    # ou executar tudo de uma vez:
    pipeline.executar_tudo()
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline as SklearnPipeline

from buscar_dados import PncpColetorService
from configurar_db import DatabaseConfigurator
from pncp_client import PncpApiClient
from preprocessar import TextPreprocessor


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------


@dataclass
class PipelineConfig:
    """
    Centraliza todos os parâmetros e caminhos de arquivo do pipeline.

    Cada etapa recebe uma instância desta classe — não há strings soltas
    espalhadas pelo código.

    Args:
        data:                    Data de referência no formato YYYY-MM-DD.
                                 Padrão: data de hoje.
        base_url:                URL base da API PNCP.
        endpoint:                Endpoint de contratações em aberto.
        codigo_modalidade:       Código da modalidade de contratação (6 = Pregão Eletrônico).
        tamanho_pagina:          Quantidade de registros por página da API.
        quantidade_para_rotular: Quantos registros exportar para rotulação manual.
        pausa_entre_paginas:     Segundos de pausa entre requisições à API.
    """

    data: str = field(default_factory=lambda: date.today().strftime("%Y-%m-%d"))

    # --- API ---
    base_url: str = "https://pncp.gov.br/api/consulta"
    endpoint: str = "/v1/contratacoes/proposta"
    codigo_modalidade: int = 6
    tamanho_pagina: int = 50
    pausa_entre_paginas: float = 0.5

    # --- Rotulação ---
    quantidade_para_rotular: int = 100

    # --- Caminhos (derivados de `data`) ---
    @property
    def url_completa(self) -> str:
        return self.base_url + self.endpoint

    @property
    def params_api(self) -> dict:
        """Parâmetros base para a requisição à API (sem 'pagina')."""
        from datetime import datetime
        return {
            "dataFinal": int(datetime.now().strftime("%Y%m%d")),
            "codigoModalidadeContratacao": self.codigo_modalidade,
            "pagina": 1,
            "tamanhoPagina": self.tamanho_pagina,
        }

    @property
    def caminho_db(self) -> str:
        return f"data/coleta/{self.data}/pncp_data.db"

    @property
    def caminho_para_rotular(self) -> str:
        return f"data/rotulacao/{self.data}/dataset_para_rotular.csv"

    @property
    def caminho_dataset(self) -> str:
        return f"data/treinamento/{self.data}/dataset.csv"

    @property
    def caminho_modelo(self) -> str:
        return f"data/treinamento/{self.data}/modelo_relevancia.joblib"

    @property
    def caminho_resultado(self) -> str:
        return f"data/resultado/{self.data}/licitacoes_filtradas.csv"

    def criar_diretorios(self) -> None:
        """Garante que todos os diretórios do pipeline existam."""
        caminhos = [
            self.caminho_db,
            self.caminho_para_rotular,
            self.caminho_dataset,
            self.caminho_modelo,
            self.caminho_resultado,
        ]
        for caminho in caminhos:
            os.makedirs(os.path.dirname(caminho), exist_ok=True)


# ---------------------------------------------------------------------------
# Etapa 1 — Coleta
# ---------------------------------------------------------------------------


class ColetaStage:
    """
    Etapa 1: consulta a API PNCP e persiste os dados no SQLite.

    Responsabilidade única: coordenar DatabaseConfigurator, PncpApiClient
    e PncpColetorService para popular o banco de dados.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config

    def executar(self) -> None:
        """Configura o DB, consulta a API e salva todos os registros."""
        print("=" * 60)
        print("ETAPA 1 — COLETA DE DADOS DA API PNCP")
        print("=" * 60)

        self._configurar_banco()
        total = self._coletar_dados()

        print(f"\n✓ Coleta concluída. Total de registros processados: {total}")
        print(f"  Banco de dados: {self._config.caminho_db}")

    def _configurar_banco(self) -> None:
        """Cria a tabela e os índices no SQLite caso ainda não existam."""
        DatabaseConfigurator(self._config.caminho_db).configurar()

    def _coletar_dados(self) -> int:
        """Instancia o cliente e o serviço de coleta e executa a iteração."""
        cliente = PncpApiClient(
            base_url=self._config.url_completa,
            pausa_entre_paginas=self._config.pausa_entre_paginas,
        )
        coletor = PncpColetorService(
            cliente=cliente,
            nome_db=self._config.caminho_db,
        )
        return coletor.executar_coleta(self._config.params_api)


# ---------------------------------------------------------------------------
# Etapa 2 — Rotulação
# ---------------------------------------------------------------------------


class RotulacaoStage:
    """
    Etapa 2: exporta registros do banco para rotulação manual.

    Responsabilidade única: ler registros do SQLite e gerar o CSV
    com a coluna 'Relevante' em branco para preenchimento manual.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config

    def executar(self) -> None:
        """Lê o banco e gera o CSV para rotulação."""
        print("=" * 60)
        print("ETAPA 2 — EXPORTAÇÃO PARA ROTULAÇÃO MANUAL")
        print("=" * 60)

        registros = self._ler_banco()
        self._exportar_csv(registros)

        print(f"\n✓ {len(registros)} licitações exportadas para:")
        print(f"  {self._config.caminho_para_rotular}")
        print("\nPróximos passos:")
        print("  1. Abra o arquivo e preencha a coluna 'Relevante' (1 ou 0)")
        print(f"  2. Copie as linhas rotuladas para: {self._config.caminho_dataset}")
        print("  3. Certifique-se de ter ao menos 10 exemplos de cada classe")
        print("  4. Execute a etapa de treinamento (Etapa 3)")

    def _ler_banco(self) -> list[dict]:
        """Lê os primeiros N registros do banco e extrai os campos relevantes."""
        quantidade = self._config.quantidade_para_rotular
        with sqlite3.connect(self._config.caminho_db) as conn:
            rows = conn.execute(
                f"SELECT dados_json FROM contratacoes LIMIT {quantidade}"
            ).fetchall()

        return [
            {
                "pncp_id": json.loads(row[0]).get("numeroControlePNCP", ""),
                "Objeto": json.loads(row[0]).get("objetoCompra", ""),
            }
            for row in rows
        ]

    def _exportar_csv(self, registros: list[dict]) -> None:
        """Cria o DataFrame com coluna 'Relevante' vazia e salva em CSV."""
        df = pd.DataFrame(registros)
        df["Relevante"] = ""
        df.to_csv(self._config.caminho_para_rotular, index=False, encoding="utf-8-sig")


# ---------------------------------------------------------------------------
# Etapa 3 — Treinamento
# ---------------------------------------------------------------------------


class TreinamentoStage:
    """
    Etapa 3: treina o modelo de classificação a partir do dataset rotulado.

    Responsabilidade única: ler o dataset.csv, pré-processar, treinar o pipeline
    sklearn (TF-IDF + SGD), avaliar e salvar o modelo em disco.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._preprocessor = TextPreprocessor()

    def executar(self) -> None:
        """Treina e salva o modelo. Lança FileNotFoundError se o dataset não existir."""
        print("=" * 60)
        print("ETAPA 3 — TREINAMENTO DO MODELO")
        print("=" * 60)
        print(f"Dataset : {self._config.caminho_dataset}")
        print(f"Modelo  : {self._config.caminho_modelo}")

        df = self._carregar_dataset()
        X_train, X_test, y_train, y_test = self._preparar_dados(df)
        modelo = self._treinar(X_train, y_train)
        self._avaliar(modelo, X_test, y_test)
        self._salvar_modelo(modelo)

    def _carregar_dataset(self) -> pd.DataFrame:
        """Lê o CSV rotulado e valida a presença das colunas esperadas."""
        df = pd.read_csv(self._config.caminho_dataset)
        print(f"\n✓ Dataset carregado: {len(df)} exemplos")
        print(df["Relevante"].value_counts().to_string())
        return df

    def _preparar_dados(
        self, df: pd.DataFrame
    ) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
        """Pré-processa textos e divide em conjuntos de treino e teste."""
        df["objeto_limpo"] = df["Objeto"].apply(self._preprocessor.processar)

        X = df["objeto_limpo"]
        y = df["Relevante"]

        return train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    def _treinar(
        self, X_train: pd.Series, y_train: pd.Series
    ) -> SklearnPipeline:
        """Constrói e ajusta o pipeline TF-IDF + SGDClassifier."""
        modelo = SklearnPipeline([
            ("tfidf", TfidfVectorizer(max_features=1000, ngram_range=(1, 2))),
            ("clf", SGDClassifier(loss="hinge", random_state=42, max_iter=100)),
        ])
        modelo.fit(X_train, y_train)
        print("\n✓ Treinamento concluído.")
        return modelo

    def _avaliar(
        self,
        modelo: SklearnPipeline,
        X_test: pd.Series,
        y_test: pd.Series,
    ) -> None:
        """Imprime acurácia e relatório de classificação."""
        y_pred = modelo.predict(X_test)
        acuracia = accuracy_score(y_test, y_pred)
        print(f"\n  Acurácia no conjunto de teste: {acuracia * 100:.2f}%")
        print(classification_report(
            y_test, y_pred,
            target_names=["Irrelevante (0)", "Relevante (1)"],
        ))

    def _salvar_modelo(self, modelo: SklearnPipeline) -> None:
        """Serializa o modelo treinado com joblib."""
        joblib.dump(modelo, self._config.caminho_modelo)
        print(f"✓ Modelo salvo em: {self._config.caminho_modelo}")


# ---------------------------------------------------------------------------
# Etapa 4 — Classificação
# ---------------------------------------------------------------------------


class ClassificacaoStage:
    """
    Etapa 4: aplica o modelo treinado ao banco e gera o CSV filtrado.

    Responsabilidade única: carregar o modelo, ler todos os registros do DB,
    prever relevância e exportar apenas os relevantes para CSV.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._preprocessor = TextPreprocessor()

    def executar(self) -> None:
        """Carrega modelo, classifica registros do DB e salva resultado."""
        print("=" * 60)
        print("ETAPA 4 — CLASSIFICAÇÃO E GERAÇÃO DO RESULTADO")
        print("=" * 60)

        modelo = self._carregar_modelo()
        df = self._carregar_dados_banco()

        if df.empty:
            print("ALERTA: O banco de dados está vazio. Execute a Etapa 1 primeiro.")
            return

        df_resultado = self._classificar(modelo, df)
        self._salvar_resultado(df_resultado, total_original=len(df))

    def _carregar_modelo(self) -> SklearnPipeline:
        """Desserializa o modelo do disco."""
        modelo = joblib.load(self._config.caminho_modelo)
        print(f"✓ Modelo carregado: {self._config.caminho_modelo}")
        return modelo

    def _carregar_dados_banco(self) -> pd.DataFrame:
        """Lê todos os registros do banco e extrai os campos necessários."""
        with sqlite3.connect(self._config.caminho_db) as conn:
            rows = conn.execute("SELECT dados_json FROM contratacoes").fetchall()

        registros = [
            {
                "pncp_id": json.loads(row[0]).get("numeroControlePNCP", ""),
                "objeto_compra": json.loads(row[0]).get("objetoCompra", ""),
            }
            for row in rows
        ]
        df = pd.DataFrame(registros)
        print(f"✓ {len(df)} registros carregados do banco.")
        return df

    def _classificar(
        self, modelo: SklearnPipeline, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Pré-processa textos, prevê relevância e retorna apenas os relevantes."""
        df["objeto_limpo"] = df["objeto_compra"].apply(self._preprocessor.processar)
        df["relevante_pred"] = modelo.predict(df["objeto_limpo"])
        return df[df["relevante_pred"] == 1].copy()

    def _salvar_resultado(
        self, df_relevante: pd.DataFrame, total_original: int
    ) -> None:
        """Exporta o DataFrame filtrado para CSV."""
        df_relevante.to_csv(
            self._config.caminho_resultado, index=False, encoding="utf-8-sig"
        )
        print(
            f"\n✓ {len(df_relevante)} de {total_original} licitações "
            "classificadas como RELEVANTES."
        )
        print(f"  Resultado salvo em: {self._config.caminho_resultado}")


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------


class PncpPipeline:
    """
    Ponto de entrada único do pipeline PNCP.

    Coordena as quatro etapas em ordem:
        1. ColetaStage        → popula o banco com dados da API
        2. RotulacaoStage     → exporta exemplos para rotulação manual
        3. TreinamentoStage   → treina o modelo de relevância
        4. ClassificacaoStage → aplica o modelo e gera o CSV final

    Cada etapa pode ser executada individualmente ou em sequência
    via `executar_tudo()`.

    Exemplo:
        config   = PipelineConfig(data="2026-03-13")
        pipeline = PncpPipeline(config)

        pipeline.coletar()
        pipeline.exportar_para_rotular()
        # ... rotular manualmente o dataset ...
        pipeline.treinar()
        pipeline.classificar()
    """

    def __init__(self, config: Optional[PipelineConfig] = None) -> None:
        """
        Inicializa o pipeline.

        Args:
            config: Configuração do pipeline. Se None, usa os defaults
                    (data de hoje, URL padrão PNCP, modalidade 6).
        """
        self._config = config or PipelineConfig()
        self._config.criar_diretorios()

        self._coleta = ColetaStage(self._config)
        self._rotulacao = RotulacaoStage(self._config)
        self._treinamento = TreinamentoStage(self._config)
        self._classificacao = ClassificacaoStage(self._config)

    # --- API pública ---

    def coletar(self) -> None:
        """Executa a Etapa 1: coleta da API e persistência no banco."""
        self._coleta.executar()

    def exportar_para_rotular(self) -> None:
        """Executa a Etapa 2: exporta CSV para rotulação manual."""
        self._rotulacao.executar()

    def treinar(self) -> None:
        """
        Executa a Etapa 3: treinamento do modelo.

        Pré-requisito: dataset.csv deve existir com pelo menos uma classe 0
        e uma classe 1 em `data/treinamento/{data}/`.
        """
        self._treinamento.executar()

    def classificar(self) -> None:
        """
        Executa a Etapa 4: classificação e geração do CSV filtrado.

        Pré-requisito: modelo treinado deve existir (execute `treinar()` antes).
        """
        self._classificacao.executar()

    def executar_tudo(self) -> None:
        """
        Executa as 4 etapas em sequência.

        Útil para reprocessamento completo quando o dataset já está rotulado.
        Para um primeiro ciclo, use os métodos individuais para rotular
        manualmente após `exportar_para_rotular()`.
        """
        print(f"\n{'#' * 60}")
        print(f"  PIPELINE PNCP — {self._config.data}")
        print(f"{'#' * 60}\n")

        self.coletar()
        self.exportar_para_rotular()
        self.treinar()
        self.classificar()

        print(f"\n{'#' * 60}")
        print("  PIPELINE CONCLUÍDO COM SUCESSO")
        print(f"{'#' * 60}\n")
