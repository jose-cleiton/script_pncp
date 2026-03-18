"""
salvar_dados.py
Responsabilidade: persistir registros de uma página da API no SQLite.
Não consulta a API, não configura o schema, não conhece regras de negócio.
"""
import sqlite3
import json
import datetime

_SQL_INSERT = """
    INSERT OR IGNORE INTO contratacoes (pagina_coleta, timestamp_coleta, dados_json)
    VALUES (?, ?, ?);
"""

_FORMATO_TIMESTAMP = "%Y-%m-%d %H:%M:%S"


class SqliteInserter:
    """Persiste registros de uma página da API na tabela `contratacoes`."""

    def __init__(self, nome_db: str = "pncp_data.db") -> None:
        """
        Inicializa o insersor.

        Args:
            nome_db: Caminho do arquivo SQLite já configurado.
        """
        self.nome_db = nome_db

    def inserir(self, dados_pagina: list, pagina_coletada: int) -> None:
        """
        Insere todos os registros de uma página no banco de dados.

        Args:
            dados_pagina:    Lista de dicts retornada pela API para a página.
            pagina_coletada: Número da página de origem (para auditoria).
        """
        try:
            with sqlite3.connect(self.nome_db) as conn:
                registros_inseridos = self._inserir_registros(
                    conn, dados_pagina, pagina_coletada
                )
                conn.commit()
            print(
                f"   -> [DB] {registros_inseridos}/{len(dados_pagina)} "
                f"registros salvos/atualizados na página {pagina_coletada}."
            )
        except sqlite3.Error as e:
            print(
                f"   -> [ERRO DB] Falha ao inserir dados: {e}. "
                "Verifique se o arquivo DB tem a estrutura correta."
            )

    def _inserir_registros(
        self,
        conn: sqlite3.Connection,
        dados_pagina: list,
        pagina_coletada: int,
    ) -> int:
        """
        Itera sobre os registros e executa o INSERT para cada um.

        Returns:
            Número de linhas efetivamente inseridas (rowcount > 0).
        """
        cursor = conn.cursor()
        timestamp_atual = datetime.datetime.now().strftime(_FORMATO_TIMESTAMP)
        registros_inseridos = 0

        for registro in dados_pagina:
            registro_json_str = json.dumps(registro)
            cursor.execute(_SQL_INSERT, (pagina_coletada, timestamp_atual, registro_json_str))
            if cursor.rowcount > 0:
                registros_inseridos += 1

        return registros_inseridos


# --- Fachada: preserva compatibilidade com o notebook e código existente ---

def inserir_dados(nome_db: str, dados_pagina: list, pagina_coletada: int) -> None:
    """
    Fachada que preserva a assinatura original.
    Instancia SqliteInserter e persiste os registros.
    """
    SqliteInserter(nome_db).inserir(dados_pagina, pagina_coletada)