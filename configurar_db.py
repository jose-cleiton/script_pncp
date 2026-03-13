"""
configurar_db.py
Responsabilidade: criar o banco de dados SQLite, a tabela e os índices.
Não insere dados, não consulta API, não conhece regras de negócio.
"""
import sqlite3

_DDL_TABELA = """
    CREATE TABLE IF NOT EXISTS contratacoes (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,

        -- Colunas de controle
        pagina_coleta    INTEGER,
        timestamp_coleta TEXT,

        -- Coluna principal: JSON bruto da API
        dados_json       TEXT,

        -- Colunas geradas (indexáveis via JSON1)
        pncp_id      TEXT AS (json_extract(dados_json, '$.numeroControlePNCP')) STORED,
        orgao_nome   TEXT AS (json_extract(dados_json, '$.orgaoEntidade.razaosocial')) STORED,
        uf_sigla     TEXT AS (json_extract(dados_json, '$.unidadeOrgao.ufSigla')) STORED,
        objeto_compra TEXT AS (json_extract(dados_json, '$.objetoCompra')) STORED,

        -- Evita duplicatas
        UNIQUE(pncp_id)
    );
"""

_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_pncp_id       ON contratacoes (pncp_id);",
    "CREATE INDEX IF NOT EXISTS idx_orgao_nome     ON contratacoes (orgao_nome);",
    "CREATE INDEX IF NOT EXISTS idx_uf_sigla       ON contratacoes (uf_sigla);",
    "CREATE INDEX IF NOT EXISTS idx_objeto_compra  ON contratacoes (objeto_compra);",
]


class DatabaseConfigurator:
    """Cria e configura o banco de dados SQLite do projeto."""

    def __init__(self, nome_db: str = "pncp_data.db") -> None:
        """
        Inicializa o configurador.

        Args:
            nome_db: Caminho do arquivo SQLite a ser criado/configurado.

        Atenção: para aplicar alterações no schema, exclua o arquivo .db
        existente antes de executar.
        """
        self.nome_db = nome_db

    def configurar(self) -> None:
        """Cria a tabela e os índices caso ainda não existam."""
        try:
            with sqlite3.connect(self.nome_db) as conn:
                print(f"Configurando banco de dados: {self.nome_db}")
                self._criar_tabela(conn)
                self._criar_indices(conn)
                conn.commit()
            print("Banco de dados, tabela e índices configurados com sucesso.")
        except sqlite3.Error as e:
            print(f"ERRO ao configurar o DB: {e}")

    def _criar_tabela(self, conn: sqlite3.Connection) -> None:
        """Executa o DDL de criação da tabela."""
        conn.execute(_DDL_TABELA)

    def _criar_indices(self, conn: sqlite3.Connection) -> None:
        """Cria os índices nas colunas geradas."""
        for ddl in _INDICES:
            conn.execute(ddl)


# --- Fachada: preserva compatibilidade com o notebook e código existente ---

def configurar_db(nome_db: str = "pncp_data.db") -> None:
    """
    Fachada que preserva a assinatura original.
    Instancia DatabaseConfigurator e executa a configuração.

    Args:
        nome_db: Caminho do arquivo SQLite.
    """
    DatabaseConfigurator(nome_db).configurar()


if __name__ == "__main__":
    configurar_db()
