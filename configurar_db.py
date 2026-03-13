import sqlite3

def configurar_db(nome_db="pncp_data.db"):
    """
    Cria o banco de dados SQLite e a tabela com Colunas Geradas para Indexação JSON1.
    
    ATENÇÃO: Para aplicar esta nova coluna, você DEVE excluir o arquivo
    pncp_data.db existente antes de rodar esta função.
    """
    try:
        conn = sqlite3.connect(nome_db)
        cursor = conn.cursor()
        
        print(f"Configurando banco de dados: {nome_db}")
        
        # 1. Criar a tabela com Colunas Geradas (JSON1)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contratacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Colunas de Controle
                pagina_coleta INTEGER,
                timestamp_coleta TEXT,
                
                -- Coluna Principal para o JSON Bruto
                dados_json TEXT, 
                
                -- Colunas Geradas (Indexáveis)
                pncp_id TEXT AS (json_extract(dados_json, '$.numeroControlePNCP')) STORED,
                orgao_nome TEXT AS (json_extract(dados_json, '$.orgaoEntidade.razaosocial')) STORED,
                uf_sigla TEXT AS (json_extract(dados_json, '$.unidadeOrgao.ufSigla')) STORED,
                
                -- *** NOVA COLUNA GERADA PARA ML ***
                objeto_compra TEXT AS (json_extract(dados_json, '$.objetoCompra')) STORED,
                
                -- Garante que não haja duplicatas
                UNIQUE(pncp_id) 
            );
        """)

        # 2. Criar Índices nas colunas geradas para performance de busca
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pncp_id ON contratacoes (pncp_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orgao_nome ON contratacoes (orgao_nome);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_uf_sigla ON contratacoes (uf_sigla);")
        
        # *** NOVO ÍNDICE PARA O CAMPO DE ML ***
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_objeto_compra ON contratacoes (objeto_compra);")
        
        conn.commit()
        conn.close()
        print("Banco de dados, tabela e índices configurados com sucesso.")
    except sqlite3.Error as e:
        print(f"ERRO ao configurar o DB: {e}")

if __name__ == "__main__":
    configurar_db()