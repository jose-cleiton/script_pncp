import sqlite3
import json
import datetime

def inserir_dados(nome_db, dados_pagina, pagina_coletada):
    """Insere uma lista de registros (uma página) no SQLite."""
    conn = None

    try:
        conn = sqlite3.connect(nome_db)
        cursor = conn.cursor()

        timestamp_atual = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        registros_inseridos = 0

        for registro in dados_pagina:
            registro_json_str = json.dumps(registro)

            cursor.execute("""
                INSERT OR IGNORE INTO contratacoes (pagina_coleta, timestamp_coleta, dados_json)
                VALUES (?, ?, ?);
            """, (pagina_coletada, timestamp_atual, registro_json_str))

            if cursor.rowcount > 0:
                registros_inseridos += 1

        conn.commit()
        print(f"   -> [DB] {registros_inseridos}/{len(dados_pagina)} registros salvos/atualizados na página {pagina_coletada}.")

    except sqlite3.Error as e:
        print(f"   -> [ERRO DB] Falha ao inserir dados: {e}. Verifique se o arquivo DB tem a estrutura correta.")

    finally:
        if conn:
            conn.close()