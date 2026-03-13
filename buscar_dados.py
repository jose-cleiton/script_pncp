import json
import time

import salvar_dados
from fetch_retry import fetch_com_retry


def buscar_dados_paginados(url_completa, parametros, nome_db="pncp_data.db"):
    """
    Busca todos os dados da API PNCP usando paginação e salva no DB a cada iteração.
    (Versão atualizada para mostrar o progresso X de Y)
    """

    pagina_atual = 1
    total_registros_processados = 0

    # Variável para armazenar o total de páginas
    total_paginas_api = '???'  # Começa como desconhecido

    print("--- INICIANDO BUSCA PAGINADA ---")

    while True:
        # 1. Preparar parâmetros
        params_paginados = parametros.copy()
        params_paginados['pagina'] = pagina_atual

        print(f"\n>>>> BUSCANDO PÁGINA: {pagina_atual} de {total_paginas_api}")

        # 2. Chamar a função de busca (espera uma tupla (dados, total_paginas) ou None)
        retorno_fetch = fetch_com_retry(url_completa, params_paginados)

        # 3. Verificar condição de parada: None (erro)
        if retorno_fetch is None:
            print(f"Fim dos dados ou erro (fetch_com_retry retornou None) na página {pagina_atual}. FIM DA BUSCA.")
            break

        # Desempacotar a tupla
        dados_pagina, total_paginas_resposta = retorno_fetch

        # Atualizar o total de páginas se informado
        if total_paginas_resposta is not None:
            total_paginas_api = total_paginas_resposta

        # Verificar condição de parada: lista vazia
        if not dados_pagina:
            print(f"Fim dos dados (lista vazia recebida) na página {pagina_atual}. FIM DA BUSCA.")
            break

        # 4. PERSISTÊNCIA: Salvar no Banco de Dados
        salvar_dados.inserir_dados(nome_db, dados_pagina, pagina_atual)
        total_registros_processados += len(dados_pagina)

        # 5. Visualizar a página (em tempo real)
        print(f"Dados recebidos (Página {pagina_atual}/{total_paginas_api}, {len(dados_pagina)} registros):")
        json_formatado = json.dumps(dados_pagina[:3], indent=4, ensure_ascii=False)
        print(json_formatado)
        print("   -> (JSON truncado, salvamento completo e indexado no DB)")

        # 6. Incrementar e prosseguir
        pagina_atual += 1
        time.sleep(0.5)  # Pausa recomendada

    print("\n--- BUSCA CONCLUÍDA ---")
    print(f"Total de registros processados: {total_registros_processados}")
    return True
