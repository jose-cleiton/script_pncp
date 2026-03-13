import json
import time

import requests


def fetch_com_retry(url, params, max_tentativas=5, backoff_inicial=1):
    """Busca dados da API com retry e backoff exponencial."""

    headers = {'accept': '*/*'}
    tentativas = 0
    backoff = backoff_inicial

    while tentativas < max_tentativas:
        try:
            resposta = requests.get(
                url, params=params, headers=headers, timeout=30
            )

            # 200 OK: Sucesso
            if resposta.status_code == 200:
                print("Código de resposta 200 (OK)")
                dados_resposta = resposta.json()

                if isinstance(dados_resposta, dict) and 'data' in dados_resposta:
                    dados_lista = dados_resposta.get('data', [])
                    total_paginas = dados_resposta.get('totalPaginas')
                    # Retorna uma TUPLA (dados, total_paginas)
                    return (dados_lista, total_paginas)
                else:
                    print("Resposta 200 OK, mas JSON em formato inesperado.")
                    return ([], None)

            # 204 No Content: Fim dos dados/paginação
            if resposta.status_code == 204:
                print("Status 204 (No Content). Fim dos dados.")
                return ([], None)

            # 400/422: Erro nos parâmetros, não adianta tentar de novo
            if resposta.status_code in [400, 422]:
                print(f"Erro de cliente {resposta.status_code}: {resposta.text}")
                return None

            # 500+: Erro de servidor, tenta de novo
            print(f"Erro {resposta.status_code}. Tentando novamente em {backoff}s...")

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}. Tentando novamente em {backoff}s...")
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON. Retornando None.")
            return None

        time.sleep(backoff)
        tentativas += 1
        backoff *= 2  # Backoff exponencial

    print("Número máximo de tentativas atingido. Falha ao buscar dados.")
    return None
