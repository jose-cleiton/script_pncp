"""
pncp_client.py
Responsabilidade: consultar a API do PNCP de forma paginada.
Não conhece SQLite, não salva dados, não orquestra fluxo de coleta.
"""
import time
from typing import Generator

from fetch_retry import fetch_com_retry


class PncpApiClient:
    """Encapsula a consulta paginada à API do PNCP."""

    def __init__(self, base_url: str, pausa_entre_paginas: float = 0.5) -> None:
        """
        Inicializa o cliente.

        Args:
            base_url: URL completa do endpoint (base + path).
            pausa_entre_paginas: Segundos de pausa entre requisições.
        """
        self.base_url = base_url
        self.pausa_entre_paginas = pausa_entre_paginas

    def buscar_pagina(self, params_base: dict, pagina: int) -> dict | None:
        """
        Consulta uma página específica da API.

        Args:
            params_base: Parâmetros base da requisição (sem 'pagina').
            pagina: Número da página desejada.

        Returns:
            Dict com chaves 'pagina', 'total_paginas', 'dados' e
            'quantidade_registros', ou None em caso de erro fatal.
        """
        params = params_base.copy()
        params['pagina'] = pagina

        retorno = fetch_com_retry(self.base_url, params)

        if retorno is None:
            return None

        dados, total_paginas = retorno
        return {
            'pagina': pagina,
            'total_paginas': total_paginas,
            'dados': dados,
            'quantidade_registros': len(dados),
        }

    def iterar_paginas(
        self, params_base: dict, pagina_inicial: int = 1
    ) -> Generator[dict, None, None]:
        """
        Itera pelas páginas da API até não haver mais dados.

        Yields:
            Dict retornado por buscar_pagina() para cada página com dados.
        """
        pagina_atual = pagina_inicial

        while True:
            resultado = self.buscar_pagina(params_base, pagina_atual)

            if resultado is None:
                print(f"[PncpApiClient] Erro na página {pagina_atual}. Encerrando iteração.")
                break

            if not resultado['dados']:
                print(f"[PncpApiClient] Página {pagina_atual} vazia. Fim dos dados.")
                break

            yield resultado

            pagina_atual += 1
            time.sleep(self.pausa_entre_paginas)
