"""
buscar_dados.py
Responsabilidade: orquestrar a coleta paginada e a persistência no banco.
Usa PncpApiClient para consultar a API e salvar_dados para persistir.
"""
import json

import salvar_dados
from pncp_client import PncpApiClient


class PncpColetorService:
    """Orquestra a coleta paginada da API PNCP e a persistência no banco."""

    def __init__(self, cliente: PncpApiClient, nome_db: str = "pncp_data.db") -> None:
        """
        Inicializa o serviço de coleta.

        Args:
            cliente: Instância de PncpApiClient responsável pelas requisições.
            nome_db: Caminho do arquivo SQLite onde os dados serão salvos.
        """
        self.cliente = cliente
        self.nome_db = nome_db

    def _log_pagina(self, resultado: dict) -> None:
        """Exibe resumo da página recebida (primeiros 3 itens)."""
        pagina = resultado['pagina']
        total_paginas = resultado['total_paginas'] or '???'
        dados = resultado['dados']
        resumo = json.dumps(dados[:3], indent=4, ensure_ascii=False)
        print(f"Página {pagina}/{total_paginas} — {len(dados)} registros:")
        print(resumo)
        print("   -> (JSON truncado, salvamento completo e indexado no DB)")

    def processar_pagina(self, resultado: dict) -> int:
        """
        Persiste os dados de uma página e exibe log resumido.

        Args:
            resultado: Dict retornado por PncpApiClient.buscar_pagina().

        Returns:
            Quantidade de registros na página processada.
        """
        dados = resultado['dados']
        salvar_dados.inserir_dados(self.nome_db, dados, resultado['pagina'])
        self._log_pagina(resultado)
        return len(dados)

    def executar_coleta(self, params_base: dict, pagina_inicial: int = 1) -> int:
        """
        Executa a coleta paginada completa.

        Args:
            params_base: Parâmetros base da requisição (sem 'pagina').
            pagina_inicial: Página a partir da qual iniciar a coleta.

        Returns:
            Total de registros processados.
        """
        total_registros = 0
        print("--- INICIANDO BUSCA PAGINADA ---")

        for resultado in self.cliente.iterar_paginas(params_base, pagina_inicial):
            pagina = resultado['pagina']
            total_paginas = resultado['total_paginas'] or '???'
            print(f"\n>>>> PROCESSANDO PÁGINA: {pagina} de {total_paginas}")
            total_registros += self.processar_pagina(resultado)

        print("\n--- BUSCA CONCLUÍDA ---")
        print(f"Total de registros processados: {total_registros}")
        return total_registros


# --- Fachada: mantém compatibilidade com o notebook e código existente ---

def buscar_dados_paginados(
    url_completa: str,
    parametros: dict,
    nome_db: str = "pncp_data.db",
) -> bool:
    """
    Fachada que preserva a assinatura original.
    Instancia PncpApiClient e PncpColetorService e executa a coleta.

    Args:
        url_completa: URL completa do endpoint da API.
        parametros: Parâmetros base da requisição.
        nome_db: Caminho do arquivo SQLite.

    Returns:
        True ao concluir.
    """
    cliente = PncpApiClient(base_url=url_completa)
    servico = PncpColetorService(cliente=cliente, nome_db=nome_db)
    servico.executar_coleta(params_base=parametros)
    return True
