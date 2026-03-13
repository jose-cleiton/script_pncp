"""
application.py
Responsabilidade: montar e executar o pipeline a partir dos argumentos da CLI.
"""

from typing import Optional

from pipeline import PipelineConfig, PncpPipeline


class PncpApplication:
    """
    Responsabilidade: montar e executar o pipeline a partir dos argumentos da CLI.

    Recebe os argumentos já interpretados e delega toda a lógica de negócio
    ao PncpPipeline — não conhece detalhes de API, banco ou ML.
    """

    _ACOES: dict = {
        "coletar": lambda p: p.coletar(),
        "exportar": lambda p: p.exportar_para_rotular(),
        "treinar": lambda p: p.treinar(),
        "classificar": lambda p: p.classificar(),
    }

    def __init__(self, etapa: Optional[str], data: Optional[str]) -> None:
        """
        Args:
            etapa: Nome da etapa a executar, ou None para executar tudo.
            data:  Data no formato YYYY-MM-DD, ou None para usar hoje.
        """
        self._etapa = etapa
        config = PipelineConfig() if data is None else PipelineConfig(data=data)
        self._pipeline = PncpPipeline(config)

    def executar(self) -> None:
        """Executa a etapa solicitada ou o pipeline completo."""
        if self._etapa is None:
            self._pipeline.executar_tudo()
        else:
            self._ACOES[self._etapa](self._pipeline)
