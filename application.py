"""
application.py
Responsabilidade: montar e executar o pipeline a partir dos argumentos da CLI.
"""

from typing import Optional

from pipeline import PipelineConfig, PncpPipeline


class PncpApplication:
    """
    Responsabilidade: montar e executar o pipeline a partir dos
    argumentos da CLI.

    Recebe os argumentos já interpretados e delega toda a lógica de
    negócio ao PncpPipeline — não conhece detalhes de API, banco ou ML.
    """

    def __init__(
        self,
        etapa: Optional[str],
        data: Optional[str],
        provedor: str = "openai",
        modelo: Optional[str] = None,
        workers: int = 1,
    ) -> None:
        """
        Args:
            etapa:    Nome da etapa a executar, ou None para executar tudo.
            data:     Data no formato YYYY-MM-DD, ou None para usar hoje.
            provedor: Provedor LLM para classificar_gpt (openai ou gemini).
            modelo:   Modelo LLM. Se None, usa o padrão do provedor.
            workers:  Número de workers paralelos para classificar_gpt.
        """
        self._etapa = etapa
        self._provedor = provedor
        self._modelo = modelo
        self._workers = workers
        config = PipelineConfig() if data is None else PipelineConfig(data=data)
        self._pipeline = PncpPipeline(config)

    def executar(self) -> None:
        """Executa a etapa solicitada ou o pipeline completo."""
        if self._etapa is None:
            self._pipeline.executar_tudo()
        elif self._etapa == "classificar_gpt":
            self._pipeline.classificar_gpt(
                provedor=self._provedor,
                modelo=self._modelo,
                workers=self._workers,
            )
        else:
            acoes = {
                "coletar": lambda p: p.coletar(),
                "exportar": lambda p: p.exportar_para_rotular(),
                "treinar": lambda p: p.treinar(),
                "classificar": lambda p: p.classificar(),
            }
            acoes[self._etapa](self._pipeline)
