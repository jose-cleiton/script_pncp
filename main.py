"""
main.py
Ponto de entrada da aplicação via linha de comando.

Uso:
    python main.py                          # todas as etapas
    python main.py --etapa coletar
    python main.py --etapa exportar
    python main.py --etapa treinar
    python main.py --etapa classificar
    python main.py --etapa treinar --data 2026-03-13
"""

import argparse
from typing import Optional

from pipeline import PipelineConfig, PncpPipeline

_ETAPAS_DISPONIVEIS = ("coletar", "exportar", "treinar", "classificar")


class CliParser:
    """Responsabilidade: definir e interpretar os argumentos da CLI."""

    def __init__(self) -> None:
        self._parser = argparse.ArgumentParser(
            description="Pipeline PNCP — coleta, rotulação, treino e classificação.",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=__doc__,
        )
        self._registrar_argumentos()

    def _registrar_argumentos(self) -> None:
        self._parser.add_argument(
            "--etapa",
            choices=_ETAPAS_DISPONIVEIS,
            default=None,
            help="Etapa a executar. Se omitido, executa todas em sequência.",
        )
        self._parser.add_argument(
            "--data",
            default=None,
            metavar="YYYY-MM-DD",
            help="Data de referência do pipeline. Padrão: data de hoje.",
        )

    def parse(self) -> argparse.Namespace:
        """Retorna os argumentos interpretados da linha de comando."""
        return self._parser.parse_args()


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


def main() -> None:
    """Ponto de entrada: lê a CLI e executa a aplicação."""
    args = CliParser().parse()
    PncpApplication(etapa=args.etapa, data=args.data).executar()


if __name__ == "__main__":
    main()
