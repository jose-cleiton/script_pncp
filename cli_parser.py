"""
cli_parser.py
Responsabilidade: definir e interpretar os argumentos da CLI.
"""

import argparse

_ETAPAS_DISPONIVEIS = ("coletar", "exportar", "treinar", "classificar")


class CliParser:
    """Responsabilidade: definir e interpretar os argumentos da CLI."""

    def __init__(self) -> None:
        self._parser = argparse.ArgumentParser(
            description="Pipeline PNCP — coleta, rotulação, treino e classificação.",
            formatter_class=argparse.RawDescriptionHelpFormatter,
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
