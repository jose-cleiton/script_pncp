"""
cli_parser.py
Responsabilidade: definir e interpretar os argumentos da CLI.
"""

import argparse

_ETAPAS_DISPONIVEIS = (
    "coletar",
    "exportar",
    "treinar",
    "classificar",
    "classificar_gpt",
)

_PROVEDORES_DISPONIVEIS = ("openai", "gemini")


class CliParser:
    """Responsabilidade: definir e interpretar os argumentos da CLI."""

    def __init__(self) -> None:
        self._parser = argparse.ArgumentParser(
            description=("Pipeline PNCP — coleta, rotulação, treino e classificação."),
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
        self._parser.add_argument(
            "--provedor",
            choices=_PROVEDORES_DISPONIVEIS,
            default="openai",
            help=(
                "Provedor LLM para a etapa classificar_gpt. "
                "Padrão: openai. Opções: openai, gemini."
            ),
        )
        self._parser.add_argument(
            "--modelo",
            default=None,
            metavar="NOME_DO_MODELO",
            help=(
                "Modelo a usar na etapa classificar_gpt. "
                "Se omitido, usa o padrão do provedor "
                "(gpt-4.1 para OpenAI, gemini-2.0-flash para Gemini)."
            ),
        )
        self._parser.add_argument(
            "--workers",
            type=int,
            default=1,
            metavar="N",
            help=(
                "Número de workers paralelos para classificar_gpt. "
                "1 = sequencial (padrão). "
                "Aumente com cuidado para não exceder o rate limit da API."
            ),
        )

    def parse(self) -> argparse.Namespace:
        """Retorna os argumentos interpretados da linha de comando."""
        return self._parser.parse_args()
