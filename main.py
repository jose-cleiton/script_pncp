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

from application import PncpApplication
from cli_parser import CliParser


def main() -> None:
    """Ponto de entrada: lê a CLI e executa a aplicação."""
    args = CliParser().parse()
    PncpApplication(etapa=args.etapa, data=args.data).executar()


if __name__ == "__main__":
    main()
