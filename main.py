"""
main.py
Ponto de entrada da aplicação via linha de comando.

Uso:
    python main.py                                        # todas as etapas
    python main.py --etapa coletar
    python main.py --etapa exportar
    python main.py --etapa treinar
    python main.py --etapa classificar
    python main.py --etapa classificar_gpt               # usa OpenAI por padrão
    python main.py --etapa classificar_gpt --provedor gemini
    python main.py --etapa classificar_gpt --provedor openai --modelo gpt-4o
    python main.py --etapa classificar_gpt --provedor gemini --workers 5
    python main.py --etapa treinar --data 2026-03-13
"""

from dotenv import load_dotenv

# Carrega .env para os.environ antes de qualquer outro import.
# Se o arquivo não existir, não faz nada.
# override=False garante que variáveis já definidas no shell têm prioridade.
load_dotenv(override=False)

from application import PncpApplication  # noqa: E402
from cli_parser import CliParser  # noqa: E402


def main() -> None:
    """Ponto de entrada: lê a CLI e executa a aplicação."""
    args = CliParser().parse()
    PncpApplication(
        etapa=args.etapa,
        data=args.data,
        provedor=args.provedor,
        modelo=args.modelo,
        workers=args.workers,
        modo=args.modo,
    ).executar()


if __name__ == "__main__":
    main()
