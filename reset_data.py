"""
reset_data.py
Apaga todos os dados gerados pelo pipeline para uma data específica,
permitindo rodar o pipeline do zero novamente.

Uso:
    python reset_data.py                  # apaga a data de hoje
    python reset_data.py --data 2026-03-13
    python reset_data.py --data 2026-03-13 --confirmar
"""

import argparse
from pathlib import Path

from pipeline import PipelineConfig


class DataResetter:
    """
    Responsabilidade: localizar e remover os artefatos gerados pelo pipeline
    para uma data específica, deixando o ambiente limpo para um novo ciclo.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._alvos: list[Path] = [
            Path(config.caminho_db),
            Path(config.caminho_para_rotular),
            Path(config.caminho_dataset),
            Path(config.caminho_modelo),
            Path(config.caminho_resultado),
        ]

    def listar(self) -> list[Path]:
        """Retorna apenas os arquivos que existem no disco."""
        return [p for p in self._alvos if p.exists()]

    def resetar(self) -> None:
        """Remove todos os arquivos existentes e os diretórios que ficarem vazios."""
        existentes = self.listar()

        if not existentes:
            print("Nenhum arquivo encontrado para a data informada.")
            return

        removidos = 0
        for caminho in existentes:
            caminho.unlink()
            print(f"  ✗  {caminho}")
            removidos += 1
            self._remover_diretorio_se_vazio(caminho.parent)

        print(f"\n{removidos} arquivo(s) removido(s).")

    def _remover_diretorio_se_vazio(self, diretorio: Path) -> None:
        """Remove o diretório de data se não restar nenhum arquivo nele."""
        try:
            diretorio.rmdir()  # só remove se estiver vazio
        except OSError:
            pass  # ainda há arquivos — mantém o diretório


class ResetCli:
    """Responsabilidade: interpretar os argumentos e coordenar o reset."""

    def __init__(self) -> None:
        self._args = self._parse()

    def _parse(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Apaga os dados do pipeline para uma data específica.",
            epilog=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "--data",
            default=None,
            metavar="YYYY-MM-DD",
            help="Data a resetar. Padrão: data de hoje.",
        )
        parser.add_argument(
            "--confirmar",
            action="store_true",
            help="Pula a confirmação interativa e executa diretamente.",
        )
        return parser.parse_args()

    def executar(self) -> None:
        config = (
            PipelineConfig()
            if self._args.data is None
            else PipelineConfig(data=self._args.data)
        )
        resetter = DataResetter(config)
        existentes = resetter.listar()

        print(f"Data alvo: {config.data}\n")

        if not existentes:
            print("Nenhum arquivo encontrado. Nada a remover.")
            return

        print("Arquivos que serão removidos:")
        for caminho in existentes:
            print(f"  • {caminho}")

        if not self._args.confirmar:
            resposta = input("\nConfirma a remoção? [s/N] ").strip().lower()
            if resposta != "s":
                print("Operação cancelada.")
                return

        print()
        resetter.resetar()


def main() -> None:
    ResetCli().executar()


if __name__ == "__main__":
    main()
