"""
preprocessar.py
Responsabilidade: limpar e normalizar textos para uso em modelos de ML.
Não conhece banco de dados, não conhece API, não treina modelos.
"""
import re
from typing import Optional

from nltk.corpus import stopwords

_STOPWORDS_FALLBACK = ['a', 'o', 'e', 'de', 'para', 'com', 'um', 'uma']
_TAMANHO_MINIMO_TOKEN = 2


def _carregar_stopwords() -> set:
    """Carrega stopwords em português com fallback seguro."""
    try:
        return set(stopwords.words('portuguese'))
    except LookupError:
        print("AVISO: Stopwords não carregadas. Usando lista mínima de fallback.")
        return set(_STOPWORDS_FALLBACK)


class TextPreprocessor:
    """Limpa e normaliza textos em português para uso em pipelines de ML."""

    def __init__(self, stop_words: Optional[set] = None) -> None:
        """
        Inicializa o preprocessador.

        Args:
            stop_words: Conjunto de stopwords customizado. Se None, carrega
                        automaticamente as stopwords em português do NLTK.
        """
        self.stop_words = stop_words if stop_words is not None else _carregar_stopwords()

    def processar(self, texto: str) -> str:
        """
        Limpa e normaliza um texto para uso em modelos de ML.

        Etapas aplicadas:
        - Converte para minúsculas
        - Remove números, pontuação e caracteres especiais
        - Tokeniza via Regex (sem dependência do NLTK punkt)
        - Remove stopwords e tokens com 2 caracteres ou menos

        Args:
            texto: Texto bruto de entrada.

        Returns:
            Texto limpo e normalizado, ou string vazia se entrada inválida.
        """
        if not isinstance(texto, str):
            return ""

        texto = texto.lower()
        texto = re.sub(r'[^a-zÀ-ú\s]', ' ', texto, flags=re.IGNORECASE)
        texto = re.sub(r'\s+', ' ', texto).strip()

        tokens = re.findall(r'\b[a-zÀ-ú]+\b', texto)
        tokens = [
            t for t in tokens
            if t not in self.stop_words and len(t) > _TAMANHO_MINIMO_TOKEN
        ]

        return " ".join(tokens)


# --- Instância e fachadas de módulo: preservam compatibilidade com o notebook ---

stop_words_pt: set = _carregar_stopwords()
_preprocessor = TextPreprocessor(stop_words=stop_words_pt)


def preprocessar_texto(texto: str) -> str:
    """
    Fachada que preserva a assinatura original.
    Usa a instância padrão de TextPreprocessor.

    Args:
        texto: Texto bruto de entrada.

    Returns:
        Texto limpo e normalizado.
    """
    return _preprocessor.processar(texto)
