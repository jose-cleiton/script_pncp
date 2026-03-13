import re
from nltk.corpus import stopwords

# --- Carregamento das Stopwords ---
try:
    lista_stopwords = stopwords.words('portuguese')
except LookupError:
    print("AVISO: Stopwords não carregadas. Usando lista mínima de fallback.")
    lista_stopwords = ['a', 'o', 'e', 'de', 'para', 'com', 'um', 'uma']

stop_words_pt = set(lista_stopwords)


def preprocessar_texto(texto: str) -> str:
    """
    Limpa e normaliza um texto para uso em modelos de ML.
    - Converte para minúsculas
    - Remove números, pontuação e caracteres especiais
    - Tokeniza usando Regex (sem dependência de NLTK punkt)
    - Remove stopwords em português e palavras com 2 caracteres ou menos
    """
    if not isinstance(texto, str):
        return ""

    # Minúsculas
    texto = texto.lower()

    # Remove tudo que não for letra ou acentuação
    texto = re.sub(r'[^a-zÀ-ú\s]', ' ', texto, flags=re.IGNORECASE)

    # Remove múltiplos espaços
    texto = re.sub(r'\s+', ' ', texto).strip()

    # Tokenização via Regex (substitui word_tokenize do NLTK)
    tokens = re.findall(r'\b[a-zÀ-ú]+\b', texto)

    # Remove stopwords e palavras curtas
    tokens = [palavra for palavra in tokens if palavra not in stop_words_pt and len(palavra) > 2]

    return " ".join(tokens)
