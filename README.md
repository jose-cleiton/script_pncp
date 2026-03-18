# 🏛️ PNCP Data Miner

Pipeline de dados em Python que **coleta licitações públicas** da API do
[Portal Nacional de Contratações Públicas (PNCP)](https://pncp.gov.br),
armazena localmente e usa **Machine Learning** para filtrar automaticamente
apenas as licitações relevantes para você.

> **Nunca programou antes?** Sem problema. Cada passo abaixo é explicado
> em detalhes. Siga a ordem e você conseguirá rodar o projeto.

---

## 📋 Índice

1. [O que este projeto faz](#1-o-que-este-projeto-faz)
2. [Pré-requisitos](#2-pré-requisitos)
3. [Instalação passo a passo](#3-instalação-passo-a-passo)
4. [Configuração do ambiente (.env)](#4-configuração-do-ambiente-env)
5. [Como rodar](#5-como-rodar)
6. [Entendendo as etapas do pipeline](#6-entendendo-as-etapas-do-pipeline)
7. [Estrutura de arquivos](#7-estrutura-de-arquivos)
8. [Usando o Notebook (modo alternativo)](#8-usando-o-notebook-modo-alternativo)
9. [Solução de problemas comuns](#9-solução-de-problemas-comuns)

---

## 1. O que este projeto faz

```
API PNCP  →  Banco de dados  →  Modelo de ML  →  CSV com licitações relevantes
```

| Etapa | O que acontece |
|---|---|
| **Coleta** | Consulta a API do PNCP e salva todas as licitações em um banco SQLite local |
| **Exportar** | Gera um CSV com as licitações para você rotular (marcar como relevante ou não) |
| **Treinar** | Usa os seus rótulos para treinar um modelo de Machine Learning |
| **Classificar** | Aplica o modelo em todas as licitações e gera um CSV só com as relevantes |

---

## 2. Pré-requisitos

Você precisará ter instalado na sua máquina:

- **Python 3.11 ou superior**
  - Para verificar: abra o terminal e digite `python3 --version`
  - Para instalar: [python.org/downloads](https://www.python.org/downloads/)
- **Git** (para clonar o projeto)
  - Para verificar: `git --version`
  - Para instalar: [git-scm.com](https://git-scm.com)
- **Conexão com a internet** (para acessar a API do PNCP)

> **Windows?** Use o **PowerShell** ou o **Terminal do VS Code**.  
> **Mac/Linux?** Use o **Terminal** normalmente.

---

## 3. Instalação passo a passo

### Passo 1 — Clone o projeto

```bash
git clone https://github.com/jose-cleiton/script_pncp.git
cd script_pncp
```

### Passo 2 — Crie o ambiente virtual (`.venv`)

O ambiente virtual isola as dependências do projeto para não interferir
com outros projetos Python da sua máquina.

```bash
python3 -m venv .venv
```

Você verá uma pasta `.venv` aparecer no projeto. Isso é normal.

### Passo 3 — Ative o ambiente virtual

Você precisa **ativar o ambiente a cada nova sessão de terminal**.

**Mac / Linux:**
```bash
source .venv/bin/activate
```

**Windows (PowerShell):**
```powershell
.venv\Scripts\Activate.ps1
```

**Windows (CMD):**
```cmd
.venv\Scripts\activate.bat
```

> ✅ Quando ativado, o terminal mostrará `(.venv)` no início da linha.

### Passo 4 — Instale as dependências

```bash
pip install -r requirements.txt
```

Isso instalará todas as bibliotecas necessárias (pandas, scikit-learn, etc.).
Aguarde a instalação terminar.

### Passo 5 — Baixe os dados do NLTK

O projeto usa o NLTK para processar texto em português. Rode **uma única vez**:

```bash
python -c "import nltk; nltk.download('stopwords')"
```

---

## 4. Configuração do ambiente (`.env`)

O projeto já vem com um arquivo `.env.example` com os valores padrão.
**Copie-o para `.env`:**

```bash
# Mac / Linux
cp .env.example .env

# Windows (PowerShell)
copy .env.example .env
```

> Na configuração atual, **não é necessário alterar nada** no `.env`.
> Os valores padrão já apontam para a API pública do PNCP com a
> modalidade Pregão Eletrônico (código 6).

O arquivo `.env` **não sobe para o Git** (está no `.gitignore`) —
use-o para guardar configurações sensíveis sem expô-las publicamente.

---

## 5. Como rodar

Com o ambiente virtual ativado (`source .venv/bin/activate`), use o
arquivo `main.py` para executar cada etapa:

### Rodar tudo de uma vez

```bash
python main.py
```

> ⚠️ Só use após ter um `dataset.csv` rotulado (veja seção 6.2).

### Rodar etapa por etapa ✅ recomendado na primeira vez

```bash
# 1. Coleta dados da API e salva no banco
python main.py --etapa coletar

# 2. Exporta licitações do banco para você rotular
python main.py --etapa exportar

# ... rotule o arquivo gerado (veja seção 6.2) ...

# 3. Treina o modelo com seus rótulos
python main.py --etapa treinar

# 4. Classifica todas as licitações e gera o resultado
python main.py --etapa classificar
```

### Ver todas as opções disponíveis

```bash
python main.py --help
```

### Especificar uma data diferente

Por padrão, tudo é salvo na pasta da **data de hoje** (`data/.../YYYY-MM-DD/`).
Para usar outra data:

```bash
python main.py --etapa treinar --data 2026-03-13
```

### Resetar os dados e começar do zero

```bash
python reset_data.py                    # reseta a data de hoje
python reset_data.py --data 2026-03-13  # reseta uma data específica
python reset_data.py --confirmar        # sem confirmação interativa
```

---

## 6. Entendendo as etapas do pipeline

### 6.1 Etapa 1 — Coleta (`--etapa coletar`)

Consulta a API do PNCP e salva os dados brutos em um banco SQLite local.

- **Entrada:** API pública do PNCP
- **Saída:** `data/coleta/YYYY-MM-DD/pncp_data.db`

O script coleta automaticamente **todas as páginas** disponíveis e
trata falhas de conexão com tentativas automáticas (backoff exponencial).

---

### 6.2 Etapa 2 — Exportar (`--etapa exportar`)

Extrai licitações do banco e gera um arquivo CSV para você rotular.

- **Entrada:** `data/coleta/YYYY-MM-DD/pncp_data.db`
- **Saída:** `data/rotulacao/YYYY-MM-DD/dataset_para_rotular.csv`

**Como rotular o arquivo gerado:**

1. Abra `dataset_para_rotular.csv` no **Excel**, **LibreOffice** ou **Google Sheets**
2. Para cada linha, preencha a coluna `Relevante`:
   - **`1`** → a licitação **é relevante** para você
   - **`0`** → a licitação **não é relevante**
3. Salve o arquivo preenchido como:
   `data/treinamento/YYYY-MM-DD/dataset.csv`

> ⚠️ Você precisa ter **pelo menos 10 linhas com `1`** e
> **10 linhas com `0`** para o modelo aprender corretamente.

---

### 6.3 Etapa 3 — Treinar (`--etapa treinar`)

Usa o seu `dataset.csv` para treinar um modelo de Machine Learning
que aprende a reconhecer o que é relevante para você.

- **Entrada:** `data/treinamento/YYYY-MM-DD/dataset.csv`
- **Saída:** `data/treinamento/YYYY-MM-DD/modelo_relevancia.joblib`

Ao final, exibe a acurácia e um relatório de qualidade do modelo.

> 💡 Se o `dataset.csv` da data atual não existir, o sistema
> usa automaticamente o mais recente disponível.

---

### 6.4 Etapa 4 — Classificar (`--etapa classificar`)

Aplica o modelo treinado em **todas** as licitações do banco e
salva apenas as relevantes em um CSV final.

- **Entrada:** modelo `.joblib` + banco `.db`
- **Saída:** `data/resultado/YYYY-MM-DD/licitacoes_filtradas.csv`

---

## 7. Estrutura de arquivos

```
script_pncp/
│
├── main.py                       ← ponto de entrada via terminal
├── pipeline.py                   ← orquestrador com todas as etapas
├── reset_data.py                 ← limpa dados de uma data específica
│
├── pncp_client.py                ← consulta paginada à API do PNCP
├── fetch_retry.py                ← requisições HTTP com retry automático
├── buscar_dados.py               ← coleta + persistência no banco
├── salvar_dados.py               ← inserção de registros no SQLite
├── configurar_db.py              ← criação da tabela e índices
├── preprocessar.py               ← limpeza de texto para ML
│
├── script_pncp_preliminar.ipynb  ← notebook interativo (modo alternativo)
│
├── data/                         ← dados gerados (não sobe pro Git)
│   ├── coleta/
│   │   └── YYYY-MM-DD/
│   │       └── pncp_data.db
│   ├── rotulacao/
│   │   └── YYYY-MM-DD/
│   │       └── dataset_para_rotular.csv
│   ├── treinamento/
│   │   └── YYYY-MM-DD/
│   │       ├── dataset.csv
│   │       └── modelo_relevancia.joblib
│   └── resultado/
│       └── YYYY-MM-DD/
│           └── licitacoes_filtradas.csv
│
├── .env                          ← suas configurações locais (não sobe pro Git)
├── .env.example                  ← modelo do .env (sobe pro Git)
├── .gitignore
├── requirements.txt              ← dependências do projeto
└── README.md
```

---

## 8. Usando o Notebook (modo alternativo)

Se preferir um ambiente interativo com explicações visuais célula por célula:

```bash
# Com o .venv ativado:
jupyter notebook script_pncp_preliminar.ipynb
```

O notebook e o `main.py` são **independentes e equivalentes** — você pode
usar qualquer um, ou os dois. As células do notebook executam exatamente
a mesma lógica que as etapas do `main.py`.

---

## 9. Solução de problemas comuns

### ❌ `zsh: command not found: python`

No Mac, o comando padrão é `python3`. Com o `.venv` ativado, use `python`.

```bash
source .venv/bin/activate  # ativa o venv
python main.py             # agora funciona
```

---

### ❌ `ModuleNotFoundError: No module named 'X'`

As dependências não foram instaladas. Com o venv ativado:

```bash
pip install -r requirements.txt
```

---

### ❌ `FileNotFoundError: dataset.csv`

Você pulou a etapa de rotulação. Siga a ordem correta:

```bash
python main.py --etapa exportar
# abra data/rotulacao/YYYY-MM-DD/dataset_para_rotular.csv
# preencha a coluna Relevante com 1 ou 0
# salve como data/treinamento/YYYY-MM-DD/dataset.csv
python main.py --etapa treinar
```

---

### ❌ `The number of classes has to be greater than one`

O `dataset.csv` tem apenas uma classe. Adicione exemplos das duas:
pelo menos 10 linhas com `Relevante=1` e 10 com `Relevante=0`.

---

### ❌ `ConnectionError` ou erro de SSL

Verifique sua internet. A API tenta automaticamente até 5 vezes.
Se persistir, aguarde alguns minutos e tente novamente.

---

## 📚 Tecnologias utilizadas

| Biblioteca | Para que serve |
|---|---|
| `requests` | Requisições HTTP à API do PNCP |
| `sqlite3` | Banco de dados local (nativo do Python) |
| `pandas` | Manipulação de dados e leitura de CSV |
| `scikit-learn` | TF-IDF e modelo de classificação SGD |
| `joblib` | Salvar e carregar o modelo treinado |
| `nltk` | Stopwords em português para limpeza de texto |
| `jupyter` | Notebook interativo (modo alternativo) |

---

## 📄 Licença

Uso livre para fins educacionais e de pesquisa.  
Os dados coletados são públicos, disponibilizados pelo
[Portal Nacional de Contratações Públicas](https://pncp.gov.br).
