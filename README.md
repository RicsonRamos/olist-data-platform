# Olist E-Commerce Data Platform

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/analytics%20engineering-dbt--postgres-orange.svg)](https://www.getdbt.com/)
[![Database: PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-336791.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/container-Docker%20Compose-2496ED.svg)](https://www.docker.com/)
[![Code Quality: Ruff](https://img.shields.io/badge/lint-Ruff-black.svg)](https://github.com/astral-sh/ruff)
[![SQL Lint: SQLFluff](https://img.shields.io/badge/sql-SQLFluff-critical.svg)](https://sqlfluff.com/)
[![Dataset: Kaggle](https://img.shields.io/badge/dataset-Olist%20E--Commerce-informational.svg)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Plataforma moderna de engenharia analítica e Lakehouse híbrido projetada para transformar dados brutos de transações de e-commerce em fontes auditáveis da verdade, utilizando Python, dbt Core, PostgreSQL e Docker.

---

## Autor

- **Nome:** Ricson Ramos
- **Perfil GitHub:** [github.com/RicsonRamos](https://github.com/RicsonRamos)
- **LinkedIn:** [linkedin.com/in/ricsonramos](https://www.linkedin.com/in/ricsonramos/)
- **Kaggle:** [kaggle.com/ricsonramos](https://www.kaggle.com/ricsonramos)
- **Portfólio:** [ricsonramos.github.io](https://ricsonramos.github.io)
- **Contato:** ricsonramos@outlook.com

---

## Contexto de Negócio e Engenharia

Ambientes transacionais de e-commerce enfrentam desafios críticos de integridade: carregamentos batch redundantes, ausência de idempotência, dados duplicados e métricas financeiras conflitantes entre departamentos.

Esta plataforma foi desenvolvida para estabelecer resiliência operacional e governança técnica:
1. **Ingestão Idempotente:** Mecanismo baseado em hashing SHA-256 e tabela de controle de auditoria para garantir processamento único e tolerância a falhas.
2. **Arquitetura Medalhão Estruturada:**
   - **Inbound / Raw:** Captura e preservação dos arquivos brutos sem perdas.
   - **Lake (Parquet):** Armazenamento colunar de alta performance para cargas de trabalho de Ciência de Dados.
   - **Warehouse (Star Schema via dbt):** Transformações em camadas (staging, intermediate e marts dimensionais) no PostgreSQL.
3. **Auditoria e Confiabilidade:** 100% de rastreabilidade de cada registro desde o mart analítico até o arquivo de origem.
4. **Governança Automatizada:** Testes unitários com pytest, testes de integridade relacional com dbt, e validação de estilo SQL com SQLFluff integrados ao CI/CD.

---

## Dataset Oficial

- **Fonte no Kaggle:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (por Olist)
- **Volume:** ~100.000 pedidos entre 2016 e 2018 no mercado brasileiro.
- **Tabelas Principais:** Pedidos (`orders`), itens de pedido (`order_items`), pagamentos (`order_payments`), avaliações (`order_reviews`), clientes (`customers`), produtos (`products`) e vendedores (`sellers`).

---

## Estrutura do Repositório

```text
olist-data-platform/
├── config/                            # Parâmetros e caminhos de configuração
├── dbt/                               # Projeto analítico dbt Core
│   ├── models/
│   │   ├── staging/                   # Limpeza, tipagem e deduplicação de tabelas fonte
│   │   ├── intermediate/              # Agregações intermediárias e regras de negócio
│   │   └── marts/                     # Tabelas fato e dimensão (Star Schema)
│   └── dbt_project.yml                # Configurações do projeto dbt
├── infra/                             # Scripts de inicialização e infraestrutura
├── notebooks/
│   └── olist_customer_delivery_analytics.ipynb # Notebook executável para o Kaggle
├── pipeline/                          # Módulos Python de ingestão, auditoria e orquestração
│   ├── ingestion.py                   # Carga incremental com validação SHA-256
│   ├── orchestrator.py                # Máquina de estados e execução sequencial
│   ├── quality.py                     # Asserções de qualidade de dados
│   └── utils.py                       # Conexões de banco e logging
├── scripts/                           # Utilitários de consulta e verificação
├── tests/                             # Testes automatizados com pytest
├── docker-compose.yml                 # Orquestração do PostgreSQL e Metabase
├── pyproject.toml                     # Gestão de dependências moderna via uv
└── requirements.txt                   # Espelho para compatibilidade
```

---

## Como Executar

### Pré-requisitos

Instale o gerenciador [`uv`](https://docs.astral.sh/uv/):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 1. Clonar e Instalar Dependências

```bash
git clone https://github.com/RicsonRamos/olist-data-platform.git
cd olist-data-platform
uv sync --all-groups
```

### 2. Configurar o Ambiente

```bash
cp .env.example .env
```

### 3. Subir a Infraestrutura (PostgreSQL)

```bash
docker compose up -d postgres
```

### 4. Download Automático do Dataset Kaggle

Obtenha os arquivos CSV do Olist via `kagglehub` e mova-os para a pasta de ingestão:

```bash
uv run python -c "import kagglehub, shutil, os, glob; p = kagglehub.dataset_download('olistbr/brazilian-ecommerce'); dest = 'data/landing'; os.makedirs(dest, exist_ok=True); [shutil.copy(f, dest) for f in glob.glob(os.path.join(p, '*.csv'))]; print('Arquivos Olist prontos em data/landing/')"
```

### 5. Executar o Pipeline de Ingestão e Modelagem dbt

```bash
# Executa a ingestão incremental com auditoria
uv run python -m pipeline.run_pipeline

# Executa as transformações dimensionais no dbt
cd dbt && uv run dbt run && uv run dbt test
```

---

## Testes e Qualidade de Código

```bash
# Testes unitários com cobertura
uv run pytest

# Linting com Ruff
uv run ruff check .

# Linting SQL com SQLFluff
uv run sqlfluff lint dbt/models/
```

---

## Notebook no Kaggle

Acesse [`notebooks/olist_customer_delivery_analytics.ipynb`](./notebooks/olist_customer_delivery_analytics.ipynb) para executar a análise exploratória, avaliação de prazos de entrega vs. satisfação do cliente (NPS) e segmentação RFM (Recência, Frequência e Valor).
