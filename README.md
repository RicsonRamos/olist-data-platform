# Olist E-Commerce Data Platform

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Storage: Parquet](https://img.shields.io/badge/lake-Apache%20Parquet-informational.svg)](https://parquet.apache.org/)
[![Database: PostgreSQL](https://img.shields.io/badge/warehouse-PostgreSQL%2016-336791.svg)](https://www.postgresql.org/)
[![Transformation: dbt](https://img.shields.io/badge/transform-dbt%20Core-FF694B.svg)](https://www.getdbt.com/)
[![CI / CD](https://img.shields.io/badge/ci%2fcd-GitHub%20Actions-brightgreen.svg)](https://github.com/RicsonRamos/olist-data-platform/actions)
[![Dataset: Kaggle](https://img.shields.io/badge/dataset-Olist%20E--Commerce-informational.svg)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Plataforma moderna de engenharia analítica e Data Lakehouse construída para o processamento robusto, idempotente e governado do conjunto público de transações do e-commerce brasileiro (Olist).

---

## 1. Problema de Negócio e Contexto

Em operações de e-commerce com dezenas de milhares de pedidos diários, dados brutos transacionais frequentemente sofrem com:
1. **Falta de Idempotência e Duplicações:** Falhas parciais em rotinas de extração geram re-ingestões com dados duplicados, corrompendo métricas críticas de faturamento e volume de pedidos.
2. **Inconsistência Relacional e Dados Órfãos:** Registros de pedidos sem correspondência de clientes, vendedores ou avaliações, inviabilizando relatórios confiáveis para áreas de negócio.
3. **Alto Custo Computacional de Consultas:** Consultar arquivos CSV ou bancos transacionais (OLTP) diretamente com queries analíticas complexas degrada os sistemas operacionais e custa caro em nuvem.

O objetivo deste projeto foi implantar uma plataforma de dados corporativa que:
- Realiza a ingestão controlada de arquivos de origem com auditoria SHA-256 para evitar duplicidade.
- Estrutura os dados segundo a Arquitetura Medalhão (Bronze, Silver e Gold).
- Transforma e valida a camada analítica com dbt Core sobre um Data Warehouse PostgreSQL em esquema Star Schema.
- Automatiza todo o ciclo com esteira de CI/CD 100% conteinerizada.

---

## 2. Processo e Racional das Escolhas Técnicas

### 2.1. Arquitetura Medalhão Híbrida
- **Bronze (Raw Inbound):** Preservação dos dados originais sem modificações para permitir repropagações futuras (*replayability*).
- **Silver (Clean Lake em Apache Parquet):** Conversão de formatos textuais para colunar Parquet particionado, com tipagem estrita de datas, geolocalização e monetários, gerando compressão de armazenamento e velocidade em scans.
- **Gold (Star Schema no Data Warehouse):** Modelagem dimensional formal (`fct_orders`, `fct_order_items`, `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`) implementada via dbt Core, facilitando o consumo por ferramentas de BI e analistas de negócio.

### 2.2. Garantia de Idempotência via Hashing SHA-256
- **Auditoria de Carga:** Antes de qualquer processamento, o pipeline calcula o hash criptográfico SHA-256 do arquivo e o valida contra a tabela de controle de auditoria de metadados. Lotes previamente processados são bloqueados imediatamente, eliminando duplicidade por re-execuções manuais ou re-tentativas de pipelines.

### 2.3. Governança e Testes Automatizados com dbt
- **Contratos e Testes de Qualidade:** Configuração de testes estruturais dbt (`unique`, `not_null`, `relationships`) para assegurar que não haja integridade referencial violada entre tabelas de dimensão e fatos.
- **Padronização SQL com SQLFluff:** Código SQL formatado segundo padrões rigorosos de engenharia analítica.

---

## 3. Resultados Atingidos e Métricas de Validação

### 3.1. Confiabilidade e Governança de Dados
- **Zero Duplicações Registradas:** Garantia absoluta de unicidade em chaves primárias e de negócio em todos os marts da camada Gold.
- **100% de Testes dbt Validados:** Validação automatizada de integridade referencial cobrindo todos os relacionamentos entre clientes, pedidos e itens vendidos.

### 3.2. Performance e Qualidade de Código
- **Esteira de CI/CD Verde:** Workflows de validação contínua (Ruff, Pytest com psycopg3, dbt e Docker) executando e passando com sucesso no GitHub Actions.
- **Cobertura de Testes de Código:** Testes unitários para utilitários de banco, loaders e hashing de arquivos cobrindo cenários com sucesso e falha.

---

## 4. Arquitetura da Solução e Estrutura

```text
olist-data-platform/
├── .github/workflows/          # Esteiras de CI (testes) e CD (build Docker)
├── dbt/                        # Projeto dbt Core com modelos dimensionais
│   ├── models/
│   │   ├── staging/            # Camada Silver (renomeação, casts e limpeza)
│   │   └── marts/              # Camada Gold (fatos e dimensões no Star Schema)
│   └── tests/                  # Testes customizados de integridade analítica
├── pipeline/                   # Código fonte Python do pipeline ETL
│   ├── download_kaggle.py      # Ingestão programática via kagglehub
│   ├── utils.py                # Conectores de banco de dados e auditoria de hash
│   ├── bronze.py               # Ingestão e validação bruta
│   ├── silver.py               # Conversão para Parquet colunar e deduplicação
│   ├── gold.py                 # Carga relacional e orquestração do dbt
│   └── orchestration.py        # Orquestrador do fluxo ponta a ponta
├── tests/                      # Bateria de testes com pytest
├── Dockerfile                  # Imagem Docker multi-stage
├── pyproject.toml              # Dependências e metadados com uv
└── ruff.toml                   # Parametrização do linter Ruff
```

---

## 5. Como Executar

### 5.1. Pré-requisitos
- Python 3.10 ou superior
- Docker e Docker Compose (para banco de dados PostgreSQL)
- Gerenciador `uv` ou `pip`

### 5.2. Execução Local com Banco Docker

1. Clone o repositório:
```bash
git clone https://github.com/RicsonRamos/olist-data-platform.git
cd olist-data-platform
```

2. Crie e ative o ambiente virtual:
```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

3. Suba o banco de dados PostgreSQL via Docker Compose:
```bash
docker compose up -d postgres
```

4. Execute a esteira de testes automatizados e o linter:
```bash
ruff check .
pytest
```

5. Execute o pipeline completo de engenharia de dados:
```bash
python -m pipeline.orchestration
```

6. Execute os testes do dbt:
```bash
cd dbt
dbt deps
dbt test
```

---

## Autor

- **Nome:** Ricson Ramos
- **Perfil GitHub:** [github.com/RicsonRamos](https://github.com/RicsonRamos)
- **LinkedIn:** [linkedin.com/in/ricsonramos](https://www.linkedin.com/in/ricsonramos/)
- **Kaggle:** [kaggle.com/ricsonramos](https://www.kaggle.com/ricsonramos)
- **Portfólio:** [ricsonramos.github.io](https://ricsonramos.github.io)
