# Plataforma de Dados Olist
![CI - Data Platform Validation](https://github.com/RicsonRamos/olist-data-platform/actions/workflows/ci.yml/badge.svg)
### Construindo uma Fundação Confiável para Inteligência de E-commerce

---

## Declaração do Problema
Empresas de e-commerce operam em um ambiente de alta velocidade onde a integridade dos dados é frequentemente comprometida por limitações no processamento em lote. Pipelines de dados tradicionais frequentemente sofrem com carregamentos redundantes, métricas não confiáveis e falta de auditabilidade. Para a Olist, o desafio é transformar milhões de registros brutos de transações em uma "fonte da verdade" em que os líderes de negócio possam confiar, sem a sobrecarga de atualizações diárias completas do sistema.

## Por que este projeto existe
A maioria das plataformas de dados é construída para o "caminho feliz", falhando quando os dados chegam atrasados ou quando os sistemas travam no meio da execução. Este projeto foi projetado para demonstrar Resiliência Operacional. Ele vai além da simples movimentação de dados para criar um sistema inerentemente autorregenerativo, idempotente e observável.

Construímos isso para provar que princípios de engenharia de dados de nível empresarial — como processamento incremental e armazenamento em camadas — podem ser implementados com clareza e precisão, fornecendo um modelo para análises escaláveis.

## Visão Geral da Solução
A Plataforma de Dados Olist é um Lakehouse Híbrido que preenche a lacuna entre arquivos brutos não estruturados e insights analíticos estruturados. Utilizando uma Arquitetura Medalhão, ela limpa e promove sistematicamente os dados através de múltiplos estágios de maturidade:

1. Inbound: Captura de dados brutos com perda zero.
2. Archive: Preservação do histórico para total capacidade de reprocessamento do sistema.
3. Lake: Otimização para análise exploratória de alta performance.
4. Warehouse: Entrega de métricas prontas para o negócio para tomada de decisão estratégica.

## Principais Funcionalidades
* Ingestão Incremental Inteligente: Processa apenas dados novos ou alterados, reduzindo drasticamente os custos computacionais.
* Idempotência do Sistema: Seguro para rodar a qualquer momento; dados idênticos são ignorados automaticamente.
* Auditabilidade de Ponta a Ponta: Cada registro é rastreável até seu arquivo de origem e lote de ingestão.
* Camada Analítica Desacoplada: Armazenamento baseado em arquivos de alta performance para cargas de trabalho de Ciência de Dados.
*   **Modelo Semântico de BI**: Camada de consumo preparada para ferramentas como Metabase e Power BI, garantindo governança e clareza.
*   **Qualidade Automatizada (CI/CD)**: Testes unitários e funcionais integrados com **80%+ de cobertura de código**, garantindo resiliência contra quebras.

## Impacto de Negócio e KPIs
| Objetivo | Indicador de Impacto |
| :--- | :--- |
| Confiabilidade | Zero registros duplicados na camada final de relatórios. |
| Eficiência Operacional | Redução de aproximadamente 90% no tempo de processamento via lógica incremental vs carregamento completo. |
| Integridade de Dados | 100% de auditabilidade desde o dashboard final até o CSV de origem. |
| Resiliência | Perda zero de dados durante falhas de execução parcial via segurança transacional. |

## Como Começar

### Instalação
```powershell
# Instalação básica (Core)
pip install .

# Instalação para desenvolvimento e testes (Recomendado)
pip install -e ".[dev]"
```

### Execução
1. **Rodar Pipeline**: `python -m pipeline.run_pipeline`
2. **Rodar Testes**: `pytest`
3. **Linting**: `ruff check .`

### Documentação Adicional
*   [Guia Power BI](./POWERBI_GUIDE.md)
*   [Guia Metabase](./METABASE_GUIDE.md)
*   [Mergulho Técnico (Arquitetura & Qualidade)](./DOCUMENTATION.md)

## Stack Tecnológica
* Infraestrutura: Docker, PostgreSQL 15
* Engenharia de Dados: Python 3.9+, Pandas
* Modelagem: dbt (Data Build Tool)
* Armazenamento: Apache Parquet, Sistema de Arquivos (Particionado)

---
Estudo de Caso de Engenharia de Dados — Focado em Corretidão, Valor de Negócio e Narrativa.
