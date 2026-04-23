# Documentação Técnica: Plataforma de Dados Olist
### Especificação de Design e Implementação do Sistema

Este documento detalha a mecânica interna, decisões de design e trade-offs arquiteturais da Plataforma de Dados Olist.

---

## 1. Arquitetura do Sistema

A plataforma foi projetada como uma Simulação Local de Lakehouse, implementando a Arquitetura Medalhão para separar responsabilidades em quatro camadas lógicas:

### 1.1. Camadas de Dados (Padrão Medalhão)
* Raw (Banco de Dados - Transiente): Recebe cargas de lote brutas do worker de ingestão. Atua como uma área de staging de alta velocidade para o dbt.
* Bronze (Sistema de Arquivos - Arquivo): Armazenamento imutável particionado por data para os arquivos CSV/Zip originais. Permite recuperação de desastres e reprocessamento histórico completo.
* Silver (Sistema de Arquivos - Parquet): Armazenamento colunar otimizado para leituras analíticas. Independente do banco de dados, consultável via DuckDB/Spark.
* Gold (Banco de Dados - Marts): Esquemas relacionais finais (staging, marts, consumption) gerenciados pelo dbt. Implementa regras de negócio e integridade referencial.

---

## 2. Ingestão de Dados e Idempotência

### 2.1. Idempotência Baseada em Hash (SHA-256)
Para garantir o processamento de arquivo exatamente uma vez (exactly-once), o worker de ingestão realiza uma verificação baseada no conteúdo:
1. Geração de Checksum: Computa um hash SHA-256 do arquivo completo na pasta data/landing/.
2. Verificação de Checkpoint: Valida o hash contra a tabela metadata.processed_files.
3. Tratamento de Conflito: Se o hash já existir, o arquivo é pulado. Isso previne ingestão duplicada independentemente do nome do arquivo ou frequência de execução.

### 2.2. Metadados e Rastreamento de Linhagem
Cada registro é enriquecido com colunas técnicas durante a carga na camada Raw:
* _metadata_ingested_at: Timestamp UTC do job de ingestão.
* _metadata_source_file: Nome do arquivo de origem para linhagem.
* _metadata_file_hash: Identificador único do lote para correlacionar linhas do banco com o arquivo na camada Bronze.

---

## 3. Estratégia de Transformação Incremental

### 3.1. dbt Incremental (Simulação de Merge)
A plataforma evita atualizações completas (Full Refreshes) no Data Warehouse utilizando a materialização incremental do dbt:
* Watermarking: Os modelos filtram os dados de origem usando _metadata_ingested_at > (select max(_metadata_ingested_at) from {{ this }}).
* Padrão de Merge no Postgres: Como o PostgreSQL não possui um comando MERGE nativo de alta performance para este contexto, o dbt implementa um padrão DELETE + INSERT seguro transacionalmente baseado em uma unique_key. Isso garante que atualizações (ex: mudanças no status do pedido) sejam refletidas sem duplicação.

---

## 4. Ciclo de Vida de Execução do Pipeline

O orquestrador (run_pipeline.py) executa um DAG sequencial com os seguintes estágios:

1. Bootstrap de Infra: Executa o docker compose seguido por um loop de verificação de saúde (healthcheck) do PostgreSQL (30 tentativas).
2. Fan-out de Ingestão: O worker Python realiza uma única leitura e entrega paralela para Bronze (Arquivo), Silver (Parquet) e Raw (Banco).
3. Checkpoint de Auditoria: O hash do arquivo de entrada e a contagem de linhas são registrados no esquema de metadados.
4. Arquivamento: Os arquivos originais são movidos para o armazenamento particionado.
5. Transformação: O dbt executa os comandos run e test.

---

## 5. Modos de Falha e Comportamento do Sistema

| Cenário | Reação do Sistema | Mecanismo de Recuperação |
| :--- | :--- | :--- |
| Falha Parcial na Ingestão | A transação do banco é revertida; o arquivo permanece em landing/. | O hashing garante que a reexecução processe o arquivo novamente de forma segura. |
| Arquivo Corrompido (Parquet/SQL) | O worker de ingestão captura a exceção e registra em audit_jobs. | Intervenção manual necessária; o arquivo permanece em landing/. |
| Timeout de Lock no Banco | O worker Python implementa backoff exponencial (3 tentativas). | O orquestrador interrompe o pipeline em caso de falha persistente para evitar estado parcial. |
| Mudança de Esquema (Drift) | O dbt on_schema_change='append_new_columns' lida com novos campos. | Campos ausentes ativam falhas nos testes do dbt no estágio de validação. |

---

## 6. Trade-offs e Decisões de Design

### 6.1. Armazenamento: Postgres vs. Parquet
* Decisão: Escrita dupla em armazenamento relacional e colunar.
* Justificativa: O Postgres fornece integridade transacional para transformações, enquanto o Parquet fornece portabilidade analítica de baixo custo.

### 6.2. Batch vs. Streaming
* Decisão: Pipeline orientado a lote (batch).
* Trade-off: Otimizado para complexidade e volume de dados em vez de latência em tempo real. Dado o dataset da Olist, o processamento em lote oferece o melhor equilíbrio para relatórios analíticos.

### 6.3. Limitações do Sistema
* Escala: Projetado para escalonamento vertical (Postgres/Disco Local). Escalonamento horizontal exigiria S3 e Snowflake/BigQuery.
* CDC: O rastreamento é baseado em hash de arquivo, não em logs (WAL). Exclusões físicas (hard deletes) na origem não são capturadas automaticamente.

---
Documentação de Engenharia Sênior — Focada em Design, Implementação e Resiliência.
