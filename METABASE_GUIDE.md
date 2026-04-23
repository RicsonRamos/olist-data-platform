# 📊 Guia de Configuração: Metabase & Semantic Layer
### Estratégia de BI e Analytics Engineering para Stakeholders

Este guia detalha como organizar o Metabase para consumir os dados da Olist de forma orientada a produto e governança.

---

## 🏗️ 1. Modelo Semântico no Metabase

Para uma análise self-service eficiente, organizamos as views em um modelo **Star Schema** (Fatos e Dimensões).

### Tabelas de Exposição (Camada Consumption)
As tabelas no esquema `consumption` devem ser as únicas expostas diretamente para os usuários finais.

*   **Fatos (Movimentação):**
    *   `view_sales_dashboard`: Core da performance de vendas.
    *   `view_delivery_analysis`: Performance operacional e logística.
*   **Dimensões (Contexto):**
    *   `stg_customers`: Segmentação geográfica de clientes.
    *   `stg_products`: Categorização e detalhes de produtos.

### Relacionamentos Recomendados
No Metabase (**Admin > Data Model**), configure as chaves estrangeiras:
*   `view_sales_dashboard.customer_id` -> `stg_customers.customer_id`
*   `view_delivery_analysis.order_id` -> `view_sales_dashboard.order_id`

---

## 📁 2. Estrutura de Coleções (Pastas)

Organizamos o Metabase por domínios de negócio para facilitar a navegação:

1.  **🚀 01. Executivo**: Dashboards de alto nível (North Star Metrics).
2.  **💰 02. Vendas & Marketing**: Receita, ticket médio e performance de categorias.
3.  **🚚 03. Operações & Logística**: SLAs de entrega, frete e atrasos.
4.  **📦 04. Inventário & Produto**: Mix de produtos e performance de sellers.

---

## 📈 3. Métricas Principais (KPIs)

| Categoria | Métrica | Definição Técnica |
| :--- | :--- | :--- |
| **Crescimento** | **GMV (Gross Merchandise Value)** | `SUM(total_value)` nas views de vendas. |
| **Crescimento** | **Volume de Pedidos** | `COUNT(DISTINCT order_id)`. |
| **Receita** | **Ticket Médio** | `GMV / Volume de Pedidos`. |
| **Performance** | **Lead Time Médio** | `AVG(days_to_deliver)` na view de logística. |
| **Qualidade** | **Taxa de Atraso** | `% de pedidos com is_delayed = true`. |

---

## 🛡️ 4. Governança e Naming

Para garantir a "Single Source of Truth":
*   **Ocultar Schemas Internos**: Oculte os esquemas `raw` e `staging` no Metabase. Os usuários não devem ver a "cozinha" do dado.
*   **Camada Oficial**: Use apenas o esquema `consumption` para criar as "Official Questions" (perguntas verificadas).
*   **Nomenclatura**: Renomeie os campos técnicos para nomes amigáveis no Data Model do Metabase (ex: `order_purchase_timestamp` -> `Data da Compra`).

---

## 🖼️ 5. Dashboard Sugerido: "Visão 360 do E-commerce"

| Gráfico | Tipo | Pergunta de Negócio |
| :--- | :--- | :--- |
| **GMV por Mês** | Linha | Qual a tendência de faturamento? |
| **Status dos Pedidos** | Pizza | Como está a saúde operacional (cancelados vs entregues)? |
| **Top 10 Categorias** | Barras | Quais categorias dominam o faturamento? |
| **Distribuição Geográfica** | Mapa | Em quais estados temos maior penetração de mercado? |
| **Lead Time por Estado** | Barras Horiz. | Qual região tem a logística mais lenta? |
| **Ticket Médio Mensal** | Indicador | O valor gasto por cliente está aumentando? |

---

## 🚀 Próximos Passos
1. Acesse o Metabase em seu navegador: `http://localhost:3001`.
2. Conecte o Metabase ao PostgreSQL (Host: `postgres`, User: `admin`, Pass: `admin123`).
3. Sincronize o esquema `consumption`.
3. Configure os tipos de campo (Datas, Moedas, IDs) no Data Model.
4. Crie a coleção "Executivo" e adicione os gráficos acima.

---
**Analytics Engineering** | Orientado a Decisão e Governança.
