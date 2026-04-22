with delivery as (
    select
        avg(days_to_deliver) as avg_days_to_deliver,
        count(case when is_delayed then 1 end) * 100.0 / count(*) as delay_rate_percentage,
        count(distinct order_id) as total_delivered_orders
    from {{ ref('fct_delivery_performance') }}
),

ltv as (
    select
        sum(lifetime_value) as total_revenue,
        avg(lifetime_value) as average_ticket,
        count(distinct customer_unique_id) as total_unique_customers,
        count(case when customer_segment = 'High Value' then 1 end) as high_value_customer_count
    from {{ ref('dim_customer_ltv') }}
),

final as (
    select
        l.total_revenue,
        d.total_delivered_orders,
        l.average_ticket,
        d.avg_days_to_deliver,
        d.delay_rate_percentage,
        l.total_unique_customers,
        l.high_value_customer_count
    from ltv l
    cross join delivery d
)

select 
    total_revenue as "Receita Total",
    total_delivered_orders as "Total de Pedidos Entregues",
    round(average_ticket::numeric, 2) as "Ticket Mdio",
    round(avg_days_to_deliver::numeric, 1) as "Mdia Dias de Entrega",
    round(delay_rate_percentage::numeric, 2) as "Taxa de Atraso (%)",
    total_unique_customers as "Total de Clientes nicos",
    high_value_customer_count as "Clientes de Alto Valor"
from final
