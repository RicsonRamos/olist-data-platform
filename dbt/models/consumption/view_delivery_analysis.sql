with delivery as (
    select * from {{ ref('fct_delivery_performance') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        d.order_id,
        d.purchase_at as "Data da Compra",
        d.delivered_customer_at as "Data de Entrega",
        d.estimated_delivery_at as "Data Estimada",
        d.days_to_deliver as "Dias para Entrega",
        d.delay_days as "Dias de Atraso",
        d.is_delayed as "Atrasado?",
        c.customer_city as "Cidade",
        c.customer_state as "Estado"
    from delivery as d
    left join customers as c on d.customer_id = c.customer_id
)

select * from final
