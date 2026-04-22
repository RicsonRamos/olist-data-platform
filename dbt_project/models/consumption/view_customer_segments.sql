with ltv as (
    select * from {{ ref('dim_customer_ltv') }}
),

final as (
    select
        customer_unique_id as "ID nico Cliente",
        total_orders as "Total de Pedidos",
        lifetime_value as "Valor Total Gasto (LTV)",
        avg_order_value as "Ticket Mdio",
        customer_segment as "Segmento de Valor"
    from ltv
)

select * from final
