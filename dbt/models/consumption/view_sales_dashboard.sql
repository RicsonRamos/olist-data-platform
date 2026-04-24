with sales as (
    select * from {{ ref('fct_sales') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        s.order_id as "ID Pedido",
        s.purchase_at as "Data do Pedido",
        s.order_status as "Status",
        c.customer_city as "Cidade",
        c.customer_state as "Estado",
        s.price_amount as "Valor Item",
        s.freight_amount as "Frete",
        s.gross_revenue_amount as "Total Bruto",
        s.payment_type as "Forma de Pagamento",
        s.payment_installments as "Parcelas"
    from sales as s
    left join customers as c on s.customer_id = c.customer_id
)

select * from final
