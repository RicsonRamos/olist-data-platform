with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select * from {{ ref('stg_order_payments') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        o.order_id as "ID Pedido",
        o.purchase_at as "Data do Pedido",
        o.order_status as "Status",
        c.customer_city as "Cidade",
        c.customer_state as "Estado",
        oi.price as "Valor Item",
        oi.freight_value as "Frete",
        (oi.price + oi.freight_value) as "Total Bruto",
        op.payment_type as "Forma de Pagamento",
        op.payment_installments as "Parcelas"
    from orders o
    inner join order_items oi on o.order_id = oi.order_id
    left join order_payments op on o.order_id = op.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from final
