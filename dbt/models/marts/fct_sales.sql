with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select * from {{ ref('stg_order_payments') }}
),

final as (
    select
        oi.order_id,
        o.customer_id,
        o.purchase_at,
        o.order_status,
        oi.price as price_amount,
        oi.freight_value as freight_amount,
        -- Canonical KPI Calculation: Gross Total
        (oi.price + oi.freight_value) as gross_revenue_amount,
        op.payment_type,
        op.payment_installments
    from order_items as oi
    inner join orders as o on oi.order_id = o.order_id
    left join order_payments as op on oi.order_id = op.order_id
)

select * from final
