with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select 
        order_id, 
        sum(payment_value) as total_order_value 
    from {{ ref('stg_order_payments') }}
    group by 1
),

customer_orders as (
    select
        c.customer_unique_id,
        o.order_id,
        p.total_order_value
    from customers c
    join orders o on c.customer_id = o.customer_id
    left join payments p on o.order_id = p.order_id
    where o.order_status != 'canceled'
),

final as (
    select
        customer_unique_id,
        count(distinct order_id) as total_orders,
        sum(total_order_value) as lifetime_value,
        min(total_order_value) as min_order_value,
        max(total_order_value) as max_order_value,
        avg(total_order_value) as avg_order_value
    from customer_orders
    group by 1
)

select 
    *,
    case 
        when lifetime_value > 500 then 'High Value'
        when lifetime_value > 100 then 'Medium Value'
        else 'Low Value'
    end as customer_segment
from final
