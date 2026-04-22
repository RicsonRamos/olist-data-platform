-- Find payments that don't have a matching order
select
    p.order_id
from {{ ref('stg_order_payments') }} p
left join {{ ref('stg_orders') }} o on p.order_id = o.order_id
where o.order_id is null
