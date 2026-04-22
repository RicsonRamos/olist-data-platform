-- Find orders where delivery date is earlier than purchase date (impossible in reality)
select
    order_id,
    purchase_at,
    delivered_customer_at
from {{ ref('stg_orders') }}
where delivered_customer_at < purchase_at
