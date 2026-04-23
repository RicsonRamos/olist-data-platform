-- Find orders that don't have a matching customer in the customers table
select
    o.order_id
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
where c.customer_id is null
