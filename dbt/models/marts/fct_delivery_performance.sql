with orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        order_id,
        customer_id,
        order_status,
        purchase_at,
        delivered_customer_at,
        estimated_delivery_at,
        -- Metrics
        extract(day from (delivered_customer_at - purchase_at)) as days_to_deliver,
        extract(day from (delivered_customer_at - estimated_delivery_at)) as delay_days,
        case 
            when delivered_customer_at > estimated_delivery_at then true 
            else false 
        end as is_delayed,
        case 
            when order_status = 'delivered' and delivered_customer_at is null then true 
            else false 
        end as is_data_error
    from orders
    where order_status = 'delivered'
)

select * from final
