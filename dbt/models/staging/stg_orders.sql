    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    description='Incremental model with hash-based change tracking'
  )
}}

with source as (
    select * from {{ source('raw', 'orders') }}
    {% if is_incremental() %}
      where _metadata_ingested_at > (select max(_metadata_ingested_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp::timestamp as purchase_at,
        order_approved_at::timestamp as approved_at,
        order_delivered_carrier_date::timestamp as delivered_carrier_at,
        order_delivered_customer_date::timestamp as delivered_customer_at,
        order_estimated_delivery_date::timestamp as estimated_delivery_at,
        _metadata_ingested_at,
        _metadata_source_file
    from source
)

select * from renamed
