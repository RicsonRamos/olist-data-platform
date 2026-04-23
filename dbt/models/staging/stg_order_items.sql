{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    on_schema_change='append_new_columns'
  )
}}

with source as (
    select * from {{ source('raw', 'order_items') }}
    {% if is_incremental() %}
      where _metadata_ingested_at > (select max(_metadata_ingested_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_at,
        price,
        freight_value,
        _metadata_ingested_at,
        _metadata_source_file
    from source
)

select * from renamed
