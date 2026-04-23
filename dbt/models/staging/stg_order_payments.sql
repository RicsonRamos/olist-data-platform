{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential'],
    on_schema_change='append_new_columns'
  )
}}

with source as (
    select * from {{ source('raw', 'order_payments') }}
    {% if is_incremental() %}
      where _metadata_ingested_at > (select max(_metadata_ingested_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _metadata_ingested_at,
        _metadata_source_file
    from source
)

select * from renamed
