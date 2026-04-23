{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns'
  )
}}

with source as (
    select * from {{ source('raw', 'customers') }}
    {% if is_incremental() %}
      where _metadata_ingested_at > (select max(_metadata_ingested_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _metadata_ingested_at,
        _metadata_source_file
    from source
)

select * from renamed
