-- customer_id is one per order; customer_unique_id identifies the real person.
with source as (

    select * from {{ source('olist_raw', 'customers') }}
    where nullif(trim(customer_id), '') is not null
    {{ latest_per_key(['customer_id']) }}

)

select
    trim(customer_id)                           as customer_id,
    trim(customer_unique_id)                    as customer_unique_id,
    {{ zip_prefix('customer_zip_code_prefix') }} as customer_zip_prefix,
    initcap({{ clean_text('customer_city') }})  as customer_city,
    upper(trim(customer_state))                 as customer_state,
    _ingested_at
from source
