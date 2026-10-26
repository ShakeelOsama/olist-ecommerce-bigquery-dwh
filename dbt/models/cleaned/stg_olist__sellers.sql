with source as (

    select * from {{ source('olist_raw', 'sellers') }}
    where nullif(trim(seller_id), '') is not null
    {{ latest_per_key(['seller_id']) }}

)

select
    trim(seller_id)                             as seller_id,
    {{ zip_prefix('seller_zip_code_prefix') }}  as seller_zip_prefix,
    initcap({{ clean_text('seller_city') }})    as seller_city,
    upper(trim(seller_state))                   as seller_state,
    _ingested_at
from source
