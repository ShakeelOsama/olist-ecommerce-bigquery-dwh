with source as (

    select * from {{ source('olist_raw', 'order_items') }}
    where nullif(trim(order_id), '') is not null
    {{ latest_per_key(['order_id', 'order_item_id']) }}

)

select
    trim(order_id)                                  as order_id,
    safe_cast(order_item_id as int64)               as order_item_id,
    trim(product_id)                                as product_id,
    trim(seller_id)                                 as seller_id,
    {{ local_ts('shipping_limit_date') }}           as shipping_limit_at,
    safe_cast(price as numeric)                     as price,
    safe_cast(freight_value as numeric)             as freight_value,
    _ingested_at
from source
