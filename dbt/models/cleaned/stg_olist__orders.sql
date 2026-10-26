with source as (

    select * from {{ source('olist_raw', 'orders') }}
    where nullif(trim(order_id), '') is not null
    {{ latest_per_key(['order_id']) }}

)

select
    trim(order_id)                                          as order_id,
    trim(customer_id)                                       as customer_id,
    {{ clean_text('order_status') }}                        as order_status,

    {{ local_ts('order_purchase_timestamp') }}              as purchased_at,
    {{ local_ts('order_approved_at') }}                     as approved_at,
    {{ local_ts('order_delivered_carrier_date') }}          as delivered_to_carrier_at,
    {{ local_ts('order_delivered_customer_date') }}         as delivered_to_customer_at,
    date(safe_cast(nullif(trim(order_estimated_delivery_date), '') as datetime))
                                                            as estimated_delivery_date,

    {{ local_date(local_ts('order_purchase_timestamp')) }}  as purchase_date,
    _ingested_at,
    _batch_id
from source
