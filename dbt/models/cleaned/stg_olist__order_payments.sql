with source as (

    select * from {{ source('olist_raw', 'order_payments') }}
    where nullif(trim(order_id), '') is not null
    {{ latest_per_key(['order_id', 'payment_sequential']) }}

)

select
    trim(order_id)                                      as order_id,
    safe_cast(payment_sequential as int64)              as payment_sequential,
    coalesce({{ clean_text('payment_type') }}, 'not_defined') as payment_type,
    safe_cast(payment_installments as int64)            as payment_installments,
    safe_cast(payment_value as numeric)                 as payment_value,
    _ingested_at
from source
