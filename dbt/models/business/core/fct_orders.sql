{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = partition_if_allowed({'field': 'purchase_date', 'data_type': 'date', 'granularity': 'day'}),
    cluster_by = ['customer_state', 'order_status'],
    on_schema_change = 'append_new_columns'
  )
}}

-- Grain: one row per order.
-- Incremental strategy: orders keep changing status for weeks, so each run rebuilds the
-- trailing `reprocess_days` of partitions instead of only appending new ones.

with orders as (

    select * from {{ ref('stg_olist__orders') }}
    {% if is_incremental() %}
    where purchase_date >= date_sub(_dbt_max_partition, interval {{ var('reprocess_days') }} day)
    {% endif %}

),

items as (

    select
        order_id,
        count(*)                        as item_count,
        count(distinct product_id)      as distinct_products,
        count(distinct seller_id)       as seller_count,
        sum(price)                      as items_value,
        sum(freight_value)              as freight_value,
        max(shipping_limit_at)          as shipping_limit_at
    from {{ ref('stg_olist__order_items') }}
    group by order_id

),

payments as (

    select
        order_id,
        sum(payment_value)                                                  as payment_value,
        max(payment_installments)                                           as max_installments,
        array_agg(payment_type order by payment_value desc limit 1)[offset(0)] as primary_payment_type,
        count(distinct payment_type) > 1                                    as is_split_payment
    from {{ ref('stg_olist__order_payments') }}
    group by order_id

),

reviews as (

    select order_id, review_score, has_comment
    from {{ ref('stg_olist__order_reviews') }}
    where true
    qualify row_number() over (
        partition by order_id order by review_answered_at desc, review_created_at desc
    ) = 1

),

customers as (

    select customer_id, customer_unique_id, customer_state, customer_city
    from {{ ref('stg_olist__customers') }}

)

select
    o.order_id,
    c.customer_unique_id,
    o.customer_id,
    c.customer_state,
    c.customer_city,
    o.order_status,

    o.purchased_at,
    o.purchase_date,
    o.approved_at,
    o.delivered_to_carrier_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_date,
    {{ local_date('o.delivered_to_customer_at') }}                     as delivered_date,

    coalesce(i.item_count, 0)                                           as item_count,
    i.distinct_products,
    i.seller_count,
    coalesce(i.items_value, 0)                                          as items_value,
    coalesce(i.freight_value, 0)                                        as freight_value,
    coalesce(i.items_value, 0) + coalesce(i.freight_value, 0)           as gross_merchandise_value,

    p.payment_value,
    p.primary_payment_type,
    p.max_installments,
    p.is_split_payment,

    r.review_score,
    r.has_comment                                                       as review_has_comment,

    -- fulfilment metrics
    timestamp_diff(o.approved_at, o.purchased_at, minute) / 60.0        as hours_to_approve,
    date_diff({{ local_date('o.delivered_to_carrier_at') }}, o.purchase_date, day)
                                                                        as days_to_carrier,
    date_diff({{ local_date('o.delivered_to_customer_at') }}, o.purchase_date, day)
                                                                        as delivery_days,
    date_diff({{ local_date('o.delivered_to_customer_at') }}, o.estimated_delivery_date, day)
                                                                        as days_vs_estimate,
    case
        when o.delivered_to_customer_at is null then null
        else {{ local_date('o.delivered_to_customer_at') }} > o.estimated_delivery_date
    end                                                                 as is_late_delivery,
    o.delivered_to_carrier_at > i.shipping_limit_at                     as is_seller_late_to_carrier,

    o.order_status = 'delivered'                                        as is_delivered,
    o.order_status in ('canceled', 'unavailable')                       as is_cancelled,
    current_timestamp()                                                 as _dbt_loaded_at

from orders as o
left join customers as c using (customer_id)
left join items     as i using (order_id)
left join payments  as p using (order_id)
left join reviews   as r using (order_id)
