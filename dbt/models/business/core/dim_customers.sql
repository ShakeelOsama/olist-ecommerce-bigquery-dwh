-- Grain: one row per real customer (customer_unique_id), with location from their latest order
-- and lifetime order stats.
with customer_orders as (

    select
        c.customer_unique_id,
        c.customer_zip_prefix,
        c.customer_city,
        c.customer_state,
        o.order_id,
        o.purchased_at,
        o.order_status
    from {{ ref('stg_olist__customers') }} as c
    join {{ ref('stg_olist__orders') }} as o using (customer_id)

),

latest_location as (

    select customer_unique_id, customer_zip_prefix, customer_city, customer_state
    from customer_orders
    where true
    qualify row_number() over (partition by customer_unique_id order by purchased_at desc) = 1

),

stats as (

    select
        customer_unique_id,
        min(purchased_at)                                           as first_order_at,
        max(purchased_at)                                           as last_order_at,
        count(distinct order_id)                                    as lifetime_orders,
        countif(order_status = 'delivered')                         as lifetime_delivered_orders
    from customer_orders
    group by customer_unique_id

)

select
    l.customer_unique_id,
    l.customer_zip_prefix,
    l.customer_city,
    l.customer_state,
    g.latitude,
    g.longitude,
    s.first_order_at,
    s.last_order_at,
    {{ local_date('s.first_order_at') }}        as first_order_date,
    s.lifetime_orders,
    s.lifetime_delivered_orders,
    s.lifetime_orders > 1                       as is_repeat_customer
from latest_location as l
join stats as s using (customer_unique_id)
left join {{ ref('stg_olist__geolocation') }} as g
    on l.customer_zip_prefix = g.zip_prefix
