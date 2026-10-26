{{
  config(
    partition_by = {'field': 'purchase_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by = ['seller_id', 'category_name']
  )
}}

-- Grain: one row per order line. Used for seller and category analysis.
select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    p.category_name,
    p.weight_band,
    s.seller_state,
    o.customer_state,
    o.purchase_date,
    o.order_status,
    i.price,
    i.freight_value,
    i.price + i.freight_value                                       as line_value,
    safe_divide(i.freight_value, i.price)                           as freight_to_price_ratio,
    s.seller_state = o.customer_state                               as is_same_state_shipment,
    i.shipping_limit_at,
    o.delivered_to_carrier_at,
    o.delivered_to_carrier_at > i.shipping_limit_at                 as is_seller_late_to_carrier,
    o.is_late_delivery,
    o.review_score
from {{ ref('stg_olist__order_items') }} as i
join {{ ref('fct_orders') }}   as o using (order_id)
left join {{ ref('dim_products') }} as p using (product_id)
left join {{ ref('dim_sellers') }}  as s using (seller_id)
