-- One row per seller: volume, revenue, service quality and a risk tier that the
-- marketplace ops team can action (coach / review / offboard).
with seller_orders as (

    -- an order can contain several sellers; evaluate each seller on the orders they took part in
    select
        seller_id,
        order_id,
        any_value(seller_state)                     as seller_state,
        any_value(purchase_date)                    as purchase_date,
        sum(line_value)                             as seller_revenue,
        logical_or(is_seller_late_to_carrier)       as late_to_carrier,
        any_value(is_late_delivery)                 as late_delivery,
        any_value(review_score)                     as review_score,
        any_value(order_status) in ('canceled', 'unavailable') as is_cancelled
    from {{ ref('fct_order_items') }}
    group by seller_id, order_id

),

scored as (

    select
        seller_id,
        any_value(seller_state)                                         as seller_state,
        min(purchase_date)                                              as first_sale_date,
        max(purchase_date)                                              as last_sale_date,
        count(*)                                                        as orders,
        sum(if(not is_cancelled, seller_revenue, 0))                    as revenue,
        avg(review_score)                                               as avg_review_score,
        safe_divide(countif(review_score <= 2), countif(review_score is not null))
                                                                        as detractor_rate,
        safe_divide(countif(late_to_carrier), countif(late_to_carrier is not null))
                                                                        as late_handover_rate,
        safe_divide(countif(late_delivery), countif(late_delivery is not null))
                                                                        as late_delivery_rate,
        safe_divide(countif(is_cancelled), count(*))                    as cancellation_rate
    from seller_orders
    group by seller_id

)

select
    *,
    percent_rank() over (order by revenue)                              as revenue_percentile,
    case
        when orders < 10 then 'insufficient_data'
        when avg_review_score < 3.5 or late_handover_rate > 0.25 or cancellation_rate > 0.05
            then 'at_risk'
        when avg_review_score < 4.0 or late_handover_rate > 0.10
            then 'needs_coaching'
        else 'healthy'
    end                                                                 as risk_tier
from scored
