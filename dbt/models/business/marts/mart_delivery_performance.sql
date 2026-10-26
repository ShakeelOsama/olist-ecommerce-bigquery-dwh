-- Logistics view: delivery speed and lateness by customer state and month, plus how much
-- lateness costs in review score. This is the table behind the "late delivery -> bad review" finding.
select
    date_trunc(purchase_date, month)                                        as month,
    customer_state,
    countif(is_delivered)                                                   as delivered_orders,
    avg(delivery_days)                                                      as avg_delivery_days,
    approx_quantiles(delivery_days, 100)[safe_offset(90)]                   as p90_delivery_days,
    avg(date_diff(estimated_delivery_date, purchase_date, day))             as avg_promised_days,
    safe_divide(countif(is_late_delivery), countif(is_delivered))           as late_delivery_rate,
    safe_divide(countif(is_seller_late_to_carrier), countif(delivered_to_carrier_at is not null))
                                                                            as seller_late_handover_rate,
    avg(if(is_late_delivery, review_score, null))                           as avg_review_late,
    avg(if(not is_late_delivery, review_score, null))                       as avg_review_on_time,
    avg(if(not is_late_delivery, review_score, null))
      - avg(if(is_late_delivery, review_score, null))                       as review_penalty_for_lateness,
    avg(freight_value)                                                      as avg_freight_value
from {{ ref('fct_orders') }}
where is_delivered
group by month, customer_state
