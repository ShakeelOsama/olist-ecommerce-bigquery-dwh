-- Executive KPI table: one row per month. Feeds the "Marketplace Health" dashboard page.
with orders as (
    select * from {{ ref('fct_orders') }}
),

first_orders as (
    select customer_unique_id, date_trunc(first_order_date, month) as cohort_month
    from {{ ref('dim_customers') }}
)

select
    date_trunc(o.purchase_date, month)                                      as month,
    count(*)                                                                as orders,
    countif(o.is_delivered)                                                 as delivered_orders,
    countif(o.is_cancelled)                                                 as cancelled_orders,
    safe_divide(countif(o.is_cancelled), count(*))                          as cancellation_rate,

    sum(if(not o.is_cancelled, o.gross_merchandise_value, 0))               as gmv,
    sum(if(not o.is_cancelled, o.freight_value, 0))                         as freight_revenue,
    safe_divide(sum(if(not o.is_cancelled, o.gross_merchandise_value, 0)),
                countif(not o.is_cancelled))                                as avg_order_value,

    count(distinct o.customer_unique_id)                                    as active_customers,
    count(distinct if(f.cohort_month = date_trunc(o.purchase_date, month),
                      o.customer_unique_id, null))                          as new_customers,

    avg(o.delivery_days)                                                    as avg_delivery_days,
    safe_divide(countif(o.is_late_delivery), countif(o.is_delivered))       as late_delivery_rate,
    avg(o.review_score)                                                     as avg_review_score,
    safe_divide(countif(o.review_score <= 2), countif(o.review_score is not null))
                                                                            as detractor_rate
from orders as o
left join first_orders as f using (customer_unique_id)
group by month
