-- Business question: how much does a late delivery hurt customer satisfaction, and where
-- should logistics investment go first?
-- Run with: dbt compile --select late_delivery_impact, then paste target/compiled/... into BigQuery.

with by_bucket as (
    select
        case
            when days_vs_estimate <= -7 then '1. 7+ days early'
            when days_vs_estimate <   0 then '2. 1-6 days early'
            when days_vs_estimate =   0 then '3. on the day'
            when days_vs_estimate <=  7 then '4. 1-7 days late'
            else                             '5. 8+ days late'
        end                                             as delivery_bucket,
        count(*)                                        as orders,
        avg(review_score)                               as avg_review,
        safe_divide(countif(review_score <= 2), countif(review_score is not null)) as detractor_rate
    from {{ ref('fct_orders') }}
    where is_delivered and days_vs_estimate is not null
    group by 1
)
select * from by_bucket order by delivery_bucket;

-- States where lateness is highest relative to order volume (priority list for logistics)
-- select customer_state, sum(delivered_orders) orders,
--        safe_divide(sum(late_delivery_rate * delivered_orders), sum(delivered_orders)) late_rate
-- from {{ ref('mart_delivery_performance') }}
-- group by 1 order by late_rate desc;
