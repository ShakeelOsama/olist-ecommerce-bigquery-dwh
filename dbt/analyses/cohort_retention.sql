-- Monthly acquisition cohorts: % of customers who placed another order N months later.
with orders as (
    select customer_unique_id, date_trunc(purchase_date, month) as order_month
    from {{ ref('fct_orders') }}
    where not is_cancelled
    group by 1, 2
),
cohorts as (
    select customer_unique_id, date_trunc(first_order_date, month) as cohort_month
    from {{ ref('dim_customers') }}
)
select
    c.cohort_month,
    date_diff(o.order_month, c.cohort_month, month)         as months_since_first_order,
    count(distinct o.customer_unique_id)                    as active_customers,
    safe_divide(
        count(distinct o.customer_unique_id),
        max(count(distinct o.customer_unique_id)) over (partition by c.cohort_month)
    )                                                       as retention_rate
from cohorts as c
join orders as o using (customer_unique_id)
group by 1, 2
order by 1, 2
