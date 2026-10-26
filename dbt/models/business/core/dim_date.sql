with bounds as (
    select
        date_trunc(min(purchase_date), year)                       as start_date,
        last_day(date_add(max(purchase_date), interval 1 year), year) as end_date
    from {{ ref('stg_olist__orders') }}
)

select
    d                                           as date_day,
    extract(year from d)                        as year,
    extract(quarter from d)                     as quarter,
    extract(month from d)                       as month,
    format_date('%Y-%m', d)                     as year_month,
    format_date('%B', d)                        as month_name,
    extract(isoweek from d)                     as iso_week,
    format_date('%A', d)                        as day_name,
    extract(dayofweek from d) in (1, 7)         as is_weekend,
    date_trunc(d, month)                        as month_start,
    date_trunc(d, isoweek)                      as week_start
from bounds, unnest(generate_date_array(start_date, end_date)) as d
