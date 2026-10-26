-- RFM segmentation for CRM campaigns. Recency is measured against the latest order date in
-- the data (not today's date), so the model stays meaningful on a historical dataset.
with base as (

    select
        customer_unique_id,
        any_value(customer_state)                   as customer_state,
        max(purchase_date)                          as last_purchase_date,
        count(distinct order_id)                    as frequency,
        sum(gross_merchandise_value)                as monetary
    from {{ ref('fct_orders') }}
    where not is_cancelled
    group by customer_unique_id

),

as_of as (
    select max(last_purchase_date) as as_of_date from base
),

scored as (

    select
        b.*,
        date_diff(a.as_of_date, b.last_purchase_date, day)              as recency_days,
        ntile(5) over (order by date_diff(a.as_of_date, b.last_purchase_date, day) desc) as r_score,
        -- ~97% of Olist customers buy once, so frequency is bucketed explicitly instead of ntile
        case when b.frequency >= 3 then 5 when b.frequency = 2 then 4 else 1 end as f_score,
        ntile(5) over (order by b.monetary)                             as m_score
    from base as b
    cross join as_of as a

)

select
    *,
    concat(cast(r_score as string), cast(f_score as string), cast(m_score as string)) as rfm_code,
    case
        when r_score >= 4 and f_score >= 4                  then 'champions'
        when f_score >= 4                                   then 'loyal_at_risk'
        when r_score >= 4 and m_score >= 4                  then 'high_value_new'
        when r_score >= 4                                   then 'recent_low_value'
        when r_score <= 2 and m_score >= 4                  then 'big_spenders_lapsed'
        when r_score <= 2                                   then 'hibernating'
        else 'needs_attention'
    end                                                     as segment
from scored
