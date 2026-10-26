-- Row-count reconciliation cleaned -> business: every cleaned order must exist in fct_orders.
select o.order_id
from {{ ref('stg_olist__orders') }} as o
left join {{ ref('fct_orders') }} as f using (order_id)
where f.order_id is null
