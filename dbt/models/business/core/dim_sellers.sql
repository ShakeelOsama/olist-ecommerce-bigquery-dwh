select
    s.seller_id,
    s.seller_zip_prefix,
    s.seller_city,
    s.seller_state,
    g.latitude,
    g.longitude
from {{ ref('stg_olist__sellers') }} as s
left join {{ ref('stg_olist__geolocation') }} as g
    on s.seller_zip_prefix = g.zip_prefix
