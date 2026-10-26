select
    product_id,
    category_name,
    category_name_pt,
    product_photos_qty,
    product_weight_g,
    product_volume_cm3,
    case
        when product_weight_g is null           then 'unknown'
        when product_weight_g < 500             then 'light (<0.5kg)'
        when product_weight_g < 5000            then 'medium (0.5-5kg)'
        when product_weight_g < 20000           then 'heavy (5-20kg)'
        else 'bulky (20kg+)'
    end                                         as weight_band
from {{ ref('stg_olist__products') }}
