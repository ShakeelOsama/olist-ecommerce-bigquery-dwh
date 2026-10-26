with products as (

    select * from {{ source('olist_raw', 'products') }}
    where nullif(trim(product_id), '') is not null
    {{ latest_per_key(['product_id']) }}

),

translation as (

    select
        {{ clean_text('product_category_name') }}         as category_pt,
        {{ clean_text('product_category_name_english') }} as category_en
    from {{ source('olist_raw', 'product_category_translation') }}
    where nullif(trim(product_category_name), '') is not null
    {{ latest_per_key(['product_category_name']) }}

)

select
    trim(p.product_id)                                      as product_id,
    {{ clean_text('p.product_category_name') }}             as category_name_pt,
    coalesce(t.category_en, {{ clean_text('p.product_category_name') }}, 'unknown')
                                                            as category_name,
    -- source column names are misspelled ("lenght"); fixed here once
    safe_cast(p.product_name_lenght as int64)               as product_name_length,
    safe_cast(p.product_description_lenght as int64)        as product_description_length,
    safe_cast(p.product_photos_qty as int64)                as product_photos_qty,
    safe_cast(p.product_weight_g as numeric)                as product_weight_g,
    safe_cast(p.product_length_cm as numeric)               as product_length_cm,
    safe_cast(p.product_height_cm as numeric)               as product_height_cm,
    safe_cast(p.product_width_cm as numeric)                as product_width_cm,
    safe_cast(p.product_length_cm as numeric)
      * safe_cast(p.product_height_cm as numeric)
      * safe_cast(p.product_width_cm as numeric)            as product_volume_cm3,
    p._ingested_at
from products as p
left join translation as t
    on {{ clean_text('p.product_category_name') }} = t.category_pt
