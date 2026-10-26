-- The raw file has ~1M rows with many points per zip prefix (and some outside Brazil).
-- Collapse to one centroid per zip prefix, keeping only coordinates inside Brazil's bounding box.
with source as (

    select
        {{ zip_prefix('geolocation_zip_code_prefix') }} as zip_prefix,
        safe_cast(geolocation_lat as float64)           as lat,
        safe_cast(geolocation_lng as float64)           as lng,
        initcap({{ clean_text('geolocation_city') }})   as city,
        upper(trim(geolocation_state))                  as state
    from {{ source('olist_raw', 'geolocation') }}
    -- reference file: only use the latest batch
    where _batch_id = (select max(_batch_id) from {{ source('olist_raw', 'geolocation') }})

)

select
    zip_prefix,
    avg(lat)                                                    as latitude,
    avg(lng)                                                    as longitude,
    approx_top_count(city, 1)[offset(0)].value                  as city,
    approx_top_count(state, 1)[offset(0)].value                 as state,
    count(*)                                                    as source_points
from source
where zip_prefix is not null
  and lat between -34.0 and 5.5
  and lng between -74.0 and -34.0
group by zip_prefix
