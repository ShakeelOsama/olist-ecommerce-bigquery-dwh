-- The source re-uses review_id across orders, so the grain is (review_id, order_id).
with source as (

    select * from {{ source('olist_raw', 'order_reviews') }}
    where nullif(trim(review_id), '') is not null
      and nullif(trim(order_id), '') is not null
    {{ latest_per_key(['review_id', 'order_id']) }}

)

select
    trim(review_id)                                     as review_id,
    trim(order_id)                                      as order_id,
    safe_cast(review_score as int64)                    as review_score,
    nullif(trim(review_comment_title), '')              as review_comment_title,
    nullif(trim(review_comment_message), '')            as review_comment_message,
    nullif(trim(review_comment_message), '') is not null as has_comment,
    {{ local_ts('review_creation_date') }}              as review_created_at,
    {{ local_ts('review_answer_timestamp') }}           as review_answered_at,
    _ingested_at
from source
