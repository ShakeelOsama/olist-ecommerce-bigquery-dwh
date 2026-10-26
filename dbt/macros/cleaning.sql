{# Olist timestamps are local Sao Paulo wall-clock strings: convert to a true UTC TIMESTAMP. #}
{% macro local_ts(column) -%}
    timestamp(safe_cast(nullif(trim({{ column }}), '') as datetime), '{{ var("source_timezone") }}')
{%- endmacro %}

{# Business date in the source timezone (what the business calls "the order date"). #}
{% macro local_date(ts_expression) -%}
    date({{ ts_expression }}, '{{ var("source_timezone") }}')
{%- endmacro %}

{# Raw keeps every ingestion batch; cleaned keeps the latest version of each key. #}
{% macro latest_per_key(key_columns) -%}
    qualify row_number() over (
        partition by {{ key_columns | join(', ') }}
        order by _ingested_at desc
    ) = 1
{%- endmacro %}

{% macro clean_text(column) -%}
    nullif(trim(lower({{ column }})), '')
{%- endmacro %}

{% macro zip_prefix(column) -%}
    lpad(nullif(trim({{ column }}), ''), 5, '0')
{%- endmacro %}
