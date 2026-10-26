-- Finance reconciliation: for delivered orders, what the customer paid should match
-- items + freight within R$1. A handful of legacy mismatches (vouchers, interest on
-- instalments) exist in the source, so warn on any and only fail if the count explodes,
-- which would indicate a broken join or a duplicated load.
{{ config(severity='error', warn_if='>0', error_if='>500') }}

select
    order_id,
    gross_merchandise_value,
    payment_value,
    payment_value - gross_merchandise_value as difference
from {{ ref('fct_orders') }}
where is_delivered
  and payment_value is not null
  and abs(payment_value - gross_merchandise_value) > 1
