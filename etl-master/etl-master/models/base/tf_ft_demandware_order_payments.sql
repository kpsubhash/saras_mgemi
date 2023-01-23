
-- This table is based on tables that are no longer being updated. As such, there is no need
-- to update them on a regular basis.
{{ config(materialized='table',
          enabled=false,
          tags=[],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_order_number on {{ this }} (order_number)")
          ]
) }}


SELECT
  oo.order_number,
  oo.order_date AS dw_storefront_created_at,
  oo.device_order_placed,
  sum(
      CASE
          WHEN (a.value ->> 'cardType'::text) ~~ 'Amex'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS amex_amount,
  sum(
      CASE
          WHEN (a.value ->> 'cardType'::text) ~~ 'Visa'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS visa_amount,
  sum(
      CASE
          WHEN (a.value ->> 'cardType'::text) ~~ 'Discover'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS discover_amount,
  sum(
      CASE
          WHEN (a.value ->> 'cardType'::text) ~~ 'Master'::text OR (a.value ->> 'cardType'::text) ~~ 'Mastercard'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS master_amount,
  sum(
      CASE
          WHEN (a.value ->> 'cardType'::text) ~~ 'EA_CREDIT_CARD'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS ea_cc_amount,
  sum(
      CASE
          WHEN (a.value ->> 'method'::text) ~~ 'PayPal'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS paypal_amount,
  sum(
      CASE
          WHEN (a.value ->> 'method'::text) ~~ 'GIFT_CERTIFICATE'::text THEN (a.value ->> 'amount'::text)::numeric
          ELSE NULL::numeric
      END) AS gc_amount
FROM raw_demandware_orders oo
LEFT JOIN LATERAL jsonb_array_elements(oo.payments) a(value) ON true
WHERE oo.is_current
GROUP BY oo.order_number, oo.order_date, oo.device_order_placed


