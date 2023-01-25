
-- This table is based on tables that are no longer being updated. As such, there is no need
-- to update them on a regular basis.
{{ config(materialized='table',
          enabled=false,
          tags=[],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_origin_order_id on {{ this }} (origin_order_id)"),
            after_commit("create index if not exists {{ this.name }}__index_on_sku on {{ this }} (sku)")
          ]
) }}


SELECT DISTINCT
  oo.order_id AS origin_order_id,
  oo.client AS storefront_order_id,
  oo.shipping_address1,
  oo.shipping_address2,
  oo.shipping_city,
  oo.shipping_state,
  oo.shipping_postalcode,
  oo.shipping_country,
  oo.billing_address1,
  oo.billing_address2,
  oo.billing_address3,
  oo.billing_city,
  oo.billing_country,
  oo.billing_postalcode,
  oo.shipping_phonework AS shipping_phone_number,
  ii.sku,
  ii.unitprice,
  oo.original_order_id,
  oo.billing_email,
  oo.billing_namefirst,
  oo.billing_namelast,
  oo.billing_state,
  oo.orderstatus_name,
  ii.quantity AS event_order_quantity,
  1.0 * oo.total_shipping_tax / NULLIF(oo.total_units, 0)::numeric AS shipping_tax,
  oo.referencefield3
FROM raw_event_order_line_items ii
JOIN raw_event_orders oo ON ii.event_id = oo.event_id
WHERE oo.is_current AND oo.ordertype_code = 'sales'

