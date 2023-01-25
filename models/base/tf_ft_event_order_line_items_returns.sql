
-- This table is based on tables that are no longer being updated. As such, there is no need
-- to update them on a regular basis.
{{ config(materialized='table',
          enabled=false,
          tags=[],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_storefront_order_id on {{ this }} (storefront_order_id)")
          ]
) }}



SELECT
  oo.order_created AS return_order_created,
  oo.order_id AS return_order_id,
  oo.client AS storefront_order_id,
  ii.sku,
  ii.unitprice,
  ii.quantity AS event_return_quantity
FROM raw_event_order_line_items ii
JOIN raw_event_orders oo ON ii.event_id = oo.event_id
WHERE
  oo.is_current
  AND oo.ordertype_code = 'return'

