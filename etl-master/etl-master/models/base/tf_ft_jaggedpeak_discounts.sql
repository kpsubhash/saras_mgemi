
-- This table is based on tables that are no longer being updated. As such, there is no need
-- to update them on a regular basis.
{{ config(materialized='table',
          enabled=false,
          tags=[],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_order_id on {{ this }} (order_id)")
          ]
) }}


SELECT
  discount.item_id,
  item.external_id,
  jp_item.sku,
  o_1.order_id,
  sum(discount.amount) AS remote_discount
FROM remote.order_item_discounts discount
JOIN remote.order_items item ON discount.item_id = item.id
JOIN raw_event_order_line_items jp_item ON item.external_id::text = jp_item.line_id::text AND item.sku::text = jp_item.sku::text
JOIN raw_event_orders o_1 ON jp_item.event_id = o_1.event_id
WHERE o_1.is_current AND item.source::text = 'JaggedPeak'::text
GROUP BY discount.item_id, item.external_id, jp_item.sku, o_1.order_id

