{{ config(materialized='view') }}


SELECT

r.pubsub_message_id,
r.created_at,
r.tstamp,
r.refund_id,
r.order_id as order_id,
r.note,
r.user_id,
r.restock,
r.order_adjustments,
r.num_refund_line_items,
r.num_transactions,
ri.line_item_refund_id,
ri.quantity,
ri.line_item_id,
ri.location_id,
ri.restock_type,
ri.subtotal,
ri.total_tax,
POSITION('Gift Card' in li.title)>0 AS is_gift_card
-- z.line_level_return_refund, 
-- z.line_level_return_giftcard, 
-- z.line_level_exchange_amount,
-- COALESCE((ri.subtotal+ri.total_tax), -1.0*(x->>'amount')::numeric, 
-- 		(z.line_level_return_refund+z.line_level_return_giftcard+z.line_level_exchange_amount) ) as refund_amount,
-- li.variant_id

FROM {{ ref('tf_shopify_raw_refunds') }} r
--order adjustments lateral join to take care of when array has more than one item and pull out stuff
--LEFT JOIN LATERAL jsonb_array_elements(r.order_adjustments) AS x ON TRUE
LEFT JOIN {{ ref('tf_shopify_raw_refund_line_items') }} ri on r.pubsub_message_id = ri.pubsub_message_id
LEFT JOIN {{ ref('tf_shopify_raw_line_items') }} li on r.pubsub_message_id = li.pubsub_message_id and ri.line_item_id::text = li.id
-- LEFT JOIN {{ ref('tf_shopify_orders_current') }} y on r.order_id = y.id
-- LEFT JOIN{{ ref('tf_loop_line_item_grouped') }} z on z.order_name = y.name --dbt_testing.loop_line_item_level
WHERE r.is_current 
---FILTER OUT WEIRD CX STUFF (EG STUFF W/O LINE LEVEL)