{{ config(materialized='view') }}


SELECT 
f.pubsub_message_id, 
f.id, 
f.tstamp, 
f.type, 
f.name, 
f.status, 
f.service, 
f.tracking_company, 
f.order_id, 
f.created_at, 
f.updated_at, 
CASE
  WHEN t.first_in_transit_event IS NOT NULL THEN t.first_in_transit_event
  WHEN l.fulfillment_status='fulfilled' THEN f.updated_at
  ELSE NULL
END AS first_in_transit_event,
f.tracking_number,
--l.pubsub_message_id, 
l.id as line_item_id, 
l.variant_id, 
l.price,
l.sku,
l.quantity
FROM {{ ref('tf_shopify_fulfillments_current') }} f
LEFT JOIN {{ ref('tf_shopify_fulfillment_in_transit_event') }} t on f.id = t.fulfillment_id
LEFT JOIN {{ ref('tf_shopify_raw_line_items') }} l ON f.pubsub_message_id = l.pubsub_message_id
WHERE f.is_current and f.status <> 'cancelled'