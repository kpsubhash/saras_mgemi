{{ config(materialized='view') }}

SELECT

pubsub_message_id,
tstamp,
type,
domain,
id as refund_id,
order_id,
created_at,
note,
user_id,
processed_at,
restock,
order_adjustments,
num_refund_line_items,
num_transactions,
(CASE WHEN ROW_NUMBER() OVER (PARTITION BY id ORDER BY tstamp DESC) = 1 THEN true ELSE false END) AS is_current


FROM {{ source('shopify', 'raw_shopify_webhook_refunds') }}
WHERE domain = 'mgemi.myshopify.com'