{{ config(materialized='view') }}

SELECT

pubsub_message_id,
id as line_item_refund_id,
quantity,
line_item_id,
location_id,
restock_type,
subtotal,
total_tax,
subtotal+total_tax as total_refund

FROM {{ source('shopify', 'raw_shopify_webhook_refund_line_items')}}