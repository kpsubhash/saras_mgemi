{{ config(materialized='view') }}


WITH loop_current as (
select 
l.pubsub_message_id,
l.tstamp,
l.created_at,
regexp_replace(l.order_name::text, '#', '', 'gi') as order_name, 
li.provider_line_item_id, 
li.parent_return_reason, 
li.return_reason,
li.line_item_id,
li.variant_id,
li.exchange_variant_id,
CASE WHEN ROW_NUMBER() OVER (PARTITION BY l.order_name, li.provider_line_item_id, li.line_item_id, l.return_id) = 1 THEN true ELSE false END as is_current

FROM {{ source('loop', 'raw_loop_webhook_return_events') }} l 
LEFT JOIN {{ source('loop', 'raw_loop_webhook_line_items') }} li on li.pubsub_message_id = l.pubsub_message_id 
WHERE state = 'open'
)

SELECT *
FROM loop_current
WHERE is_current
