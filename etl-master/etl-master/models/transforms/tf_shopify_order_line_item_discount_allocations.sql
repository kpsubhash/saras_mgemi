{{ config(materialized='view') }}

SELECT 
pubsub_message_id, 
id as line_item_id,
quantity,
price, 
d->>'discount_application_index' as discount_application_index_line, 
(d->>'amount')::numeric as discount_amount
FROM {{ ref('tf_shopify_raw_line_items') }} 
LEFT JOIN LATERAL jsonb_array_elements(discount_allocations) d on true