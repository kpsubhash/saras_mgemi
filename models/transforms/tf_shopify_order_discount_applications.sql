{{ config(materialized='view') }}


SELECT 

pubsub_message_id, 
tstamp, 
type, 
id as order_id, 
num_line_items, 
num_shipping_lines, 
num_discount_codes, 
(discount_application_index - 1) as discount_application_index, 
d->>'type' as discount_type, 
d->>'value' as discount_value, 
CASE WHEN d ? 'code' AND (d->>'code' like 'LL-%') THEN d->>'value' ELSE NULL END as loyalty_discount_value,
CASE WHEN d ? 'code' AND (d->>'code' like 'LL-%') THEN NULL ELSE d->>'value' END as non_loyalty_discount_value,
d->>'allocation_method' as discount_allocation_method, 
d->>'target_selection' as discount_target_selection, 
d->>'target_type' as discount_target_type, 
d->>'value_type' as discount_value_type,

---get discount title, description, code when they exist
CASE WHEN d ? 'title' THEN d->>'title' ELSE NULL END as discount_title,
CASE WHEN d ? 'description' THEN d->>'description' ELSE NULL END as discount_description,
CASE WHEN d ? 'code' THEN d->>'code' ELSE NULL END as discount_code

FROM {{ ref('tf_shopify_orders_current') }} soc
LEFT JOIN LATERAL jsonb_array_elements(soc.discount_applications) WITH ORDINALITY AS d (d,discount_application_index) on true
WHERE soc.is_current
