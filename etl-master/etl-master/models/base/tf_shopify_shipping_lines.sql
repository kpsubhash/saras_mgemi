{{ config(materialized='view') }}


SELECT 
pubsub_message_id, 
id, 
code,
title,
source,
presentment_title,
num_tax_lines,
price::numeric as price,
x.discounted_price as discounted_price
FROM {{ source('shopify', 'raw_shopify_webhook_shipping_lines') }} as t
cross join lateral (
  select coalesce(sum((x.obj->>'amount')::numeric), 0) as discounted_price
  from (
    select jsonb_array_elements(t.applied_discounts::jsonb)
    where jsonb_typeof(t.applied_discounts) = 'array'
  ) as x(obj)
) as x
