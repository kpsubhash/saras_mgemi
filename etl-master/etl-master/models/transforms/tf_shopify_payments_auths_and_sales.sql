{{ config(materialized='view') }}



SELECT 
*, 
(case when ROW_NUMBER() over (partition by order_id, id, gateway, amount order by created_at desc) = 1 then true else false end) as is_current
FROM {{ ref('tf_shopify_order_transactions') }}
WHERE kind IN ('authorization', 'sale') 
