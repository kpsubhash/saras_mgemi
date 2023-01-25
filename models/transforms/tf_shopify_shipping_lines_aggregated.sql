{{ config(materialized='view') }}


SELECT 
pubsub_message_id, 
id, 
code, 
sum(price) as shipping_revenue_pre_discount,
sum(discounted_price) as shipping_discount
FROM {{ ref('tf_shopify_shipping_lines') }}
GROUP BY 1,2,3
