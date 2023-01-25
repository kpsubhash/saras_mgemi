{{ config(materialized='view') }} 


SELECT 

sd.pubsub_message_id,
sd.order_id,
sd.line_item_id,


---Rearranging this table so that it can act as the "discount_table" as welll
STRING_AGG(sd.discount_title,'-') as line_discount_titles,
STRING_AGG(sd.discount_description,'-') as line_discount_descriptions,
STRING_AGG(sd.discount_code,'-') as line_discount_codes,
--should be sum of other two cases
SUM(sd.discount_amount) as total_shop_now_loop_discount 

FROM {{ ref('tf_shopify_discount_applications') }} sd
WHERE discount_title = 'loop-discount'
GROUP BY 1,2,3