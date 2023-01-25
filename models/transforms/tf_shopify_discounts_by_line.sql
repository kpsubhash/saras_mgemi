{{ config(materialized='view') }}


SELECT 

sd.pubsub_message_id,
sd.order_id,
sd.line_item_id,


---Rearranging this table so that it can act as the "discount_table" as welll
--Nullif(,'~') is to just make consistent and not confuse people with random ~ hanging out
STRING_AGG(DISTINCT COALESCE(sd.discount_title,''),'~') as line_discount_titles,
STRING_AGG(DISTINCT COALESCE(sd.discount_description,''),'~') as line_discount_descriptions,
STRING_AGG(DISTINCT COALESCE(sd.discount_code,''),'~') as line_discount_codes,
--should be sum of other two cases
SUM(sd.discount_amount) as total_line_discounts,
SUM(sd.loyalty_discount_amount) as total_loyalty_line_discounts,
SUM(sd.non_loyalty_discount_amount) as total_non_loyalty_line_discounts

FROM {{ ref('tf_shopify_discount_applications') }} sd
--we don't want to count loop-discount here b/c its not _really_ a discount
WHERE discount_title <> 'loop-discount'
GROUP BY 1,2,3