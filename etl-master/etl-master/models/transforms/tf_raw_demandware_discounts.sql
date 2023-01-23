{{ config(materialized='view') }}

with ocapi_discounts as (
select r.order_number as order_number5,
x.value->>'product_id' as sku2,
count(*)  as items_in_order,

--Parsing out line level discount information

(CASE WHEN jsonb_array_length(data->'order_price_adjustments') > 1 THEN (data->'order_price_adjustments'->0->>'price')::numeric + (data->'order_price_adjustments'->1->>'price')::numeric ELSE (data->'order_price_adjustments'->0->>'price')::numeric END) as order_level_discount,
(CASE WHEN jsonb_array_length(data->'order_price_adjustments') > 1 THEN CONCAT(data->'order_price_adjustments'->0->>'item_text', '--',data->'order_price_adjustments'->1->>'price') ELSE data->'order_price_adjustments'->0->>'item_text' END) as order_level_campaign_id,
(CASE WHEN jsonb_array_length(data->'order_price_adjustments') > 1 THEN CONCAT(data->'order_price_adjustments'->0->>'promotion_id', '--',data->'order_price_adjustments'->1->>'promotion_id') ELSE data->'order_price_adjustments'->0->>'promotion_id' END) as order_level_promotion_id,
(CASE WHEN jsonb_array_length(data->'order_price_adjustments') > 1 THEN CONCAT(data->'order_price_adjustments'->0->>'promotion_link', '--', data->'order_price_adjustments'->0->>'promotion_link') ELSE data->'order_price_adjustments'->0->>'promotion_link' END) as order_level_promo_link,
(CASE WHEN jsonb_array_length(data->'order_price_adjustments') > 1 THEN CONCAT(data->'order_price_adjustments'->0->>'coupon_code', '--', data->'order_price_adjustments'->0->>'coupon_code') ELSE data->'order_price_adjustments'->0->>'coupon_code' END) as order_level_coupon_code,
(CASE WHEN jsonb_array_length(x.value->'price_adjustments') > 1 THEN (x.value->'price_adjustments'->0->>'price')::numeric + (x.value->'price_adjustments'->1->>'price')::numeric ELSE (x.value->'price_adjustments'->0->>'price')::numeric END) as line_level_discount,
(CASE WHEN jsonb_array_length(x.value->'price_adjustments') > 1 THEN CONCAT(x.value->'price_adjustments'->0->>'item_text', '--', x.value->'price_adjustments'->0->>'item_text') ELSE x.value->'price_adjustments'->0->>'item_text' END) as line_level_discount_campaign_id,
(CASE WHEN jsonb_array_length(x.value->'price_adjustments') > 1 THEN CONCAT(x.value->'price_adjustments'->0->>'promotion_id', '--', x.value->'price_adjustments'->1->>'promotion_id') ELSE x.value->'price_adjustments'->0->>'promotion_id' END) as line_level_discount_promotion_id,
(CASE WHEN jsonb_array_length(x.value->'price_adjustments') > 1 THEN CONCAT(x.value->'price_adjustments'->0->>'coupon_code', '--', x.value->'price_adjustments'->1->>'coupon_code') ELSE x.value->'price_adjustments'->0->>'coupon_code' END) as line_level_discount_coupon_code,
(CASE WHEN jsonb_array_length(x.value->'price_adjustments') > 1 THEN CONCAT(x.value->'price_adjustments'->0->>'promotion_link', '--', x.value->'price_adjustments'->1->>'promotion_link') ELSE x.value->'price_adjustments'->0->>'promotion_link' END) as line_level_discount_promo_link
from {{ source('public', 'raw_demandware_ocapi_order') }} r
left join lateral (select * from jsonb_array_elements(data->'product_items')) x on true
group by 1,2,4,5,6,7,8,9,10,11,12,13
)

,aggregate_discount_levels as (
SELECT 

order_number5 as order_number,
sku2 as sku,
items_in_order,

(order_level_discount / items_in_order) as discount_order_level,
line_level_discount as discount_line_level,

STRING_AGG(order_level_campaign_id, '-') as discount_order_campaign_ids,
STRING_AGG(line_level_discount_campaign_id, '-') as discount_line_campaign_ids,

STRING_AGG(order_level_promotion_id, '-') as discount_order_promotion_ids,
STRING_AGG(line_level_discount_promotion_id,'-') as discount_line_promotion_ids,

STRING_AGG(line_level_discount_coupon_code, '-') as discount_order_codes,
STRING_AGG(line_level_discount_coupon_code,'-') as discount_line_codes



FROM ocapi_discounts
GROUP BY 1,2,3,4,5
)



SELECT 

order_number,
sku as order_sku,

(discount_order_level + discount_line_level) as line_discounts,

CONCAT( discount_order_campaign_ids,'~',discount_line_campaign_ids ) as discount_campaign_ids,
CONCAT( discount_order_promotion_ids,'~', discount_line_promotion_ids) as discount_promotion_ids,
CONCAT( discount_order_codes,'~', discount_line_codes) as discount_codes

FROM aggregate_discount_levels
