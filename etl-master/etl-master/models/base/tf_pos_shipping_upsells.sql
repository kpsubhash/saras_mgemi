{{ config(materialized='view') }}

SELECT 

pubsub_message_id,
id,
properties,
quantity,
variant_id::text,
key,
discounted_price,
discounts,
gift_card,
grams,
line_price,
original_line_price,
original_price,
price::numeric,
product_id,
sku,
taxable,
title,
total_discount,
vendor,
variant_title,
variant_inventory_management,
name,
product_exists,
fulfillment_service,
fulfillable_quantity,
fulfillment_status,
requires_shipping,
discount_allocations,
num_tax_lines

FROM {{ source('shopify','raw_shopify_webhook_line_items') }}
--These are the variant_id's of the two Shipping upsells in POS that are "line items"
--Next Day Shipping, 2nd Day Shipping
WHERE variant_id IN ('20461593395259', '20461593886779')

