{{ config(materialized='table',
			tags=["hourly", "nightly"]) }}

WITH shopify_online_inventory_data as (
	 select 
	 	inventory_item_id, 
 		available, 
 		row_number() over (partition by inventory_item_id order by updated_at desc) = 1 as is_current
	 from shopify.raw_shopify_inventory_levels_webhook i 
	 where domain = 'mgemi.myshopify.com' and location_id = '20453326907' --online store location identifier
	 )

SELECT p.handle, p.variant_title, p.product_id, p.variant_id, p.inventory_item_id, i.available, p.size, p.product_type
FROM {{ ref('tf_shopify_product_data_current') }} p
LEFT JOIN shopify_online_inventory_data i on i.is_current and i.inventory_item_id::text = p.inventory_item_id
