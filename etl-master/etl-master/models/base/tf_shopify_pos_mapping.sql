{{ config(materialized='table',
		  tags=["hourly", "nightly"]) }}



SELECT p.device_id, p.order_location_id, dl.id, ol.shopify_location_id, dl.name
FROM  {{ source('public', 'shopify_pos_devices') }} p 
LEFT JOIN {{ source('public', 'dim_locations') }} dl on dl.id = p.order_location_id
LEFT JOIN {{ source('public', 'order_locations') }} ol on ol.id = dl.id