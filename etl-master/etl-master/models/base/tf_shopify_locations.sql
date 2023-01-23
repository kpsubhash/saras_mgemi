{{ config(materialized='view') }}


SELECT

distinct on (id)
pubsub_message_id,
tstamp,
type,
domain,
id,
name,
address1,
address2,
city,
zip,
province,
country,
phone,
created_at,
updated_at,
country_code,
country_name,
province_code,
legacy,
active,
CASE WHEN id = 20453326907 THEN 'Warehouse'
     WHEN  id = 30886232150 THEN 'Office'
     ELSE 'Store'
     END as location_type

FROM {{ source('shopify', 'raw_shopify_webhook_location') }}
WHERE domain = 'mgemi.myshopify.com' and type IN ('locations/create', 'locations/update')
ORDER BY id, tstamp desc