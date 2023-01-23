{{ config(materialized='view') }}


SELECT
*,
(CASE WHEN ROW_NUMBER() OVER (PARTITION BY id ORDER BY tstamp DESC) = 1 THEN true ELSE false END) AS is_current
FROM {{ source('shopify', 'raw_shopify_webhook_orders') }}
--Tag legacy orders are from demandware import. For now, remove and just rely on tf_ft_master_orders. Might not be very performant
--We are removing legacy orders because these are handled by tf_ft_master_orders and we would be double counting otherwise
WHERE domain = 'mgemi.myshopify.com' and not test and tags not like '%Legacy%'

