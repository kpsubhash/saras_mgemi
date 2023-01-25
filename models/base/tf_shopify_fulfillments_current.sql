{{ config(materialized='view') }}



SELECT
*,
(case when ROW_NUMBER() over (partition by id order by tstamp desc) = 1 then true else false end) as is_current
FROM {{ source('shopify', 'raw_shopify_webhook_fulfillments') }}
