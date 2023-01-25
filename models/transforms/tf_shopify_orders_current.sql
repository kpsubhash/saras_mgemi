{{ config(materialized='view') }}


SELECT *
FROM {{ ref('tf_shopify_orders_all')}}
WHERE is_current and financial_status <> 'voided' and name not like 'LGC-%' 