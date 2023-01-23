{{ config(materialized='view') }}


SELECT *
FROM {{ ref('tf_shopify_orders_all')}}
WHERE financial_status <> 'voided' and name like 'LGC-%' and is_current