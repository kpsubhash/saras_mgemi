{{ config(materialized='view') }}


SELECT order_id, line_item_id, restock_type
FROM {{ ref('tf_shopify_refunds_all') }}
WHERE line_item_id is not null and restock_type = 'cancel'
GROUP BY 1,2,3


