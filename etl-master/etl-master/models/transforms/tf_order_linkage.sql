{{ config(materialized='view') }}

SELECT 
x.storefront_order_id, 
SPLIT_PART(y.storefront_order_id,'-', 2) as l_storefront_order_id
FROM {{ ref('tf_master_orders_shopify') }}  x
LEFT JOIN {{ ref('tf_loop_return_current') }} l on x.line_item_id = l.provider_line_item_id 
LEFT JOIN {{ ref('tf_master_orders_shopify') }}  y on y.is_shopnow_order and x.storefront_order_id = SPLIT_PART(y.storefront_order_id,'-', 2)
GROUP BY 1,2