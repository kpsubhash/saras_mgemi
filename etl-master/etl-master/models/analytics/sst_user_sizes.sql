{{ config(materialized='table',
			tags=["nightly"]) }}

SELECT x.billing_email, y.category, y.subcategory, y.identifier as size, sum(x.sales_order_quantity)
FROM {{ ref('sst_master_orders_analytics') }} x
-- LEFT JOIN {{ ref('tf_shopify_product_data_current') }} y on x.sales_order_sku = y.sku
LEFT JOIN {{ ref('sst_product_data') }} y on y.upc = x.sales_order_sku
GROUP BY 1,2,3,4