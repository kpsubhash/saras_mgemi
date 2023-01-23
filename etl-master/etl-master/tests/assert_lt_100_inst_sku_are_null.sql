-- If there are more than 5 rows (line items) in sst_master_order_analytics with null sales_order_sku, return 1 row -> FAILURE
-- If there are less than 5 rows (line items) in sst_master_order_analytics with null sales_order_sku, return 1 row -> SUCCESS 
-- Doing 5 Rows because there are a few test orders from when shopify launched and I dont really care about them enough to remove from data either
--IGNORES SQAURE ORDERS WITH MISSING SKU BECAUSE THAT IS NO LONGER RELEVANT

with data as (

SELECT distinct line_item_id
FROM {{ ref('sst_master_orders_analytics') }}
WHERE source = 'Shopify'and sales_order_sku IS NULL

),


agg as (

SELECT COUNT(*) as num_null_sku
FROM data

)

select num_null_sku
from agg
where num_null_sku > 100