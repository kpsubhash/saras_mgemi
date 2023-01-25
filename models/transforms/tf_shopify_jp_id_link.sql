{{ config(materialized='view') }}

WITH raw_sales as (

SELECT 

o.external_id,
d.upc

FROM {{ source('public', 'raw_sales_order_items') }} i
LEFT JOIN {{ source('public', 'raw_sales_orders') }} o ON o.id = i.order_id
LEFT JOIN {{ ref('sst_product_data') }} d ON i.sku_id = d.product_sku_id
)

,raw_events_shopify_jp_link as (

select o.referencefield2, o.referencefield3, o.order_id, o.client, l.sku
FROM {{ source('public', 'raw_event_order_line_items') }} l
JOIN {{ source('public', 'raw_event_orders') }} o ON l.event_id = o.event_id
WHERE o.source_code <> 'replacement' and o.is_current AND o.ordertype_code::text = 'sales'::text 

)



select  x.*, y.* 
from raw_sales x
left join raw_events_shopify_jp_link y on x.external_id::text = y.order_id::text AND x.upc::text = y.sku::text
where referencefield3 = 'Shopify'
group by 1,2,3,4,5,6,7
