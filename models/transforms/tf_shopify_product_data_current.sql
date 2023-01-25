{{ config(materialized='incremental', unique_key='sku') }}


with product_info as (

select *, case when row_number() over (partition by id order by updated_at desc) = 1 then true else false end as is_current
from {{ source('shopify', 'raw_shopify_webhook_products') }}
where type <> 'products/delete' and domain = 'mgemi.myshopify.com'

)

, product_variant_info as (

select *, case when row_number() over (partition by id order by updated_at desc) = 1 then true else false end as is_current
from {{ source('shopify', 'raw_shopify_webhook_product_variants') }}

)


select

distinct on (v.sku)

p.updated_at,
p.id as product_id,
v.id as variant_id,
p.product_type,
v.inventory_item_id,
p.title as product_title,
p.tags,
p.product_set_collection,
p.published_scope,
v.title as variant_title,
v.price,
v.sku,
p.image::jsonb->>'src' as image_link,
p.handle,
v.option2 as size

from product_info p
left join product_variant_info v on v.product_id = p.id
where p.is_current and v.is_current
order by v.sku, p.updated_at desc
