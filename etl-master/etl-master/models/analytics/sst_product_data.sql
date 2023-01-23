{{ config(materialized='table',
			tags=["hourly", "nightly"],
			post_hook=[
        after_commit("create index if not exists {{ this.name }}__index_on_upc on {{ this }} (upc)"),
        after_commit("create index if not exists {{ this.name }}__index_on_size on {{ this }} (identifier)"),
        after_commit("create index if not exists {{ this.name }}__index_on_product_name on {{ this }} (product_name)"),
        after_commit("create index if not exists {{ this.name }}__index_on_shopify_variant_id on {{ this }} (shopify_variant_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_category on {{ this }} (category)"),
        after_commit("create index if not exists {{ this.name }}__index_on_subcategory on {{ this }} (subcategory)"),
        after_commit("create index if not exists {{ this.name }}__index_on_product_variant_id on {{ this }} (product_variant_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_cast_upc on {{ this }} ((upc::text))")
      ]) }}



WITH vendor_data as (

select distinct on (x.id) x.id, x.product_name, x.variant_name, x.color, x.product_style_code, y.name, y.code, y.product_subcategory_id, y.product_vendor_id, z.id as vendor_id, z.factory_id, z.name, z.shipment_info
from {{ source('public', 'remote_mgemi_product_variants') }} x
left join {{ source('public', 'remote_mgemi_products') }} y on x.product_style_code = COALESCE(y.code, CONCAT('00_',y.id))
left join {{ source('public', 'remote_mgemi_product_vendors') }} z on z.id = y.product_vendor_id

)



SELECT

ps.id,
ps.upc,
ps.identifier,
ps.size_id,
ps.product_variant_id,
ps.launch_ats,
pv.launch,
pv.expire,
pv.first_order_date,
ps.alltime_received,
ps.damage_level_id,
pv.variant_name,
pv.color,
pv.color_code,
pv.material,
pv.product_name,
pv.factory_region,
pv.product_id,
pv.id as variant_id,
sv.shopify_variant_id,
sv.product_sku_id,
sv.shopify_inventory_item_id,

--this logic is complicated because its basically just trying to piece different patches of the data together almost like a coalesce() to make the most accurate information we have
CASE WHEN (pv.product_data->>'guestBrand' = 'Yes') OR (v.product_vendor_id IS NOT NULL) THEN 'Guest Brand'
     WHEN (pv.category ilike 'men%' and v.product_vendor_id is null) OR pv.product_data->>'gender' = 'Male' THEN 'Men''s'
     WHEN ((pv.category ilike 'shoes%' OR pv.category = 'Handbags') and v.product_vendor_id is null) OR pv.product_data->>'gender' = 'Female' THEN 'Women''s'
     WHEN (pv.category ilike '%gift cards%' OR pv.category is null) and v.product_vendor_id is null THEN 'Gift Cards'
     ELSE 'Unknown'
   END as department,
COALESCE(hp.revised_category,pv.category) as category,
pv.cost_eur,
pv.units_landed_cost_eur,
--we want to prefer price from shopify data bc planner can be out of date now
COALESCE(sp.price::numeric, pv.retail_usd) as retail_usd,
pv.variant_primary_image as variant_primary_image,
COALESCE(hp.revised_subcategory,pv.subcategory) subcategory,
pv.edition_code,
pv.group_id,
pv.group_code,
sp.handle


FROM {{ source('public', 'dim_product_skus') }}  ps
LEFT JOIN {{ source('public', 'dim_product_variants') }} pv on ps.product_variant_id = pv.id
LEFT JOIN {{ source('public', 'shopify_variant_ids') }}sv on sv.product_sku_id = ps.id 
--Need this join for fields where planner data is sometimes out of date - eg: retail_usd
LEFT JOIN {{ ref('tf_shopify_product_data_current') }} sp on sp.sku = upc
--PATCH PRODUCT HIERARCHY WITH TANIA'S NEW RULES
--ALSO NEED TO USE PRODUCT VENDOR ID TO PREMPT PROBLEMS WITH DEPARTMENT BEING WRONG IN FUTURE
LEFT JOIN {{ ref('product_hierarchy_patch')}} hp on hp.product_variant_information_category = pv.category and hp.product_variant_information_subcategory = pv.subcategory
--Workaround to get vendor_id+data into this table
--legacy solution that probably isnt updated
LEFT JOIN vendor_data v on v.id = pv.id

WHERE pv.category <> 'Beauty'


---UNION "ORPHANED SKUS"
--(skus that are not in dim_product_skus/dim_product_variants which the first half of the union is based on - these are mostly exceptions to the rule)
UNION

SELECT 

null::bigint as id,
x.variant_id::text as upc,
null::text as identifier,
null::int4 as size_id,
null::int4 as product_variant_id,
null::int4 as launch_ats,
null::timestamptz as launch,
null::timestamptz as expire,
null::date as first_order_date,
null::int4 as alltime_received,
null::int4 as damage_level_id,
concat(x.product_title, ' - ', x.variant_title) as variant_name,
null::text as color,
null::text as color_code,
null::text as material,
x.product_title,
null::text as factory_region,
null::int4 as product_id,
null::int8 as variant_id,
x.variant_id::text as shopify_variant_id,
null::int4 as product_sku_id,
x.inventory_item_id as shopify_inventory_item_id,
(case when x.product_type = '' then 'Other' when x.product_type = 'Gift Card' then 'Gift Cards' else 'Unknown' end) as department,
(case when x.product_type = '' then 'Other' when x.product_type = 'Gift Card' then 'Gift Cards' else x.product_type end) as category,
null::numeric as cost_eur,
null::numeric as units_landed_cost_eur,
x.price::numeric as retail_usd,
null::text as variant_primary_image,
null::text as subcategory,
null::text as edition_code,
null::int4 as group_id,
null::text as group_code,
x.handle

FROM {{ ref('tf_shopify_product_data_current') }} x
where x.sku not in (select upc from {{ source('public', 'dim_product_skus')}})



