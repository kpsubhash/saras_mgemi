{{ config(materialized='view') }}

WITH ft_master_partial as (
SELECT

order_id,
line_item_id,
source,
unit_price,
revenue_gross,
revenue_merchandise,
revenue_tax,
revenue_discount,
remote_discount,
remote_loyalty_discount,
remote_non_loyalty_discount,
revenue_net,
revenue_shipping::numeric - shipping_discount::numeric as revenue_shipping,
shipping_discount,
internal_oms_id,
external_oms_id,
internal_shopify_order_id,
order_date,
updated_at,
order_status,
notes,
created_by,
ship_date,
type_,
location_id,
sales_order_sku,
sales_order_quantity,

properties,
sales_order_quantity_sum_by_order,
line_item_count_by_order,
origin_order_id,
storefront_order_id,
original_order_id,
billing_email,
billing_first_name,
billing_last_name,
shipping_address1,
shipping_address2,
shipping_city,
shipping_state,
shipping_postalcode,
shipping_country,
billing_address1,
billing_address2,
billing_address3,
billing_city,
billing_country,
billing_postalcode,
shipping_phone_number,
return_order_id,
return_order_created,
unitprice,
event_order_quantity,
shipping_tax,
return_sku,
orderstatus_name,
receipt_date,
return_order_number,
product_status,
return_quantity,
event_return_quantity,
dw_storefront_created_at,
amex_amount,
visa_amount,
master_amount,
discover_amount,
ea_cc_amount,
paypal_amount,
shopify_installment_amount,
amazonpay_amount,
gc_amount,
shopnow_credit_amount,
stripe_amount,
klarna_amount,
gateways,
date_shipped,
shipment_order_number,
freight_cost,
shipment_quantity,
sku,
carrier,
service_level,
tracking_id,
return_tracking_id,
rma_exchange_order_line_id,
originating_line_unit_price,
originating_revenue_tax,
originating_revenue_shipping,
originating_shipping_tax,
originating_remote_discount,
exch_flow_id,
device_order_placed,
billing_state,
fulfillment_location_id,
order_location_id,
is_shopnow_order,

--discount data
discount_titles,
discount_descriptions,
discount_codes

FROM {{ ref('tf_master_orders_shopify') }}

--This union matches together the old ft_master_orders data with the tf_master_orders_shopify look-alike
--so that we can report on everything from one table in a downstream transformation (sst_master_orders_analytics)
UNION

SELECT

order_id,
null::text as line_item_id,
source,
unit_price,
revenue_gross,
revenue_merchandise,
revenue_tax,
revenue_discount,
remote_discount,
null::numeric as remote_loyalty_discount,
null::numeric as remote_non_loyalty_discount,
revenue_net,
revenue_shipping,
null::numeric as shipping_discount,
internal_oms_id,
external_oms_id,
null::text as internal_shopify_order_id,
order_date,
order_date as updated_at,
order_status,
notes,
created_by,
ship_date,
type_,
location_id,
sales_order_sku,
sales_order_quantity,
null::jsonb as properties,
sales_order_quantity_sum_by_order,
line_item_count_by_order,
origin_order_id,
storefront_order_id,
original_order_id,
billing_email,
billing_namefirst,
billing_namelastt,
shipping_address1,
shipping_address2,
shipping_city,
shipping_state,
shipping_postalcode,
shipping_country,
billing_address1,
billing_address2,
billing_address3,
billing_city,
billing_country,
billing_postalcode,
shipping_phone_number,
return_order_id,
return_order_created,
unitprice,
event_order_quantity,
shipping_tax,
return_sku,
orderstatus_name,
receipt_date,
return_order_number,
product_status,
return_quantity,
event_return_quantity,
dw_storefront_created_at,
amex_amount,
visa_amount,
master_amount,
discover_amount,
ea_cc_amount,
paypal_amount,
null::numeric as shopify_installment_amount,
null::numeric as amazonpay_amount,
gc_amount,
null::numeric as shopnow_credit_amount,
null::numeric as stripe_amount,
null::numeric as klarna_amount,
null::text as gateways,
date_shipped,
shipment_order_number,
freight_cost,
shipment_quantity,
sku,
carrier,
service_level,
tracking_id,
return_tracking_id,
rma_exchange_order_line_id,
originating_line_unit_price,
originating_revenue_tax,
originating_revenue_shipping,
originating_shipping_tax,
originating_remote_discount,
exch_flow_id,
device_order_placed,
billing_state,

(CASE WHEN source = 'JaggedPeak' THEN 1
      ELSE location_id
      END) as fulfillment_location_id,
location_id as order_location_id,
false as is_shopnow_order,

--demandware discount data (was pulled out of a looker transformation previously)
rd.discount_campaign_ids as discount_titles,
rd.discount_promotion_ids as discount_descriptions,
rd.discount_codes as discount_codes

FROM {{ ref('tf_ft_master_orders') }}
LEFT JOIN {{ ref('tf_raw_demandware_discounts') }} rd on rd.order_number = storefront_order_id and rd.order_sku = sales_order_sku
)


-- ft_tracking_id_to_fulfillment_location is a map from tracking id to fulfillment location. The old code assumes
-- that all online orders come from Quiet Logistics, which is no longer true. We use "distinct on" instead of "distinct"
-- because it's possible for the same tracking id to be in the system for different shipments (due to manual data entry
-- error), and it's better to have a rare wrong location name instead of multiple rows.
, ft_tracking_id_to_fulfillment_location AS (
  SELECT DISTINCT ON (shipment_tracking_number)
    shipment_tracking_number AS tracking_id,
    fulfillment_source_name AS fulfillment_location_name
  FROM {{ source('shopify','raw_skubana_orders') }}
  ORDER BY shipment_tracking_number, id DESC
)


, ft_master_transform_one as (
--Use this cte to join in location id and calc_user_order_Seq. might reorg later, but this works for now
select
x.*,
COALESCE(tfl.fulfillment_location_name, dl.name, sl.name) as fulfillment_location_name,
COALESCE(dl.type, sl.location_type) as fulfillment_location_type,
--"Internal" handles orders that were "draft orders" AKA created internally via shopify interface, an edge case
CASE WHEN x.device_order_placed = 'shopify_draft_order' THEN 'Internally Drafted' ELSE COALESCE(dl2.name, sl2.name) END as order_location_name,
CASE WHEN x.device_order_placed = 'shopify_draft_order' THEN 'Internally Drafted' ELSE COALESCE(dl2.type, sl2.location_type) END as order_location_type,
--Different calculations needed depending on the source
(CASE WHEN x.source = 'Square' THEN (CASE WHEN x.source = 'Square' THEN (x.revenue_merchandise+coalesce(x.revenue_discount, 0)) ELSE x.revenue_gross END)
	 WHEN x.source = 'Shopify' THEN x.revenue_gross
     WHEN x.originating_line_unit_price IS NOT NULL THEN x.originating_line_unit_price* x.sales_order_quantity
     ELSE (x.unit_price * x.sales_order_quantity)
     END)::numeric as gross_revenue,
--Different calculations needed depending on the source
(CASE WHEN x.source = 'Square' then coalesce(-x.revenue_discount,0)
     WHEN x.source = 'Shopify' THEN COALESCE(x.remote_discount, 0)
     WHEN (x.unit_price <> 0) THEN COALESCE(x.remote_discount, 0)
     ELSE COALESCE(x.originating_remote_discount* x.sales_order_quantity, 0) --calculating discount on old exchange items based on original order's discount field info
     END)::numeric as discount,
(CASE WHEN x.source = 'Shopify' THEN COALESCE(x.remote_loyalty_discount, 0)
     WHEN (x.unit_price <> 0) THEN COALESCE(x.remote_loyalty_discount, 0)
     ELSE 0 --copying the above discount logic for loyalty
     END)::numeric as loyalty_discount,
(CASE WHEN x.source = 'Square' then coalesce(-x.revenue_discount,0)
     WHEN x.source = 'Shopify' THEN COALESCE(x.remote_non_loyalty_discount, 0)
     WHEN (x.unit_price <> 0) THEN COALESCE(x.remote_non_loyalty_discount, 0)
     ELSE COALESCE(x.originating_remote_discount* x.sales_order_quantity, 0) --calculating discount on old exchange items based on original order's discount field info
     END)::numeric as non_loyalty_discount,
dense_rank() OVER (PARTITION BY x.billing_email ORDER BY x.order_date) as user_order_seq,
(CASE WHEN original_order_id IS NULL and rma_exchange_order_line_id IS NULL THEN 'regular_order'
                   WHEN rma_exchange_order_line_id IS NOT NULL and original_order_id IS NULL THEN 'first_order_in_exchange'
                   WHEN original_order_id IS NOT NULL THEN 'subsequent_order_in_exchange'
                   END) as order_type,
--indicates whether a shopify order had a shop now and/or exchange order
es.l_storefront_order_id as subsequent_order,
--These fields to plug holes in orders' state data (tax region) for tax reporting into avalara
dl.state as order_location_state,
sl.province_code as order_location_state_shopify

from ft_master_partial x
--changing these two join for order location & fulfillment location
left join dim_locations dl on dl.id = x.fulfillment_location_id
left join {{ ref('tf_shopify_locations') }} sl on sl.id = x.fulfillment_location_id
LEFT JOIN ft_tracking_id_to_fulfillment_location tfl ON tfl.tracking_id=x.tracking_id
left join dim_locations dl2 on dl2.id = x.order_location_id
left join {{ ref('tf_shopify_locations') }} sl2 on sl2.id = x.order_location_id
---identifying shopify's loop exchange/shopnow
left join {{ ref('tf_order_linkage') }} es on es.storefront_order_id = x.storefront_order_id and x.source = 'Shopify'
)

, sizes_in_order as (
SELECT
coalesce(external_oms_id, internal_shopify_order_id) as order_tmp_id,
COUNT(DISTINCT pd.identifier) as sizes_in_order
FROM ft_master_transform_one
left join {{ ref('sst_product_data') }} pd on pd.upc = sales_order_sku
GROUP BY 1
)

-------Multiple Sizes Per Style Per Order
, sizes_per_style_in_order as (
SELECT
coalesce(external_oms_id, internal_shopify_order_id) as order_tmp_id,
product_variant_id,
COUNT(DISTINCT pd.identifier) as sizes_per_style_in_order
FROM ft_master_transform_one
left join {{ ref('sst_product_data') }} pd on pd.upc = sales_order_sku
GROUP BY 1,2
)

, sizes_per_style_in_order_max as (
SELECT
order_tmp_id,
MAX(sizes_per_style_in_order) as max_sizes_per_style_in_order
FROM sizes_per_style_in_order
GROUP BY 1
)

select
order_id,
line_item_id,
source,
unit_price,
revenue_gross,
revenue_merchandise,
revenue_tax,
revenue_discount,
remote_discount,
revenue_net,
revenue_shipping,
shipping_discount,
internal_oms_id,
external_oms_id,
internal_shopify_order_id,
order_date,
updated_at,
order_status,
notes,
created_by,
ship_date,
type_,
location_id,
sales_order_sku,
sales_order_quantity,
properties,
sales_order_quantity_sum_by_order,
line_item_count_by_order,
origin_order_id,
storefront_order_id,
original_order_id,
billing_email,
billing_first_name,
billing_last_name,
shipping_address1,
shipping_address2,
shipping_city,
shipping_state,
shipping_postalcode,
shipping_country,
billing_address1,
billing_address2,
billing_address3,
billing_city,
billing_country,
billing_postalcode,
shipping_phone_number,
return_order_id,
return_order_created,
unitprice,
event_order_quantity,
shipping_tax,
return_sku,
orderstatus_name,
receipt_date,
return_order_number,
product_status,
return_quantity,
event_return_quantity,
dw_storefront_created_at,
amex_amount,
visa_amount,
master_amount,
discover_amount,
ea_cc_amount,
paypal_amount,
shopify_installment_amount,
amazonpay_amount,
gc_amount,
shopnow_credit_amount,
--Used to split out klarna payment (and maybe stripe as well)
SUM(gross_revenue) over (partition by coalesce(external_oms_id, internal_shopify_order_id)) as order_total_gross_revenue,
stripe_amount,
klarna_amount,
gateways,
date_shipped,
shipment_order_number,
freight_cost,
shipment_quantity,
sku,
carrier,
service_level,
tracking_id,
return_tracking_id,
rma_exchange_order_line_id,
originating_line_unit_price,
originating_revenue_tax,
originating_revenue_shipping,
originating_shipping_tax,
originating_remote_discount,
exch_flow_id,
device_order_placed,
billing_state,
fulfillment_location_id,
order_location_id,
is_shopnow_order,
discount_titles,
discount_descriptions,
discount_codes,
fulfillment_location_name,
fulfillment_location_type,
order_location_name,
order_location_type,
gross_revenue,
discount,
loyalty_discount,
non_loyalty_discount,
user_order_seq,
order_type,
subsequent_order,
order_location_state,
order_location_state_shopify,

CASE WHEN source IN ('Shopify', 'Square', 'PredictSpring') and ph.full_price_on_date <> unit_price THEN True
     WHEN source = 'JaggedPeak' and ph.full_price_on_date <> (COALESCE(originating_line_unit_price, unit_price) + discount) THEN True
     ELSE False
     END as markdown,

(x.sizes_in_order > 1) as multi_size_order,
(y.max_sizes_per_style_in_order > 1) as multi_size_style_in_order,

ph.full_price_on_date

from ft_master_transform_one
left join {{ ref('sst_product_data') }} pd on pd.upc = sales_order_sku
--markdown vs full price distinction (putting here because i need the discount field)
left join {{ ref('sst_price_history') }} ph on ph.pvid = pd.product_variant_id::text and ph.the_date = date_trunc('day', order_date)
--joins for fields around sizes per order (context: sometimes useful for returns analysis)
left join sizes_in_order x on x.order_tmp_id = coalesce(external_oms_id, internal_shopify_order_id)
left join sizes_per_style_in_order_max y on y.order_tmp_id = coalesce(external_oms_id, internal_shopify_order_id)



