{{ config(materialized='view') }}

SELECT 

order_id,
line_item_id,
source,
unit_price,
--have to handle different scenarios differently
--subsequent_order_in_exchange is relevant for pre-Shopify orders: it indicates it is an exchange order which must be handled differently
(CASE WHEN source = 'Shopify' THEN revenue_tax 
     WHEN order_type <> 'subsequent_order_in_exchange' THEN COALESCE(revenue_tax*sales_order_quantity,0) 
     ELSE COALESCE(originating_revenue_tax*sales_order_quantity,0) END)::numeric  as revenue_tax,
CASE WHEN order_type <> 'subsequent_order_in_exchange' THEN COALESCE(revenue_shipping,0) ELSE COALESCE(originating_revenue_shipping, 0) END as revenue_shipping,
COALESCE(shipping_discount, 0) as shipping_discount,
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
CASE WHEN order_type <> 'subsequent_order_in_exchange' THEN COALESCE(shipping_tax,0) ELSE COALESCE(originating_shipping_tax, 0) END as shipping_tax,
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
order_total_gross_revenue,
--prorating based on relative proportion of revenue by line for financial reporting
stripe_amount * (1.0*(gross_revenue/nullif(order_total_gross_revenue,0)) )  as stripe_amount,
klarna_amount * (1.0*(gross_revenue/nullif(order_total_gross_revenue,0)) )  as klarna_amount,
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
--coalesce for tax reporting purposes
COALESCE(billing_state, order_location_state, order_location_state_shopify) as billing_state,
fulfillment_location_id,
order_location_id,
fulfillment_location_name,
fulfillment_location_type,
order_location_name,
order_location_type,
is_shopnow_order,
discount_titles,
discount_descriptions,
discount_codes,
gross_revenue,
(COALESCE((return_quantity/NULLIF(sales_order_quantity,0)),0) *  gross_revenue) as returned_revenue,
discount,
loyalty_discount,
non_loyalty_discount,
user_order_seq,
order_type,
subsequent_order,
markdown,
multi_size_order,
multi_size_style_in_order,
full_price_on_date,
--revenue before discount (discount is NOT the same as markdown)
--discount means a code or equivlent is used; markdown means the item was, for example, in Before They Go and got price slashed
(f.gross_revenue + f.discount) as gross_merchandise,
case when f.user_order_seq = 1 then 'new' else 'repeat' end as new_vs_repeat,

---Used for calculating payments by line: currently part of the derived table in the view
SUM( 
	CASE WHEN is_shopnow_order THEN (COALESCE(amex_amount,0)+COALESCE(visa_amount,0)+COALESCE(master_amount,0)+COALESCE(discover_amount,0)+COALESCE(ea_cc_amount,0)+COALESCE(paypal_amount,0)+COALESCE(shopify_installment_amount,0)+COALESCE(amazonpay_amount,0)+COALESCE(gc_amount,0)+COALESCE(shopnow_credit_amount,0)+COALESCE(stripe_amount,0)+COALESCE(klarna_amount,0))
	ELSE
	(CASE WHEN source = 'Shopify' THEN (unit_price * sales_order_quantity) - discount ELSE COALESCE(originating_line_unit_price, unit_price, 0::numeric) * sales_order_quantity END)  + 
	COALESCE((CASE WHEN source = 'Shopify' THEN revenue_tax WHEN order_type <> 'subsequent_order_in_exchange' THEN COALESCE(revenue_tax*sales_order_quantity,0) ELSE COALESCE(originating_revenue_tax,0) END),0::numeric) + 
	COALESCE(revenue_shipping, 0::numeric) + COALESCE(CASE WHEN source = 'Shopify' then shipping_tax/sales_order_quantity_sum_by_order else shipping_tax end, 0::numeric)
	END
	) OVER (PARTITION BY coalesce(internal_shopify_order_id::text, external_oms_id::text)) as total_order_payments,

--Combo ship date
COALESCE(
  ship_date,
  date_shipped,
  -- Use order_date as a catch-all for orders that weren't from Shopify or that weren't fulfilled from the Warehouse.
  -- If we always use order_date as the catch-all, then we will attribute pre-order items to the wrong month.
  -- (c.f. https://mgemieng.atlassian.net/browse/MG-2899)
  CASE
    WHEN source <> 'Shopify' THEN order_date
    WHEN fulfillment_location_type <> 'Warehouse' THEN order_date
    WHEN gateways = 'flow' THEN order_date
    ELSE NULL
  END
) as combo_ship_date,

md5(concat(coalesce(external_oms_id::text,internal_shopify_order_id::text), storefront_order_id::text, sales_order_sku::text, order_date::text)) as primekey



FROM {{ ref('tf_master_orders_union') }} f



