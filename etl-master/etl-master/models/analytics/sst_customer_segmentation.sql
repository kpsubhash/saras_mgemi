{{ config(materialized='table',
			tags=["nightly"]) }}



WITH orders AS (
  SELECT
    order_id,
    MIN(LOWER(billing_email)) AS lower_billing_email,
    SUM(sales_order_quantity) AS sales_order_quantity,
    COALESCE(SUM(return_quantity), 0) AS return_quantity,
    MAX(subsequent_order) AS subsequent_order,
    SUM(discount) AS discount,
    SUM(gross_revenue) AS gross_revenue,
    MIN(order_date) AS order_date
  FROM {{ ref('sst_master_orders_analytics') }}
  GROUP BY order_id
),


first_orders AS (
  SELECT DISTINCT ON (lower_billing_email)
    lower_billing_email,
    sales_order_quantity,
    return_quantity,
    subsequent_order IS NOT NULL AS has_exchange,
    discount,
    gross_revenue,
    order_date
  FROM orders
  ORDER BY lower_billing_email, order_date
),


prelim_fields AS (

SELECT 
*,
lower(billing_email) as lower_billing_email,
FIRST_VALUE(lower(billing_first_name)) OVER (PARTITION BY lower(billing_email)) as first_val_billing_first_name,
FIRST_VALUE(lower(billing_last_name)) OVER (PARTITION BY lower(billing_email)) as first_val_billing_last_name,
MIN(order_date) OVER (PARTITION BY lower(billing_email)) as first_order_date,
MAX(order_date) OVER (PARTITION BY lower(billing_email)) as last_order_date,
FIRST_VALUE(lower(billing_state)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_billing_state,
FIRST_VALUE(lower(billing_city)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_billing_city,
FIRST_VALUE(lower(SPLIT_PART(billing_postalcode, '-',1))) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_billing_postalcode,
FIRST_VALUE(lower(billing_address1)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_billing_address1,
FIRST_VALUE(lower(billing_address2)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_billing_address2,


FIRST_VALUE(lower(shipping_state)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_shipping_state,
FIRST_VALUE(lower(shipping_city)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_shipping_city,
FIRST_VALUE(lower(shipping_postalcode)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_shipping_postalcode,
FIRST_VALUE(lower(shipping_address1)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_shipping_address1,
FIRST_VALUE(lower(shipping_address2)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_shipping_address2,
FIRST_VALUE(lower(shipping_country)) OVER (PARTITION by lower(billing_email) order by order_date desc) as last_shipping_country

FROM {{ ref('sst_master_orders_analytics') }}
)



SELECT 

f.lower_billing_email as billing_email,
f.first_val_billing_first_name as billing_first_name,
f.first_val_billing_last_name as billing_last_name,
f.first_order_date,
fo.sales_order_quantity as first_sales_order_quantity,
fo.return_quantity as first_return_quantity,
fo.has_exchange as first_has_exchange,
fo.discount as first_discount,
fo.gross_revenue as first_gross_revenue,
f.last_order_date,
f.last_billing_state,
f.last_billing_city,
f.last_billing_postalcode,
f.last_billing_address1,
f.last_billing_address2,
f.last_shipping_state,
f.last_shipping_city,
f.last_shipping_postalcode,
f.last_shipping_address1,
f.last_shipping_address2,
f.last_shipping_country,

MIN(CASE WHEN f.user_order_seq = 1 and f.original_order_id IS NULL THEN f.order_location_type ELSE NULL END) as acquisition_channel,
SUM(f.sales_order_quantity - COALESCE(f.return_quantity, 0)) as user_net_units,
MAX(CASE WHEN f.user_order_seq > 1 THEN 1 ELSE 0 END) as ever_repeated,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '12 months' THEN 1 ELSE 0 END) as repeater_1yr,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '30 days' THEN 1 ELSE 0 END) as repeater_30d,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '60 days' THEN 1 ELSE 0 END) as repeater_60d,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '90 days' THEN 1 ELSE 0 END) as repeater_90d,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '12 months'
	and f.original_order_id IS NULL and NOT f.is_shopnow_order THEN 1 ELSE 0 END) as repeater_1yr_no_exch,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '30 days'
	and f.original_order_id IS NULL and NOT f.is_shopnow_order THEN 1 ELSE 0 END) as repeater_30d_no_exch,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '60 days'
	and f.original_order_id IS NULL and NOT f.is_shopnow_order THEN 1 ELSE 0 END) as repeater_60d_no_exch,
MAX(CASE WHEN f.order_date > f.first_order_date and f.order_date <= f.first_order_date + interval '90 days'
	and f.original_order_id IS NULL and NOT f.is_shopnow_order THEN 1 ELSE 0 END) as repeater_90d_no_exch,
MAX(CASE WHEN f.user_order_seq > 1 and f.original_order_id IS NULL and NOT f.is_shopnow_order THEN 1 ELSE 0 END) as ever_repeated_no_exch,
COUNT(DISTINCT CASE WHEN f.order_date <= f.first_order_date + interval '12 months' THEN order_id ELSE NULL END) as frequency_12m,
COUNT(DISTINCT CASE WHEN f.order_date <= f.first_order_date + interval '90 days' THEN order_id ELSE NULL END) as frequency_90d,
COUNT(DISTINCT CASE WHEN f.order_date <= f.first_order_date + interval '12 months' and original_order_id IS NULL and NOT f.is_shopnow_order THEN order_id ELSE NULL END) as frequency_12m_no_exch,
SUM(CASE WHEN f.user_order_seq > 1 THEN f.gross_revenue ELSE 0 END) as repeat_gross_revenue,
COUNT(DISTINCT CASE WHEN f.user_order_seq > 1 THEN f.order_id ELSE NULL END) as repeat_order_count,
STRING_AGG(CASE WHEN user_order_seq=1 THEN p.product_name ELSE NULL END, ',') as first_styles,
STRING_AGG(CASE WHEN user_order_seq=1 THEN p.subcategory ELSE NULL END, ',') as first_subcategories,
STRING_AGG(DISTINCT p.subcategory, ',') as subcategories,
STRING_AGG(DISTINCT p.product_name, ',') as styles,
STRING_AGG(DISTINCT pv.heel_structure, ',') as heel_structures, 
STRING_AGG(DISTINCT pv.heel, ',') as heel_types, 
STRING_AGG(DISTINCT pv.material, ',') as materials, 
STRING_AGG(DISTINCT pv.color_family, ',') as colors, 
SUM( CASE WHEN f.order_date <= f.first_order_date + interval '12 months' THEN f.gross_revenue ELSE 0 END) as ltv_12_month,
SUM(CASE WHEN f.order_date=f.last_order_date THEN f.sales_order_quantity-coalesce(f.return_quantity,0) ELSE NULL END ) as user_net_units_last_order,

SUM(f.gross_revenue - COALESCE(f.returned_revenue,0)) as net_lifetime_revenue,

CASE WHEN (MIN(p.department) like 'Women%' and MAX(p.department) like 'Women%') THEN 'womens_only'
    when (MIN(p.department) like 'Men%' and MAX(p.department) like 'Women%') THEN 'womens_and_mens'
    when (MIN(p.department) like 'Men%' and MAX(p.department) like 'Men%') THEN 'mens_only'
    ELSE 'other' END as buyer_department,
SUM(CASE WHEN f.user_order_seq=1 THEN (f.sales_order_quantity - COALESCE(f.return_quantity, 0)) ELSE NULL END) as user_net_units_first_order


FROM prelim_fields f --{{ ref('sst_master_orders_analytics') }} f
LEFT JOIN first_orders fo ON fo.lower_billing_email=f.lower_billing_email
LEFT JOIN {{ ref('sst_product_data') }} p on p.upc = f.sales_order_sku
LEFT JOIN {{ source('datascience', 'dim_master_variant') }} pv on p.product_variant_id::text=pv.pvid
WHERE f.lower_billing_email <> ''
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
