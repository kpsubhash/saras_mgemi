{{ config(materialized='incremental',
    unique_key='unique_incremental_key',
			post_hook= [
        after_commit("create index if not exists {{ this.name }}__index_on_unique_key on {{ this }} (unique_incremental_key)"),
        after_commit("create index if not exists {{ this.name }}__index_on_order_date on {{ this }} (order_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_storefront_order_id on {{ this }} (storefront_order_id)")
      ]
) }}


SELECT
concat(o.id,i.id) as unique_incremental_key,
o.id as order_id,
i.id as line_item_id,
'Shopify'::text as source,
i.price as unit_price,
((i.price - (1.0*COALESCE(ld.total_line_discounts,0)/i.quantity))  *i.quantity) as revenue_gross, 
null::numeric as revenue_merchandise, -- this is just used for calculating Square revenue
t.total_object_tax as revenue_tax,
null::numeric as revenue_discount, --THIS IS ONLY USED FOR DOING SQUARE, WHICH WILL BE GOING AWAY
ld.total_line_discounts::numeric as remote_discount,
ld.total_loyalty_line_discounts::numeric as remote_loyalty_discount,
ld.total_non_loyalty_line_discounts::numeric as remote_non_loyalty_discount,
null::numeric as revenue_net,
CASE WHEN posship.price is null THEN (1.0 * sl.shipping_revenue_pre_discount / o.num_line_items) ELSE  ( 1.0 * posship.price / (o.num_line_items-1)) END as revenue_shipping, --((posship.price - (1.0*COALESCE(ld.total_line_discounts,0)/i.quantity)) * i.quantity) --The case statement is swapping in "normal revenue" from shipping lines to be revenue shipping --(((i.price - (1.0*COALESCE(ld.total_line_discounts,0)/i.quantity))  *i.quantity))    
null::integer as internal_oms_id,--one piece of logic uses; can be replaced with external_oms_id i think
patch.external_id::text as external_oms_id, --not line level
o.id::text as internal_shopify_order_id,
o.processed_at as order_date,
o.updated_at as updated_at,
o.type as order_status,
o.note as notes,
fli.first_in_transit_event as ship_date, 
null::text as type_, --no longer used in business logic. really just useful for planners/internal orders
null::text as created_by,
null::int as location_id,
pm.upc as sales_order_sku, 
i.quantity as sales_order_quantity,
i.properties as properties,
sum(i.quantity) OVER (PARTITION BY o.id) as sales_order_quantity_sum_by_order,
CASE WHEN posship.price is null THEN o.num_line_items ELSE o.num_line_items-1 END as line_item_count_by_order, -- logic here is to adjust for my removal/handling of shopify POS "Next Day Shipping" as a line item
o.id as origin_order_id, 
regexp_replace(o.name::text, '#', '', 'gi') as storefront_order_id, 
null::integer as original_order_id, 
lower(o.email) as billing_email,
sb.first_name as billing_first_name,
sb.last_name as billing_last_name,
ss.address1 as shipping_address1,
ss.address2 as shipping_address2,
ss.city as shipping_city,
ss.province_code as shipping_state,
ss.zip as shipping_postalcode,
ss.country_code as shipping_country,
sb.address1 as billing_address1,
sb.address2 as billing_address2,
null::text as billing_address3,
sb.city as billing_city,
sb.country_code as billing_country,
sb.zip as billing_postalcode,
ss.phone as shipping_phone_number,
null::integer as return_order_id, --return order id not used anywhere in business logic
null::timestamp with time zone as return_order_created, --doesnt look like its used
null::numeric as unitprice, --we dont use this in any business logic
null::integer as event_order_quantity, --we dont use this in any business logic
st.total_object_tax as shipping_tax, 
(case when rq.sku is not null then pm.shopify_variant_id else null end) as return_sku,
null::text as orderstatus_name, --not used in business logic
COALESCE(rq.receipt_date, rq2.receipt_date, rqgc.created_at) as receipt_date,
lrt.return_id::text as return_order_number,
rq.product_status as product_status,
NULLIF(COALESCE(rq.quantity, 0) + COALESCE(rq2.quantity, 0) + COALESCE(rqgc.quantity, 0), 0) as return_quantity,
null::integer as event_return_quantity, --not used in business logic
null::timestamp with time zone as dw_storefront_created_at, --not used in business logic

op.amex as amex_amount,
op.visa as visa_amount,
op.mastercard as master_amount,
op.discover as discover_amount,
op.ea_cc as ea_cc_amount,
op.paypal as paypal_amount,
op.shopify_installments as shopify_installment_amount,
op.amazonpay as amazonpay_amount,
op.giftcard as gc_amount,
COALESCE(sn.total_shop_now_loop_discount,op.return_exchange_credit) as shopnow_credit_amount,
NULLIF(TRIM(SPLIT_PART(SPLIT_PART(o.note,'-- Stripe charge collected on this return: $',2), '. (', 1)),'')::numeric as stripe_amount,
op.klarna as klarna_amount,
gateways,

fli.first_in_transit_event as date_shipped,
fli.name as shipment_order_number,
null::numeric as freight_cost, --we don't use freight_cost in business logic
fli.quantity as shipment_quantity,
fli.variant_id::text as sku, --shopify variant_id is the equiv of sku
fli.tracking_company as carrier, --will tracking company always == carrier?
sl.code as service_level, --this will get hairy if there's ever more than one ship method on an order!
CASE WHEN sl.shipping_discount::numeric > 0 THEN
    CASE WHEN posship.price is null THEN (1.0 * sl.shipping_discount / o.num_line_items) ELSE (1.0 * sl.shipping_discount / (o.num_line_items-1)) END
ELSE
    CASE WHEN posship.price is null THEN (1.0 * sld.discount_value / o.num_line_items) ELSE (1.0 * sld.discount_value / (o.num_line_items-1)) END
END as shipping_discount,
--->> shipping lines has this even though fulfillment seems not to? but shipping lines i cant tie to a line?
fli.tracking_number as tracking_id,
lrt.return_tracking_number as return_tracking_id, --lr.return_tracking_number
null::text as rma_exchange_order_line_id, --el.exchange_order_id::text
null::float8 as originating_line_unit_price, --el.originating_line_unit_price::float8
null::numeric as originating_revenue_tax, --el.originating_revenue_tax
null::numeric as originating_revenue_shipping, --el.originating_revenue_shipping
null::numeric as originating_shipping_tax, --el.originating_shipping_tax
null::numeric as originating_remote_discount, --el.originating_remote_discount
null::text as exch_flow_id, --el.exch_flow_id
o.source_name as device_order_placed, --o.source_name
sb.province_code as billing_state, --sb.province
COALESCE(o.location_id, 20453326907::int8) as fulfillment_location_id, --WHEN THIS IS NULL IT MEANS ITS A WEB STORE ORDER SO WE WANT TO SAY IT'S COMING FROM "QUIET" WHICH IS HOW IT LOOKS NOW
CASE WHEN source_name = 'pos' THEN COALESCE(spm.shopify_location_id, spm2.shopify_location_id, o.location_id, 20453326907::int8) 
	 ELSE COALESCE(o.location_id, 20453326907::int8) END as order_location_id,
---shopnow flag
CASE WHEN o.name::text like 'L-%' THEN true ELSE false END as is_shopnow_order,
--discount fields
ld.line_discount_titles as discount_titles,
ld.line_discount_descriptions as discount_descriptions,
ld.line_discount_codes as discount_codes


FROM {{ ref('tf_shopify_orders_current') }} o
LEFT JOIN {{ ref('tf_shopify_raw_line_items') }} i on i.pubsub_message_id = o.pubsub_message_id
--line item tax 
LEFT JOIN {{ ref('tf_aggregated_taxes') }} t on t.pubsub_message_id = i.pubsub_message_id and t.parent_id = i.id
--get shipping and also billing info using two joins
LEFT JOIN {{ source('shopify', 'raw_shopify_webhook_address') }} sb on sb.pubsub_message_id = o.pubsub_message_id and sb.type = 'billing_address'
LEFT JOIN {{ source('shopify', 'raw_shopify_webhook_address') }} ss on ss.pubsub_message_id = o.pubsub_message_id and ss.type = 'shipping_address'
--shopify supports multiple shipping lines aka shipping methods on an order, but i assume we will not be using this
LEFT JOIN {{ ref('tf_shopify_shipping_lines_aggregated') }} sl on sl.pubsub_message_id = o.pubsub_message_id  
--handling shipping revenue of POS shipping upsells that show up as line items / products, weirdly
LEFT JOIN {{ ref('tf_pos_shipping_upsells') }} posship on posship.pubsub_message_id = o.pubsub_message_id
--shipping tax
LEFT JOIN {{ ref('tf_aggregated_taxes') }} st on st.pubsub_message_id = i.pubsub_message_id and st.parent_id = sl.id
--payment data
LEFT JOIN {{ ref('tf_shopify_payments_order_flat') }} op on op.order_id = o.id
--(Shopify variant id <--> old mgemi sku) mapping
LEFT JOIN {{ ref('sst_product_data') }} pm on pm.shopify_variant_id = i.variant_id::text
--line level fulfillment data
LEFT JOIN {{ ref('tf_shopify_fulfillment_line_items') }} fli on fli.order_id = o.id and fli.line_item_id = i.id
--return_tracking_id: we give this its own join so we can see it in transit and not just when it reaches the warehouse (if we joined w/ raw_quiet_data)
LEFT JOIN {{ ref('tf_loop_line_item_grouped') }} lrt on lrt.order_name = o.name and lrt.variant_id = i.variant_id::text
--Link shopify id to jp id (we pass orders through JP still at the moment so every shopify order also has a JP order - we filter these out in tf_ft_master_orders so they are not duplicated - one from each source)
--QL is processing returns alternating between shopify id and the JP id associated with the shopify id, so i need to join raw_quiet_returns two times and take GREATEST() so I dont miss any
LEFT JOIN {{ ref('tf_shopify_jp_id_link') }} patch on (case when patch.referencefield2 not like '#%' and patch.referencefield2 not like 'L%' then patch.referencefield2::int8 else patch.client::int8 end) = o.id and patch.sku = pm.upc
--check for return against shopify id
LEFT JOIN {{ ref('tf_raw_quiet_returns_grouped') }} rq on rq.oms_order_number = regexp_replace(o.name::text, '#', '', 'gi') and rq.sku = pm.upc and rq.receipt_date > o.processed_at
--check for return against jp id
LEFT JOIN {{ ref('tf_raw_quiet_returns_grouped') }} rq2 on regexp_replace(rq2.oms_order_number, '[^0-9]+', '','g') = patch.external_id::text and rq2.sku = patch.sku and rq2.receipt_date > o.processed_at
--check for gift card returns
LEFT JOIN {{ ref('tf_shopify_refunds_all') }} rqgc on rqgc.line_item_id::text=i.id and rqgc.is_gift_card
--discount amounts line level aggregated ADDING THIS JOIN SO WE CAN GET REMOTE_DISCOUNT SO I CAN CALC BASIC REV + GMV + TOTAL DISCOUNTS ETC 
LEFT JOIN {{ ref('tf_shopify_discounts_by_line') }} ld on ld.pubsub_message_id = i.pubsub_message_id and ld.line_item_id = i.id
--joining shopify shipping discount lines
LEFT JOIN (select pubsub_message_id, sum(discount_value::numeric) as discount_value from {{ ref('tf_shopify_discount_applications') }}
    where discount_target_type = 'shipping_line' group by pubsub_message_id) sld
    on sld.pubsub_message_id = o.pubsub_message_id
--loop discount
LEFT JOIN {{ ref('tf_shop_now_loop_discount_credits') }} sn on sn.pubsub_message_id = i.pubsub_message_id and sn.line_item_id = i.id
---TAKE CARE OF CANCELLED ORDERS / LINE ITEMS
--use o.cancelled_at and also cancel restock type from refund line items to get rid of line level cancellations
LEFT JOIN {{ ref('tf_shopify_line_cancel_refunds_grouped') }} sr on sr.line_item_id::text = i.id --sst_shopify_line_refunds
--take care of store order location
--there are two period of time where tags were not flowing through so we need to rely on pos device id / table.
--first instance was using shopify POS before actual launch (early may) and second instance was Aug 2nd to Aug8th 2019 - a bug with tagging happened and then was fixed
LEFT JOIN (select distinct shopify_location_id, name from {{ ref('tf_shopify_pos_mapping') }}) spm on spm.name = NULLIF(SPLIT_PART(SPLIT_PART(o.tags, 'Store:', 2), ',', 1),'') --spm.device_id = o.device_id
LEFT JOIN {{ ref('tf_shopify_pos_mapping') }} as spm2 ON spm2.device_id = o.device_id

where o.is_current 
--this is fine because a cancellation action is by definition on the whole order
--to partially cancel you would need to issue a partial refund and it would result in a partial fulfillment
--line level cancellations also taken care of by shopify refunds
and o.cancelled_at is null and COALESCE(sr.restock_type,'') <> 'cancel'

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run [I think i need to reference o.created_at directly bc of alias not applying in where clauses]
  and o.updated_at > (select max(updated_at) from {{ this }})
{% endif %}


