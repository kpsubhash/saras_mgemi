
{{ config(materialized='incremental',
          tags=["exclude_hourly"],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_order_date on {{ this }} (order_date)"),
            after_commit("create index if not exists {{ this.name }}__index_on_storefront_order_id on {{ this }} (storefront_order_id)")
          ]
) }}
--add unique_key constraint later?



SELECT

    i.order_id,
    i.source,
    i.unit_price,
    i.revenue_gross,
    i.revenue_merchandise,
    i.revenue_tax,
        CASE
            WHEN o.source::text = 'Square'::text THEN COALESCE(i.revenue_discount, o.revenue_discount * (i.revenue_merchandise / sum(i.revenue_merchandise) OVER (PARTITION BY o.id)))
            ELSE i.revenue_discount
        END AS revenue_discount,
    COALESCE(z4.remote_discount, 0.00) AS remote_discount,
    i.revenue_net,
    1.0 * o.revenue_shipping / o.units AS revenue_shipping,
    o.id AS internal_oms_id,
    o.external_id AS external_oms_id,
    o.order_date,
    o.order_status,
    o.notes,
    diu.full_name as created_by,
    o.ship_date,
    o.type AS type_,
    o.from_location_id AS location_id,
    d.upc AS sales_order_sku,
    i.quantity AS sales_order_quantity,
    sum(i.quantity) OVER (PARTITION BY o.id) AS sales_order_quantity_sum_by_order,
    count(*) OVER (PARTITION BY o.id) AS line_item_count_by_order,
    x.origin_order_id,
    x.storefront_order_id,
    x.original_order_id,
    lower(COALESCE(x.billing_email::text, p1.customer_email)) AS billing_email,
    x.billing_namefirst,
    x.billing_namelast AS billing_namelastt,
    x.shipping_address1,
    x.shipping_address2,
    x.shipping_city,
    x.shipping_state,
    x.shipping_postalcode,
    x.shipping_country,
    x.billing_address1,
    x.billing_address2,
    x.billing_address3,
    x.billing_city,
    x.billing_country,
    x.billing_postalcode,
    x.shipping_phone_number,
    null::int8 as user_order_seq,--dense_rank() OVER (PARTITION BY (lower(COALESCE(x.billing_email::text, p1.customer_email))), o.type ORDER BY o.order_date) AS user_order_seq,
    y.return_order_id,
    y.return_order_created,
    y.unitprice,
    x.event_order_quantity,
    x.shipping_tax,
    y.sku AS return_sku,
    x.orderstatus_name,
    z.receipt_date,
    z.order_number AS return_order_number,
    z.product_status,
    z.quantity AS return_quantity,
    y.event_return_quantity,
    COALESCE(z1.dw_storefront_created_at, p1.created_at) AS dw_storefront_created_at,
    COALESCE(z1.amex_amount,
        CASE
            WHEN p1.store_name <> 'bloomingdales-001'::text AND p1.payment_card_type = 'Amex'::text THEN p1.payment_auth_amount
            ELSE NULL::numeric
        END) AS amex_amount,
    COALESCE(z1.visa_amount,
        CASE
            WHEN p1.store_name <> 'bloomingdales-001'::text AND p1.payment_card_type = 'Visa'::text THEN p1.payment_auth_amount
            ELSE NULL::numeric
        END) AS visa_amount,
    COALESCE(z1.master_amount,
        CASE
            WHEN p1.store_name <> 'bloomingdales-001'::text AND p1.payment_card_type = 'MasterCard'::text THEN p1.payment_auth_amount
            ELSE NULL::numeric
        END) AS master_amount,
    COALESCE(z1.discover_amount,
        CASE
            WHEN p1.store_name <> 'bloomingdales-001'::text AND p1.payment_card_type = 'Discover'::text THEN p1.payment_auth_amount
            ELSE NULL::numeric
        END) AS discover_amount,
    z1.ea_cc_amount,
    z1.paypal_amount,
    COALESCE(z1.gc_amount,
        CASE
            WHEN p1.store_name <> 'bloomingdales-001'::text AND p1.payment_gc_request_id IS NOT NULL THEN p1.payment_gc_auth_amount
            ELSE NULL::numeric
        END) AS gc_amount,
    z2.date_shipped,
    z2.order_number AS shipment_order_number,
    z2.freight_cost,
    z2.quantity AS shipment_quantity,
    z2.sku,
    z2.carrier,
    z2.service_level,
    z2.tracking_id,
    z2.return_tracking_id,
    COALESCE(z3.rma_exchange_order_line_id, z6.line_id::text) AS rma_exchange_order_line_id,
    COALESCE(z3.originating_line_unit_price, z5.prior_unit_price::double precision) AS originating_line_unit_price,
    first_value(i.revenue_tax) OVER (PARTITION BY (NULLIF(concat(z3.original_order_id, '-', z3.original_sku), '-'::text)) ORDER BY o.order_date) AS originating_revenue_tax,
    first_value(1.0 * o.revenue_shipping / o.units) OVER (PARTITION BY (NULLIF(concat(z3.original_order_id, '-', z3.original_sku), '-'::text)) ORDER BY o.order_date) AS originating_revenue_shipping,
    first_value(x.shipping_tax) OVER (PARTITION BY (NULLIF(concat(z3.original_order_id, '-', z3.original_sku), '-'::text)) ORDER BY o.order_date) AS originating_shipping_tax,
    first_value(COALESCE(z4.remote_discount, 0.00)) OVER (PARTITION BY (NULLIF(concat(z3.original_order_id, '-', z3.original_sku), '-'::text)) ORDER BY o.order_date) AS originating_remote_discount,
    NULLIF(concat(z3.original_order_id, '-', z3.original_sku), '-'::text) AS exch_flow_id,
    z1.device_order_placed,
    x.billing_state
   FROM raw_sales_order_items i
     LEFT JOIN raw_sales_orders o ON o.id = i.order_id
     LEFT JOIN dim_product_skus d ON i.sku_id = d.id
     LEFT JOIN analytics.tf_ft_event_order_line_items_sales x ON o.external_id::text = x.origin_order_id::text AND d.upc::text = x.sku::text
     LEFT JOIN LATERAL ( SELECT
            return_order_created,
            return_order_id,
            storefront_order_id,
            sku,
            unitprice,
            event_return_quantity
           FROM analytics.tf_ft_event_order_line_items_returns rr
          WHERE rr.storefront_order_id::text = x.storefront_order_id::text AND rr.sku::text = d.upc::text
          ORDER BY rr.return_order_created
         LIMIT 1) y ON true
     LEFT JOIN LATERAL ( SELECT rr.event_id,
            rr.rma_number,
            rr.receipt_date,
            rr.notes,
            rr.line_no,
            rr.reason,
            rr.quantity,
            rr.return_uom,
            rr.sku,
            rr.order_number,
            rr.product_status,
            rr.oms_order_number
           FROM raw_quiet_returns rr
          WHERE ("left"(rr.order_number::text, '-1'::integer) = o.external_id::text OR rr.oms_order_number = x.storefront_order_id) AND rr.sku::text = d.upc::text
          ORDER BY rr.receipt_date
         LIMIT 1) z ON true
     LEFT JOIN analytics.tf_ft_demandware_order_payments z1 ON z1.order_number::text = x.storefront_order_id::text
     LEFT JOIN raw_predictspring_order_items p1 ON (p1.order_number = x.storefront_order_id::text OR p1.order_number = o.external_id::text) AND p1.line_product_sku = d.upc::text
     LEFT JOIN LATERAL ( SELECT ss.event_id,
            ss.date_shipped,
            ss.order_number,
            ss.carton_count,
            ss.line_item_id,
            ss.tracking_id,
            ss.return_tracking_id,
            ss.weight,
            ss.carrier,
            ss.carton_id,
            ss.surcharge,
            ss.freight_cost,
            ss.handling_fee,
            ss.carton_number,
            ss.service_level,
            ss.quantity,
            ss.sku
           FROM raw_quiet_shipments ss
          WHERE "left"(ss.order_number::text, '-1'::integer) = o.external_id::text AND ss.sku::text = d.upc::text
          ORDER BY ss.date_shipped
         LIMIT 1) z2 ON true
     LEFT JOIN ft_exchange_lookup z3 ON z3.oms_id = o.external_id::text AND z3.sku = d.upc::text
     LEFT JOIN vw_exchange_revenue_patch_bk z5 ON z5.order_id = o.external_id::text AND z5.sku::text = d.upc::text
     LEFT JOIN vw_exchange_revenue_patch_bk z6 ON z6.original_order_id::character varying::text = o.external_id::text AND z6.original_sku::text = d.upc::text
     LEFT JOIN analytics.tf_ft_jaggedpeak_discounts z4 ON o.external_id::text = z4.order_id::text AND d.upc::text = z4.sku::text

     LEFT JOIN dim_internal_users diu on diu.id = o.created_by

  WHERE (x.referencefield3 <> 'Shopify' and i.source::text = 'JaggedPeak'::text AND x.orderstatus_name::text <> 'Cancelled'::text OR i.source::text <> 'JaggedPeak'::text)

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  and order_date > (select max(order_date) from {{ this }})
{% endif %}

