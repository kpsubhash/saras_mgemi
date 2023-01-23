{{ config(materialized='view') }}


(
  SELECT
    order_number,
    oms_order_number,
    sku,
    MIN(receipt_date) as receipt_date,
    SUM(quantity) as quantity,
    MIN(product_status) as product_status,
    'raw_quiet_returns'::VARCHAR AS source
  FROM {{ source('public', 'raw_quiet_returns')}}
  GROUP BY 1,2,3
)
UNION
(
  WITH delivered_line_items AS (
    SELECT
      id,
      jsonb_array_elements(line_items) AS line_item,
      total,
      refund,
      carrier,
      currency,
      customer,
      exchange,
      loop_order_id,
      gift_card,
      created_at,
      order_name,
      -- note that the Shopify "order_number" value is actually 1000 less than the actual order, so we add 1000
      (order_number::INTEGER + 1000) AS order_number,
      tracking_number,
      label_updated_at
    FROM {{ source('public', 'loop_return_status')}}
    WHERE state='closed' AND label_status='delivered'
  )
  SELECT
    d.order_number::VARCHAR AS order_number,
    d.order_number::TEXT AS oms_order_number,
    d.line_item->>'sku' as sku,
    MIN(d.label_updated_at)::TIMESTAMPTZ as receipt_date,
    COUNT(*) as quantity,
    'GOOD'::VARCHAR AS product_status,
    'loop_return_status'::VARCHAR AS source
  FROM delivered_line_items d
  LEFT JOIN {{ source('public', 'raw_quiet_returns')}} q ON q.order_number=d.order_number::VARCHAR AND d.line_item->>'sku'=q.sku
  WHERE q.rma_number IS NULL
  GROUP BY 1,2,3
)
UNION
(
  SELECT DISTINCT
    o.order_number::VARCHAR as order_number,
    o.order_number::TEXT AS oms_order_number,
    li.sku::VARCHAR as sku,
    r.processed_at::TIMESTAMPTZ AS receipt_date,
    rli.quantity::INTEGER as quantity,
    'GOOD'::VARCHAR AS product_status,
    'bloomingdales'::VARCHAR AS source
  FROM shopify.raw_shopify_webhook_refunds r
  INNER JOIN shopify.raw_shopify_webhook_refund_line_items rli ON rli.pubsub_message_id=r.pubsub_message_id
  INNER JOIN {{ source('shopify', 'raw_shopify_webhook_orders') }} o ON o.id=r.order_id AND o.type='orders/create' AND o.gateway='Bloomingdale’s'
  INNER JOIN shopify.raw_shopify_webhook_line_items li ON li.pubsub_message_id=rli.pubsub_message_id
  WHERE r.type='refunds/create' AND r.domain='mgemi.myshopify.com'
  ORDER BY r.processed_at desc
)
