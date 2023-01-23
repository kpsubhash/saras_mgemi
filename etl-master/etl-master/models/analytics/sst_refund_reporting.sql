{{ config(materialized='table',
		  tags=["hourly", "nightly"]) }}



select
r.order_id,
r.refund_id,
--l.line_item_id,
CONCAT(r.note, '----',r2.note),

--r2.order_id,
--r2.refund_id,
--r2.note,
-- l2.line_item_id,
-- l2.subtotal,
-- l2.total_tax,

r.created_at,
x.amount::numeric / count(*) over (partition by r.order_id, r.refund_id) as gc_refund_amount,
x2.amount::numeric / count(*) over (partition by r.order_id, r.refund_id) as paypal_refund_amount,
x3.amount::numeric / count(*) over (partition by r.order_id, r.refund_id) as other_refund_amount,
--x.amount::numeric / count(*) over (partition by r.order_id, x.id)  as amount,
COALESCE(l.line_item_id, l2.line_item_id) as line_item_id,
COALESCE(l.subtotal, l2.subtotal) as line_item_subtotal,
COALESCE(l.total_tax, l2.total_tax) as line_item_tax,

--l.subtotal,
--l.total_tax,
--case when x.gateway = 'gift_card' then 'gift_card' when  x.gateway = 'paypal' then 'paypal' else 'other' end as refund_method,
sb.province_code as billing_state

-- COALESCE(l.line_item_id, l2.line_item_id) as line_item_id,
-- COALESCE(l.subtotal, l2.subtotal) as line_item_subtotal,
-- COALESCE(l.total_tax, l2.total_tax) as line_item_tax

from {{ ref('tf_shopify_raw_refunds') }} r
--do i try this?
left join {{ ref('tf_shopify_raw_refunds') }} r2 on r2.is_current and r2.order_id = r.order_id and r2.note ilike 'marking inventory as returned &/or restocked for loop return on order %'
left join {{ source('shopify','raw_shopify_webhook_refund_line_items') }} l2 on l2.pubsub_message_id = r2.pubsub_message_id

left join {{ source('shopify', 'raw_shopify_webhook_order_transactions') }} x on x.pubsub_message_id = r.pubsub_message_id and x.kind = 'refund' and gateway = 'gift_card'
left join {{ source('shopify', 'raw_shopify_webhook_order_transactions') }} x2 on x2.pubsub_message_id = r.pubsub_message_id and x2.kind = 'refund' and x2.gateway = 'paypal'
left join {{ source('shopify', 'raw_shopify_webhook_order_transactions') }} x3 on x3.pubsub_message_id = r.pubsub_message_id and x3.kind = 'refund' and x3.gateway NOT IN ('gift_card', 'paypal')

left join {{ source('shopify','raw_shopify_webhook_refund_line_items') }} l on l.pubsub_message_id = r.pubsub_message_id
left join {{ ref('tf_shopify_orders_current') }} o on o.id = r.order_id and o.is_current
LEFT JOIN {{ source('shopify','raw_shopify_webhook_address') }} sb on sb.pubsub_message_id = o.pubsub_message_id and sb.type = 'billing_address'
where r.is_current and r.num_transactions >= 1 --and r.order_id = '1141816459323'
