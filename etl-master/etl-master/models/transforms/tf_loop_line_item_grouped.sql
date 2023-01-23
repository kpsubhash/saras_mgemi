{{ config(materialized='view') }}

--<<<<<<<<<<<THIS WON"T COVER THE EDGE CASE WHERE A QUANTITY=2 LINE ITEM GETS RETURNED IN TWO SEPARATE RETURN "ORDERS"/IDS/FLOWS>>>>>>>>>>>
WITH loop_transform as (SELECT 

l.order_name,
l.return_id,
l.created_at,
l.carrier,
l.tracking_number,
l.exchange_total::numeric,
l.return_gift_card::numeric,
l.return_refund::numeric,
li.provider_line_item_id,
li.variant_id,
li.sku,
li.line_item_id,
li.price::numeric,



SUM(li.price::numeric) OVER (PARTITION BY l.order_name) as returns_total_line_prices

---refund data --prorate at the line level i guess



FROM {{ source('loop', 'raw_loop_webhook_return_events') }} l
LEFT JOIN {{ source('loop', 'raw_loop_webhook_line_items') }}  li on li.pubsub_message_id = l.pubsub_message_id 
WHERE state = 'open' --********will be 'closed', using open for now******** 
--Brandon via email: "We don't trigger update events at state change yet, so you're not going to get a webhook for pushing "process". Currently the only event that triggers update webhooks are tracking event changes: new to in transit for example. "
)


select

order_name,
provider_line_item_id,
variant_id,
sku,
MIN(return_id) as return_id, 
MIN(created_at) as first_created_at, 
MIN(carrier) as carrier,
MIN(tracking_number) as return_tracking_number,  
COUNT(distinct line_item_id) as quantity_returned, -- each item (NOT LINE ITEM) is an entry in loop's line_item array
SUM((1.0* price/NULLIF(returns_total_line_prices,0)) * return_refund) as line_level_return_refund,
SUM((1.0* price/NULLIF(returns_total_line_prices,0)) * return_gift_card) as line_level_return_giftcard,
SUM((1.0* price/NULLIF(returns_total_line_prices,0)) * exchange_total) as line_level_exchange_amount
FROM loop_transform
GROUP BY 1,2,3,4