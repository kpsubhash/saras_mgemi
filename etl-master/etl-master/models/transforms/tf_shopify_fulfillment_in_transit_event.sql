{{ config(materialized='view') }}

-- NOTE that we allow "delivered" events, which should always come AFTER "in_transit" events,
-- to account for cases in which a product was shipped and delivered, but there were no recorded
-- "in_transit" events (e.g., order id 2881140162619)

SELECT
  fulfillment_id,
  MIN(happened_at) AS first_in_transit_event
FROM {{ source('shopify', 'raw_shopify_fulfillment_events_webhook')}}
WHERE status IN ('in_transit', 'delivered')
GROUP BY 1
