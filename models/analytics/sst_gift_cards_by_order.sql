{{ config(materialized='table',
          tags=["nightly"],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_order_id on {{ this }} (order_id)"),
            after_commit("create index if not exists {{ this.name }}__index_on_name on {{ this }} (name)"),
            after_commit("create index if not exists {{ this.name }}__index_on_order_date on {{ this }} (order_date)")
          ]) }}



with sales as (
  select distinct on (id)
    *,
    (receipt->>'gift_card_id')::BIGINT AS gift_card_id
  from {{ source('shopify', 'raw_shopify_webhook_order_transactions') }}
  where
    status='success'
    and kind='sale'
    and gateway='gift_card'
  order by id, tstamp
),
refunds as (
  select distinct on (id)
    *,
    (receipt->>'gift_card_id')::BIGINT AS gift_card_id
  from {{ source('shopify', 'raw_shopify_webhook_order_transactions') }}
  where
    status='success'
    and kind='refund'
    and gateway='gift_card'
  order by id, tstamp
),
gift_cards AS (
  SELECT DISTINCT ON (id)
    id,
    order_id as gift_card_order_id,
    note IS NOT NULL AND note LIKE 'code:%' AS is_clutch_gift_card,
    note IS NOT NULL AND note  ~* 'order\s*#?[0-9]+' AS is_physical_gift_card,
    note IS NOT NULL AND note ILIKE '%STCREDIT%' AS is_store_credit,
    created_at,
    CASE WHEN note LIKE 'code:%' THEN NULL ELSE REGEXP_REPLACE(note, '[\r\n]', ' ', 'g') END AS note
  FROM {{ source('shopify','raw_shopify_webhook_gift_cards') }}
  ORDER BY id, tstamp DESC
),
processed_gift_cards AS (
  SELECT
    id,
    gift_card_order_id,
    is_clutch_gift_card,
    is_physical_gift_card,
    is_store_credit,
    is_clutch_gift_card=FALSE AND is_physical_gift_card=FALSE AND is_store_credit=FALSE AND gift_card_order_id IS NULL AS is_appeasement,
    created_at,
    note
  FROM gift_cards
),
orders AS (
  SELECT DISTINCT ON (id)
    id,
    name,
    processed_at
  FROM {{ source('shopify', 'raw_shopify_webhook_orders') }}
  ORDER BY id
)
SELECT
  o.id AS order_id,
  o.name,
  o.processed_at::DATE AS order_date,
  ARRAY_TO_STRING(ARRAY_AGG(gc.note), E'\n') AS note,
  SUM(CASE WHEN gc.is_appeasement THEN 0 ELSE s.amount::NUMERIC END) - COALESCE(SUM(CASE WHEN gc.is_appeasement THEN 0 ELSE r.amount::NUMERIC END), 0) AS amount,
  SUM(CASE WHEN gc.is_appeasement THEN s.amount::NUMERIC ELSE 0 END) - COALESCE(SUM(CASE WHEN gc.is_appeasement THEN r.amount::NUMERIC ELSE 0 END), 0) AS appeasement_amount,
  ARRAY_AGG(gc.id) AS gift_card_ids
FROM sales s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN processed_gift_cards gc ON gc.id=s.gift_card_id
LEFT JOIN orders o_gc ON o_gc.id = gc.gift_card_order_id
LEFT JOIN refunds r ON r.parent_id=s.id
GROUP BY 1,2,3

