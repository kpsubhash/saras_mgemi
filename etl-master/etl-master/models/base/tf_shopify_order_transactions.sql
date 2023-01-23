{{ config(materialized='view') }}

SELECT
*,
REPLACE(COALESCE(receipt->'payment_method_details'->'card'->'wallet'->'type', receipt->'charge'->'payment_method_details'->'card'->'wallet'->'type')::text, '"', '') as wallet_type
FROM {{ source('shopify', 'raw_shopify_webhook_order_transactions') }}
WHERE test = False and status = 'success'

