{{ config(materialized='view') }}

SELECT 

pubsub_message_id,
parent_id,
rate,
price::numeric,
title


FROM {{ source('shopify', 'raw_shopify_webhook_tax_lines') }}
