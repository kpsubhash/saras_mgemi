{{ config(materialized='view') }}


SELECT pubsub_message_id, parent_id, SUM(price) as total_object_tax
FROM {{ ref('tf_shopify_tax_lines') }}
GROUP BY 1,2

