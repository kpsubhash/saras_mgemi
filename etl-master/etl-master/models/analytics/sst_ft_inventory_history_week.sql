{{ config(
      materialized='table',
      tags=["morning"],
      post_hook=[
        after_commit("create index if not exists {{ this.name }}__index_on_the_date on {{ this }} (the_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_sku on {{ this }} (sku)"),
        after_commit("create index if not exists {{ this.name }}__index_on_upc on {{ this }} (upc)"),
        after_commit("create index if not exists {{ this.name }}__index_on_start_avail on {{ this }} (start_avail)"),
        after_commit("create index if not exists {{ this.name }}__index_on_end_avail on {{ this }} (end_avail)"),
        after_commit("create index if not exists {{ this.name }}__index_on_start_received on {{ this }} (start_received)"),
        after_commit("create index if not exists {{ this.name }}__index_on_end_received on {{ this }} (end_received)")
      ])
}}



SELECT
    DATE(sample_timestamp) AS the_date,
    s.id AS sku,
    s.upc AS upc,
    s.product_variant_id AS product_variant_id,

    MIN(CASE WHEN i.first_of_day THEN event_id END) AS start_quiet_id,
    MIN(CASE WHEN i.last_of_day THEN event_id END)  AS end_quiet_id,

    MIN(CASE WHEN i.first_of_day THEN sample_timestamp END) AS start_quiet_loadtime,
    MIN(CASE WHEN i.last_of_day  THEN sample_timestamp END) AS end_quiet_loadtime,

    MIN(CASE WHEN i.first_of_day THEN received END) AS start_received,
    MIN(CASE WHEN i.first_of_day THEN avail END)    AS start_avail,
    MIN(CASE WHEN i.first_of_day THEN alloc END)    AS start_alloc,
    MIN(CASE WHEN i.first_of_day THEN dam END)      AS start_dam,

    MIN(CASE WHEN i.last_of_day THEN received END) AS end_received,
    MIN(CASE WHEN i.last_of_day THEN avail END)    AS end_avail,
    MIN(CASE WHEN i.last_of_day THEN alloc END)    AS end_alloc,
    MIN(CASE WHEN i.last_of_day THEN dam END)      AS end_dam
FROM {{ source('public', 'raw_quiet_inventory_summary') }} i
INNER JOIN {{ ref('sst_product_data') }} s ON i.sku = s.upc

-- the following line restricts the addition of values to recent values
WHERE i.sample_timestamp > (current_date - interval '7 day')

GROUP BY s.id, s.upc, s.product_variant_id, the_date


