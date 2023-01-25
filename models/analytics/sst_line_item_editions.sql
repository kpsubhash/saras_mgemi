{{ config(materialized='table',
      tags=["nightly", "hourly"],
      post_hook=[
        after_commit("create index if not exists {{ this.name }}__index_on_line_item_id on {{ this }} (line_item_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_edition_id on {{ this }} (edition_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_edition_date on {{ this }} (edition_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_product_variant_id on {{ this }} (product_variant_id)")
      ]) }}


-- NOTE 1: it's possible for a single line item to have multiple units that are split into multiple shipments, which is why we have to include tracking_id
-- NOTE 2: each line_item_id is associated with a specific SKU, which is why we don't have to include the sku in the DISTINCT ON clause
SELECT DISTINCT ON (o.line_item_id, o.tracking_id)
  o.storefront_order_id,
  o.line_item_id,
  o.tracking_id,
  o.order_date,
  o.sales_order_quantity,
  me.id AS edition_id,
  me.product_variant_id,
  me.edition_date,
  me.retail_usd AS edition_retail_usd
FROM {{ ref('sst_master_orders_analytics') }} o
LEFT JOIN {{ ref('sst_master_edition') }} me ON me.sku=o.sales_order_sku AND me.edition_date <= o.order_date
ORDER BY o.line_item_id, o.tracking_id, me.edition_date DESC
