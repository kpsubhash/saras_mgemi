{{ config(materialized='table',
      tags=["nightly", "hourly"],
      post_hook=[
        after_commit("create index if not exists {{ this.name }}__index_on_id on {{ this }} (id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_product_variant_id on {{ this }} (product_variant_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_shopify_product_id on {{ this }} (shopify_product_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_sku on {{ this }} (sku)"),
        after_commit("create index if not exists {{ this.name }}__index_on_edition_date on {{ this }} (edition_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_product_name on {{ this }} (product_name)")
      ]) }}


SELECT
  me.id AS id,
  me.product_variant_id AS product_variant_id,
  me.edition_identifier AS edition_date,
  me.retail_usd AS retail_usd,
  srs.shopify_product_id,
  srs.upc AS sku,
  srs.product_name,
  srs.product_variant_name,
  srs.color
FROM {{ source('public', 'raw_master_edition') }} me
INNER JOIN {{ source('public', 'sku_rosetta_stone') }} srs ON srs.product_variant_id=me.product_variant_id
