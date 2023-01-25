{{ config(materialized='table',
	tags=["hourly", "nightly"]) }}

select 

sk.upc, 
sk.id, 
x.start_avail, 
x.start_dam, 
rct.last_sku_received_at

from {{ ref('sst_ft_inventory_history') }} x
join {{ ref('sst_product_data') }} sk on x.sku = sk.id
left join lateral
	(
	select max(receive_date) as last_sku_received_at
	from {{ source('public', 'raw_quiet_po_receipt') }}  por
	join {{ source('public', 'ft_purchase_orders') }} po on cast(po_id as text) = por.po_number
	where receive_quantity > 0
		and item_number = sk.upc
		and receive_date < x.the_date
	) rct on true
where x.the_date = current_date - '1 day'::interval


-- The following code should be the same as the above code, but is a little simpler

-- WITH most_recent_receipt AS (
-- 	SELECT
--     por.item_number AS upc,
--     max(por.receive_date) AS last_sku_received_at
-- 	FROM {{ source('public', 'raw_quiet_po_receipt') }} por
-- 	JOIN {{ source('public', 'ft_purchase_orders') }} po ON CAST(po_id AS TEXT) = por.po_number
-- 	WHERE receive_quantity > 0 AND por.receive_date < current_date - '1 day'::interval
--   GROUP BY 1
-- )
-- SELECT
--   sk.upc,
--   sk.id,
--   x.start_avail,
--   x.start_dam,
--   rct.last_sku_received_at
-- FROM {{ ref('sst_product_data') }} sk
-- INNER JOIN {{ ref('sst_ft_inventory_history') }} x on x.sku = sk.id AND x.the_date = current_date - '1 day'::interval
-- LEFT JOIN most_recent_receipt rct ON rct.upc=x.upc
