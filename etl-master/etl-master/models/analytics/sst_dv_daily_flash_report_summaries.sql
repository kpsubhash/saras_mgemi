{{ config(materialized='table',
			tags=["nightly", "morning"]) }}




SELECT
('1. yesterday (' || (order_date AT TIME ZONE 'America/New_York')::DATE || ')')::text as period,
sales_order_quantity AS number_units_ordered,
COALESCE(external_oms_id, internal_shopify_order_id)  AS oms_id,
--revenue_gross  AS exchange_order_flag,
order_type,
gross_revenue,
source,
sales_order_quantity,
originating_line_unit_price


FROM {{ ref('sst_master_orders_analytics') }}
WHERE ((order_date AT TIME ZONE 'America/New_York') >= (SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + (-1 || ' day')::INTERVAL))
AND (order_date AT TIME ZONE 'America/New_York') < (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL)))

UNION ALL

SELECT
'3. last_week'::text as period,
sales_order_quantity AS number_units_ordered,
COALESCE(external_oms_id, internal_shopify_order_id) AS oms_id,
--revenue_gross  AS exchange_order_flag,
order_type,
gross_revenue,
source,
sales_order_quantity,
originating_line_unit_price
FROM {{ ref('sst_master_orders_analytics') }}
WHERE ((order_date AT TIME ZONE 'America/New_York') >= (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + ((MOD(6 + 0 - EXTRACT(DOW FROM DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'))::integer, 7) - 6) || ' day')::INTERVAL) + (-7 || ' day')::INTERVAL))
AND (order_date AT TIME ZONE 'America/New_York') < (SELECT (((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + ((MOD(6 + 0 - EXTRACT(DOW FROM DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'))::integer, 7) - 6) || ' day')::INTERVAL) + (-7 || ' day')::INTERVAL) + (7 || ' day')::INTERVAL))
AND (order_date AT TIME ZONE 'America/New_York') < (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL)))

UNION ALL

SELECT
'2. week_to_date'::text as period,
sales_order_quantity AS number_units_ordered,
COALESCE(external_oms_id, internal_shopify_order_id) AS oms_id,
--revenue_gross  AS exchange_order_flag,
order_type,
gross_revenue,
source,
sales_order_quantity,
originating_line_unit_price
FROM {{ ref('sst_master_orders_analytics') }}
WHERE ((order_date AT TIME ZONE 'America/New_York') >= (SELECT (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + ((MOD(6 + 0 - EXTRACT(DOW FROM DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'))::integer, 7) - 6) || ' day')::INTERVAL)) AND (order_date AT TIME ZONE 'America/New_York') < (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + ((MOD(6 + 0 - EXTRACT(DOW FROM DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'))::integer, 7) - 6) || ' day')::INTERVAL) + (7 || ' day')::INTERVAL))
AND (order_date AT TIME ZONE 'America/New_York') < (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL)))

UNION ALL

SELECT
'4. month_to_date'::text as period,
sales_order_quantity AS number_units_ordered,
COALESCE(external_oms_id, internal_shopify_order_id) AS oms_id,
--revenue_gross  AS exchange_order_flag,
order_type,
gross_revenue,
source,
sales_order_quantity,
originating_line_unit_price
FROM {{ ref('sst_master_orders_analytics') }}
WHERE ((order_date AT TIME ZONE 'America/New_York') >= (SELECT DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'))) AND (order_date AT TIME ZONE 'America/New_York') < (SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')) + (1 || ' month')::INTERVAL))
AND (order_date AT TIME ZONE 'America/New_York') < (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL)))

UNION ALL

SELECT
'5. year_to_date'::text as period,
sales_order_quantity AS number_units_ordered,
COALESCE(external_oms_id, internal_shopify_order_id) AS oms_id,
--revenue_gross  AS exchange_order_flag,
order_type,
gross_revenue,
source,
sales_order_quantity,
originating_line_unit_price
FROM {{ ref('sst_master_orders_analytics') }}
WHERE ((order_date AT TIME ZONE 'America/New_York') >= (SELECT DATE_TRUNC('year', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'))) AND (order_date AT TIME ZONE 'America/New_York') < (SELECT (DATE_TRUNC('year', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')) + (1 || ' year')::INTERVAL))
AND (order_date AT TIME ZONE 'America/New_York') < (SELECT ((DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York') + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL)))



