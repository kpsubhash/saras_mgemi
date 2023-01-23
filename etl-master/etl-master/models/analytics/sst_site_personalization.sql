{{ config(materialized='table',
		  tags=["nightly"]) }}

SELECT

sh.email,
sh.current_segment='Lead' as is_lead,
sh.latest_order_date

FROM {{ ref('sst_file_segmentation_history') }} sh
--get only current week	
WHERE sh.date_week = date_trunc('week',(CURRENT_TIMESTAMP+'1 day'::interval))-'1 day'::interval

