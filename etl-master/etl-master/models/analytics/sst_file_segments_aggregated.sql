{{ config(materialized='table',
		  tags=["nightly"]) }}


SELECT

date_week,
date_trunc('month', date_week) as date_month,
is_mailable,
CASE WHEN lifetime_items=0 THEN '0'
     WHEN lifetime_items=1 THEN '1'
     WHEN lifetime_items=2 THEN '2'
     WHEN lifetime_items=3 THEN '3'
     WHEN lifetime_items=4 THEN '4'
     WHEN lifetime_items>=5 AND lifetime_items<=10 THEN '5-10'
     WHEN lifetime_items>10 then '10+'
     ELSE 'Below 0'
     END as net_units,
lifetime_orders>1 as multi_buyer,
current_segment,
proposed_segment_definition,
count(*) as total

FROM {{ ref('sst_file_segmentation_history') }}
WHERE date_week IS NOT NULL
GROUP BY 1,2,3,4,5,6,7
