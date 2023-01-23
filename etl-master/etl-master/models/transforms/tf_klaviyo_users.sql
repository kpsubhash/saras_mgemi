{{ config(materialized='view',
		  tags=["nightly"])
}}


SELECT
  email,
  DATE_TRUNC('week',(tstamp+'1 day'::interval))-'1 day'::interval as updated_week,
  COALESCE(person->>'Date Added', person->>'created')::TIMESTAMPTZ AS signup_date,
  MAX(CASE
    WHEN event_name='Subscribed to List' THEN 1
    ELSE 0
  END) AS is_mailable
FROM {{ source('public', 'raw_klaviyo_timeline_events') }}
WHERE event_name IN ('Subscribed to List', 'Unsubscribed', 'Unsubscribed from List')
GROUP BY 1,2,3
