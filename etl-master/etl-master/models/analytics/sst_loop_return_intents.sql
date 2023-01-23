{{ config(materialized='table',
			tags=["hourly", "nightly"]) }}


SELECT *
FROM {{ ref('tf_loop_return_current') }}