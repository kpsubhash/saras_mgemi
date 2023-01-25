{{ config(materialized='table',
		  tags=["nightly"],
			post_hook=[
        after_commit("create index if not exists {{ this.email }}__index_on_email on {{ this }} (email)"),
        after_commit("create index if not exists {{ this.signup_date }}__index_on_signup_date on {{ this }} (signup_date)"),
        after_commit("create index if not exists {{ this.updated_week }}__index_on_updated_week on {{ this }} (updated_week)"),
        after_commit("create index if not exists {{ this.current_segment }}__index_on_current_segment on {{ this }} (current_segment)")
      ]) }}


SELECT

date_week,
signup_date,
email,
updated_week,
is_mailable,
lifetime_orders,
lifetime_items,
first_order_date,
latest_order_date,
days_since_last_order,
order_this_week,
orders_in_past_year,
current_segment,
proposed_segment_definition

FROM {{ ref('sst_file_segmentation_history') }}
WHERE date_week = CASE WHEN EXTRACT('DOW' FROM CURRENT_DATE)=0 THEN DATE_TRUNC('DAY', CURRENT_DATE) ELSE DATE_TRUNC('WEEK', CURRENT_DATE) - INTERVAL '1 DAY' END
