{{ config(materialized='table',
		  tags=["nightly"],
			post_hook=[
        after_commit("create index if not exists {{ this.date_week }}__index_on_date_week on {{ this }} (date_week)"),
        after_commit("create index if not exists {{ this.is_latest }}__index_on_is_latest on {{ this }} (is_latest, email)"),
        after_commit("create index if not exists {{ this.email }}__index_on_email on {{ this }} (email)"),
        after_commit("create index if not exists {{ this.signup_date }}__index_on_signup_date on {{ this }} (signup_date)"),
        after_commit("create index if not exists {{ this.updated_week }}__index_on_updated_week on {{ this }} (updated_week)"),
        after_commit("create index if not exists {{ this.current_segment }}__index_on_current_segment on {{ this }} (current_segment)")
      ]) }}


SELECT


x.date_week,
x.signup_date,
x.email,
x.updated_week,
x.is_mailable,
y.lifetime_orders,
y.lifetime_items,
y.first_order_date,
y.latest_order_date,
y.days_since_last_order,
y.order_this_week,
y.orders_in_past_year,

x.date_week = (CASE WHEN EXTRACT('DOW' FROM CURRENT_DATE)=0 THEN DATE_TRUNC('DAY', CURRENT_DATE) ELSE DATE_TRUNC('WEEK', CURRENT_DATE) - INTERVAL '1 DAY' END) AS is_latest,

CASE
  WHEN COALESCE(y.lifetime_orders, 0) = 0 THEN 'Lead'
  WHEN y.days_since_last_order <= 60 THEN 'Recent Buyers (2 months)'
  WHEN y.days_since_last_order > 60 and y.days_since_last_order <= 180 THEN 'Active Buyers (60-180 days)'
  WHEN y.days_since_last_order > 180 and y.days_since_last_order <= 365 THEN 'Active Buyers (180-365 days)'
  WHEN y.days_since_last_order > 365 and y.days_since_last_order <= 730 THEN 'Lapsed Buyers (1-2 years)'
  WHEN y.days_since_last_order > 730 and y.days_since_last_order <= 1095 THEN 'Dormant Buyers (2-3 years)'
  WHEN y.days_since_last_order > 1095 THEN 'Dormant - Gone (3+ years)'
END as current_segment,

CASE
  WHEN y.orders_in_past_year = 1 THEN 'active_one_time'
  WHEN y.orders_in_past_year = 2 OR y.orders_in_past_year = 3 THEN 'active_repeater'
  WHEN y.orders_in_past_year >= 4 THEN 'active_vip'
  WHEN y.lifetime_orders > 0 AND y.orders_in_past_year = 0 THEN 'lapsed'
  WHEN (COALESCE(y.lifetime_orders,0) = 0 and (x.date_week - x.signup_date) <= INTERVAL '60 Days') OR (y.lifetime_items = 0 and ((x.date_week - y.latest_order_date) <= INTERVAL '60 Days')) THEN 'nonbuyers_recent_lead_returners'
  WHEN (COALESCE(y.lifetime_orders,0) = 0 and (x.date_week - x.signup_date) > INTERVAL '60 Days') OR  (y.lifetime_items = 0 and  ((x.date_week - y.latest_order_date) > INTERVAL '60 Days')) THEN 'nonbuyers_nonrecent_lead_returners'
  ELSE 'other'
END as proposed_segment_definition

FROM {{ ref('tf_iterable_user_over_time') }} x
LEFT JOIN {{ ref('sst_user_order_summary') }} y on x.email = y.email and x.date_week = y.date_week