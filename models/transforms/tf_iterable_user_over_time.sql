{{ config(materialized='table',
		  tags=["nightly", "exclude_hourly"],
			post_hook= [
        after_commit("create index if not exists {{ this.name }}__index_on_date_week on {{ this }} (date_week)"),
        after_commit("create index if not exists {{ this.name }}__index_on_signup_date on {{ this }} (signup_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_email on {{ this }} (email)"),
        after_commit("create index if not exists {{ this.name }}__index_on_updated_week on {{ this }} (updated_week)"),
        after_commit("create index if not exists {{ this.name }}__index_on_is_mailable on {{ this }} (is_mailable)"),
        after_commit("create index if not exists {{ this.name }}__index_on_email_date_week on {{ this }} (email, date_week)")
      ])
}}


WITH dates(date_week) as (SELECT * FROM generate_series('2016-01-03'::DATE, CURRENT_DATE, '7 day'))

, distinct_users as (select email, MAX(signup_date) AS signup_date FROM {{ ref('tf_email_users_union') }} GROUP BY 1)

, mailable_over_time as (
  SELECT
    d.date_week,
    du.signup_date,
    du.email,
    u.updated_week,
    u.is_mailable,
    COUNT(u.is_mailable) OVER (PARTITION BY du.email ORDER BY d.date_week) as grouping
  FROM distinct_users du
  LEFT JOIN dates d on d.date_week >= du.signup_date
  LEFT JOIN {{ ref('tf_email_users_union') }} u on du.email = u.email and d.date_week = u.updated_week
  ORDER BY 1
)

SELECT 
date_week,
signup_date,
email,
updated_week,
coalesce(MAX(is_mailable) OVER (PARTITION BY email, grouping),1) as is_mailable
FROM mailable_over_time

