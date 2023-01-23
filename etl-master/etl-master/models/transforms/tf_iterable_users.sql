{{ config(materialized='table',
      enabled=false,
      tags=[],
      post_hook= [
        after_commit("create index if not exists {{ this.name }}__index_on_email on {{ this }} (email)"),
        after_commit("create index if not exists {{ this.name }}__index_on_updated_week on {{ this }} (updated_week)"),
        after_commit("create index if not exists {{ this.name }}__index_on_is_mailable on {{ this }} (is_mailable)"),
        after_commit("create index if not exists {{ this.name }}__index_on_signup_date on {{ this }} (signup_date)")
      ])
}}

-- NOTE: this table does not need to be generated more than once, since the underlying data is static.
-- Hence, it does not have "nightly" or "hourly" tags

SELECT
  lower(email) as email,
  date_trunc('week',(profile_updated_at+'1 day'::interval))-'1 day'::interval as updated_week,
  signup_date,
  --true if person was mailable at all during this week period
  MAX(CASE WHEN email_list_ids @> '{15513}' AND NOT (unsubscribed_channel_ids @> '{3562}') THEN 1
      WHEN email_list_ids @> '{15512}' AND NOT (unsubscribed_channel_ids @> '{3562}') THEN 1
      WHEN email_list_ids @> '{15539}' AND  NOT (unsubscribed_channel_ids @> '{3562}') THEN 1
    ELSE 0 END) as is_mailable
FROM {{ source('public','raw_iterable_users')}}
GROUP BY 1, 2, 3

