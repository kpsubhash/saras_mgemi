{{ config(materialized='table',
		  tags=["nightly", "exclude_hourly"],
			post_hook= [
        after_commit("create index if not exists {{ this.name }}__index_on_signup_date on {{ this }} (signup_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_email on {{ this }} (email)"),
        after_commit("create index if not exists {{ this.name }}__index_on_email_signup_date on {{ this }} (email, signup_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_updated_week on {{ this }} (updated_week)"),
        after_commit("create index if not exists {{ this.name }}__index_on_is_mailable on {{ this }} (is_mailable)")
      ])
}}


select * from analytics.tf_iterable_users
UNION
select * from {{ ref('tf_klaviyo_users') }}
