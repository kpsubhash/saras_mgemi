{{ config(materialized='table',
          tags=["nightly"],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_email on {{ this }} (email)"),
            after_commit("create index if not exists {{ this.name }}__index_on_date_week on {{ this }} (date_week)"),
            after_commit("create index if not exists {{ this.name }}__index_on_orders_in_past_year on {{ this }} (orders_in_past_year)")
          ])
}}

with dates(date_week) as (select * from generate_series('2016-01-03'::date, current_date, '7 day') )

SELECT
  lower(billing_email) as email,
  date_week,
  CASE WHEN date_week = date_trunc('week', current_date)-'1 day'::interval then true else false end as is_current_week,
  COUNT(distinct a.order_id) as lifetime_orders,
  COUNT(DISTINCT case when a.order_date >= (date_week - INTERVAL '1 year') then a.order_id else null end) as orders_in_past_year,
  SUM(sales_order_quantity-coalesce(return_quantity,0)) as lifetime_items,
  MIN(a.order_date) as first_order_date,
  MAX(a.order_date) as latest_order_date,
  DATE_PART('days', date_week - MAX(a.order_date)) as days_since_last_order,
  DATE_PART('days', date_week - MAX(CASE WHEN a.order_date < date_week - interval '7 days' THEN a.order_date else NULL END))
  as days_since_last_order_before,
  MAX(CASE WHEN a.user_order_seq = 1 AND discount > 0 THEN 1 ELSE 0 END) as first_order_discount,
  CASE WHEN DATE_PART('days', date_week - MAX(a.order_date)) < 7
  THEN 1 ELSE 0 end as order_this_week
FROM {{ ref('sst_master_orders_analytics') }} a
left join lateral (SELECT lower(billing_email) as email,
      b.date_week,
      a.order_date
      from dates as b where date_trunc('week',a.order_date) < b.date_week) as chec
      ON true
GROUP BY 1,2