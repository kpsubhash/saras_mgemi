{{ config(materialized='incremental',
          tags=["hourly", "nightly"]) }}


with untransformed_dor_data as (
select
r.store,
r.date::text,
x.*,
r.data->'data'->'weather'->>'min_temperature_f' as daily_min_temp,
r.data->'data'->'weather'->>'max_temperature_f' as daily_max_temp,
r.data->'data'->'weather'->>'probability_precipitation_am' as probability_precipitation_am,
r.data->'data'->'weather'->>'probability_precipitation_pm' as probability_precipitation_pm
from {{ source('public', 'raw_dor_data') }} r
left join lateral (select right(value->>'datetime',2) as hour, value->>'in_count' as traffic
          from jsonb_array_elements(r.data->'data'->'hours')) x on true

order by date desc, hour desc)

select
md5(
coalesce(generate_series::date::text,' ') ||
coalesce(extract('hour' from generate_series)::text,' ') ||
coalesce(w2.traffic::text,' ') ||
coalesce(w1.store::text,' ') ||
coalesce(w1.daily_min_temp::text,' ') ||
coalesce(w1.daily_max_temp::text,' ') ||
coalesce(w1.probability_precipitation_am::text,' ') ||
coalesce(w1.probability_precipitation_pm::text,' ')
) as uid,

generate_series::date as date, 
extract('hour' from generate_series) as hour,
w2.traffic, 
w1.store, 
w1.daily_min_temp, 
w1.daily_max_temp, 
w1.probability_precipitation_am, 
w1.probability_precipitation_pm 

from generate_series('2015-01-01'::date, CURRENT_TIMESTAMP::date, '1 hour'::interval)
left join (select store, 
					date, 
					data->'data'->'weather'->>'max_temperature_f' as daily_max_temp, 
					data->'data'->'weather'->>'min_temperature_f' as daily_min_temp,
        			data->'data'->'weather'->>'probability_precipitation_am' as probability_precipitation_am, 
        			data->'data'->'weather'->>'probability_precipitation_pm' as probability_precipitation_pm
    		from raw_dor_data
    ) w1 on w1.date::text = generate_series::date::text 

left join untransformed_dor_data w2 on w2.store = w1.store and w2.date::text = generate_series::date::text and w2.hour = extract('hour' from generate_series)::text

--order by generate_series::date desc, extract('hour' from generate_series) desc

{% if is_incremental() %}
  where
  generate_series::date > (select max(date) from {{ this }})
  and
  extract('hour' from generate_series) > (select max(hour) from {{ this }} where (generate_series::date = (select max(date) from {{ this }})))
{% endif %}