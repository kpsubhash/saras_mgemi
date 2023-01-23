{{ config(materialized='table',
		  tags=["hourly", "nightly"],
          post_hook=[
            after_commit("create index if not exists {{ this.name }}__index_on_the_date on {{ this }} (the_date)"),
            after_commit("create index if not exists {{ this.name }}__index_on_pvid on {{ this }} (pvid)"),
            after_commit("create index if not exists {{ this.name }}__index_on_price on {{ this }} (full_price_on_date)")
          ]) }}


with drops as (
--need launch_date here for time of actual happening
select pvid, style, launch_date as event_date, event, retail_price
from {{ source('datascience', 'dim_weekly_drop') }} d 
where variant_drop_rank=1
)

, price_changes as (
--need master date here for time of actual happening
select pvid, style, master_date as event_date, event, retail_price
from {{ source('datascience','dim_master_events') }} e
where event = 'price_change' 

UNION

--price changes can occur on subsequent drops as well
select pvid, style, launch_date as event_date, event, retail_price
from {{ source('datascience', 'dim_weekly_drop') }} d 
where variant_drop_rank<>1
)

,int_t1 as(
select 

distinct on (x, d.pvid)
x, 
d.pvid as drop_pvid, 
d.event_date, 
case when d.event_date = x then d.retail_price else null end as first_drop_retail_price, 
p.pvid, 
p.event_date, 
p.retail_price as price_change_retail_price, 
last_value(coalesce(p.retail_price,case when d.event_date = x then d.retail_price else null end)) over (partition by d.pvid order by x) as current_price

from drops d
left join generate_series('2015-01-01', CURRENT_DATE + interval '1 day', INTERVAL '1 day') x on x >= d.event_date
left join price_changes p on p.pvid = d.pvid and x = p.event_date
)

,int_t2 as (
select *, sum(current_price) over (partition by drop_pvid order by x asc rows unbounded preceding) as dummy_group
from int_t1
)

,int_t3 as (
select 
x as the_date, 
drop_pvid as pvid, 
FIRST_VALUE(current_price) over (partition by drop_pvid, dummy_group order by x ) as full_price_on_date
from int_t2
)

select *
from int_t3
