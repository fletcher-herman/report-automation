with push_sessions as 
(
select * from
(
select 
  REGEXP_REPLACE(
    case 
      when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
      else geo.country
   end, ' ', '') as country
  ,event_timestamp
  ,PARSE_DATE('%Y%m%d',event_date) as event_date
  ,event_name
  ,user_pseudo_id
  ,param.key as event_key
  ,param.value.string_value as event_key_value 
  ,up.key as user_properties_key
  ,up.value.int_value as ga_session_id
from
`cotton-on-e41b2.analytics_195776711.events_*` cross join unnest(event_params) as param cross join unnest(user_properties) as up
where event_name = 'appOpen' 
)
where event_key_value = 'Push' and user_properties_key = 'ga_session_id' 
group by 1,2,3,4,5,6,7,8,9 
order by 4,2 desc
), --130,812

ecomm_purch as
(
select
  REGEXP_REPLACE(
    case 
      when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
      else geo.country
   end, ' ', '') as country
  ,event_timestamp
  ,PARSE_DATE('%Y%m%d',event_date) as event_date
  ,event_name
  ,user_pseudo_id
  ,up.value.int_value as ga_session_id
  ,sum(event_value_in_usd) as sales_usd
from
`cotton-on-e41b2.analytics_195776711.events_*` cross join unnest(user_properties) as up
where event_name = 'ecommerce_purchase' and up.key = 'ga_session_id'
group by 1,2,3,4,5,6
),  

push_sessions_rev as
(
select 
  ps.country
  ,ps.event_date
  ,count(*) as push_sessions
  ,count(distinct(ps.user_pseudo_id)) as distinct_push_user
  ,sum(sales_usd) as push_sales_usd
from 
  push_sessions ps left join ecomm_purch ep on ps.user_pseudo_id = ep.user_pseudo_id and ps.ga_session_id = ep.ga_session_id
group by 1,2
), 

transactions
as
(
select
  PARSE_DATE('%Y%m%d',event_date) as event_date
  ,REGEXP_REPLACE(
  case 
    when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
    else geo.country
  end, ' ', '') as country  
  ,count(*) as transactions
  from `cotton-on-e41b2.analytics_195776711.events_*` ev 
where event_name = 'ecommerce_purchase'
group by 1,2
),

sessions as
(
select
  event_date
  ,TradeWeekCode
  ,week_commencing
  ,country
  ,concat(ed, country) as joinKey  
  ,sessions
from 
(
 select
  PARSE_DATE('%Y%m%d',event_date) as event_date
  ,event_date as ed
  ,TradeWeekCode
  ,date_trunc(Date, WEEK(MONDAY)) as week_commencing
  ,REGEXP_REPLACE(
    case 
      when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
      else geo.country
   end, ' ', '') as country
  ,count(*) as sessions
from `cotton-on-e41b2.analytics_195776711.events_*` ev inner join `merchandise-reporting.RMS_reporting.dateDimension` lk
on ev.event_date = cast(lk.DateKey as STRING)
where event_name = 'session_start'
group by 1,2,3,4,5)
), 

first_opens
as
(
select int.*
  ,sum(int.first_opens) over (partition by int.country order by int.country, int.event_date) as cumulative_opens
from
(
select
  PARSE_DATE('%Y%m%d',event_date) as event_date
  ,REGEXP_REPLACE(
  case 
    when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
    else geo.country
  end, ' ', '') as country  
  ,count(*) as first_opens
  from `cotton-on-e41b2.analytics_195776711.events_*` ev 
where event_name = 'first_open'
group by 1,2) int 
)

select
  s.event_date
  ,s.TradeWeekCode
  ,s.week_commencing
  ,s.country
  ,s.joinKey  
  ,ifnull(fo.first_opens, 0) as first_opens
  ,ifnull(fo.cumulative_opens, 0) as cumulative_opens
  ,ifnull(s.sessions, 0) as sessions
  ,ifnull(t.transactions, 0) as transactions
  ,ifnull(psr.push_sessions, 0) as push_sessions
  ,ifnull(psr.distinct_push_user, 0) as distinct_push_sessions
  ,ifnull(psr.push_sales_usd, 0) as push_sales_usd
from 
  sessions s left join first_opens fo on s.event_date = fo.event_date and s.country = fo.country
  left join transactions t on s.event_date = t.event_date and s.country = t.country
  left join push_sessions_rev psr on s.event_date = psr.event_date and s.country = psr.country
  order by 4,1