{{ config(materialized='table',
		  tags=["morning"],
			post_hook=[
        after_commit("create index if not exists {{ this.name }}__index_on_the_date on {{ this }} (the_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_source on {{ this }} (the_date, source)"),
        after_commit("create index if not exists {{ this.name }}__index_on_medium on {{ this }} (the_date, medium)"),
        after_commit("create index if not exists {{ this.name }}__index_on_campaign on {{ this }} (the_date, campaign)"),
        after_commit("create index if not exists {{ this.name }}__index_on_ad_content on {{ this }} (the_date, ad_content)"),
        after_commit("create index if not exists {{ this.name }}__index_on_channel on {{ this }} (the_date, channel)"),
        after_commit("create index if not exists {{ this.name }}__index_on_device_category on {{ this }} (the_date, device_category)")
      ]) }}


SELECT
  id,
  medium,
  source,
  campaign,
  ad_content,
  the_date,
  device_category,
  event_category,
  event_action,
  sessions,
  users,
  new_users,
  transactions,
  revenue,
  CASE
    WHEN POSITION('email' IN medium)>0 THEN 'Email'
    WHEN POSITION('cpc' IN medium)>0 THEN 'Paid Search'
    WHEN POSITION('-ad' IN medium)>0 THEN 'Paid Social'
    WHEN POSITION('paidsocial' IN medium)>0 THEN 'Paid Social'
    WHEN POSITION('referral' IN medium)>0 THEN 'Referral'
    WHEN POSITION('social' IN medium)>0 THEN 'Organic Social'
    WHEN POSITION('facebook' IN source)>0 THEN 'Paid Social'
    WHEN POSITION('talkable' IN source)>0 THEN  'Refer a Friend'
    WHEN POSITION('criteo' IN source)>0 THEN  'Display'
    WHEN POSITION('organic' IN medium)>0 THEN 'SEO'
    WHEN POSITION('googlesyndication' IN source)>0 THEN  'Paid Search'
    WHEN POSITION('doubleclick' IN source)>0 THEN  'Paid Search'
    WHEN POSITION('criteo' IN source)>0 THEN  'Display'
    WHEN POSITION('influencer' IN medium)>0 THEN  'Influencers'
    WHEN POSITION('facebook' IN source)>0 THEN  'Paid Social'
    WHEN POSITION('instagram' IN source)>0 THEN  'Paid Social'
    WHEN POSITION('none' IN medium)>0 THEN 'Direct'
    WHEN POSITION('affiliate' IN medium)>0 THEN 'Affiliate'
    WHEN POSITION('display' IN medium)>0 THEN 'Display'
    WHEN POSITION('influencer' IN medium)>0 THEN 'Partnerships'
    WHEN POSITION('sms' IN medium)>0 THEN 'SMS'
    WHEN POSITION('partner' IN medium)>0 THEN 'Partnerships'
    WHEN POSITION('reminder' IN medium)>0 THEN 'Referral'
    WHEN POSITION('retention' IN medium)>0 THEN 'Partnerships'
    WHEN POSITION('not set' IN medium)>0 THEN 'Direct'
    WHEN POSITION('facebook' IN medium)>0 THEN 'Paid Social'
    WHEN POSITION('podcast' IN medium)>0 THEN 'Display'
    WHEN POSITION('pla' IN medium)>0 THEN 'Paid Search'
    WHEN POSITION('blog' IN medium)>0 THEN 'Partnerships'
    WHEN POSITION('prganic' IN medium)>0 THEN 'Paid Social'
    WHEN POSITION('text' IN medium)>0 THEN 'SMS'
    WHEN POSITION('twitter' IN medium)>0 THEN 'Paid Social'
    WHEN POSITION('instagram' IN medium)>0 THEN 'Paid Social'
    WHEN POSITION('media' IN medium)>0 THEN 'Display'
    WHEN POSITION('app' IN medium)>0 THEN 'Referral'
    WHEN POSITION('shopdirectory' IN medium)>0 THEN 'Referral'
    ELSE 'Direct'
  END AS channel
FROM {{ source('public', 'raw_ga_traffic_with_events') }}
