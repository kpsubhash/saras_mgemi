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
  e.id,
  e.medium,
  e.source,
  e.campaign,
  e.ad_content,
  e.the_date,
  e.device_category,
  CASE
    WHEN POSITION('email' IN e.medium)>0 THEN 'Email'
    WHEN POSITION('cpc' IN e.medium)>0 THEN 'Paid Search'
    WHEN POSITION('-ad' IN e.medium)>0 THEN 'Paid Social'
    WHEN POSITION('paidsocial' IN e.medium)>0 THEN 'Paid Social'
    WHEN POSITION('referral' IN e.medium)>0 THEN 'Referral'
    WHEN POSITION('social' IN e.medium)>0 THEN 'Organic Social'
    WHEN POSITION('facebook' IN e.source)>0 THEN 'Paid Social'
    WHEN POSITION('talkable' IN e.source)>0 THEN  'Refer a Friend'
    WHEN POSITION('criteo' IN e.source)>0 THEN  'Display'
    WHEN POSITION('organic' IN e.medium)>0 THEN 'SEO'
    WHEN POSITION('googlesyndication' IN e.source)>0 THEN  'Paid Search'
    WHEN POSITION('doubleclick' IN e.source)>0 THEN  'Paid Search'
    WHEN POSITION('criteo' IN e.source)>0 THEN  'Display'
    WHEN POSITION('influencer' IN e.medium)>0 THEN  'Influencers'
    WHEN POSITION('facebook' IN e.source)>0 THEN  'Paid Social'
    WHEN POSITION('instagram' IN e.source)>0 THEN  'Paid Social'
    WHEN POSITION('none' IN e.medium)>0 THEN 'Direct'
    WHEN POSITION('affiliate' IN e.medium)>0 THEN 'Affiliate'
    WHEN POSITION('display' IN e.medium)>0 THEN 'Display'
    WHEN POSITION('influencer' IN e.medium)>0 THEN 'Partnerships'
    WHEN POSITION('sms' IN e.medium)>0 THEN 'SMS'
    WHEN POSITION('partner' IN e.medium)>0 THEN 'Partnerships'
    WHEN POSITION('reminder' IN e.medium)>0 THEN 'Referral'
    WHEN POSITION('retention' IN e.medium)>0 THEN 'Partnerships'
    WHEN POSITION('not set' IN e.medium)>0 THEN 'Direct'
    WHEN POSITION('facebook' IN e.medium)>0 THEN 'Paid Social'
    WHEN POSITION('podcast' IN e.medium)>0 THEN 'Display'
    WHEN POSITION('pla' IN e.medium)>0 THEN 'Paid Search'
    WHEN POSITION('blog' IN e.medium)>0 THEN 'Partnerships'
    WHEN POSITION('prganic' IN e.medium)>0 THEN 'Paid Social'
    WHEN POSITION('text' IN e.medium)>0 THEN 'SMS'
    WHEN POSITION('twitter' IN e.medium)>0 THEN 'Paid Social'
    WHEN POSITION('instagram' IN e.medium)>0 THEN 'Paid Social'
    WHEN POSITION('media' IN e.medium)>0 THEN 'Display'
    WHEN POSITION('app' IN e.medium)>0 THEN 'Referral'
    WHEN POSITION('shopdirectory' IN e.medium)>0 THEN 'Referral'
    ELSE 'Direct'
  END AS channel,
  e.sessions as impression_sessions,
  e.transactions as impression_transactions,
  e.revenue as impression_revenue,
  evt_submit_email.sessions as submit_email_sessions,
  evt_submit_email.transactions as submit_email_transactions,
  evt_submit_email.revenue as submit_email_revenue,
  evt_submit_sms.sessions as submit_sms_sessions,
  evt_submit_sms.transactions as submit_sms_transactions,
  evt_submit_sms.revenue as submit_sms_revenue
FROM {{ source('public', 'raw_ga_traffic_with_events') }} e

LEFT JOIN {{ source('public', 'raw_ga_traffic_with_events') }} evt_submit_email ON
  evt_submit_email.medium = e.medium
  AND evt_submit_email.source = e.source
  AND evt_submit_email.campaign = e.campaign
  AND evt_submit_email.ad_content = e.ad_content
  AND evt_submit_email.the_date = e.the_date
  AND evt_submit_email.device_category = e.device_category
  AND evt_submit_email.event_category = e.event_category
  AND evt_submit_email.event_action ='submitEmail'

LEFT JOIN {{ source('public', 'raw_ga_traffic_with_events') }} evt_submit_sms ON
  evt_submit_sms.medium = e.medium
  AND evt_submit_sms.source = e.source
  AND evt_submit_sms.campaign = e.campaign
  AND evt_submit_sms.ad_content = e.ad_content
  AND evt_submit_sms.the_date = e.the_date
  AND evt_submit_sms.device_category = e.device_category
  AND evt_submit_sms.event_category = e.event_category
  AND evt_submit_sms.event_action ='submitSMS'

WHERE e.event_category = 'Attentive' AND e.event_action = 'impression'
