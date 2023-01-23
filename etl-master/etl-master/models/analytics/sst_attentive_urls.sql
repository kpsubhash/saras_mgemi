{{ config(materialized='table',
		  tags=["nightly", "hourly"],
			post_hook=[
        after_commit("create index if not exists {{ this.name }}__index_on_email on {{ this }} (email)"),
        after_commit("create index if not exists {{ this.name }}__index_on_the_date on {{ this }} (the_date)"),
        after_commit("create index if not exists {{ this.name }}__index_on_channel on {{ this }} (channel)"),
        after_commit("create index if not exists {{ this.name }}__index_on_utm_source on {{ this }} (utm_source)"),
        after_commit("create index if not exists {{ this.name }}__index_on_utm_medium on {{ this }} (utm_medium)")
      ]) }}


SELECT DISTINCT ON (email)
  id,
  the_date,
  email,
  phone,
  ss_ref,
  version,
  referrer,
  link,
  pathname,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_content,
  url,
  CASE
    WHEN POSITION('email' IN utm_medium)>0 THEN 'Email'
    WHEN POSITION('CPC' IN utm_medium)>0 THEN 'Paid Search'
    WHEN POSITION('-ad' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('paidsocial' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('Social' IN utm_medium)>0 THEN 'Organic Social'
    WHEN POSITION('facebook' IN utm_source)>0 THEN 'Paid Social'
    WHEN POSITION('talkable' IN utm_source)>0 THEN 'Refer a Friend'
    WHEN POSITION('Criteo' IN utm_source)>0 THEN 'Display'
    WHEN POSITION('googlesyndication' IN utm_source)>0 THEN 'Paid Search'
    WHEN POSITION('doubleclick' IN utm_source)>0 THEN 'Paid Search'
    WHEN POSITION('criteo' IN utm_source)>0 THEN 'Display'
    WHEN POSITION('Influencer' IN utm_medium)>0 THEN 'Influencers'
    WHEN POSITION('facebook' IN utm_source)>0 THEN 'Paid Social'
    WHEN POSITION('Instagram' IN utm_source)>0 THEN 'Paid Social'
    WHEN POSITION('None' IN utm_medium)>0 THEN 'Direct'
    WHEN POSITION('affiliate' IN utm_medium)>0 THEN 'Affiliate'
    WHEN POSITION('display' IN utm_medium)>0 THEN 'Display'
    WHEN POSITION('influencer' IN utm_medium)>0 THEN 'Partnerships'
    WHEN POSITION('sms' IN utm_medium)>0 THEN 'sms'
    WHEN POSITION('partner' IN utm_medium)>0 THEN 'Partnerships'
    WHEN POSITION('reminder' IN utm_medium)>0 THEN 'Referral'
    WHEN POSITION('retention' IN utm_medium)>0 THEN 'Partnerships'
    WHEN POSITION('not set' IN utm_medium)>0 THEN 'Direct'
    WHEN POSITION('facebook' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('podcast' IN utm_medium)>0 THEN 'Display'
    WHEN POSITION('pla' IN utm_medium)>0 THEN 'Paid Search'
    WHEN POSITION('blog' IN utm_medium)>0 THEN 'Partnerships'
    WHEN POSITION('prganic' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('text' IN utm_medium)>0 THEN 'SMS'
    WHEN POSITION('twitter' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('Instagram' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('Media' IN utm_medium)>0 THEN 'Display'
    WHEN POSITION('Instagram' IN utm_medium)>0 THEN 'Paid Social'
    WHEN POSITION('Media' IN utm_medium)>0 THEN 'Display'
    WHEN POSITION('app' IN utm_medium)>0 THEN 'Referral'
    WHEN POSITION('shopdirectory' IN utm_medium)>0 THEN 'Referral'
    WHEN POSITION('google' IN ss_ref)>0 THEN 'SEO'
    WHEN POSITION('Bing' IN ss_ref)>0 THEN 'SEO'
    WHEN POSITION('email' IN url)>0 THEN 'Email'
    WHEN POSITION('social' IN url)>0 THEN 'Paid Social'
    WHEN POSITION('display' IN url)>0 THEN 'Display'
    WHEN POSITION('affiliate' IN url)>0 THEN 'Affiliate'
    WHEN POSITION('text' IN url)>0 THEN 'SMS'
    WHEN POSITION('cpc' IN url)>0 THEN 'Paid Search'
    WHEN POSITION('p-ad' IN url)>0 THEN 'Paid Social'
    WHEN POSITION('facebook' IN url)>0 THEN 'Paid Social'
    WHEN POSITION('pinterest' IN url)>0 THEN 'Paid Social'
    WHEN POSITION('talkable' IN url)>0 THEN 'Refer a Friend'
    WHEN POSITION('organic' IN url)>0 THEN 'Organic'
    ELSE NULL
  END AS channel
FROM {{ source('public', 'raw_attentive_urls') }}
ORDER BY email, the_date


