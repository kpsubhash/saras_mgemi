{{ config(materialized='view') }}


--This CTE does handle cases where jsonb_array could have more than 1 item if it happens
WITH parsed_flow as (
select 

shopify_order_id, 
regexp_replace(lower(d->>'description'), '[^a-zA-Z]', '', 'g') as payment_method, 
sum((d->'total'->'base'->>'amount')::numeric) as payment_amount


from {{ source('shopify', 'raw_flow_order') }} 
left join lateral jsonb_array_elements(data->'payments') d(data) on true
where domain = 'mgemi.myshopify.com'
group by 1,2

)


, flatten_flow as (


SELECT 

--This macro will make new columns for every payment method flow describes (visa, amex, paypal, and new ones)
--In case when novel payment method is introduced, it will automatically create the column
shopify_order_id,
{{get_flow_payment_method_amounts()}}

FROM parsed_flow
GROUP BY 1

)

SELECT 

c.order_id, 
SUM(COALESCE(f.americanexpress_amount,0.0) + case when coalesce(c.payment_details_credit_card_company, a.payment_details_credit_card_company) = 'American Express'  then c.amount::numeric else 0::numeric end) as amex,
SUM(COALESCE(f.visa_amount,0.0) + case when coalesce(c.payment_details_credit_card_company, a.payment_details_credit_card_company) = 'Visa' then c.amount::numeric else 0::numeric end) as visa,
SUM(COALESCE(f.mastercard_amount,0.0) + case when coalesce(c.payment_details_credit_card_company, a.payment_details_credit_card_company) = 'Mastercard' then c.amount::numeric else 0::numeric end) as mastercard,
SUM(case when coalesce(c.payment_details_credit_card_company, a.payment_details_credit_card_company) = 'Discover' then c.amount::numeric else 0::numeric end) as discover,
SUM(case when coalesce(c.payment_details_credit_card_company, a.payment_details_credit_card_company) = 'EA_CREDIT_CARD' then c.amount::numeric else 0::numeric end) as ea_cc,
SUM(COALESCE(f.paypal_amount,0.0) + case when coalesce(c.gateway,a.gateway) = 'paypal' then c.amount::numeric else 0::numeric end) as paypal,
SUM(case when coalesce(c.gateway,a.gateway) = 'amazon_payments' then c.amount::numeric else 0::numeric end) as amazonpay,
SUM(case when coalesce(c.gateway,a.gateway) = 'shopify_installments' then c.amount::numeric else 0::numeric end) as shopify_installments,
SUM(case when coalesce(c.gateway,a.gateway) = 'gift_card' then c.amount::numeric else 0::numeric end) as giftcard,
SUM(case when coalesce(c.gateway,a.gateway) = 'exchange-credit' then c.amount::numeric else 0::numeric end) as return_exchange_credit,
SUM(case when coalesce(c.gateway,a.gateway) = 'slice_it_pay_over_time_with_klarna' or coalesce(c.gateway,a.gateway) = 'Klarna' then c.amount::numeric else 0::numeric end) as klarna,
STRING_AGG(coalesce(c.wallet_type,a.wallet_type,c.gateway,a.gateway),'~') as gateways

FROM {{ ref('tf_shopify_payments_order_captures') }} c
LEFT JOIN {{ ref('tf_shopify_payments_auths_and_sales') }} a on coalesce(c.parent_id, c.id) = a.id


--Flow, coalesce into things
LEFT JOIN flatten_flow f on f.shopify_order_id = c.order_id::text



WHERE c.is_current and a.is_current
GROUP BY 1
