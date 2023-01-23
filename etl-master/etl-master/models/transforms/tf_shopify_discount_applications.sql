{{ config(materialized='table',
    post_hook= [
        after_commit("create index if not exists {{ this.name }}__index_on_pubsub_message_id on {{ this }} (pubsub_message_id)"),
        after_commit("create index if not exists {{ this.name }}__index_on_discount_target_type on {{ this }} (discount_target_type)"),
        after_commit("create index if not exists {{ this.name }}__index_on_discount_value_type on {{ this }} (discount_value_type)"),
        after_commit("create index if not exists {{ this.name }}__index_on_discount_title on {{ this }} (discount_title)")
]) }}


SELECT 

soa.pubsub_message_id, 
soa.type,
soa.order_id,
sla.line_item_id,
sla.price,
sla.quantity,
soa.tstamp,
soa.num_line_items,
soa.num_shipping_lines,
soa.num_discount_codes,
soa.discount_application_index,
COALESCE(soa.discount_title,'') as discount_title,
COALESCE(soa.discount_description,'') as discount_description,
COALESCE(soa.discount_code,'') as discount_code,
soa.discount_type,
soa.discount_value::numeric,
soa.discount_allocation_method,
soa.discount_target_selection,
soa.discount_target_type,
soa.discount_value_type,
sla.discount_application_index_line,
--Even though shopify does the prorating already, I can't rely on it in the edge case of one of the "line items" being the weird Shopify POS shipping upsell "line items" 
--I need to recalculate the prorating since I'm excluding that "line" --one, each, across
--think one + each are functionally the same, due to discount app index,  so its really IN ('one', 'each') vs 'across' as an adidtional combo of WHENS
CASE WHEN discount_value_type = 'percentage' THEN (sla.price*sla.quantity) * (soa.discount_value::numeric/100.0)
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'across' THEN COALESCE((sla.price*sla.quantity)/NULLIF(SUM(sla.price*sla.quantity) OVER (PARTITION BY soa.pubsub_message_id, soa.order_id, soa.discount_application_index),0) * soa.discount_value::numeric , 0)
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'one' THEN soa.discount_value::numeric
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'each' THEN soa.discount_value::numeric*sla.quantity
     END as discount_amount,
CASE WHEN discount_value_type = 'percentage' THEN (sla.price*sla.quantity) * (soa.loyalty_discount_value::numeric/100.0)
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'across' THEN COALESCE((sla.price*sla.quantity)/NULLIF(SUM(sla.price*sla.quantity) OVER (PARTITION BY soa.pubsub_message_id, soa.order_id, soa.discount_application_index),0) * soa.loyalty_discount_value::numeric , 0)
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'one' THEN soa.loyalty_discount_value::numeric
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'each' THEN soa.loyalty_discount_value::numeric*sla.quantity
     END as loyalty_discount_amount,
CASE WHEN discount_value_type = 'percentage' THEN (sla.price*sla.quantity) * (soa.non_loyalty_discount_value::numeric/100.0)
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'across' THEN COALESCE((sla.price*sla.quantity)/NULLIF(SUM(sla.price*sla.quantity) OVER (PARTITION BY soa.pubsub_message_id, soa.order_id, soa.discount_application_index),0) * soa.non_loyalty_discount_value::numeric , 0)
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'one' THEN soa.non_loyalty_discount_value::numeric
     WHEN discount_value_type = 'fixed_amount' and discount_allocation_method = 'each' THEN soa.non_loyalty_discount_value::numeric*sla.quantity
     END as non_loyalty_discount_amount

FROM {{ ref('tf_shopify_order_discount_applications')}} soa
LEFT JOIN {{ ref('tf_shopify_order_line_item_discount_allocations')}} sla on sla.pubsub_message_id = soa.pubsub_message_id and sla.discount_application_index_line::int = soa.discount_application_index
 