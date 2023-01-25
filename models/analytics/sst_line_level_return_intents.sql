{{ config(materialized='table',
			tags=["hourly", "nightly"]) }}

SELECT

order_name,
provider_line_item_id,

-- Min Date
MIN(created_at) as min_line_level_return_intent_date,

-- Line Item (quantity could be > 1, could have > 1 returns) had at least one exchange vs return
MAX((exchange_variant_id IS NOT NULL AND exchange_variant_id <> '')::int) as max_had_at_least_one_exchange_return,

--Parent Reasons
--Macro to do this logic for all parent_return_reason's that exist
{{get_loop_return_intents_parent_reasons_logic()}},

--Parent-Child Reasons
--Macro to do this logic for all parent_return_reasons that exist
{{get_loop_return_intents_child_returns_logic()}}






-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' THEN 1 ELSE 0 END) as num_parent_fit,
-- SUM(CASE WHEN parent_return_reason = 'Ordered multiple sizes' THEN 1 ELSE 0 END) as num_parent_sizes,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the item' THEN 1 ELSE 0 END) as num_parent_item,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the color' THEN 1 ELSE 0 END) as num_parent_color,
-- SUM(CASE WHEN parent_return_reason = 'Looks different online' THEN 1 ELSE 0 END) as num_parent_diff,
-- SUM(CASE WHEN parent_return_reason = 'Quality unsatisfactory' THEN 1 ELSE 0 END) as num_parent_quality,
-- SUM(CASE WHEN parent_return_reason = 'Other' THEN 1 ELSE 0 END) as num_parent_other,

-- --Dont Like The Fit
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Too long' THEN 1 ELSE 0 END) as num_reason_fit_long,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Too wide' THEN 1 ELSE 0 END) as num_reason_fit_wide,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Both too long & too wide' THEN 1 ELSE 0 END) as num_reason_fit_both_long_wide,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Too short' THEN 1 ELSE 0 END) as num_reason_fit_too_short,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Too narrow' THEN 1 ELSE 0 END) as num_reason_fit_too_narrow,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Both too short & too narrow' THEN 1 ELSE 0 END) as num_reason_fit_both_short_narrow,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Rubs foot/ankle and may cause blisters' THEN 1 ELSE 0 END) as num_reason_fit_rubs_foot,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Not enough arch support' THEN 1 ELSE 0 END) as num_reason_fit_arch,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'This item is not a shoe' THEN 1 ELSE 0 END) as num_reason_fit_not_shoe,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the fit' and return_reason = 'Other' THEN 1 ELSE 0 END) as num_reason_fit_other,

-- --Ordered Multiple Items
-- SUM(CASE WHEN parent_return_reason = 'Ordered multiple sizes' and return_reason = 'This size was too big' THEN 1 ELSE 0 END) as num_reason_sizes_big,
-- SUM(CASE WHEN parent_return_reason = 'Ordered multiple sizes' and return_reason = 'This size was too small' THEN 1 ELSE 0 END) as num_reason_sizes_small,
-- SUM(CASE WHEN parent_return_reason = 'Ordered multiple sizes' and return_reason = 'I''m returning both/all sizes' THEN 1 ELSE 0 END) as num_reason_sizes_return_both,

-- --Dont like the item
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the item' and return_reason = 'Don''t like the style' THEN 1 ELSE 0 END) as num_reason_item_style,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the item' and return_reason = 'Don''t like the material' THEN 1 ELSE 0 END) as num_reason_item_material,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the item' and return_reason = 'Item looks different online' THEN 1 ELSE 0 END) as num_reason_item_diff_online,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the item' and return_reason = 'Item isn''t worth the price' THEN 1 ELSE 0 END) as num_reason_item_worth,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the item' and return_reason = 'Other' THEN 1 ELSE 0 END) as num_reason_item_other,

-- --Dont like the color
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the color' and return_reason = 'Looks different online' THEN 1 ELSE 0 END) as num_reason_color_online,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the color' and return_reason = 'Wrong color' THEN 1 ELSE 0 END) as num_reason_color_wrong,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the color' and return_reason = 'Don''t like color after trying on' THEN 1 ELSE 0 END) as num_reason_color_try,
-- SUM(CASE WHEN parent_return_reason = 'Don''t like the color' and return_reason = 'Other' THEN 1 ELSE 0 END) as num_reason_color_other,

-- --Looks different online
-- SUM(CASE WHEN parent_return_reason = 'Looks different online' and return_reason = 'Color looks different online' THEN 1 ELSE 0 END) as num_reason_diff_color,
-- SUM(CASE WHEN parent_return_reason = 'Looks different online' and return_reason = 'Quality looks better online' THEN 1 ELSE 0 END) as num_reason_diff_quality,
-- SUM(CASE WHEN parent_return_reason = 'Looks different online' and return_reason = 'Looks more comfortable online' THEN 1 ELSE 0 END) as num_reason_diff_comfort,
-- SUM(CASE WHEN parent_return_reason = 'Looks different online' and return_reason = 'Material Looks different online' THEN 1 ELSE 0 END) as num_reason_diff_material,
-- SUM(CASE WHEN parent_return_reason = 'Looks different online' and return_reason = 'Other' THEN 1 ELSE 0 END) as num_reason_diff_other,

-- --Quality Unsatisfactory
-- SUM(CASE WHEN parent_return_reason = 'Quality unsatisfactory' and return_reason = 'Item was damaged when it arrived' THEN 1 ELSE 0 END) as num_reason_quality_damaged,
-- SUM(CASE WHEN parent_return_reason = 'Quality unsatisfactory' and return_reason = 'Item does not function correctly' THEN 1 ELSE 0 END) as num_reason_quality_functiion,
-- SUM(CASE WHEN parent_return_reason = 'Quality unsatisfactory' and return_reason = 'Material or components do not meet expectations' THEN 1 ELSE 0 END) as num_reason_quality_components,
-- SUM(CASE WHEN parent_return_reason = 'Quality unsatisfactory' and return_reason = 'Item isn''t worth the price' THEN 1 ELSE 0 END) as num_reason_quality_price,
-- SUM(CASE WHEN parent_return_reason = 'Quality unsatisfactory' and return_reason = 'Other' THEN 1 ELSE 0 END) as num_reason_quality_other,

-- --Other
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'Received wrong item' THEN 1 ELSE 0 END) as num_reason_other_wrong_item,
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'No longer need item' THEN 1 ELSE 0 END) as num_reason_other_need,
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'Found another item I like more' THEN 1 ELSE 0 END) as num_reason_other_found,
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'Item took too long to ship' THEN 1 ELSE 0 END) as num_reason_other_ship,
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'Don''t like the packaging' THEN 1 ELSE 0 END) as num_reason_other_packaging,
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'Item isn''t worth the price' THEN 1 ELSE 0 END) as num_reason_other_price,
-- SUM(CASE WHEN parent_return_reason = 'Other' and return_reason = 'Other' THEN 1 ELSE 0 END) as num_reason_other_other



FROM {{ ref('sst_loop_return_intents') }} --analytics.sst_loop_return_intents
GROUP BY order_name, provider_line_item_id