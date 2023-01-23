{% macro get_loop_return_intent_parent_reasons() %}

	{%- call statement('loop_return_intent_parent_reasons', fetch_result=True) -%}
	
	SELECT

	distinct lower(regexp_replace(REPLACE(parent_return_reason,' ', '_'), '\W+', '', 'g')) as parent_return_reason

	from {{ ref('sst_loop_return_intents') }} 
	where parent_return_reason is not null


	{%- endcall -%}


	{%- if execute -%}
  
	{%- set parent_return_reasons = load_result('loop_return_intent_parent_reasons').table.columns['parent_return_reason'].values() -%}

	{%- else -%}

	{%- set parent_return_reasons = [] -%}
	  
	{%- endif -%}

	{{return(parent_return_reasons)}}


{% endmacro %}