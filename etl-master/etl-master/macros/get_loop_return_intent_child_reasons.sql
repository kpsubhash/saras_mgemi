{% macro get_loop_return_intent_child_reasons(parent) %}

	{%- call statement('loop_return_intent_child_reasons', fetch_result=True) -%}
		
	SELECT

	distinct lower(regexp_replace(REPLACE(return_reason,' ', '_'), '\W+', '', 'g')) as return_reason

	from {{ ref('sst_loop_return_intents') }} 
	where return_reason is not null and lower(regexp_replace(REPLACE(parent_return_reason,' ', '_'), '\W+', '', 'g')) = '{{ parent }}'

	{%- endcall -%}


	{%- if execute -%}
  
	{%- set return_reasons = load_result('loop_return_intent_child_reasons').table.columns['return_reason'].values() -%}

	{%- else -%}

	{%- set return_reasons = [] -%}
	  
	{%- endif -%}

	{{return(return_reasons)}}


{% endmacro %}