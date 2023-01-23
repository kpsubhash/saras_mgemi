{% macro get_flow_payment_methods() %}

	{%- call statement('flow_payment_methods', fetch_result=True) -%}
		
	SELECT

	distinct regexp_replace(lower(d->>'description'), '[^a-zA-Z]', '', 'g') as payment_method

	from {{ source('shopify', 'raw_flow_order') }} 
	left join lateral jsonb_array_elements(data->'payments') d(data) on true
	where domain = 'mgemi.myshopify.com' and regexp_replace(lower(d->>'description'), '[^a-zA-Z]', '', 'g') is not null

	{%- endcall -%}


	{%- if execute -%}
  
	{%- set payment_methods = load_result('flow_payment_methods').table.columns['payment_method'].values() -%}

	{%- else -%}

	{%- set payment_methods = [] -%}
	  
	{%- endif -%}

	{{return(payment_methods)}}


{% endmacro %}