{% macro get_loop_return_intents_child_returns_logic() %}

  {% for parent_reason in get_loop_return_intent_parent_reasons() -%}


	{% for child_reason in get_loop_return_intent_child_reasons(parent_reason) -%}

    SUM(case when lower(regexp_replace(REPLACE(parent_return_reason,' ', '_'), '\W+', '', 'g')) = '{{parent_reason}}' and lower(regexp_replace(REPLACE(return_reason,' ', '_'), '\W+', '', 'g')) = '{{child_reason}}' THEN 1 ELSE 0  end) as num_reason_{{parent_reason}}_and_{{child_reason}}
    
    {%- if not loop.last -%} , {%- endif %}

	{% endfor -%}

	{%- if not loop.last -%} , {%- endif %}
  
  {% endfor -%}

{% endmacro %}