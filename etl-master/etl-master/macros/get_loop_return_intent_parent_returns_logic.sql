{% macro get_loop_return_intents_parent_reasons_logic() %}

  {% for parent_reason in get_loop_return_intent_parent_reasons() -%}

    SUM(case when lower(regexp_replace(REPLACE(parent_return_reason,' ', '_'), '\W+', '', 'g')) = '{{parent_reason}}' THEN 1 ELSE 0 end) as num_parent_{{parent_reason}}

    {%- if not loop.last -%} , {%- endif %}

  {% endfor -%}

{% endmacro %}