{% macro get_flow_payment_method_amounts() %}

  {% for payment_method in get_flow_payment_methods() -%}

    SUM(case when payment_method = '{{payment_method}}' then payment_amount else 0::numeric end) as {{payment_method}}_amount
    {%- if not loop.last -%} , {%- endif %}

  {% endfor -%}

{% endmacro %}