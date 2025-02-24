{% macro get_order_linenos() -%}
 
{% set order_linenos_query %}
select distinct
lineno
from {{ref('fct_orders')}}
order by 1
{% endset %}
 
{% set results = run_query(order_linenos_query) %}
 
{% if execute %}
{# Return the first column#}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{return(results_list) }}
{%- endmacro %}

{% macro get_min_orderdate() -%}
 
{% set min_order_query %}
select min(orderdate)
from {{ref('fct_orders')}}
{% endset %}
 
{% set results = run_query(min_order_query) %}
 
{% if execute %}
{# Return the first column#}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{return(results_list) }}
{%- endmacro %}

{% macro get_max_orderdate() -%}
 
{% set max_order_query %}
select max(orderdate)
from {{ref('fct_orders')}}
{% endset %}
 
{% set results = run_query(max_order_query) %}
 
{% if execute %}
{# Return the first column#}
{% set results_list = results.columns[0][0] %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{return(results_list) }}
{%- endmacro %}