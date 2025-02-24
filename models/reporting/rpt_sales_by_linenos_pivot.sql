{{config (materialized = 'view',schema ='reporting_dev')}}
 
 
--{% set linenos =[1,2,3,4,5] %}  

{% set linenos = get_order_linenos() %}

select orderid,
{% for lineno in linenos %}
nvl(sum(case when LINENO = {{lineno}} then linesalesamount end),0)  as lineno{{lineno}}_sales,
{% endfor %}
sum(linesalesamount) as total_sales
from
{{ ref('fct_orders') }}  
group by 1