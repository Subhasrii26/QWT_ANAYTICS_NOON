{{ config(materialized = 'incremental', unique_key = ['orderid']) }}
 
select *
from
{{ source('raw_qwt','Orders')}}
 
{% if is_incremental() %}
 
where orderdate > (select max(orderdate) from {{this}} )
 
{% endif %}