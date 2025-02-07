{{ config(materialized = 'incremental', unique_key = ['orderid','lineno']) }}
 
select od.*,o.orderdate
from
{{ source('raw_qwt','Orders')}} o
join {{ source('raw_qwt','OrderDetails')}} od
on o.orderid = od.orderid
 
{% if is_incremental() %}
 
where o.orderdate > (select max(orderdate) from {{this}} )
 
{% endif %}