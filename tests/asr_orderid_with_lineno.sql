select orderid, count(lineno) as total_linenos
from 
{{ref('fct_orders')}}
group by orderid
having total_linenos < 1