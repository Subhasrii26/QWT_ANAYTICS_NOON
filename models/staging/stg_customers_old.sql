{{ config(materialized = 'table', database = 'QWT_ANALYTICS_UAT') }}
 
select *
from
{{ source('raw_qwt','Customers')}}