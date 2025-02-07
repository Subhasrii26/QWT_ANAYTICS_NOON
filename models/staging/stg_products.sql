{{ config(materialized = 'table', transient = false) }}
 
--transient = false -> to create a permanent table instead of transient table
select *
from
{{ source('raw_qwt','Products')}}