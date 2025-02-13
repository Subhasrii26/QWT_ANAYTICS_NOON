{{ config(materialized = 'table', database = 'QWT_ANALYTICS_DEV') }}
 
select * from
{{ source('raw_qwt','Suppliers')}}