{{ config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA','transforming_dev')) }}
 
select
cust.customerid,
cust.companyname,
cust.contactname,
cust.city,
cust.country,
div.divisionname,
cust.address,
cust.fax,
cust.phone,
cust.postalcode,
IFF(cust.stateprovince = '','NA',cust.stateprovince ) as stateprovincename
 
from {{ref("stg_customers")}} as cust
join {{ref("divisions")}} as div
on cust.divisionid = div.divisionid