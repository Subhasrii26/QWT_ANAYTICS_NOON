{{config(materialized='table',schema='transforming_dev')}}

select 
ss.OrderID,
ss.lineNo,
sh.companyname,
ss.ShipmentDate,
ss.Status as CurrentStatus
from
{{ref('shipments_snapshot')}} as ss
join {{ref('shippers')}} as sh
on ss.ShipperID = sh.ShipperID
where ss.dbt_valid_to is null