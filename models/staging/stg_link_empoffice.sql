{{ config(materialized='table')}}

select
MD5(OfficeHashKey||empID) as empofficehashkey,
current_timestamp as LoadDateTime,
'Office' as RecordSrc,
OfficeHashKey,
empID
from 
{{ref('stg_employee')}} as e inner join
{{ref('stg_hub_office')}} as o on e.office = o.officeid