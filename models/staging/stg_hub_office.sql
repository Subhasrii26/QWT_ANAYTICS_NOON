{{ config(materialized = 'table', database = 'QWT_ANALYTICS_DEV') }}
 
select 
OfficeHashKey,
LoadDateTime,
RecordSrc,
OfficeID
from
{{ source('raw_qwt','Offices')}}