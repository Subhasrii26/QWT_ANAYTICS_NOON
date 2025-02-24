{{ config(materialized = 'table') }}
 
select 
OfficeHashKey,
LoadDateTime,
OfficeAddress ,
OfficePostalCode ,
OfficeCity ,
OfficeStateProvince ,
OfficePhone ,
OfficeFax ,
OfficeCountry 
from
{{ source('raw_qwt','Offices')}}