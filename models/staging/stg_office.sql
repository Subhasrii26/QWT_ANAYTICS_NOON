{{ config(materialized = 'table', database = 'QWT_ANALYTICS_DEV') }}
 
select 
OfficeID,
OfficeAddress as Address,
OfficePostalCode as PostalCode,
OfficeCity as City,
OfficeStateProvince as StateProvince,
OfficePhone as Phone,
OfficeFax as Fax,
OfficeCountry aS country

from
{{ source('raw_qwt','Offices')}}