{{ config(materialized = 'table', database = 'QWT_ANALYTICS_DEV') }}
 
select 
EmpID ,
Last_Name ,
First_Name ,
Title ,
Hire_Date ,
Office ,
IFF(Extension = '-','NA',Extension) as Extension ,
Reports_To ,
Year_Salary 
from
{{ source('raw_qwt','Employee')}}