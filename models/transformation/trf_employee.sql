{{ config(materialized = 'table', schema = 'transforming_dev') }}

select
emp.EmpID,
emp.First_Name,
emp.Last_Name,
emp.Title,
emp.Hire_Date,
emp.Extension,
iff(mgr.First_Name IS NULL, emp.First_Name, mgr.First_Name) as Manager_Name,
iff(mgr.Title IS NULL, emp.Title, mgr.Title) as Manager_Title,
emp.Year_Salary,
ofc.Address,
ofc.city,
ofc.country
from
{{ref('stg_employee')}} as emp
left join {{ref('stg_employee')}} as mgr
on emp.Reports_To = mgr.EmpID
left join {{ref('stg_office')}} as ofc
on emp.office = ofc.office
