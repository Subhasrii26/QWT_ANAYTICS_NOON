-- {{ config(materialized = 'table', schema = 'transforming_dev') }}

-- select
-- emp.EmpID,
-- emp.First_Name,
-- emp.Last_Name,
-- emp.Title,
-- emp.Hire_Date,
-- emp.Extension,
-- iff(mgr.First_Name IS NULL, emp.First_Name, mgr.First_Name) as Manager_Name,
-- iff(mgr.Title IS NULL, emp.Title, mgr.Title) as Manager_Title,
-- emp.Year_Salary,
-- ofc.Address,
-- ofc.city,
-- ofc.country
-- from
-- {{ref('stg_employee')}} as emp
-- left join {{ref('stg_employee')}} as mgr
-- on emp.Reports_To = mgr.EmpID
-- left join {{ref('stg_office')}} as ofc
-- on emp.office = ofc.office

{{ config(materialized = 'table', schema = 'transforming_dev') }}
 
with recursive managers
      (indent, office_id, employee_id, employee_name, employee_title, manager_id, manager_name, manager_title)
    as
    (
 
        select '*' as indent,
                    office as office_id,
                    empid as employee_id,
                    first_name as employee_name,
                    title as employee_title,
                    empid as manager_id,
                    first_name as manager_name,
                    title as manager_title
          from {{ref("stg_employee")}} where title = 'President'
 
        union all
 
        select mgr.indent || '*',
            emp.office as office_id,
            emp.empid as employee_id ,
            emp.first_name as employee_name,
            emp.title as employee_title,
            mgr.employee_id as manager_id,
            mgr.employee_name as manager_name,
            mgr.employee_title as manager_title
          from {{ref("stg_employee")}} as emp inner join managers as mgr
            on emp.reports_to = mgr.employee_id
      ),
 
      offices(office_id, office_city, office_country)
      as
      (
      select office as office_id, city, country from {{ref("stg_office")}}
      )
 
  select indent, employee_id, employee_name, employee_title , manager_id, manager_name, manager_title, ofc.office_city, ofc.office_country
    from managers as mgr inner join offices as ofc on ofc.office_id = mgr.office_id
