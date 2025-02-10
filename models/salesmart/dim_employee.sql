 
{{ config(materialized = 'view', schema = 'salesmart_dev') }}
 
 select
 employee_id,
 employee_name,
 employee_title,
 manager_id,
 manager_name,
 manager_title,
 office_city as city,
 office_country as country
  from
 {{ref("trf_employee")}}