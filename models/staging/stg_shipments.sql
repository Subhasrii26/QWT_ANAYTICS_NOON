{{config(materialized='table', database = 'QWT_ANALYTICS_DEV')}}

select
OrderID ,
LineNo ,
ShipperID ,
CustomerID ,
ProductID ,
EmployeeID ,
to_date(ShipmentDate,'MM/DD/YYYY HH24:MI') as ShipmentDate ,
Status 
from {{source('raw_qwt','Shipments')}}