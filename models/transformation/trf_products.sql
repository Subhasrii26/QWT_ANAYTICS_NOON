{{ config(materialized = 'table', schema = 'transforming_dev') }}

select 
p.ProductID,
p.ProductName,
c.CategoryName,
s.CompanyName AS SupplierCompany,
s.ContactName AS SupplierContact,
s.City AS SupplierCity,
s.Country AS SupplierCountry,
p.QuantityPerUnit,
p.UnitCost,
p.UnitPrice,
p.UnitsInStock,
p.UnitsOnorder,
p.UnitCost - UnitPrice AS profit,
IFF(UnitsInStock > UnitsOnorder,'Available', 'Not Available') AS ProductAvailability
from {{ref("stg_products")}} as p
inner join {{ref("trf_suppliers")}} as s on p.SupplierID = s.SupplierID
inner join {{ref("categories")}} as c on p.CategoryID = c.CategoryID