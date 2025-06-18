/*
===================================================
Quality Checks 
===================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schemas. It includes checks for:
- Null or duplicate PKs.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid data ranges and orders.
- Data consistency between related fields.

Usage notes:
 - Run these checks after loading data is in the silver layer.
 - Investigate and resolve any discrepancies found during the checks. 
===================================================


-- Check for NULLS or Negative Numbers
-- Expectation: No Results
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null 

--Check for INvalid Date Orders
select *
from bronze.crm_details 
where prd_end_dt < prd_start

-- Check for Invalid Dates
select
nullif(sls_due_dt, 0) sls_due_dt
from bronze.crm_details 
where sls_due_dt <= 0 or len(sls_order_dt) != 8 or sls_due_dt > 20501101 or sls_due_dt < 19000101

-- Check for Invalid Date Orders
select
*
from bronze.crm_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt 

select distinct 
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) 
	then sls_quantity * abs(sls_price) 
	else sls_sales 
end as sls_sales, 
case when sls_price is null or sls_price <= 0 
	then sls_sales / nullif(sls_quantity, 0) 
	else sls_price 
end as sls_price
from bronze.crm_details


where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <= 0 or sls_quantity  <= 0 or sls_price  <= 0
order by 
sls_sales,
sls_quantity,
sls_price

--Gender Check

SELECT gen
FROM bronze.erp_cust_az12
WHERE gen IS NULL 
   OR (gen NOT IN ('Male', 'Female'))
