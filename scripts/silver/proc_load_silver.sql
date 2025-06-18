/*
=================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=================================================
Script Purpose:
  This stored procedure performs the ETL (extract, transform, load) process to populate the 'silver schema tables form the 'bronze' scheme. 
It performs the following actions:
  -Truncates the silver tables
  - Inserts transformed and cleaned data from the Bronze into Silver tables

Parameters:
  None.

Usage Example:
  exec silver.load_bronze;
=================================================
*/


create or alter procedure silver.load_silver as 
begin 
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try 
		set @batch_start_time = getdate(); 
		print '===============================' 
		print 'Loading Silver Layer'
		print '==============================='


		print '-------------------------------'
		print 'Loading CRM Tables'
		print '-------------------------------'

		set @start_time = getdate()
	set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'
		----------------------------------------------------------------------
	set @start_time = getdate()
	print '>> Truncating table: silver.crm_cust_info'; 
	truncate table silver.crm_cust_info;
	print '>> Inserting Data Into: silver.crm_info';

	insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)

	select 
	cst_id,
	cst_key,
	trim (cst_firstname) as cst_firstname,
	trim (cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status)) = 'M' then 'Married'
			 when upper(trim(cst_material_status)) = 'S' then 'Single'
			 else 'n/a'
			 end as cst_material_status,-- Normalize marital status values to readbale format
		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M' then 'Male'
			 else 'n/a'
			 end as cst_gndr,-- Normalize gender values to readable format 

	cst_create_date
	from (
		select
		*,
		row_number () over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		)t
		where flag_last = 1 
	set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'
		----------------------------------------------------------------------
	set @start_time = getdate()
	print '>> Truncating table: silver.crm_details'; 
	truncate table silver.crm_details;
	print '>> Inserting Data Into: silver.crm_details';
	insert into silver.crm_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
		else cast(cast(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
		else cast(cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
		else cast(cast(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) 
		then sls_quantity * abs(sls_price) 
		else sls_sales 
	end as sls_sales, 
	sls_quantity,
	case when sls_price is null  or sls_price <= 0 
		then sls_sales / nullif(sls_quantity, 0) 
		else sls_price 
	end as sls_price
	from bronze.crm_details  

	set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'
		----------------------------------------------------------------------
	set @start_time = getdate()
	print '>> Truncating table: silver.crm_prd_info'; 
	truncate table silver.crm_prd_info;
	print '>> Inserting Data Into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start,
		prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key_suffix,  -- changed alias to avoid conflict
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start AS DATE) AS prd_start,
		DATEADD(DAY, -1, LEAD(prd_start) OVER (PARTITION BY prd_key ORDER BY prd_start)) AS prd_end_dt
	FROM bronze.crm_prd_info;
	set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'
		----------------------------------------------------------------------

	
		print '-------------------------------'
		print 'Loading ERP Tables'
		print '-------------------------------'

	set @start_time = getdate()
	print '>> Truncating table: silver.erp_cust_az12'; 
	truncate table silver.erp_cust_az12;
	print '>> Inserting Data Into: silver.erp_cust_az12';

	insert into silver.erp_cust_az12 (cid, bdate, gen) 
	select
	case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))  
		else cid 
	end as cid, 
	case 
		when bdate > getdate () then null
		else bdate 
	end as bdate,
	case 
		when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M', 'MALE') then 'Male'
		else 'n/a' 
	end gen 
	from bronze.erp_cust_az12 
	set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'
		----------------------------------------------------------------------
		set @start_time = getdate()
	print '>> Truncating table: silver.erp_loc_a101'; 
	truncate table silver.erp_loc_a101;
	print '>> Inserting Data Into: silver.erp_loc_a101';

	insert into silver.erp_loc_a101
	(cid, cntry)
	select
	replace (cid, '-', '') cid,
	case 
		when trim (cntry) = 'DE' then 'Germany'
		when trim (cntry) in ('USA', 'US') then 'United States'
		when trim (cntry) = '' or cntry is null then 'n/a'
		else trim(cntry) 
	end as cntry 	
	from bronze.erp_loc_a101
set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'
		----------------------------------------------------------------------
		set @start_time = getdate()
	print '>> Truncating table: silver.erp_px_cat_g1v2'; 
	truncate table silver.erp_px_cat_g1v2;
	print '>> Inserting Data Into: silver.erp_px_cat_g1v2';

	insert into silver.erp_px_cat_g1v2
	(
	id,
	cat,
	subcat,
	maintenance
	)

	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2 

	set @end_time = getdate()
		print '>> Load Duration: ' + cast(datediff (second, @start_time, @end_time) as nvarchar) + ' seconds'; 
		print '-------------------------------'

		set @batch_end_time = getdate();
		print '==============================================='
		print 'Loading Silver Layer is Completed'
		print '    - Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar ) + ' seconds';
		print '==============================================='

	end try
	begin catch
		print '==============================================='
		print 'ERROR OCCURED DURING LOADING SILVER LAYER'
		print '==============================================='
	end catch 
end 
