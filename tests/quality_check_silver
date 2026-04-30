/* 
===============================================================================  
Quality Checks
===============================================================================  
Script Purpose:
  This scripts performs various quality checks for data consistency, accuracy
  and standardization across the 'Silver' schema. 
  It includes check for:  
    - Null or duplicate primary key. 
    - Unwanted spaces in string field.
    - Data standardization and consistency.
    - Invalid data range and orders.
    - Data consistency between related field. 

Usage Noted:
  - Run these checks after data loading into Silver schema.
  - Investigate and resolve any discrepancies found during checks. 
===============================================================================  
*/

-- ===================================================================================
-- Doing quality issue for crm_cust_info. 
-- ===================================================================================
-- Check for Nulls or Duplicates in Primary Key.
-- Expectation: No results ! ! !
select 
	cst_id,
	count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 
or cst_id is null 

-- Check for unwanted spaces.
-- Expectation: No results ! ! !
select 
	cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select 
	cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);


select 
	cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr);


-- Check for Data Standardization & Consistency.
select 
	distinct cst_gndr
from silver.crm_cust_info;

select 
	distinct cst_marital_status
from silver.crm_cust_info;

-- Check for Date Variables.
select 
	is_date(cst_create_date) as cst_create_date
from silver.crm_cust_info


-- ===================================================================================
-- After Inserting Clean data into Silver
-- Rerun the check to ensure data integration and quality. 
-- ===================================================================================
select 
	cst_id,
	count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 
or cst_id is null 

-- Check for unwanted spaces.
-- Expectation: No results ! ! !
select 
	cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select 
	cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);


select 
	cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr);


-- Check for Data Standardization & Consistency.
select 
	distinct cst_gndr
from silver.crm_cust_info;

select 
	distinct cst_marital_status
from silver.crm_cust_info;

-- Check for Date Variables.
select 
	is_date(cst_create_date) as cst_create_date
from silver.crm_cust_info

-- ===================================================================================
-- data quality check for crm_prd_info
-- ===================================================================================
-- inspect data
select * from bronze.crm_prd_info;

-- Check for Nulls or Duplicates in Primary Key.
-- Expectation: No results ! ! !
select 
	prd_id,
	count(*) 
from silver.crm_prd_info
group by prd_id
having count(*) > 1 
or prd_id is null; 

-- Check unwanted spaces
select 
	prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check Nulls or Negative Numbers 
-- Expectation: No results ! ! !
select 
	prd_cost
from silver.crm_prd_info
where prd_cost < 0 
or prd_cost is null;

-- Check for Data Standardization & Consistency.
select 
	distinct prd_line
from silver.crm_prd_info;

-- Check for Invalid Dates.
select 
	*
from silver.crm_prd_info
where prd_end_dt < prd_start_dt



-- ===================================================================================
-- data quality check for crm_sales_details 
-- ===================================================================================
-- check string contains any unecessary spaces 
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from silver.crm_sales_details
where sls_ord_num != trim (sls_ord_num)

-- check the integrity of sls_prd_key column and sls_cust_id
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

-- check for invalid date because negative numbers, 
-- length not equal to a normal date length, 
-- outliers... 
select 
	nullif(sls_order_dt, 0) as sls_order_dt
from silver.crm_sales_details 
where sls_order_dt <= 0 
or length (sls_order_dt::text) != 8 
or sls_order_dt > 20500101
or sls_order_dt < 19900101;

select 
	nullif(sls_due_dt, 0) as sls_ship_dt
from silver.crm_sales_details 
where sls_due_dt <= 0 
or length (sls_due_dt::text) != 8 
or sls_due_dt > 20500101
or sls_due_dt < 19900101

-- check for invalid date. 
select 
	*
from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check data consistency: Between Sales, Quantity, and Price 
-- >> Sales = Quantity*Price
-- >> Values must not be negative, nulls and zeros. 
select 
	sls_sales,
	sls_quantity,
	sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null  
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 
order by sls_sales, sls_quantity, sls_price

-- Checking old with new data 
select 
	sls_sales as old_sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
	case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	 		then sls_quantity * abs(sls_price)		 
		 else sls_sales
	end as sls_sales,
	case when sls_price is null or sls_price <= 0 
			 then sls_sales/ nullif(sls_quantity, 0)
		 else sls_price 
	end as sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null  
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 
order by sls_sales, sls_quantity, sls_price

-- ===================================================================================
-- data quality check for erp_cust_az12
-- ===================================================================================
-- cid is not the same value with their pk and fk. 
select 
	case when cid like 'NAS%' then substring(cid,4, length(cid))  
		 else cid
	end as cid,
	bdate,
	gen
from silver.erp_cust_az12

--Identify out of range dates
select 
	bdate
from silver.erp_cust_az12
where bdate > now()

-- data normalization 
select 
	distinct gen 
from silver.erp_cust_az12

-- ===================================================================================
-- data quality check for erp_loc_a101
-- ===================================================================================
-- Identify the cid quality 
select 
	cid,
	cntry
from silver.erp_loc_a101
where cid not in (select cst_key from silver.crm_cust_info)

-- Data Consistency check 
select 
	distinct cntry as old,
	case  
		 when trim(cntry) = 'DE' then 'Germany'
		 when trim(cntry) in ('US','USA') then 'United States'
		 when trim(cntry) = '' or cntry is null then 'n/a'
		 else trim(cntry)
	end as cntry
from silver.erp_loc_a101


-- ===================================================================================
-- data quality check for erp_loc_a101
-- ===================================================================================
-- check for unwanted spaces 
select 
	*
from silver.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

-- data standardization
select 
	distinct cat 
from silver.erp_px_cat_g1v2

select 
	distinct subcat 
from silver.erp_px_cat_g1v2

select 
	distinct maintenance 
from silver.erp_px_cat_g1v2


  
