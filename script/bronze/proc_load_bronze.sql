/*
================================================================================
Stored Procedure: Load Bronze Layer (Sources --> Bronze)

================================================================================
Script Purpose: 
  This stored procedure load data into 'bronze' schema from external csv files.
  It performs the following actions:
    - Truncate the bronze table before loading data. 
    - Uses the 'Copy' command to load data from csv files to bronze tables. 

Parameters:
    None.
  This stored procedure does not accpet any parameters or return any values.

Usage Eaxmple:
  call bronze.load_bronze();
=================================================================================
*/

-- Develope SQL Load Scripts  

create or replace procedure bronze.load_bronze ()
language plpgsql
as $$ 
declare 
	v_start_time timestamp;
	v_end_time timestamp;
	v_duration interval;
	batch_start_time timestamp;
	batch_end_time timestamp; 
begin

	batch_start_time:= clock_timestamp();
	raise notice '===========================================';
	raise notice 'Loading Bronze Layer';
	raise notice '===========================================';

	raise notice ' ';

	raise notice '-------------------------';
	raise notice 'Loading CRM Tables';
	raise notice '-------------------------';

	raise notice ' ';

	v_start_time := clock_timestamp();
	raise notice '>> Truncate Table: bronze.crm_cust_info';
	truncate table bronze.crm_cust_info;
	
	raise notice '>> Inserting Data into: bronze.crm_cust_info';
	copy bronze.crm_cust_info from '/usr/share/datasets/source_crm/cust_info.csv'
	with (format csv, header, delimiter ',');
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	raise notice '>> Loading Duration: %', v_duration;
	raise notice '---------------------------------------';

	v_start_time := clock_timestamp();
	raise notice '>> Truncate Table: bronze.crm_prd_info';
	truncate table bronze.crm_prd_info;
	raise notice '>> Inserting Data into: bronze.crm_prd_info';
	copy bronze.crm_prd_info from '/usr/share/datasets/source_crm/prd_info.csv' 
	with (format csv, header, delimiter ',');
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	raise notice '>> Loading Duration: %', v_duration;
	raise notice '---------------------------------------';

	v_start_time := clock_timestamp();
	raise notice '>> Truncate Table: bronze.crm_sales_details';
	truncate table bronze.crm_sales_details;
	raise notice '>> Inserting Data into: bronze.crm_sales_details';
	copy bronze.crm_sales_details from '/usr/share/datasets/source_crm/sales_details.csv' 
	with (format csv, header, delimiter ',');
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	raise notice '>> Loading Duration: %', v_duration;
	raise notice '---------------------------------------';

	raise notice ' ';
	
	raise notice '-------------------------';
	raise notice 'Loading ERP Tables';
	raise notice '-------------------------';

	raise notice ' ';

	v_start_time := clock_timestamp();
	raise notice '>> Truncate Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12;
	raise notice '>> Inserting Data into: bronze.erp_cust_az12';
	copy bronze.erp_cust_az12 from '/usr/share/datasets/source_erp/CUST_AZ12.csv' 
	with (format csv, header, delimiter ',');
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	raise notice '>> Loading Duration: %', v_duration;
	raise notice '---------------------------------------';

	v_start_time := clock_timestamp();
	raise notice '>> Truncate Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101;
	raise notice '>> Inserting Data into: bronze.erp_loc_a101';
	copy bronze.erp_loc_a101 from '/usr/share/datasets/source_erp/LOC_A101.csv' 
	with (format csv, header, delimiter ',');
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	raise notice '>> Loading Duration: %', v_duration;
	raise notice '---------------------------------------';

	v_start_time := clock_timestamp();
	raise notice '>> Truncate Table: bronze.erp_px_cat_g1v2';	
	truncate table bronze.erp_px_cat_g1v2;
	raise notice '>> Inserting Data into: bronze.erp_px_cat_g1v2';
	copy bronze.erp_px_cat_g1v2 from '/usr/share/datasets/source_erp/PX_CAT_G1V2.csv' 
	with (format csv, header, delimiter ',');
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	raise notice '>> Loading Duration: %', v_duration;
	raise notice '---------------------------------------';

	batch_end_time:= clock_timestamp();
	v_duration := batch_end_time - batch_start_time; 
	raise notice '==========================================';
	raise notice 'Loading Broze Layer is Compeleted';
	raise notice '- Total Load Duration: %', v_duration;
	raise notice '===========================================';

exception 
	when others then 
		raise notice '================================';
		raise notice 'Error Occurred During Bronze Load'; 
		raise notice 'Error Message: %', SQLERRM;
		raise notice 'Error Code: %', SQLSTATE; 
		raise notice '================================';
end; 
$$; 
