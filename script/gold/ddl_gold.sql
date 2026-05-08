/*
===============================================================================
DDL Scripts: Create Gold Views 
===============================================================================
Scripts purposes:
  This scripts create views in Gold Layer in the data warehouse. 
  The gold layer represent the final dimension and fact tables (Star Schema). 

  Each views perform transformation and combine data from the silver layer
  to produce a clean, enriched and business-ready dataset. 

Usage:
  - These views can be queries directly for analytics and reporting. 
================================================================================
*/

-- =============================================================================
--  Create dimension: gold.dim_customers 
-- =============================================================================
drop view if exists gold.dim_customers;
create or replace view gold.dim_customers as 
select 
    row_number() over(order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    lc.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr 
         else coalesce(az.gen, 'n/a')
    end as gender,
    az.bdate as brithdate,
    ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as az
    on ci.cst_key = az.cid
left join silver.erp_loc_a101 as lc 
    on ci.cst_key = lc.cid;

-- =============================================================================
--  Create dimension: gold.dim_products
-- =============================================================================
drop view if exists gold.dim_products;
create or replace view gold.dim_products as 
select 
    row_number() over(order by prd_start_dt, prd_key) as product_key, -- surrogate key
    pi.prd_id as product_id,
    pi.prd_key as product_number,
    pi.prd_nm as product_name,
    pi.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance,
    pi.prd_cost as product_cost,
    pi.prd_line as product_line,
    pi.prd_start_dt as start_date
from silver.crm_prd_info as pi
left join silver.erp_px_cat_g1v2 as pc
    on pi.cat_id = pc.id 
where pi.prd_end_dt is null


-- =============================================================================
--  Create dimension: gold.fact_sales
-- =============================================================================
drop view if exists gold.fact_sales;
create or replace view gold.fact_sales as 
select 
    sd.sls_ord_num as order_number,
    dp.product_key, -- replacing sd.sls_prd_key
    dc.customer_key, -- replacing sd.sls_cust_id
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_products as dp
    on dp.product_number = sd.sls_prd_key 
left join gold.dim_customers as dc    
    on dc.customer_id = sd.sls_cust_id
