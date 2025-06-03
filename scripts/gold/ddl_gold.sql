-- DDL SCRIPT TO CREATE GOLD VIEWS

/* 
----------------------------------------------------------------------------
DIM_CUSTOMERS VIEW  

*/

-- we hv started from master table and done left join so that we won't be losing imp data
-- this is a dimension table
--GOLD LAYER: we hv combined 3 tables into 1 and chosen good names
--select cst_id, count(*) from(

	create view gold.dim_customers as
	select 
		row_number() over (order by cst_id )as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		ci.cst_marital_status as marital_status,
		case when ci.cst_gndr != 'N/A' then ci.cst_gndr -- cz CRM is the master for gndr
			else COALESCE(ca.gen,'N/A') -- coalesce will o/p the first non-null value
		end as gender,
-- there are customers in crm table that are not their in erp table; 
-- master src of customer is the crm table so we hv to stick with the info in that (this is a business rule)
		ci.cst_create_date,
		ca.bdate as birthdate,
		la.cntry as country
	from silver.crm_cust_info ci  --here ci is an alias to ease joining
	left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid

--)t group by cst_id -- to check duplicate rows
--having count(*)>1



/* 
----------------------------------------------------------------------------
DIM_PRODUCTS VIEW  

*/
create view gold.dim_products as
select
	row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,	
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null --filter out all historical data


/* 
----------------------------------------------------------------------------
FACT_SALES VIEW  

*/
create view gold.fact_sales as
select
  sd.sls_ord_num,
  pr.product_key, --surrgogate key; to connect fact table to dim table
  cs.customer_key, --surrogate key
  sd.sls_order_dt as order_date,
  sd.sls_ship_dt as shipping_date,
  sd.sls_due_dt as due_date,
  sd.sls_sales as sales_amount,
  sd.sls_quantity as quantity,
  sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cs
on sd.sls_cust_id = cs.customer_id
