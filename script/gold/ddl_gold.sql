


/*
===============================================================================================================================
	
  DDL SCRIPT: CREATE GOLD VIEWs

================================================================================================================================

	Script Purpose:
	This script create gold view in data warehouse
	The gold layer represent the final dimension and fact tables ( star schema)

	Each view perfome  transformation and combine data from the silver layer a clean, enrich and business ready data set


	Usage:
	This view can can be queried directly for analytics and reporting

================================================================================================================================
*/







/*
===========================================================
	
   CREATE  DIMENSION CUSTOMER VIEW

===========================================================
*/

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER( ORDER BY CustomerID) AS customer_key, -- sorrugate key
	CustomerID AS customer_id,
	CustomerName AS customer_name,
	Email AS email,
	Age AS age,
	Country AS country,
	SignupDate AS sign_up_date
FROM silver.crm_cust_info 
GO




/*
===========================================================
	
   CREATE DIMENSION PRODUCTION VIEW

===========================================================
*/
IF OBJECT_ID('gold.dim_production','V') IS NOT NULL
DROP VIEW gold.dim_production
GO

CREATE  VIEW gold.dim_production AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pro_id) AS production_key, -- sorrugate key
	pro_id AS production_id,
	pro_name AS production_name,
	pro_category AS production_category,
	pro_price AS production_price,
	pro_stock_quantity AS production_stock_quantity,
	pro_added_date AS production_added_date
FROM silver.crm_prod

GO








/*
===========================================================
	
	CREATE FACT ORDER (because  its connecting multiple dimension)

===========================================================
*/

IF OBJECT_ID('gold.fact_order','V') IS NOT NULL
DROP VIEW gold.fact_order
GO
CREATE VIEW gold.fact_order AS
SELECT 
	o.order_id,
	c.customer_key,
	p.production_key,
	o.order_quantity,
	o.order_total_amount,
	o.order_date
FROM silver.crm_order o
LEFT JOIN gold.dim_customers c
ON o.order_product_id = c.customer_id
LEFT JOIN gold.dim_production p
ON o.order_product_id = p.production_id
