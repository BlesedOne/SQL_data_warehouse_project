
/* 
		Qulaity Check Silver
==============================================================
	Script Purpose:
	This script performs various quality checks for data consistency
	accuracy, and standardization across the 'silver schema'. 
	It includes checks for:
	-Null od duplicates primary keys
	- Unwanted spaces in string fields
	- Invalide date ranges and consistency
	- Data consistency between related field

	Usage:
	- Run these checks after data load silver layer
	- Investigate and resolve any discrepancies found during the checks
=============================================================
	*/
/* 
	GET ALL DATA FROM bronze.crm_emp schema
	*/
	SELECT *
FROM bronze.crm_emp





-- ==============================================================
--   CHECKING FOR DUPLICATE OR NULL IN PRIMARY KEY 
-- ==============================================================

SELECT emp_id,
COUNT(*) AS duplicates
FROM bronze.crm_emp
GROUP BY emp_id
HAVING COUNT(*) > 1 OR emp_id IS NULL





/*
=================================================================================
Check and remove unwanted spaces in the data .
====================================================================================
*/

SELECT emp_last_nane
FROM bronze.crm_emp
WHERE emp_last_nane != TRIM(emp_last_nane)

 



/*
=================================================================================================
	Check data consistency in the columns. eg gender having M,F and Male is not consistence
==================================================================================================
*/
SELECT DISTINCT emp_dept
FROM silver.crm_emp

/*
=============================================
	CHECK FOR NAGATIVE/LOW/NULL COST OR SALARY
=============================================
*/

SELECT emp_salary
FROM bronze.crm_emp
WHERE emp_salary < 10 OR emp_salary IS NULL



/*
=============================================
	CHECK FOR INVALIDE DATE ORDER
=============================================
*/
SELECT *
FROM bronze.crm_emp
WHER table_start_date > table_end_date





-- Update oreder_product id and foriegn keys
SELECT *,
	ROW_NUMBER() OVER( ORDER BY order_product_id ASC) AS new_id
FROM bronze.crm_order
