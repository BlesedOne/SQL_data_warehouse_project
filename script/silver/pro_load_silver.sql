	/*
Store Procdeure: Load Silver Layer (Bronze to Silver)
===========================================================================================

This procedure load  cleaned and tranformed data into Silver layer from the bronze layer.
The script TRUNCATE the table before loading.
============================================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
			-----------------------------------------------------------------
	
		SET @batch_start_time = GETDATE()
		PRINT 'Loading the Silver Layer'
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'


		SET @start_time = GETDATE()
		PRINT('========================================')
		PRINT('TRUBNCATING silver.crm_cust_info ...  ')
		PRINT('========================================')
		TRUNCATE TABLE silver.crm_cust_info
		PRINT('========================================')
		PRINT('>>  INSERTING INTO silver.crm_cust_info ')
		PRINT('========================================')
		INSERT INTO silver.crm_cust_info (
		CustomerID,
		CustomerName,
		Email,
		Age,
		Country,
		Signupdate
		) 

		SELECT 
			CustomerID,
			TRIM(CustomerName) AS CustomerName,
			Email,
			CASE WHEN Age = 90 THEN 00
			WHEN Age = 100 THEN 00
			ELSE Age
			END Age,
			Country,
			SignupDate
	
		FROM(
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY SignupDate DESC) AS Rank_value
			FROM bronze.crm_cust_info
		)x WHERE Rank_value = 1;
		SET @end_time = GETDATE()
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

		/*
		==============================================================================
			CLEANING AND TRANSFORMING THE EMPLOYEE BRONZE LAYER TO SILVER LAYER
		==============================================================================
		*/



		SET @start_time = GETDATE()
		PRINT('========================================')
		PRINT('TRUNCATING silver.crm_emp ...  ')
		PRINT('========================================')
		TRUNCATE TABLE silver.crm_emp
		PRINT('========================================')
		PRINT('>>  INSERTING INTO silver.crm_emp')
		PRINT('========================================')
		INSERT INTO silver.crm_emp (
		emp_id,
		emp_first_name,
		emp_last_nane,
		emp_dept,
		emp_salary,
		emp_hire_date
		)
		SELECT
			emp_id,
			emp_first_name,
			emp_last_nane,
			CASE UPPER(TRIM( emp_dept)) 
				WHEN  'HR' THEN 'Human Resources'
				WHEN 'IT' THEN 'Information Technology'
				ELSE emp_dept
			END emp_dept,
			emp_salary,
			emp_hire_date
		FROM bronze.crm_emp
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second'
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
		
		/*
		==============================================================================
			CLEANING AND TRANSFORMING THE PRODUCT BRONZE LAYER TO SILVER LAYER
		==============================================================================
		*/
		SET @start_time = GETDATE()
		PRINT('========================================')
		PRINT('TRUBNCATING silver.crm_prod ...  ')
		PRINT('========================================')
		TRUNCATE TABLE silver.crm_prod
		PRINT('========================================')
		PRINT('>>  INSERTING INTO silver.crm_prod')
		PRINT('========================================')
		INSERT INTO silver.crm_prod(
			pro_id,
			pro_name,
			pro_category,
			pro_price,
			pro_added_date,
			pro_stock_quantity
		)
		SELECT 
			pro_id,
			pro_name,
			pro_category,
			pro_price,
			pro_added_date,
			pro_stock_quantity
		FROM bronze.crm_prod
		SET @end_time = GETDATE();
		PRINT'>> Load Duration ' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second'
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'


		/*
		==============================================================================
			CLEANING AND TRANSFORMING THE ORDER BRONZE LAYER TO SILVER LAYER
		==============================================================================
		*/
		SET @start_time = GETDATE()
		PRINT('========================================')
		PRINT('TRUBNCATING silver.crm_order ...  ')
		PRINT('========================================')
		TRUNCATE TABLE silver.crm_order
		PRINT('========================================')
		PRINT('>>  INSERTING INTO silver.crm_order')
	
		INSERT INTO silver.crm_order(
			order_id,
			order_customer_id,
			order_product_id,
			order_quantity,
			order_total_amount,
			order_date	
		)
		SELECT
			order_id  % 100 AS order_id, -- dividing by 100 and taking the remainder aa the ID
			ROW_NUMBER() OVER(ORDER BY order_customer_id ASC) AS order_customer_id,
			ROW_NUMBER() OVER(ORDER BY order_product_id ASC) AS order_product_id,
			order_quantity,
			order_total_amount,
			order_date
		FROM bronze.crm_order
		SET @end_time = GETDATE();
		PRINT'>> Load Duration ' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second'
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'


		/*
		==============================================================================

			CLEANING AND TRANSFORMING THE Supply BRONZE LAYER TO SILVER LAYER

		==============================================================================
		*/
		SET @start_time = GETDATE()
		PRINT('========================================')
		PRINT('TRUBNCATING silver.crm_spl ...  ')
		PRINT('========================================')
		TRUNCATE TABLE silver.crm_spl
		PRINT('========================================')
		PRINT('>>  INSERTING INTO silver.crm_spl ')
		PRINT('========================================')

		INSERT INTO silver.crm_spl(
			spl_id,
			spl_name,
			spl_contact_email,
			spl_country,
			sl_rating,
			spl_contract_date
		)
		SELECT
			spl_id,
			spl_name,
			spl_contact_email,
			spl_country,
			sl_rating,
			spl_contract_date
		 FROM bronze.crm_spl
		 SET @end_time = GETDATE();
		PRINT'>> Load Duration ' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second'
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

		SET @batch_end_time = GETDATE();
		PRINT'============================================================================================'
		PRINT' Total Load Duation ' +  CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds'
		PRINT'============================================================================================'

	END TRY
	BEGIN CATCH
	PRINT'>>   HANDLING ERRROR'
	SELECT 
		ERROR_NUMBER() AS ErrorNumber,
		ERROR_MESSAGE() AS ErrorMessage,
		ERROR_MESSAGE() AS ErrorMessage,
		ERROR_LINE() AS ErrorLine;
	PRINT'End of Error Statement ====================================='
	END CATCH;
END



