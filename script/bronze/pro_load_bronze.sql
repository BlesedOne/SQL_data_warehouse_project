/*
Store Procdeure: Load Bronze Layer (Source to Bronze)
===========================================================================================

This procedure load bronze layer from the csv file into the bronze using BULK INSERT.
The script TRUNCATE the table before loding.
============================================================================================

No Parameter required.
============================================================================================
*/
use DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME ;
	-- -----------------------------------------------------------------------------

		SET @batch_start_time = GETDATE();

		PRINT '===============================================';
		PRINT ' Loading bronze layer'
		PRINT '===============================================';

	
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT 'Inserting in : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sql\dwh_project\sample_customer_data.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_emp
		PRINT 'Inserting in : bronze.crm_emp';
		BULK INSERT bronze.crm_emp
		FROM 'C:\Users\sql\dwh_project\employee_sample_data.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prod
		PRINT 'Inserting in : bronze.crm_prod';
		BULK INSERT bronze.crm_prod
		FROM 'C:\Users\sql\dwh_project\product_sample_data.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_order
		PRINT 'Inserting in : bronze.crm_order';
		BULK INSERT bronze.crm_order
		FROM 'C:\Users\sql\dwh_project\sample_orders.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_st
		PRINT 'Inserting in : bronze.crm_st';
		BULK INSERT bronze.crm_st
		FROM 'C:\Users\sql\dwh_project\student_sample_data.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_spl
		PRINT 'Inserting in : bronze.crm_spl';
		BULK INSERT bronze.crm_spl
		FROM 'C:\Users\sql\dwh_project\supplier_data.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			--ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)  + ' seconds';
		PRINT '---------------------------------------'

		PRINT '================================================'
		SET @batch_end_time = GETDATE();
		PRINT '>>>> Total Load Duration =  ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================'

	END TRY
	BEGIN CATCH
		PRINT '================================================'
		PRINT 'Handling Error';
		PRINT '================================================'

		PRINT 'ERROR MESSAGES' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END


