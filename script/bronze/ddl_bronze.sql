
use DataWarehouse
/*
DDL SCRIPT: CREATE BRONZE TABLES
===========================================================
This script create 6 tables in the bronze schema.
It drops the able if exist and create  new ones.
===========================================================
*/
--CustomerID,CustomerName,Email,Age,Country,SignupDate

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info(
CustomerID INT,
CustomerName NVARCHAR(50),
Email NVARCHAR(30),
Age INT,
Country NVARCHAR(50),
SignupDate DATE
);


--EmployeeID,FirstName,LastName,Department,Salary,HireDate
IF OBJECT_ID('bronze.crm_emp','U') IS NOT NULL
	DROP TABLE bronze.crm_emp
CREATE TABLE bronze.crm_emp(
emp_id INT,
emp_first_name VARCHAR (20),
emp_last_nane VARCHAR(20),
emp_dept VARCHAR(50),
emp_salary DECIMAL(12,1),
emp_hire_date DATE
);

--ProductID,ProductName,Category,Price,StockQuantity,AddedDate
IF OBJECT_ID('bronze.crm_prod','U') IS NOT NULL
	DROP TABLE bronze.crm_prod
CREATE TABLE bronze.crm_prod(
pro_id int,
pro_name NVARCHAR(50),
pro_category NVARCHAR(50),
pro_price DECIMAL(12,2),
pro_stock_quantity int,
pro_added_date DATE
);

--OrderID,CustomerID,ProductID,Quantity,TotalAmount,OrderDate
IF OBJECT_ID('bronze.crm_order','U') IS NOT NULL
	DROP TABLE bronze.crm_order
CREATE TABLE bronze.crm_order(
order_id int,
order_customer_id int,
order_product_id int,
order_quantity int,
order_total_amount DECIMAL(12,2),
order_date DATE
);

--StudentID,StudentName,Course,Grade,Age,EnrollmentDate
IF OBJECT_ID('bronze.crm_st','U') IS NOT NULL
	DROP TABLE bronze.crm_st
CREATE TABLE bronze.crm_st(
st_id int,
st_name NVARCHAR(50),
st_course NVARCHAR(50),
st_grade NVARCHAR(10),
st_age int,
st_enrollment_date DATE
);


--SupplierID,SupplierName,ContactEmail,Country,Rating,ContractDate
IF OBJECT_ID('bronze.crm_spl','U') IS NOT NULL
	DROP TABLE bronze.crm_spl
CREATE TABLE bronze.crm_spl(
spl_id int,
spl_name NVARCHAR(60),
spl_contact_email NVARCHAR(60),
spl_country NVARCHAR(100),
sl_rating int,
spl_contract_date DATE
);
