
/* Note ...
=============================================================================
Creat Database and Schemas
==============================================================================

This script create a new database named 'DataWarehouse'. It first check if the database exists
if exists it will drop the database and create it agian. The script also creat 3 schemas
Namely 'bronze', 'gold' and 'silver'.


WARNING!!!!
This script  will  delete every data in the database permanently and recreate a new database if exists
CAUTION:  Ensure you have a proper backup before running this script

*/

USE master
GO

IF EXISTS  (SELECT 1 FROM sys.databases WHERE NAME = 'DataWarehouse')

BEGIN 
DROP DATABASE DataWarehouse;
PRINT('Database DataWarehouse has been droped')
END;

GO
--   Create Database

CREATE DATABASE DataWarehouse;
PRINT('Database DataWarehouse has been created')
GO

USE DataWarehouse;
GO

--      Create Schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA gold;
GO
CREATE SCHEMA silver;
GO
