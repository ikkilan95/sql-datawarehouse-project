USE master;
GO

-- ===================================================================
-- Creating Database
-- ===================================================================
CREATE DATABASE DataWarehouse;

-- Use DB
USE DataWarehouse;

-- ===================================================================
-- If error due to database already created, drop DB
-- ===================================================================
IF EXISTS
(
    SELECT 1
    FROM sys.databases 
    WHERE name = 'DataWarehouse'
)
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE DataWarehouse
END;
GO



-- ===================================================================
-- CREATE SCHEMA
-- ===================================================================
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
