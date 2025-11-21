/* 
Create Database and Schemas
========================================================================================================================================================================
Script Purpose:
  This script creates a new database name 'DataWarehouse' After checking if it already exists.
  If the database exists, it is dropped and recreate. Additionally, the script sets up three schemas 
  within the database: 'bronze', 'silver', and 'gold'.

Warning;
  Running this script will drop the entrie 'DataWarehouse' database if it exists.
  All data in the database will parmanently deleted. Proceed with caution and ensure
  you have proper backup before running this script
*/

-- Create Database 'DataWarehouse'
USE master;

-- Check if the database already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO


-- Create the database
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

-- Create the Schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
