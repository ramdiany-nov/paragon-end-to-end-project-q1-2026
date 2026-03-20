/*==============================================================================
  Project        : Paragon Data Warehouse
  File           : init_database.sql
  Purpose        : Initialise Paragon DW database and Medallion schemas
  Platform       : Microsoft SQL Server
==============================================================================*/

-- Create database
CREATE DATABASE paragon;

-- Create schema
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
