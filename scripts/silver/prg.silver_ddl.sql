/*
================================================================================
 Project    : Paragon End-to-End Project (Q1 2026)
 File       : prg.ddl_silver.sql
 Purpose    : Create Silver layer tables for Paragon Data Warehouse
 Author     : Ramdiany
 Platform   : PostgreSQL
 Layer      : Silver (Clean)
 Repository : paragon-end-to-end-project-q1-2026

 Description:
   This script creates the tables required in the Silver layer. The number of 
   tables is the same as in the Bronze layer. These tables will store the cleaned 
   version of the data.
   
   Table structure:
   - Dimension
        > dim_customer  -> contains customer data of PT Paragon Technology
        > dim_product   -> contains product data from all PT Paragon Technology brands
        > dim_promo     -> contains all promotion data
        > dim_store     -> contains all stores (offline/online) selling Paragon products
   - Fact
        > fact_sales    -> contains order/transaction data

 Change Log:
   2026-03-20  Initial version
================================================================================
*/


-- ----------------------------------------------------------------------------
-- SET ENVIRONMENT
-- ----------------------------------------------------------------------------

-- Drop Tables for Refresh
DROP TABLE IF EXISTS silver.fact_sales_raw;
DROP TABLE IF EXISTS silver.dim_customer;
DROP TABLE IF EXISTS silver.dim_product;
DROP TABLE IF EXISTS silver.dim_store;
DROP TABLE IF EXISTS silver.dim_promo;


-- ----------------------------------------------------------------------------
-- CREATE TABLES (Clean Storage)
-- ----------------------------------------------------------------------------

-- dim_product
    DROP TABLE IF EXISTS silver.dim_product;
    CREATE TABLE IF NOT EXISTS silver.dim_product (
        product_id          VARCHAR(30),
        product_name        VARCHAR(100),
        brand               VARCHAR(30),
        category            VARCHAR(30),
        sub_category        VARCHAR(30),
        unit_price_idr      NUMERIC(12),
        data_source	        VARCHAR(30),
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- dim_customer
    DROP TABLE IF EXISTS silver.dim_customer;
    CREATE TABLE IF NOT EXISTS silver.dim_customer (
        customer_id         VARCHAR(30),
        customer_name       VARCHAR(150),
        age                 NUMERIC(3),
        gender              VARCHAR(20),
        city                VARCHAR(30),
        membership_level    VARCHAR(20),
        data_source         VARCHAR(30),
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- dim_store
    DROP TABLE IF EXISTS silver.dim_store;
    CREATE TABLE IF NOT EXISTS silver.dim_store (
        store_id            VARCHAR(30),
        store_name          VARCHAR(150),
        store_type     		VARCHAR(20),
        category            VARCHAR(50),
        data_source		    VARCHAR(30),
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- dim_promo
    DROP TABLE IF EXISTS silver.dim_promo; 
    CREATE TABLE IF NOT EXISTS silver.dim_promo (
        promo_id            VARCHAR(30),
        promo_name          VARCHAR(150),
        discount_percentage NUMERIC(3),
        data_source         VARCHAR(30),
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- fact_sales
    DROP TABLE IF EXISTS silver.fact_sales; 
    CREATE TABLE IF NOT EXISTS silver.fact_sales (
        order_id            VARCHAR(30),
        order_date          DATE,
        customer_id         VARCHAR(30),
        product_id          VARCHAR(30),
        brand               VARCHAR(30),
        store_id            VARCHAR(30),
        promo_id            VARCHAR(30),
        quantity            NUMERIC(10),
        unit_price_idr      NUMERIC(12),
        payment_method      VARCHAR(30),
        total_amount_idr    NUMERIC(15),
        data_source         VARCHAR(30),
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

