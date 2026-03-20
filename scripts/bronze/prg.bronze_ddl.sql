/*
================================================================================
 Project    : Paragon End-to-End Project (Q1 2026)
 File       : prg.ddl_bronze.sql
 Purpose    : Create Bronze layer tables for Paragon Data Warehouse
 Author     : Ramdiany
 Platform   : PostgreSQL
 Layer      : Bronze (Raw / Ingestion)
 Repository : paragon-end-to-end-project-q1-2026

 Description:
   This script creates the necessary tables for the Bronze layer of the Paragon Data Warehouse.
   In this layer, all data from various source systems is loaded into predefined tables.
   Data is stored in its raw form without any transformation, using TEXT data types to avoid errors.
   A timestamp column is included in each table to track when the data was loaded into the Data Warehouse.

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
DROP TABLE IF EXISTS bronze.fact_sales_raw;
DROP TABLE IF EXISTS bronze.dim_customer;
DROP TABLE IF EXISTS bronze.dim_product;
DROP TABLE IF EXISTS bronze.dim_store;
DROP TABLE IF EXISTS bronze.dim_promo;


-- ----------------------------------------------------------------------------
-- CREATE TABLES (Raw Storage)
-- ----------------------------------------------------------------------------

-- dim_product
CREATE TABLE IF NOT EXISTS bronze.dim_product (
    product_id          TEXT,
    product_name        TEXT,
    brand               TEXT,
    category            TEXT,
    sub_category        TEXT,
    price_per_unit      TEXT,
    source_system       TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_customer
CREATE TABLE IF NOT EXISTS bronze.dim_customer (
    customer_id         TEXT,
    name                TEXT,
    age                 TEXT,
    gender              TEXT,
    city                TEXT,
    membership_level    TEXT,
    source_system       TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_store
CREATE TABLE IF NOT EXISTS bronze.dim_store (
    store_id            TEXT,
    store_name          TEXT,
    channel_type        TEXT,
    region              TEXT,
    source_system       TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_promo
CREATE TABLE IF NOT EXISTS bronze.dim_promo (
    promo_id            TEXT,
    promo_name          TEXT,
    discount_percentage TEXT,
    source_system       TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- fact_sales_raw
CREATE TABLE IF NOT EXISTS bronze.fact_sales_raw (
    order_id            TEXT,
    date                TEXT,
    customer_id         TEXT,
    product_id          TEXT,
    brand_raw           TEXT,
    store_id            TEXT,
    promo_id            TEXT,
    quantity            TEXT,
    unit_price          TEXT,
    payment_method      TEXT,
    total_amount        TEXT,
    source_system       TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



