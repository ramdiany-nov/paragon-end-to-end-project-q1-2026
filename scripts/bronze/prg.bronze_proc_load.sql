/*
================================================================================
 Project    : Paragon End-to-End Project (Q1 2026)
 File       : prg.proc_bronze.sql
 Purpose    : Orchestrates full reload of the Bronze layer from raw CSV sources into staging tables.
 Author     : Ramdiany
 Platform   : PostgreSQL
 Layer      : Bronze (Raw / Ingestion)
 Repository : paragon-end-to-end-project-q1-2026

 Description:
   This script loads raw data as-is from CSV files into the predefined tables in the Bronze layer of the Paragon database.
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

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $bronze$
DECLARE
    start_time        TIMESTAMP;
    end_time          TIMESTAMP;
    start_whole_time  TIMESTAMP;
    end_whole_time    TIMESTAMP;
BEGIN

    start_whole_time := clock_timestamp();

    RAISE NOTICE '==================================================';
    RAISE NOTICE '-------------- LOADING BRONZE LAYER --------------';
    RAISE NOTICE '==================================================';

    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE '             Loading Dimension Tables             ';
    RAISE NOTICE '--------------------------------------------------';

    /* dim_customer */
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating table: bronze.dim_customer';
    TRUNCATE TABLE bronze.dim_customer;

    RAISE NOTICE '>> Inserting data into: bronze.dim_customer';
    COPY bronze.dim_customer(customer_id, name, age, gender, city, membership_level)
    FROM 'D:/Real Company_Synthetic Datasets/paragon_dataset/dim_customer.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    RAISE NOTICE '------------------------';


    /* dim_product */
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating table: bronze.dim_product';
    TRUNCATE TABLE bronze.dim_product;

    RAISE NOTICE '>> Inserting data into: bronze.dim_product';
    COPY bronze.dim_product(product_id, product_name, brand, category, sub_category, price_per_unit)
    FROM 'D:/Real Company_Synthetic Datasets/paragon_dataset/dim_product.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    RAISE NOTICE '------------------------';


    /* dim_promo */
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating table: bronze.dim_promo';
    TRUNCATE TABLE bronze.dim_promo;

    RAISE NOTICE '>> Inserting data into: bronze.dim_promo';
    COPY bronze.dim_promo(promo_id, promo_name, discount_percentage)
    FROM 'D:/Real Company_Synthetic Datasets/paragon_dataset/dim_promo.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    RAISE NOTICE '------------------------';


    /* dim_store */
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating table: bronze.dim_store';
    TRUNCATE TABLE bronze.dim_store;

    RAISE NOTICE '>> Inserting data into: bronze.dim_store';
    COPY bronze.dim_store(store_id, store_name, channel_type, region)
    FROM 'D:/Real Company_Synthetic Datasets/paragon_dataset/dim_store.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    RAISE NOTICE '------------------------';


    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE '               Loading Fact Table                 ';
    RAISE NOTICE '--------------------------------------------------';

    /* fact_sales */
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncating table: bronze.fact_sales_raw';
    TRUNCATE TABLE bronze.fact_sales_raw;

    RAISE NOTICE '>> Inserting data into: bronze.fact_sales_raw';
    COPY bronze.fact_sales_raw(order_id, date, customer_id, product_id, brand_raw, store_id, promo_id, quantity, unit_price, payment_method, total_amount)
    FROM 'D:/Real Company_Synthetic Datasets/paragon_dataset/fact_sales_raw.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    RAISE NOTICE '------------------------';


    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE '             Source System Mapping                ';
    RAISE NOTICE '--------------------------------------------------';

    start_time := clock_timestamp();

    RAISE NOTICE '>> Update metadata for fact table';
    UPDATE bronze.fact_sales_raw fs
    SET source_system =
        CASE
            WHEN LOWER(TRIM(ds.region)) = 'social commerce'  THEN 'Social Platform'
            WHEN LOWER(TRIM(ds.region)) = 'department store' THEN 'Retail POS'
            WHEN LOWER(TRIM(ds.region)) = 'd2c'              THEN 'Brand Website'
            WHEN LOWER(TRIM(ds.region)) = 'e-commerce'       THEN 'Marketplace'
            WHEN LOWER(TRIM(ds.region)) = 'distributor'      THEN 'B2B System'
            WHEN LOWER(TRIM(ds.region)) = 'pharmacy'         THEN 'Pharmacy POS'
            WHEN LOWER(TRIM(ds.region)) = 'beauty retail'    THEN 'Beauty Store POS'
            WHEN LOWER(TRIM(ds.region)) = 'general trade'    THEN 'Traditional POS'
            WHEN LOWER(TRIM(ds.region)) = 'modern trade'     THEN 'Modern Retail POS'
            WHEN LOWER(TRIM(ds.region)) = 'flagship store'   THEN 'Brand Store POS'
            ELSE 'Unknown POS'
        END
    FROM bronze.dim_store ds
    WHERE fs.store_id = ds.store_id;

    RAISE NOTICE '>> Update metadata for dimension tables';
    UPDATE bronze.dim_customer SET source_system = 'CRM System'       WHERE source_system IS NULL;
    UPDATE bronze.dim_product  SET source_system = 'ERP System'       WHERE source_system IS NULL;
    UPDATE bronze.dim_promo    SET source_system = 'Marketing System' WHERE source_system IS NULL;
    UPDATE bronze.dim_store    SET source_system = 'Store Ops System' WHERE source_system IS NULL;

    end_time := clock_timestamp();
    RAISE NOTICE 'Mapping duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;


    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE '        Performance Optimization: Indexing        ';
    RAISE NOTICE '--------------------------------------------------';

    start_time := clock_timestamp();

    RAISE NOTICE '>> Create indexes';
    CREATE INDEX IF NOT EXISTS idx_fact_customer_id ON bronze.fact_sales_raw(customer_id);
    CREATE INDEX IF NOT EXISTS idx_fact_store_id    ON bronze.fact_sales_raw(store_id);
    CREATE INDEX IF NOT EXISTS idx_fact_date        ON bronze.fact_sales_raw(date);
    CREATE INDEX IF NOT EXISTS idx_fact_product_id  ON bronze.fact_sales_raw(product_id);

    end_time := clock_timestamp();
    end_whole_time := clock_timestamp();

    RAISE NOTICE 'Indexing duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Total duration: % ms', EXTRACT(EPOCH FROM (end_whole_time - start_whole_time)) * 1000;
    RAISE NOTICE '==================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE;
END;
$bronze$;