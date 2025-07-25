/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start TIMESTAMP;
    batch_end TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    batch_start = clock_timestamp();
    -- CRM: Customer Info
    RAISE NOTICE 'Loading table bronze.crm_cust_info...';
    start_time := clock_timestamp();
    
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM 'C:\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
    
    end_time := clock_timestamp();
    RAISE NOTICE 'Loaded bronze.crm_cust_info in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

    -- CRM: Product Info
    RAISE NOTICE 'Loading table bronze.crm_prd_info...';
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM 'C:\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');

    end_time := clock_timestamp();
    RAISE NOTICE 'Loaded bronze.crm_prd_info in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

    -- CRM: Sales Details
    RAISE NOTICE 'Loading table bronze.crm_sales_details...';
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM 'C:\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');

    end_time := clock_timestamp();
    RAISE NOTICE 'Loaded bronze.crm_sales_details in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP: Customer AZ12
    RAISE NOTICE 'Loading table bronze.erp_cust_az12...';
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM 'C:\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');

    end_time := clock_timestamp();
    RAISE NOTICE 'Loaded bronze.erp_cust_az12 in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP: Location A101
    RAISE NOTICE 'Loading table bronze.erp_loc_a101...';
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM 'C:\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');

    end_time := clock_timestamp();
    RAISE NOTICE 'Loaded bronze.erp_loc_a101 in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP: Price Category G1V2
    RAISE NOTICE 'Loading table bronze.erp_px_cat_g1v2...';
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');

    end_time := clock_timestamp();
    RAISE NOTICE 'Loaded bronze.erp_px_cat_g1v2 in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

    RAISE NOTICE 'All bronze tables successfully loaded.';
    batch_end = clock_timestamp();
    RAISE NOTICE 'Loaded WHOLE BATCH in % seconds.', EXTRACT(SECOND FROM batch_end- batch_start);
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'An error occurred while loading bronze tables: %', SQLERRM;
END;
$$;
