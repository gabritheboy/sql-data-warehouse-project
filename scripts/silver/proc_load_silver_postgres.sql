/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to 
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None. 
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE SILVER.LOAD_SILVER () LANGUAGE PLPGSQL AS $$
DECLARE
    batch_start TIMESTAMP;
    batch_end TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
   	batch_start = clock_timestamp();
    -- CRM: Customer Info
    RAISE NOTICE 'Loading table silver.crm_cust_info...';
    start_time = clock_timestamp();
        RAISE NOTICE 'Loading Silver Layer';
        RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '------------------------------------------------';
		batch_start = clock_timestamp();
    	-- CRM: Customer Info
   	 	RAISE NOTICE 'Loading table silver.crm_cust_info...';
    	start_time := clock_timestamp();
    
 		
   
		TRUNCATE TABLE silver.crm_cust_info;
		RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;
		 -- Select the most recent record per customer
			end_time := clock_timestamp();
    		RAISE NOTICE 'Loaded silver.crm_cust_info in % seconds.', EXTRACT(SECOND FROM end_time - start_time);

		-- Loading silver.crm_prd_info
        start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)

		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
		prd_nm,
		CASE
		WHEN prd_cost IS NULL THEN 0
		ELSE prd_cost
		END AS prd_cost,
		CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'Unknown'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt ASC) - INTERVAL '1 DAY' AS DATE) AS prd_end_dt_test
		FROM bronze.crm_prd_info;
		end_time := clock_timestamp();
        RAISE NOTICE 'Loaded bronze.crm_cust_info in % seconds.', EXTRACT(SECOND FROM end_time - start_time);
        RAISE NOTICE '>> -------------';

        -- Loading crm_sales_details
        start_time = clock_timestamp();
		RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt as VARCHAR)) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) as DATE)
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt as VARCHAR)) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) as DATE)
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt <= 0 OR LENGTH(CAST(sls_due_dt as VARCHAR)) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) as DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price <=0 OR sls_price IS NULL
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;
		
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded silver.crm_sales_details in % seconds.', EXTRACT(SECOND FROM end_time - start_time);
        RAISE NOTICE '>> -------------';

        RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '------------------------------------------------';
		
		-- Loading erp_cust_az12
        start_time = clock_timestamp();
		RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
		cid,bdate,gen
		)
		SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > NOW() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN TRIM(UPPER(gen)) IN ('M','MALE') THEN 'Male'
			WHEN TRIM(UPPER(gen)) IN ('M','FEMALE') THEN 'Female'
			ELSE 'n/a'
		END AS gen
		FROm bronze.erp_cust_az12;
		end_time := clock_timestamp();
        RAISE NOTICE 'Loaded silver.erp_cust_az12 in % seconds.', EXTRACT(SECOND FROM end_time - start_time);
        RAISE NOTICE '>> -------------';

		

        -- Loading erp_loc_a101
        start_time = clock_timestamp();
		RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101 ;
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		SELECT
		REPLACE(cid,'-','') as cid,
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;
	    end_time := clock_timestamp();
		RAISE NOTICE 'Loaded silver.erp_loc_a101 in % seconds.', EXTRACT(SECOND FROM end_time - start_time);
        RAISE NOTICE '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		start_time = clock_timestamp();
		RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2; 
		end_time := clock_timestamp();
		RAISE NOTICE 'Loaded silver.erp_px_cat_g1v2 in % seconds.', EXTRACT(SECOND FROM end_time - start_time);
        RAISE NOTICE '>> -------------';

		batch_end = clock_timestamp();
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Loading Silver Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: in % seconds', EXTRACT(SECOND FROM batch_start - batch_end);
		RAISE NOTICE '==========================================';
		
END
$$