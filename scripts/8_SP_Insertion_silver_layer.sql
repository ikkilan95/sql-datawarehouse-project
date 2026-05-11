/*
=================================================================================================
STORED PROCEDURE FOR INSERTION OF SILVER LAYER FROM BRONZE LAYER
=================================================================================================
- Below scripts are the transformations made based on the raw data in bronze layer
- WARNING! Running the script below will truncate all existing data in the silver layer tables 
*/
--EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME

    BEGIN TRY

        SET @start_time = GETDATE()
            PRINT '>> Truncating: silver.crm_cust_info'
            TRUNCATE TABLE silver.crm_cust_info

            PRINT '>> Insertion: silver.crm_cust_info'
            INSERT INTO silver.crm_cust_info
            (
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date
            )
            select
                cst_id,
                cst_key,
                trim(cst_firstname) cst_firstname,
                trim(cst_lastname) cst_lastname,
                CASE UPPER(cst_marital_status)  
                    WHEN 'S' THEN 'SINGLE'
                    WHEN 'M' THEN 'MARRIED'
                    ELSE 'N/A'
                END cst_marital_status,
                CASE UPPER(cst_gndr)
                    WHEN 'M' THEN 'MALE'
                    WHEN 'F' THEN 'FEMALE'
                    ELSE 'N/A'
                END cst_gndr,
                cst_create_date
            FROM
            (
                SELECT 
                    *,
                    row_number() over(partition by cst_id order by cst_create_date DESC) flag_last
                FROM bronze.crm_cust_info
                where cst_id is not null
            ) t
            where flag_last = 1;
        SET @end_time = GETDATE()
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        SET @start_time = GETDATE()
            PRINT '>> Truncating: silver.crm_prd_info'
            TRUNCATE TABLE silver.crm_prd_info 

            PRINT '>> Insertion: silver.crm_prd_info'
            INSERT INTO silver.crm_prd_info 
            (
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
                replace(substring(prd_key, 1, 5), '-','_') cat_id,
                substring(prd_key, 7, len(prd_key)) prd_key,
                prd_nm,
                coalesce(prd_cost, 0) prd_cost,
                case UPPER(TRIM(prd_line))
                    when 'M' then 'Mountain'
                    when 'R' then 'Road'
                    when 'S' then 'Other Sales'
                    when 'T' then 'Touring'
                    else 'N/A'
                end prd_line,
                CAST(prd_start_dt AS DATE) prd_start_dt,
                CAST(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 AS DATE) prd_end_dt
            from bronze.crm_prd_info;
        SET @end_time = GETDATE()
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        SET @start_time = GETDATE()
            PRINT '>> Truncating: silver.crm_sales_details'
            TRUNCATE TABLE silver.crm_sales_details

            PRINT '>> Insertion: silver.crm_sales_details'
            INSERT INTO silver.crm_sales_details
            (
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt,
                sls_ship_dt,
                sls_due_dt,
                sls_sales,
                sls_quantity,
                sls_price
            )
            SELECT
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                CASE 
                    WHEN sls_order_dt <= 0 OR len(sls_order_dt) != 8 THEN NULL
                    ELSE cast(cast(sls_order_dt as VARCHAR) as DATE)
                END sls_order_dt,
                CASE 
                    WHEN sls_ship_dt <= 0 OR len(sls_ship_dt) != 8 THEN NULL
                    ELSE cast(cast(sls_ship_dt as VARCHAR) as DATE)
                END sls_ship_dt,
                CASE 
                    WHEN sls_due_dt <= 0 OR len(sls_due_dt) != 8 THEN NULL
                    ELSE cast(cast(sls_due_dt as VARCHAR) as DATE)
                END sls_due_dt,
                case when sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (sls_quantity * ABS(sls_price))
                    THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END sls_sales,
                sls_quantity,
                case when sls_price IS NULL OR sls_price <= 0 
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END sls_price
            from bronze.crm_sales_details;
        SET @end_time = GETDATE()
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'
    
        SET @start_time = GETDATE()
            PRINT '>> Truncating: silver.erp_cust_az12'
            TRUNCATE TABLE silver.erp_cust_az12

            PRINT '>> Insertion: silver.erp_cust_az12'
            INSERT INTO silver.erp_cust_az12
            (
                CID,
                BDATE,
                GEN
            )
            SELECT
                CASE
                    WHEN CID LIKE 'NAS%' THEN UPPER(TRIM(SUBSTRING(CID, 4, LEN(CID))))
                    ELSE CID
                END CID,
                CASE 
                    WHEN BDATE > GETDATE() THEN NULL
                    ELSE BDATE
                END BDATE,
                CASE 
                    -- Checks if 'F' exists and 'M' does not (to avoid matching 'Female' as 'Male')
                    WHEN UPPER(GEN) LIKE '%F%' AND UPPER(GEN) NOT LIKE '%MALE%' THEN 'Female'
                    -- Checks if 'M' exists
                    WHEN UPPER(GEN) LIKE '%M%' THEN 'Male'
                    ELSE 'N/A'
                END AS GEN
            FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE()
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        SET @start_time = GETDATE()
            PRINT '>> Truncating: silver.erp_loc_a101'
            TRUNCATE TABLE silver.erp_loc_a101

            PRINT '>> Insertion: silver.erp_loc_a101'
            INSERT INTO silver.erp_loc_a101
            (
                CID,
                CNTRY
            )
            SELECT
                REPLACE(CID, '-', '') CID,
                -- This nested REPLACE targets the 'Enter' keys specifically
                TRIM(REPLACE(REPLACE(CNTRY, CHAR(13), ''), CHAR(10), '')) CNTRY
            FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE()
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        SET @start_time = GETDATE()
            PRINT '>> Truncating: silver.erp_px_cat_g1v2'
            TRUNCATE TABLE silver.erp_px_cat_g1v2

            PRINT '>> Insertion: silver.erp_px_cat_g1v2'
            INSERT INTO silver.erp_px_cat_g1v2
            (
                ID,
                CAT,
                SUBCAT,
                MAINTENANCE
            )
            SELECT DISTINCT
                ID,
                CAT,
                SUBCAT,
                -- This nested REPLACE targets the 'Enter' keys specifically
                TRIM(REPLACE(REPLACE(MAINTENANCE, CHAR(13), ''), CHAR(10), '')) MAINTENANCE
            FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE()
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

    END TRY
    BEGIN CATCH
        PRINT '==============================================';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH
END;