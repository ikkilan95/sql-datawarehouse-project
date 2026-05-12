-- ==========================================================================================
-- Stored procedure for bulk load source to tables created
-- ==========================================================================================
CREATE OR ALTER PROCEDURE bronze.load_bronze_layer AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY

        PRINT '==========================================';
        PRINT 'Loading Bronze Layer From Source: CRM csv';
        PRINT '==========================================';

        -- ===== ingest & valdiate cust_info =====
        SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_cust_info;

            BULK INSERT bronze.crm_cust_info
            FROM '/var/opt/mssql/datasets/cust_info.csv'
            WITH 
            (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK

            );
        SET @end_time = GETDATE();
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        -- ===== ingest & valdiate prd_info =====
        SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_prd_info;
            
            BULK INSERT bronze.crm_prd_info
            FROM '/var/opt/mssql/datasets/prd_info.csv'
            WITH 
            (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK

            );
        SET @end_time = GETDATE();
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        -- ===== ingest & valdiate sales_details =====
        SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_sales_details;
            
            BULK INSERT bronze.crm_sales_details
            FROM '/var/opt/mssql/datasets/sales_details.csv'
            WITH 
            (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK

            );
        SET @end_time = GETDATE();
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        PRINT '==========================================';
        PRINT 'Loading Bronze Layer From Source: ERP csv';
        PRINT '==========================================';
        
        -- ===== ingest & valdiate CUST_AZ12 =====
        SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_cust_az12;
            
            BULK INSERT bronze.erp_cust_az12
            FROM '/var/opt/mssql/datasets/CUST_AZ12.csv'
            WITH 
            (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK

            );
        SET @end_time = GETDATE();
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        -- ===== ingest & valdiate LOC_A101 =====
        SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_loc_a101;
            
            BULK INSERT bronze.erp_loc_a101
            FROM '/var/opt/mssql/datasets/LOC_A101.csv'
            WITH 
            (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK

            );
        SET @end_time = GETDATE();
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

        -- ===== ingest & valdiate PX_CAT_G1V2 =====
        SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;
            
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM '/var/opt/mssql/datasets/PX_CAT_G1V2.csv'
            WITH 
            (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK

            );
        SET @end_time = GETDATE();
        PRINT '> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '-------------------------------'

    END TRY

    BEGIN CATCH
        PRINT '==============================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH
END;