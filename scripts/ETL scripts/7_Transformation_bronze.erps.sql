-- ==============================================
-- Checking data quality of bronze.erp_cust_az12
-- ==============================================
SELECT TOP (1000) * FROM bronze.erp_cust_az12 -- for observation

-- =====> Checking the FK (CID colmnn ) of bronze.erp_cust_az12 <=====
-- this column can be connected with crm_cust_info
SELECT
    CID 
from bronze.erp_cust_az12
where CID not in (select cst_key from bronze.crm_cust_info)
-- found new entries where the CID having initials of 'NAS'

--Addressing the issue: Confirmed with the users, we can remove 'NAS'
SELECT
    CASE
        WHEN CID LIKE 'NAS%' THEN UPPER(TRIM(SUBSTRING(CID, 4, LEN(CID))))
        ELSE CID
    END CID
FROM bronze.erp_cust_az12

-- =====> Checking the BDATE: NULLs and invalid bdate <=====
SELECT
    BDATE
FROM bronze.erp_cust_az12
WHERE BDATE IS NULL
-- No NULLs

SELECT
    BDATE
FROM bronze.erp_cust_az12
WHERE BDATE > GETDATE()
-- 16 rows with far ahead of BDATE registerered as data

-- Addressing the issue: NULL the far ahead BDATE
SELECT
    CASE
        WHEN CID LIKE 'NAS%' THEN UPPER(TRIM(SUBSTRING(CID, 4, LEN(CID))))
        ELSE CID
    END CID,
    CASE 
        WHEN BDATE > GETDATE() THEN NULL
        ELSE BDATE
    END BDATE
FROM bronze.erp_cust_az12

-- =====> Checking the GEN column <=====

SELECT
    distinct GEN 
FROM bronze.erp_cust_az12
-- FOUND 9 rows with inconsistent cardinality


-- Addressing the issue: transform to only MALE AND FEMALE
-- Below script will be used as insertion for silver layer
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
    END AS Gen_2
FROM bronze.erp_cust_az12

-- ==============================================
-- Checking data quality of bronze.erp_loc_a101
-- ==============================================
SELECT TOP (1000) * FROM bronze.erp_loc_a101 -- for observation


-- =====> Handling '-' in CID column <=====
-- This column is a FK can be connected with customer table
SELECT
    REPLACE(CID, '-', '')
FROM bronze.erp_loc_a101

-- =====> Handling CNTRY Column <=====
SELECT DISTINCT CNTRY FROM bronze.erp_loc_a101 ORDER BY CNTRY
-- found 13 rows with inconsistent format & empty spaces

-- Double checking the len before and after trim()
SELECT DISTINCT CNTRY original, LEN(CNTRY) Len_original, TRIM(CNTRY) TRans, LEN(TRIM(CNTRY)) Len_TRans FROM bronze.erp_loc_a101 ORDER BY CNTRY
-- Found that there are carriage return (CR) in some of the value.

-- To address the issue: remove the CR identified as CHAR(13)
-- Script below will be used in the stored procedure for silver layer insertion
SELECT
    REPLACE(CID, '-', '') CID,
    -- This nested REPLACE targets the 'Enter' keys specifically
    TRIM(REPLACE(REPLACE(CNTRY, CHAR(13), ''), CHAR(10), '')) CNTRY
FROM bronze.erp_loc_a101

-- ==============================================
-- Checking data quality of bronze.erp_px_cat_g1v2
-- ==============================================
SELECT TOP (1000) * FROM bronze.erp_px_cat_g1v2-- for observation
-- No NULL or unwated spaces for the 1st three columns

-- =====> Handling Inconsistencies in the MAINTENANCE COLUMN <=====
SELECT DISTINCT MAINTENANCE, len(MAINTENANCE) FROM bronze.erp_px_cat_g1v2
-- found inconsistencies where CR found, No NULL and no empty space.

--Addressing the issue: Remove the CR
-- Below script will be used for the stored procedure for inserting to silver layer
SELECT DISTINCT
    ID,
    CAT,
    SUBCAT,
    -- This nested REPLACE targets the 'Enter' keys specifically
    TRIM(REPLACE(REPLACE(MAINTENANCE, CHAR(13), ''), CHAR(10), '')) MAINTENANCE
FROM bronze.erp_px_cat_g1v2