use DataWarehouse;

-- ==============================================
-- Checking data quality of bronze.crm_cust_info
-- ==============================================
select * from bronze.crm_cust_info -- for observation

-- =====> Check duplicate in PK <=====
SELECT 
    cst_id,
    count(*) 
FROM bronze.crm_cust_info
group by cst_id
having count(*) >1 OR cst_id IS NULL;
-- Identified 6 PK with duplicate & missing value issues:


-- Addressing the duplicate issue: The last entry is the correct data with all values are filled with no NULL & missing PK will be dropped
select
    *
FROM
(
    SELECT 
        *,
        row_number() over(partition by cst_id order by cst_create_date DESC) flag_last
    FROM bronze.crm_cust_info
    where cst_id is not null
) t
where flag_last = 1;

-- =====> Check white spaces <=====
-- [cst_firstname] column
SELECT
    cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)
-- identified 15 rows with white space issue

-- [cst_lastname] column
SELECT
    cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)
-- identified 17 rows with white space issue

-- Addressing the whitespace issue: trimming the columns where we have unecessary white space
select
    cst_id,
    cst_key,
    trim(cst_firstname) cst_firstname,
    trim(cst_lastname) cst_lastname,
    cst_marital_status,
    cst_gndr,
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

-- =====> Check the caridnality of column [cst_marital_status] & [cst_gndr] <=====
-- [cst_marital_status] column
SELECT
    distinct cst_marital_status
from bronze.crm_cust_info
-- There are NULLs. As per business requirement, replace with 'N/A'.

-- [cst_gndr] column
SELECT
    distinct cst_gndr
from bronze.crm_cust_info
-- There are NULLs. As per business requirement, replace with 'N/A'.

-- Addessing the issue: as per business requirement, provide full word instead of a single leter i.e 'M' to 'MALE'
-- This query will be used in the Stored Procedure: ingestion from bronze to silver layer
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

