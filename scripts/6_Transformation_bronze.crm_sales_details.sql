-- ==============================================
-- Checking data quality of bronze.crm_sales_details
-- ==============================================
SELECT TOP (20) * FROM bronze.crm_sales_details -- for observation

-- =====> Check unwanted spaces on Order Number <=====
SELECT
    sls_ord_num
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)
-- No unwanted spaces

-- =====> Check integrity of [sls_prd_key] & [sls_cust_id] FK <=====
select 
    *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)
-- No issue found

select 
    *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.sls_cust_id)
-- No issue found

-- =====> Handling date issue for tables: [sls_order_dt] , [sls_ship_dt] , [sls_due_dt] <=====
-- these tables are in INT data type

-- checking if there is zeroes in the column | not having the same len()
SELECT
    sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 OR len(sls_order_dt) != 8;
-- 19 rows found with 0 value & not in len() of 8 as a date, coalesce as NULLs.

-- Addresing the issue: invalid date format cast as NULLs and cast as DATE.
SELECT
    sls_ord_num,
    sls_prd_key,
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
    END sls_due_dt
from bronze.crm_sales_details

-- =====> Checking integrity of [sls_sales] , [sls_quantity] , [sls_price] table <=====
-- as per business requirement: all values must be positive
select
    *
from bronze.crm_sales_details
where sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
-- found 10 rows with <= 0

-- as per business requirement: no NULLs
select
    *
from bronze.crm_sales_details
where sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
-- found 15 rows with NULLs

-- as per business rules: values must satisfy: sales = quantity * price
select
    *
from bronze.crm_sales_details
where sls_sales != (sls_quantity * sls_price)
-- found 20 rows which does not meet the business rule

-- Addressing issue: After confirming with the users, below are the criteria:-
        -- for sales: derive using quantity * price
        -- for quantity: derive from the business rule
        -- for price: calculate sales * quantity | if price is negative, convert to positive

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
from bronze.crm_sales_details

