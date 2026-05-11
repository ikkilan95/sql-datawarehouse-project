-- ==============================================
-- Checking data quality of bronze.crm_prd_info
-- ==============================================
SELECT TOP (20) * FROM bronze.crm_prd_info -- for observation

-- =====> Check duplicate in PK <=====
select
    prd_id,
    count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) != 1
-- No NULL and no duplicate

-- =====> taking substring from [prd_key] as a foreign key to join with the erp_px_cat_g1v2 & crm_sales_details tables <=====
SELECT 
    prd_id,
    prd_key,
    replace(substring(prd_key, 1, 5), '-','_') cat_id,
    substring(prd_key, 7, len(prd_key)) prd_key
from bronze.crm_prd_info

-- =====> check unwanted spaces for [prd_nm] column <=====
SELECT prd_nm 
from bronze.crm_prd_info 
where prd_nm != trim(prd_nm)
-- No unwanted spaces

-- =====> check NULLs and negative value in [prd_cost] column <=====
select prd_cost 
from bronze.crm_prd_info 
where prd_cost is null or  prd_cost < 0;
-- 2 NULLs found

-- Addressing NULLs: as per business requirements, NULLs in cost = 0

SELECT 
    prd_id,
    prd_key,
    replace(substring(prd_key, 1, 5), '-','_') cat_id,
    substring(prd_key, 7, len(prd_key)) prod_key,
    prd_nm,
    coalesce(prd_cost, 0) prd_cost
from bronze.crm_prd_info

-- =====> check cardinality of prd_line column <=====
select distinct prd_line
from bronze.crm_prd_info
-- we have NULL & abbreviation. As per business requirement, change abbreviation to full value.

-- Addressing the cardinality
SELECT 
    prd_id,
    prd_key,
    replace(substring(prd_key, 1, 5), '-','_') cat_id,
    substring(prd_key, 7, len(prd_key)) prod_key,
    prd_nm,
    coalesce(prd_cost, 0) prd_cost,
    case UPPER(TRIM(prd_line))
        when 'M' then 'Mountain'
        when 'R' then 'Road'
        when 'S' then 'Other Sales'
        when 'T' then 'Touring'
        else 'N/A'
    end prd_line
from bronze.crm_prd_info

-- =====> validifying the start & end date of [prd_start_dt] & [prd_end_dt] columns <=====
-- ensure no end date that is earlier than start date
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt
-- 200 rows having issue where prd_end_dt is ealier than prd_start_dt

-- Addressing the issue: assigning the end date as start date based on next updated cost on the following years minus 1 day
-- This query will be used in the Stored Procedure: ingestion from bronze to silver layer
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
from bronze.crm_prd_info