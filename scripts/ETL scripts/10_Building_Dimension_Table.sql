/*
select top 5 * from silver.crm_cust_info

select top 5 * from silver.crm_prd_info

select top 5 * from silver.crm_sales_details

select top 5 * from silver.erp_cust_az12

select top 5 * from silver.erp_loc_a101

select top 5 * from silver.erp_px_cat_g1v2
*/

-- ===============================================
-- BUIDLIND DIMENSION TABLES FOR CUSTOMER OBJECT
-- ===============================================
-- silver.crm_cust_info is the Master table with cst_key as the FK
-- silver.erp_cust_az12 will be joined with Master table with its CID column as the FK
-- silver.erp_loc_a101 will be joined with Master table with its CID column as the FK
SELECT
    ci.cst_id cst_id,
    ci.cst_key cst_key,
    ci.cst_firstname cst_firstname,
    ci.cst_lastname cst_lastname,
    ci.cst_marital_status cst_marital_status,
    ci.cst_gndr cst_gndr,
    ci.cst_create_date cst_create_date,
    ca.BDATE birthdate,
    ca.GEN gender,
    cl.CNTRY country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ca.CID = ci.cst_key
LEFT JOIN silver.erp_loc_a101 cl ON cl.CID = ci.cst_key;

-- select top 5 * from silver.erp_cust_az12
-- select top 5 * from silver.erp_loc_a101

-- ========== DATA VALIDATION FOR THE CUSTOMER OBJECT ==========
-- We need to check if there is any duplications after joining the tables
with cte as 
(
    SELECT
        ci.cst_id cst_id,
        ci.cst_key cst_key,
        ci.cst_firstname cst_firstname,
        ci.cst_lastname cst_lastname,
        ci.cst_marital_status cst_marital_status,
        ci.cst_gndr cst_gndr,
        ci.cst_create_date cst_create_date,
        ca.BDATE birthdate,
        ca.GEN gender
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ca.CID = ci.cst_key
    LEFT JOIN silver.erp_loc_a101 cl ON cl.CID = ci.cst_key

)
SELECT cst_id, count(cst_id) from cte group by cst_id having count(cst_id) != 1
-- No dupe found

-- We have two columns representing the same data, which is the gender (cst_gndr - from crm_cust_info, gender - erp_cust_az12)
-- We need to validate that both tables have the same data for each rows
SELECT DISTINCT
        ci.cst_gndr cst_gndr,
        ca.GEN gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ca.CID = ci.cst_key;
-- found inconsistencies: not matching gender for some rows. As per the user's feedback, Master table is the source of truth.
-- To address the issue (Data Enrichment):
SELECT DISTINCT
        ci.cst_gndr,
        ca.GEN,
        CASE 
            WHEN ci.cst_gndr != 'N/A' THEN UPPER(ci.cst_gndr)
            ELSE COALESCE(UPPER(ca.GEN), 'N/A')
        END gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ca.CID = ci.cst_key;

-- Revised Customer Object Dimension Table Creation (View Table):
-- Generation of surrogate key via row_number() AS customer_key
CREATE VIEW gold.dim_customers AS
SELECT
    row_number() over(order by ci.cst_id) customer_key,
    ci.cst_id customer_id,
    ci.cst_key customer_number,
    ci.cst_firstname first_name,
    ci.cst_lastname last_name,
    cl.CNTRY country,
    ci.cst_marital_status marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN UPPER(ci.cst_gndr)
        ELSE COALESCE(UPPER(ca.GEN), 'N/A')
    END gender,
    ca.BDATE birthdate,
    ci.cst_create_date create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ca.CID = ci.cst_key
LEFT JOIN silver.erp_loc_a101 cl ON cl.CID = ci.cst_key;

-- Validating the creation of the view table:
select * from gold.dim_customers
select distinct gender from gold.dim_customers

-- ===============================================
-- BUIDLIND DIMENSION TABLES FOR PRODUCT OBJECT
-- ===============================================
-- silver.crm_prd_info is the Master table with cat_id as FK
-- silver.erp_px_cat_g1v2 will be joined with its ID column as FK
SELECT
    pi.prd_id,
    pi.cat_id,
    pi.prd_key,
    pi.prd_nm,
    pi.prd_cost,
    pi.prd_line,
    pi.prd_start_dt,
    pc.CAT,
    pc.SUBCAT,
    pc.MAINTENANCE
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pc.ID = pi.cat_id
WHERE pi.prd_end_dt IS NULL -- filtering to only have current active data

select top 5 * from silver.crm_prd_info
select top 5 * from silver.erp_px_cat_g1v2

-- ========== DATA VALIDATION FOR THE PRODUCT OBJECT ==========
-- check the uniqueness of the product key
select prd_key, count(prd_key)
FROM 
(
SELECT
    pi.prd_id,
    pi.cat_id,
    pi.prd_key,
    pi.prd_nm,
    pi.prd_cost,
    pi.prd_line,
    pi.prd_start_dt,
    pc.CAT,
    pc.SUBCAT,
    pc.MAINTENANCE
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pc.ID = pi.cat_id
WHERE pi.prd_end_dt IS NULL
)t
GROUP BY prd_key
having count(prd_key) != 1;
-- No duplicate

-- Revised Product Object Dimension Table Creation (View Table):
-- Generation of surrogate key via row_number() AS customer_key
CREATE VIEW gold.dim_products AS
SELECT
    row_number() over(order by pi.prd_start_dt, pi.prd_key) product_key,
    pi.prd_id product_id,
    pi.prd_key product_number,
    pi.prd_nm product_name,
    pi.cat_id category_id,
    pc.CAT category,
    pc.SUBCAT subcategory,
    pc.MAINTENANCE Maintenance,
    pi.prd_cost cost,
    pi.prd_line product_line,
    pi.prd_start_dt start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pc.ID = pi.cat_id
WHERE pi.prd_end_dt IS NULL

-- validating the view dim_products table
select * from gold.dim_products