-- ===============================================
-- BUIDLIND FACT TABLES FOR SALES OBJECT
-- ===============================================
-- crm_sales_details is the Master table
-- We will replace sls_prd_key and sls_cust_id with the defined surrogate keys from the products and customers tables
-- Then we create a Fact Table for the Sales Object:
DROP VIEW gold.fact_sales
CREATE VIEW gold.fact_sales AS
SELECT
    sls_ord_num order_number,
    dp.product_key,
    dc.customer_key,
    sls_order_dt order_date,
    sls_ship_dt shipping_date,
    sls_due_dt due_date,
    sls_sales sales_amount,
    sls_quantity quantity,
    sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc ON sd.sls_cust_id = dc.customer_id;
--select top 5 * from silver.crm_sales_details
--select top 5 * from gold.dim_products 
--select top 5 * from gold.dim_customers

-- Validating the gold.fact_sales table
select * from gold.fact_sales 

-- Validating FK integrity (Fact joins Dimension Tables)
SELECT
    *
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp ON dp.product_key = fs.product_key
LEFT JOIN gold.dim_customers dc ON dc.customer_key = fs.customer_key
where dc.customer_key IS NULL OR dp.product_key IS NULL;
-- No issue with the FK joins

