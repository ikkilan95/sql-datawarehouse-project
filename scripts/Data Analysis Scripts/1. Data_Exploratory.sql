use DataWarehouse;
-- ========================================================
-- DATA EXPLORATORY
-- ========================================================
-- 1. Exploring the objects in the DB
-- 2. Exploring the columns
-- 3. Exploring the dimension tables
-- 4. Exploring the fact table
-- 5. Magnitude analysis
-- 6. Ranking analysis
-- ========================================================


-- ========================================================
-- Explore all objects in the DB
-- ========================================================

-- ============== objects in DB ============== 
SELECT * FROM INFORMATION_SCHEMA.TABLES
-- 12 base tables, 3 view tables

-- ============== columns in DB to understand the metadata of the tables ============== 
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
-- 101 columns

-- ============== dim_customers table ==============
SELECT TOP (50) [customer_key]
      ,[customer_id]
      ,[customer_number]
      ,[first_name]
      ,[last_name]
      ,[country]
      ,[marital_status]
      ,[gender]
      ,[birthdate]
      ,[create_date]
  FROM [DataWarehouse].[gold].[dim_customers]

  -- ============== dim_products table ==============
  SELECT TOP (50) [product_key]
      ,[product_id]
      ,[product_number]
      ,[product_name]
      ,[category_id]
      ,[category]
      ,[subcategory]
      ,[Maintenance]
      ,[cost]
      ,[product_line]
      ,[start_date]
  FROM [DataWarehouse].[gold].[dim_products]

  -- ============== fact_sales table ==============
  SELECT TOP (50) [order_number]
      ,[product_key]
      ,[customer_key]
      ,[order_date]
      ,[shipping_date]
      ,[due_date]
      ,[sales_amount]
      ,[quantity]
      ,[price]
  FROM [DataWarehouse].[gold].[fact_sales]


-- ========================================================
-- DIMENSIONS EXPLORATION
-- ========================================================
-- Here we are identifying the unique values (or categories) in each dimension.
-- We are ecognizing how data might be grouped or segmented, which is useful for later analysis.
-- Common syntax use case: DISTINCT

-- Exploring dim_customers dimension columns

-- Country column
SELECT  DISTINCT Country from gold.dim_customers -- 6 unique dimensions, 1 N/A

-- Exploring dim_products dimension columns  

--  Major Category
SELECT DISTINCT Category FROM gold.dim_products; -- 4 unique products, 1 NULL

-- Subcategory
SELECT DISTINCT Category, Subcategory FROM gold.dim_products; -- 26 Types of subcategories, 1 NULL

-- Product name
SELECT DISTINCT Category, Subcategory, product_name FROM gold.dim_products; -- 288 Products, 7 NULL Categories


-- ========================================================
-- DIMENSIONS EXPLORATION
-- ========================================================
-- Identify the earliest and latest dates (date boundaries)
-- Understood the scope of data and the timespan.
-- Common syntax use case: MIN(), MAX()

-- Date Boundaries In Fact Table 

-- boundaries of order date
SELECT
    MIN(order_date) earliest_date,
    MAX(order_date) latest_date,
    DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) order_range_years,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) order_range_months
FROM gold.fact_sales; -- 4 years ~ 37 months of boundaries for the orders recorded in the table

-- boundaries of customer age
SELECT
    DATEDIFF(YEAR, MIN(birthdate), GETDATE()) oldest_cust,
    DATEDIFF(YEAR, MAX(birthdate), GETDATE()) youngest_cust
FROM gold.dim_customers -- oldest_cust: 110 , youngest_cust: 40


-- ========================================================
-- MEASURES EXPLORATION - select * from gold.fact_sales
-- ========================================================
-- Calculate the key metrics of the business (Big Numbers)
-- Highest level of aggregations | Lowest level of details
-- Common syntax use case: SUM(), AVG(), COUNT()

-- Find the Total Sales
select sum(sales_amount) total_sales from gold.fact_sales; -- 29356250

-- Find how many items are sold
select sum(quantity) items_sold from gold.fact_sales -- 60423

-- Find the average selling price
select avg(price) avg_sell_price from gold.fact_sales; -- 486

-- Find the Total number of Orders
select count(order_number) total_orders from gold.fact_sales; -- 60398 total order records in db
select count(distinct order_number) total_orders from gold.fact_sales; -- 27659 actual total orders

-- Find the total number of products
select count(product_key) total_products from gold.dim_products; -- 295 total product records in db
select count(distinct product_key) total_products from gold.dim_products; -- 295 actual products available as per in db


-- Find the total number of customers
select count(customer_key) total_customers from gold.dim_customers; -- 18484 total customers registered in db

-- Find the total number of customers that has placed an order
select count(distinct customer_key) Nr_Customer_Placing_Order from gold.fact_sales; -- 18484 total customers placeed an ordered

-- Generate simple high level reports
select 'Total Sales' Measures, sum(sales_amount) measure_value from gold.fact_sales
UNION ALL
select 'Items Sold' Measure, sum(quantity) measure_value from gold.fact_sales
UNION ALL
select 'Avg Sell Price' Measure, avg(price) measure_value from gold.fact_sales
UNION ALL
select 'Total Orders' Measure, count(distinct order_number) measure_value from gold.fact_sales
UNION ALL
select 'Total Products' Measure, count(distinct product_key) measure from gold.dim_products
UNION ALL
select 'Total Customers' Measure, count(customer_key) measure from gold.dim_customers
UNION ALL
select 'Customers Placed Orders' Measure, count(distinct customer_key) measure from gold.fact_sales;


-- ========================================================
-- MAGNITUDE ANALYSIS
-- ========================================================
-- Compare the measure values by categries.
-- It helps us understand the importance of different categories.
-- Common syntax use case: aggregation: [measure] GROOUP BY [dimension].

-- Find total customers by countries
select 
    country, 
    count(customer_key) total_cust
from gold.dim_customers 
group by country 
order by total_cust desc;

-- Find total customers by gender
select 
    gender, 
    count(customer_key) total_cust
from gold.dim_customers 
group by gender 
order by total_cust desc;

-- Find total products by category
select
    category,
    count(product_key) Total_Products
from gold.dim_products
group by category
order by Total_Products desc;

-- What is the average costs in each category?
select
    category,
    AVG(cost) avg_cost
from gold.dim_products
group by category
order by avg_cost desc;

-- What is the total revenue generated for each category?
select
    dp.category,
    sum(fs.sales_amount) total_revenue
from gold.fact_sales fs 
LEFT JOIN gold.dim_products dp ON dp.product_key = fs.product_key
group by dp.category
order by total_revenue desc;

--Find total revenue is generated by each customer
select
    dc.customer_id,
    dc.first_name +' '+ dc.last_name cust_name,
    sum(fs.sales_amount) total_revenue
from gold.fact_sales fs 
LEFT JOIN gold.dim_customers dc ON dc.customer_key = fs.customer_key
group by  dc.customer_id, dc.first_name +' '+ dc.last_name
order by total_revenue desc;


-- What is the distribution of sold items across countries?
select
    dc.country,
    sum(fs.quantity) sold_items
from gold.fact_sales fs 
LEFT JOIN gold.dim_customers dc ON dc.customer_key = fs.customer_key
group by dc.country 
order by sold_items desc;

-- ========================================================
-- RANKING ANALYSIS
-- ========================================================
-- Order the values of dimensions by measure.
-- Identifying top N performers | Bottom N Performers
-- Common syntax use case:- ranking: [measure] GROOUP BY [dimension].

-- which 5 products generate the highest revenue?
SELECT TOP 5
p.product_name,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;
---------------using window ranking function-------------------
SELECT *
FROM (
SELECT
p.product_name,
SUM(f.sales_amount) total_revenue,
ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name)t
WHERE rank_products <= 5

-- Find top 3 customers with the fewest orders placed
SELECT TOP 3
c.customer_key,
c.first_name,
c.last_name,
COUNT (DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_orders