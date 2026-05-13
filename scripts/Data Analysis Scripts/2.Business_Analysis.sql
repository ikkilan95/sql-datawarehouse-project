-- ========================================================
-- BUSINESS ANALYSIS
-- ========================================================
-- 1. Change Over Time (Trend)
-- 2. Cumulative Analysis
-- 3. Performance Analysis
-- 4. Part To Whole
-- 5. Data Segmentation
-- 6. Reporting
-- ========================================================

-- ========================================================
-- 1. Change Over Time (Trend)
-- ========================================================
-- Analyze how a measure evolves over time.
-- Helps track trends and identify seasonality in your data.

-- Analyse Sales Performance over year.
SELECT
YEAR (order_date) as order_year,
SUM(sales_amount) as total_Sales,
COUNT (DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

-- ========================================================
-- 2. Running Total
-- ========================================================
-- Aggregate the data progressively over time.
-- Helps to understand whether our business is growing or declining.
-- Common syntax use case: window function - [Cumulative measure] by [Date dimension]

--  Running Total Sales By Month partition by year:

select
    date,
    total_sales,
    sum(total_sales) over ( order by date) running_total
FROM 
(
    select
        DATETRUNC(month, order_date) date,
        sum(sales_amount) total_sales
    from gold.fact_sales
    where order_date is not null
    group by DATETRUNC(month, order_date)
)t;
GO

--  Moving Average of Sales By Month:
-- select * from gold.fact_sales;
SELECT
    month,
    avg_price,
    avg(avg_price) over (order by month) moving_average
FROM 
(
    SELECT
        DATETRUNC(month, order_date) month,
        avg(price)  avg_price
    FROM gold.fact_sales
    where order_date is not null
    group by DATETRUNC(month, order_date)
)t;
GO

-- ========================================================
-- 3. Performance Analysis
-- ========================================================
-- Comparing the current value to a target value.
-- Helps measure success and compare performance.
-- common syntax use case: window function - current[measure] - target[measure]

-- Analyze the yearly performance of products by comparing each product's sales to both its average sales performance and the previous year's sales. (Year-Over-Year Analysis)
WITH yearly_product_sales AS
(
    SELECT
        year(f.order_date) order_year,
        p.product_name,
        sum(f.sales_amount) current_sales
    from gold.fact_sales f
    LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
    where f.order_date is not null
    group by year(f.order_date), p.product_name
)
select
    order_year,
    product_name,
    current_sales,
    avg(current_sales) over(PARTITION BY product_name) avg_sales_year,
    current_sales - avg(current_sales) over(PARTITION BY product_name) difference_avg,
    lag(current_sales) OVER (partition by product_name order by order_year) prev_year_sales,
    current_sales - lag(current_sales) OVER (partition by product_name order by order_year) prev_year_diff
from yearly_product_sales
order by product_name, order_year

-- ========================================================
-- 4. Part To Whole Analysis
-- ========================================================
-- Analyze how an individual part is performing compared to the overall, allowing us to understand which category has the greatest impact on the business.
-- common syntax use case:  [measure] / total[measure] * 100 by dimension

-- which categories contribute the most overall sales?
-- select top 3 * from gold.fact_sales;
-- select top 3 * from gold.dim_products;
with category_sales as 
(
    select
        p.category prod_category,
        sum(f.sales_amount) total_sales
    from gold.fact_sales f
    left join gold.dim_products p on p.product_key = f.product_key
    group by p.category
)
select
    prod_category,
    total_sales,
    round((cast(total_sales as decimal) / sum(total_sales) over())*100,2) percent_contribution
from category_sales
order by percent_contribution desc

-- ========================================================
-- 5. Data Segmentation
-- ========================================================
-- Group the data based on a specific range.
-- Helps understand the correlation between two measures.
-- common syntax use case: case when - [measure] by [measure]

-- segment products into cost ranges and count how many products fall into each segment
-- select * from gold.dim_products;
with cte as
(
    select 
        product_key,
        product_name,
        cost,
        case
            when cost < 100 then 'Below 100'
            when cost between 100 and 500 then '100-500'
            when cost between 500 and 1000 then '500-1000'
            else 'Above 1000'
        end cost_range
    from gold.dim_products
)
SELECT
cost_range,
count(product_key) total_products
from cte
group by cost_range
order by total_products desc

/*
Group customers into three segments based on their spending behavior:
VIP: Customers with at least 12 months of history and spending more than €5,000.
Regular: Customers with at least 12 months of history but spending €5,000 or less.
New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

--select top 3 * from gold.fact_sales
--select top 3 * from gold.dim_customers

with cust_spending as
(
    select
        f.customer_key,
        sum(f.sales_amount) total_spending,
        min(f.order_date) earliest_order,
        max(f.order_date) latest_order,
        DATEDIFF(month, min(f.order_date), max(f.order_date)) lifespan
    from gold.fact_sales f
    LEFT JOIN gold.dim_customers c on f.customer_key = c.customer_key
    group by f.customer_key
)

SELECT
    cust_segment,
    count(customer_key) total_cust
FROM
(
    SELECT
        customer_key,
        case 
            when lifespan >= 12 and total_spending > 5000 then 'VIP'
            when lifespan >= 12 and total_spending <= 5000 then 'Regular'
            else 'New'
        end cust_segment
    from cust_spending
)t
group by cust_segment



