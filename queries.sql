

-- Top 5 customers by total spend
SELECT c.customer_id, c.name, SUM(f.total_amount) AS total_spend
FROM 'fact_orders.csv' AS f
JOIN 'dim_customers.csv' AS c USING (customer_id)
GROUP BY c.customer_id, c.name
ORDER BY total_spend DESC
LIMIT 5;

-- Monthly revenue trend
SELECT d.year, d.month, SUM(f.total_amount) AS revenue
FROM 'fact_orders.csv' AS f
JOIN 'dim_date.csv' AS d USING (date_key)
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Best-selling product category (by revenue)
SELECT p.category, SUM(f.total_amount) AS revenue
FROM 'fact_orders.csv' AS f
JOIN 'dim_products.csv' AS p USING (product_id)
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 5;

-- Revenue by city
SELECT c.city, SUM(f.total_amount) AS revenue
FROM 'fact_orders.csv' AS f
JOIN 'dim_customers.csv' AS c USING (customer_id)
GROUP BY c.city
ORDER BY revenue DESC;
