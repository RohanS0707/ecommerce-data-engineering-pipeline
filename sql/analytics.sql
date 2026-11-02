-- Revenue and average order value by month
SELECT d.year, d.month, d.month_name, SUM(f.net_revenue) AS revenue,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.net_revenue) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS aov
FROM fact_orders f JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name ORDER BY d.year, d.month;

-- Customer lifetime value and repeat order count
SELECT c.customer_id, c.first_name, c.last_name,
       SUM(f.net_revenue) AS lifetime_value, COUNT(DISTINCT f.order_id) AS orders
FROM dim_customer c JOIN fact_orders f USING (customer_id)
GROUP BY c.customer_id, c.first_name, c.last_name ORDER BY lifetime_value DESC;

-- Product performance
SELECT p.category, p.product_name, SUM(f.quantity) AS units,
       SUM(f.net_revenue) AS revenue
FROM fact_orders f JOIN dim_product p USING (product_id)
GROUP BY p.category, p.product_name ORDER BY revenue DESC;

