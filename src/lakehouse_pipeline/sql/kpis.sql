-- name: kpi_daily_revenue
SELECT DATE_TRUNC('day', order_ts) AS order_day, SUM(gross_revenue) AS revenue
FROM fact_orders
GROUP BY 1
ORDER BY 1;

-- name: kpi_top_categories
SELECT p.category, SUM(f.gross_revenue) AS revenue
FROM fact_orders f
JOIN dim_product p USING(product_id)
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5;

-- name: kpi_order_rank_per_customer
SELECT
  customer_id,
  order_ts,
  gross_revenue,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY gross_revenue DESC) AS rn
FROM fact_orders;

-- name: kpi_revenue_lag
SELECT
  DATE_TRUNC('day', order_ts) AS order_day,
  SUM(gross_revenue) AS revenue,
  LAG(SUM(gross_revenue)) OVER (ORDER BY DATE_TRUNC('day', order_ts)) AS prev_day_revenue
FROM fact_orders
GROUP BY 1
ORDER BY 1;

-- name: kpi_running_revenue
SELECT
  DATE_TRUNC('day', order_ts) AS order_day,
  SUM(gross_revenue) AS revenue,
  SUM(SUM(gross_revenue)) OVER (ORDER BY DATE_TRUNC('day', order_ts)) AS running_revenue
FROM fact_orders
GROUP BY 1
ORDER BY 1;

-- name: kpi_customer_ntile
SELECT
  customer_id,
  SUM(gross_revenue) AS total_revenue,
  NTILE(4) OVER (ORDER BY SUM(gross_revenue)) AS revenue_quartile
FROM fact_orders
GROUP BY 1;

-- name: kpi_avg_order_value
SELECT AVG(gross_revenue) AS avg_order_value FROM fact_orders;

-- name: kpi_orders_per_customer
SELECT customer_id, COUNT(*) AS order_count
FROM fact_orders
GROUP BY 1
ORDER BY order_count DESC;

-- name: kpi_peak_hour
SELECT EXTRACT('hour' FROM order_ts) AS order_hour, COUNT(*) AS orders
FROM fact_orders
GROUP BY 1
ORDER BY orders DESC;

-- name: kpi_top_products
SELECT product_id, SUM(gross_revenue) AS revenue
FROM fact_orders
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5;

-- name: kpi_revenue_by_week
SELECT DATE_TRUNC('week', order_ts) AS order_week, SUM(gross_revenue) AS revenue
FROM fact_orders
GROUP BY 1
ORDER BY 1;

-- name: kpi_quantity_by_category
SELECT p.category, SUM(f.quantity) AS units
FROM fact_orders f
JOIN dim_product p USING(product_id)
GROUP BY 1
ORDER BY units DESC;
