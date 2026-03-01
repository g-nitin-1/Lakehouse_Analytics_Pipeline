WITH first_orders AS (
  SELECT customer_id, DATE_TRUNC('month', MIN(order_ts)) AS cohort_month
  FROM fact_orders
  GROUP BY 1
),
activity AS (
  SELECT
    f.customer_id,
    fo.cohort_month,
    DATE_TRUNC('month', f.order_ts) AS active_month
  FROM fact_orders f
  JOIN first_orders fo USING(customer_id)
),
retention AS (
  SELECT
    cohort_month,
    DATE_DIFF('month', cohort_month, active_month) AS month_number,
    COUNT(DISTINCT customer_id) AS active_customers
  FROM activity
  GROUP BY 1, 2
),
cohort_size AS (
  SELECT cohort_month, active_customers AS cohort_size
  FROM retention
  WHERE month_number = 0
)
SELECT
  r.cohort_month,
  r.month_number,
  r.active_customers,
  cs.cohort_size,
  ROUND(r.active_customers::DOUBLE / NULLIF(cs.cohort_size, 0), 4) AS retention_rate
FROM retention r
JOIN cohort_size cs USING(cohort_month)
ORDER BY 1, 2;
