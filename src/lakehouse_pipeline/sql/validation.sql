-- name: validation_duplicates_before
SELECT COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows_before
FROM bronze_orders;

-- name: validation_duplicates_after
SELECT COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows_after
FROM silver_orders;

-- name: validation_null_rates_silver
SELECT
  AVG(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS order_id_null_rate,
  AVG(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS customer_id_null_rate,
  AVG(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS product_id_null_rate,
  AVG(CASE WHEN order_ts IS NULL THEN 1 ELSE 0 END) AS order_ts_null_rate,
  AVG(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_null_rate
FROM silver_orders;

-- name: validation_referential_integrity
SELECT
  SUM(CASE WHEN dc.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customer_fk,
  SUM(CASE WHEN dp.product_id IS NULL THEN 1 ELSE 0 END) AS missing_product_fk
FROM fact_orders f
LEFT JOIN dim_customer dc ON f.customer_id = dc.customer_id
LEFT JOIN dim_product dp ON f.product_id = dp.product_id;
