# Data Model

## Fact
- `fact_orders(order_id, customer_id, product_id, category_sk, order_ts, order_date, amount, quantity, unit_amount, trip_duration_min, gross_revenue)`

## Dimensions
- `dim_customer(customer_id)`
- `dim_vendor(vendor_id, vendor_code)`
- `dim_product(product_id, category)`
- `dim_category(category_sk, category)`
- `dim_date(order_date, year, month, week_of_year, day_of_week)`
