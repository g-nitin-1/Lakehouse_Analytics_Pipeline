import duckdb


def test_kpi_daily_revenue_query_runs() -> None:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE fact_orders AS
        SELECT * FROM (
          VALUES
            ('o1', 'c1', 'p1', TIMESTAMP '2026-01-01 00:00:00', 10.0, 1, 10.0),
            ('o2', 'c2', 'p2', TIMESTAMP '2026-01-02 00:00:00', 20.0, 1, 20.0)
        ) AS t(order_id, customer_id, product_id, order_ts, amount, quantity, gross_revenue);
        """
    )
    con.execute(
        """
        CREATE TABLE dim_product AS
        SELECT * FROM (VALUES ('p1', 'a'), ('p2', 'b')) AS t(product_id, category);
        """
    )

    result = con.execute(
        """
        SELECT DATE_TRUNC('day', order_ts) AS order_day, SUM(gross_revenue) AS revenue
        FROM fact_orders
        GROUP BY 1
        ORDER BY 1;
        """
    ).fetchall()
    assert len(result) == 2
