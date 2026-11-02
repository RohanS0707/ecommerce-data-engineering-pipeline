# Power BI dashboard guide

## Connect

1. Start the project with `docker compose up --build` and wait for `Pipeline complete`.
2. Open Power BI Desktop → **Get data** → **PostgreSQL database**.
3. Server: `localhost:5432`; database: `ecommerce`; choose **Import** (recommended for this small demo).
4. Sign in with database `ecommerce`, username `ecommerce`, password `ecommerce_dev`.
5. Select `fact_orders`, `fact_payments`, `dim_customer`, `dim_product`, `dim_date`, and `dim_location`.

## Model

Create one-to-many, single-direction relationships from each dimension to `fact_orders`:

* `dim_customer[customer_id]` → `fact_orders[customer_id]`
* `dim_product[product_id]` → `fact_orders[product_id]`
* `dim_date[date_key]` → `fact_orders[date_key]`
* `dim_location[location_key]` → `fact_orders[location_key]`
* `fact_orders[order_id]` → `fact_payments[order_id]` (one-to-many)

Mark `dim_date` as the date table using `order_date`. Hide surrogate/key columns from report view if desired.

## Measures (DAX)

```DAX
Revenue = SUM(fact_orders[net_revenue])
Orders = DISTINCTCOUNT(fact_orders[order_id])
Average Order Value = DIVIDE([Revenue], [Orders])
Units Sold = SUM(fact_orders[quantity])
Customer Lifetime Value =
    CALCULATE([Revenue], ALLEXCEPT(dim_customer, dim_customer[customer_id]))
Paid Amount = SUM(fact_payments[amount])
```

## Suggested report page

Add KPI cards for Revenue, Orders, Average Order Value, and Units Sold. Add a line chart with `dim_date[order_date]` and Revenue; a bar chart with `dim_product[category]` and Revenue; a customer table with name, Orders, and Customer Lifetime Value; and slicers for date, category, and state. Format revenue measures as currency. The `data/gold/dq_report` and rejected CSV outputs are useful for a separate quality-monitoring page.

