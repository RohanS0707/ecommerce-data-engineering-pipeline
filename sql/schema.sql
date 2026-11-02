-- PostgreSQL star schema. Spark JDBC overwrite mode creates/replaces these tables on each run.
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT,
  signup_date DATE, city TEXT, state TEXT, country TEXT
);
CREATE TABLE IF NOT EXISTS dim_product (
  product_id TEXT PRIMARY KEY, product_name TEXT, category TEXT, unit_price DOUBLE PRECISION, cost DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS dim_date (
  order_date DATE, date_key INTEGER PRIMARY KEY, year INTEGER, quarter INTEGER,
  month INTEGER, month_name TEXT, day INTEGER, day_of_week TEXT
);
CREATE TABLE IF NOT EXISTS dim_location (
  location_key TEXT PRIMARY KEY, city TEXT, state TEXT, country TEXT
);
CREATE TABLE IF NOT EXISTS fact_orders (
  order_id TEXT PRIMARY KEY, customer_id TEXT, product_id TEXT, date_key INTEGER,
  location_key TEXT, quantity INTEGER, unit_price DOUBLE PRECISION, discount DOUBLE PRECISION,
  gross_revenue DOUBLE PRECISION, net_revenue DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS fact_payments (
  payment_id TEXT PRIMARY KEY, order_id TEXT, date DATE, amount DOUBLE PRECISION,
  method TEXT, status TEXT, paid_amount DOUBLE PRECISION, payment_count BIGINT
);
CREATE INDEX IF NOT EXISTS ix_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_orders_date ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_orders_product ON fact_orders(product_id);
CREATE INDEX IF NOT EXISTS ix_fact_payments_order ON fact_payments(order_id);

