# E-Commerce Data Engineering Pipeline

A runnable local portfolio project using Python, PySpark, PostgreSQL, Docker, and Power BI. It demonstrates a Bronze → Silver → Gold pipeline over synthetic customer, product, order, and payment data.

## What it produces

* **Bronze:** schema-applied source snapshots in Parquet.
* **Silver:** normalized dimensions, deduplicated valid orders and payments, rejected order/payment records, and a CSV quality summary.
* **Gold:** `fact_orders`, `fact_payments`, `dim_customer`, `dim_product`, `dim_date`, `dim_location`, plus daily revenue, customer metrics, and customer order history.
* **Metrics:** gross/net revenue, average order value, and customer lifetime value. Transformations demonstrate joins, aggregations, and window functions.
* **Data quality:** null/missing keys, duplicate order IDs, nonpositive prices/quantities, orphan customers/products, and orphan/rejected-order payments.
* **Spark techniques:** explicit schemas, date partitioned Parquet, repartitioning, coalescing report outputs, broadcast joins, caching, and unpersisting.

The generated fixture deliberately includes a few bad rows so the rejection outputs are visible. All names and emails are synthetic.

## Requirements

For the Docker route, install Docker Desktop with Compose enabled. For the local route, install Python 3.11 and Java 17. Native Windows also needs compatible Hadoop `winutils.exe` utilities for Spark's local filesystem; alternatively run the local route in WSL2/Linux. Power BI Desktop is optional and available on Windows.

## Quick start in VS Code (Docker)

1. Open this folder in VS Code.
2. Open the integrated terminal in the project root.
3. Run:

   ```powershell
   docker compose up --build
   ```

   Compose starts PostgreSQL, waits for its health check, builds the Spark image, creates the synthetic files if needed (already included), runs ETL, and loads the six star-schema tables. The first run may take a few minutes while Spark fetches the PostgreSQL JDBC driver.

4. When the log says `Pipeline complete`, inspect `data/bronze`, `data/silver`, and `data/gold`. PostgreSQL is available at `localhost:5432`, database `ecommerce`, user `ecommerce`, password `ecommerce_dev`.
5. Stop services with `Ctrl+C`; remove the database volume only when you want a clean database using `docker compose down -v`.

## Local Python run (without Docker)

In a VS Code PowerShell terminal from this folder:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m src.generate_data
$env:SPARK_MASTER = "local[*]"
$env:DATA_DIR = "data"
python -m src.pipeline
```

That creates Bronze/Silver/Gold Parquet and quality reports. To load PostgreSQL too, start a PostgreSQL server, set `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`, then run the pipeline. Spark downloads the JDBC driver through its package resolver. If your network blocks Maven access, use Docker Compose, which has the same requirement on first image run.

On Windows, leave `POSTGRES_HOST` unset for a local Parquet-only run so Spark skips the JDBC driver download. Native Windows still needs Hadoop's compatible `winutils.exe` helper for Parquet file operations. Follow the [Apache Hadoop Windows guidance](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems) to set `HADOOP_HOME` to a Hadoop directory containing `bin\winutils.exe`; Apache does not publish a native Windows binary distribution. A Linux environment such as WSL2 avoids this Windows-specific dependency. Docker runs on Linux and loads PostgreSQL as configured above.

## Run tests

```powershell
python -m pip install -r requirements.txt
python -m pytest
```

Tests validate that the deterministic generator creates expected files and intentionally invalid examples. They do not require Spark or PostgreSQL.

## Inspect the data and SQL

* `sql/schema.sql` documents PostgreSQL star-schema types and indexes.
* `sql/analytics.sql` has revenue/AOV, CLV, and product performance queries.
* `data/gold/dq_report/` is Spark CSV output (one or more `part-*.csv` files).
* `data/gold/rejected_orders_csv/` and `rejected_payments_csv/` contain row-level rejects and reasons.
* The dimensional Gold datasets are Parquet folders and load into PostgreSQL as tables.

## Power BI

See [docs/POWER_BI.md](docs/POWER_BI.md) for connection details, relationships, measures, and suggested visuals.

## Layout

```text
src/       schemas, fixture generator, Spark ETL
sql/       PostgreSQL schema and sample analytics
tests/     fixture and quality-data checks
data/raw/  included synthetic CSV and JSON inputs
docs/      Power BI setup guide
```

## Notes

The compose file uses demo-only credentials and is intended for a local machine. JDBC `overwrite` recreates the six loaded tables each time the pipeline runs. The generator is deterministic; run `python -m src.generate_data` to regenerate the source files. The `data/bronze`, `data/silver`, and `data/gold` output directories are ignored by Git so generated outputs do not bloat source control.

