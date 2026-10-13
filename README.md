# Olist E-Commerce Analytics — Databricks Lakehouse Pipeline

A production-grade, end-to-end data engineering and analytics project built on the **Databricks Lakehouse** platform. Raw Olist Brazilian e-commerce data flows through a medallion architecture (Bronze → Silver → Gold) into an AI/BI dashboard for business intelligence.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Architecture Overview](#architecture-overview)
3. [Unity Catalog Layout](#unity-catalog-layout)
4. [Layer 1 — Ingestion (Raw → Bronze)](#layer-1--ingestion-raw--bronze)
5. [Layer 2 — Silver Pipeline (Bronze → Silver)](#layer-2--silver-pipeline-bronze--silver)
6. [Layer 3 — Gold Pipeline (Silver → Gold)](#layer-3--gold-pipeline-silver--gold)
7. [Dashboard (Gold → BI)](#dashboard-gold--bi)
8. [Data Quality & Quarantine](#data-quality--quarantine)
9. [Performance Optimizations](#performance-optimizations)
10. [Idempotency & Re-runnability](#idempotency--re-runnability)
11. [How to Run](#how-to-run)
12. [Future Enhancements](#future-enhancements)

---

## Project Structure

```
databricks_ecommerce/
├── ingestion/
│   └── ingestion                  # Auto Loader notebook: CSV → Bronze Delta tables
├── silver/
│   └── silver_pipeline            # Bronze → Silver: clean, validate, MERGE upsert
├── gold/
│   └── gold_pipeline              # Silver → Gold: dimensional model + aggregates
├── E-Commerce Sales Analytics     # AI/BI Lakeview dashboard (2 pages)
├── README.md
└── .git/                          # Version controlled in Databricks Git folders
```

**Data flow:**

```
CSV Files (UC Volume)  →  Auto Loader  →  Bronze (raw)  →  Silver (clean)  →  Gold (analytics)  →  Dashboard
         /Volumes/ecommerce_data/raw/raw_files/          ecommerce_data.bronze   ecommerce_data.silver   ecommerce_data.gold
```

---

## Architecture Overview

The project follows the **Databricks medallion architecture** with three layers of increasing refinement:

| Layer | Schema | Purpose | Write Pattern |
|---|---|---|---|
| **Bronze** | `ecommerce_data.bronze` | Raw, unmodified data as ingested | Auto Loader streaming append |
| **Silver** | `ecommerce_data.silver` | Cleaned, typed, validated, deduplicated | MERGE upsert (idempotent) |
| **Gold** | `ecommerce_data.gold` | Business-ready dimensional model & aggregates | Overwrite (idempotent for static data) |

Each layer is a separate Databricks notebook that can be run independently. The pipeline is designed for **batch processing** with `trigger(availableNow=True)` on Auto Loader, meaning it processes all new files in one batch and stops.

---

## Unity Catalog Layout

All data is governed under a single Unity Catalog catalog:

```
ecommerce_data  (catalog)
├── raw           (schema)  — UC Volume for source CSV files + Auto Loader checkpoints
├── bronze        (schema)  — 9 raw Delta tables (all StringType)
├── silver        (schema)  — 9 clean Delta tables + 8 quarantine tables
└── gold          (schema)  — 5 dimensions, 2 facts, 4 aggregates (11 tables total)
```

### Bronze Tables (9)

| Table | Rows | Grain | Primary Key |
|---|---|---|---|
| `orders` | 99,441 | One row per order | `order_id` |
| `customers` | 99,441 | One row per customer-order | `customer_id` |
| `order_items` | 112,650 | One row per order item | `order_id`, `order_item_id` |
| `order_payments` | 103,886 | One row per payment | `order_id`, `payment_sequential` |
| `order_reviews` | 102,958 | One row per review | `review_id` |
| `products` | 32,951 | One row per product | `product_id` |
| `sellers` | 3,095 | One row per seller | `seller_id` |
| `geolocation` | 1,000,163 | One row per zip+lat+lng | (no single PK) |
| `category_translations` | 71 | One row per category | `product_category_name` |

All Bronze columns are `StringType` because Auto Loader infers schema in rescue mode. The `_rescued_data` column captures any fields that don't match the inferred schema.

### Silver Tables (9 clean + 8 quarantine)

Same 9 tables as Bronze, but with:
- Correct data types (timestamp, int, decimal, double)
- Standardized text (trimmed, lowercased cities, uppercased state codes)
- Deduplication on primary keys
- Data quality validation with `is_valid` flag
- Derived columns (delivery duration, date parts, total item value)
- Written via MERGE upsert for idempotent incremental processing

Plus 8 quarantine tables (`_quarantine_*`) that persist rejected records with rejection reasons.

### Gold Tables (11)

| Type | Table | Rows | Grain |
|---|---|---|---|
| Dimension | `dim_date` | 774 | One row per calendar day |
| Dimension | `dim_geolocation` | 19,015 | One row per zip code prefix |
| Dimension | `dim_customers` | 96,096 | One row per unique customer |
| Dimension | `dim_products` | 32,951 | One row per product |
| Dimension | `dim_sellers` | 3,095 | One row per seller |
| Fact | `fact_order_items` | 112,650 | One row per order item |
| Fact | `fact_orders` | 99,441 | One row per order |
| Aggregate | `agg_daily_sales` | 634 | One row per day |
| Aggregate | `agg_monthly_category_revenue` | 1,274 | One row per month × category |
| Aggregate | `agg_seller_performance` | 3,095 | One row per seller |
| Aggregate | `agg_customer_summary` | 96,096 | One row per unique customer |

---

## Layer 1 — Ingestion (Raw → Bronze)

**Notebook:** `ingestion/ingestion`

The ingestion layer uses **Databricks Auto Loader** (`cloudFiles` format) to incrementally load CSV files from a Unity Catalog volume into Bronze Delta tables.

### How It Works

1. Creates schemas: `ecommerce_data.bronze`, `silver`, `gold`, and `raw`
2. Creates a UC volume: `ecommerce_data.raw.raw_files` for source CSV storage
3. For each of the 9 source tables, runs an Auto Loader stream:
   - **Format:** CSV with header
   - **Schema inference:** `cloudFiles.schemaLocation` stores evolving schema
   - **Checkpointing:** `cloudFiles.checkpointLocation` for exactly-once processing
   - **Trigger:** `availableNow=True` — processes all pending files in one batch, then stops
   - **Output mode:** `append` — new files add rows, never overwrite
4. All columns land as `StringType` (rescue mode captures malformed fields in `_rescued_data`)

### Source Files

Raw CSV files are organized by table in the UC volume:

```
/Volumes/ecommerce_data/raw/raw_files/
├── orders/                  # olist_orders_dataset.csv
├── order_items/             # olist_order_items_dataset.csv
├── order_payments/          # olist_order_payments_dataset.csv
├── order_reviews/           # olist_order_reviews_dataset.csv
├── products/                # olist_products_dataset.csv
├── sellers/                # olist_sellers_dataset.csv
├── customers/              # olist_customers_dataset.csv
├── category_translations/  # product_category_name_translation.csv
├── geolocation/            # olist_geolocation_dataset.csv
├── _schemas/               # Auto Loader schema inference state
└── _checkpoints/           # Auto Loader checkpoint state
```

---

## Layer 2 — Silver Pipeline (Bronze → Silver)

**Notebook:** `silver/silver_pipeline`

The Silver layer transforms raw Bronze tables into clean, typed, validated, and deduplicated Delta tables. This is where data quality enforcement happens.

### Transformation Steps (per table)

1. **Load** — Read from `ecommerce_data.bronze.*`
2. **Deduplicate** — `dropDuplicates()` on primary key columns
3. **Type conversion** — Cast `StringType` columns to appropriate types:
   - Timestamps: `to_timestamp()` for date columns
   - Numbers: `cast()` to `IntegerType`, `DoubleType`, `DecimalType`
   - Text: remains `StringType` with `trim()` and `lower()`/`upper()` standardization
4. **Validation** — Apply business rules and create an `is_valid` boolean flag:
   - Required columns not null
   - Status/category values match allowed set
   - Numeric values in valid range (e.g., price >= 0)
   - `try_cast()` and `try_to_timestamp()` for graceful corruption handling
5. **Split** — Separate into `*_valid` and `*_rejected` DataFrames based on `is_valid`
6. **Derived columns** — Add computed fields like:
   - `order_purchase_date` (date extraction from timestamp)
   - `delivery_duration_days` (datediff between purchase and delivery)
   - `total_item_value` (price + freight_value)
7. **Write** — MERGE upsert into `ecommerce_data.silver.*` by primary key
8. **Quarantine** — Write rejected rows to `ecommerce_data.silver._quarantine_*` tables

### Write Strategy: MERGE Upsert

Silver uses `MERGE INTO` instead of full overwrite:

- **New records** → `WHEN NOT MATCHED THEN INSERT`
- **Updated records** → `WHEN MATCHED THEN UPDATE`
- **Duplicate keys** → eliminated by `dropDuplicates()` before MERGE
- **First run** → table created with `saveAsTable` (overwrite mode if table doesn't exist)
- **Re-runs** → only new/changed rows are upserted (idempotent)

### Validation Suite

After writing, the pipeline runs 18 data-quality checks:
- No null primary keys
- Valid status codes and state codes
- Non-negative prices and freight values
- Row count reconciliation

### Key Data Quality Issue: Corrupted Review Scores

The `order_reviews` table had 4,548 rows where Auto Loader's rescue mode mixed Portuguese comment text into the `review_score` column. The Silver pipeline handles this with `try_cast(review_score as int)` — garbage values become null, and rows with null scores are rejected and quarantined.

---

## Layer 3 — Gold Pipeline (Silver → Gold)

**Notebook:** `gold/gold_pipeline`

The Gold layer transforms Silver tables into a **star schema** dimensional model with fact tables, dimensions, and pre-aggregated summary tables optimized for dashboard queries.

### Architecture: 2 Facts + 5 Dimensions + 4 Aggregates

**Dimensions** (lookup tables):

| Table | Source | Key | Description |
|---|---|---|---|
| `dim_date` | Derived | `date` | Calendar with year, month, quarter, day-of-week |
| `dim_geolocation` | `silver.geolocation` | `geolocation_zip_code_prefix` | Aggregated from 738K rows to 19K zip codes (avg lat/lng, mode city/state) |
| `dim_customers` | `silver.customers` + `silver.orders` | `customer_unique_id` | Deduplicated from 99K order-level customers to 96K unique customers |
| `dim_products` | `silver.products` + `silver.category_translations` | `product_id` | Product attributes with English category names and computed volume |
| `dim_sellers` | `silver.sellers` | `seller_id` | Direct pass-through (already at correct grain) |

**Facts** (event tables):

| Table | Grain | Source | Key Columns |
|---|---|---|---|
| `fact_order_items` | One row per order item | `order_items` ⋈ `orders` ⋈ `customers` ⋈ `products` ⋈ `sellers` | Revenue, price, freight, delivery metrics, customer/seller/product attributes |
| `fact_orders` | One row per order | `orders` + aggregated `order_payments` + `order_reviews` | Order revenue, payment count, review score, delivery duration |

**Aggregates** (pre-computed summaries for dashboard performance):

| Table | Grain | Metrics |
|---|---|---|
| `agg_daily_sales` | Day | Revenue, freight, item count, unique products, orders, customers, reviews |
| `agg_monthly_category_revenue` | Month × category | Revenue, price, freight, item/order counts, averages |
| `agg_seller_performance` | Seller | Revenue, volume, delivery metrics, review scores |
| `agg_customer_summary` | Customer | CLV, order count, avg order value, first/last order, segment (VIP/Returning/New) |

### Write Strategy: Overwrite

Gold uses `mode("overwrite")` with `overwriteSchema=True`. Since the Olist dataset is static (no new data arrives after initial load), full overwrite is idempotent — re-running produces identical results. This is simpler than MERGE and appropriate for the data volume (under 1M rows per table).

### Validation Suite

After writing, the Gold pipeline validates:
- Row count summary for all 11 tables
- Duplicate primary key checks
- Null primary key checks
- Referential integrity (fact → dimension foreign keys)
- Revenue reconciliation (Gold fact vs. Silver source)
- Order count reconciliation
- Double-counting detection

---

## Dashboard (Gold → BI)

**Asset:** `E-Commerce Sales Analytics` (Databricks AI/BI Lakeview dashboard)

The dashboard has two pages, both powered by SQL datasets querying Gold tables:

### Executive Overview

Business-level KPIs and trends for leadership:

| Widget | Dataset | Description |
|---|---|---|
| KPI cards | `exec_kpis` | Total revenue, order count, avg order value, avg review score |
| Monthly revenue trend | `monthly_revenue` | Line chart of revenue over time |
| Monthly order volume | `monthly_volume` | Bar chart of order counts |
| Order status distribution | `order_status` | Breakdown by order status |
| Top product categories | `top_categories` | Horizontal bar chart by revenue |
| Delivery performance | `delivery_perf` | Average delivery time trend |
| Monthly detail table | `monthly_detail` | Tabular drill-down by month |

All widgets are bound to a shared date filter (`order_purchase_date`) at daily grain, with semantic relationships enabling cross-entity filter propagation.

### Product Performance

Product-level analytics for category and SKU analysis:

| Widget | Description |
|---|---|
| Product KPIs | Revenue, item count, avg price |
| Category revenue trend | Time series by category |
| Top products | Best-selling SKUs |
| Category breakdown | Revenue distribution |

### Filter Architecture

Both pages use a date range filter widget. The Executive Overview page uses entity-qualified SQL datasets with an explicit semantic relationship graph connecting all entities on `order_purchase_date`, enabling the filter to propagate to every widget (KPIs, charts, and tables) at daily granularity.

---

## Data Quality & Quarantine

### Silver Layer Validation

Each Silver transformation cell applies domain-specific validation rules:

| Table | Validation Rules |
|---|---|
| `orders` | Non-null `order_id`, valid `order_status` (8 allowed values) |
| `customers` | Non-null `customer_id`, valid 2-letter Brazil state code |
| `order_items` | Non-null `order_id` + `order_item_id`, `price >= 0`, `freight >= 0` |
| `order_payments` | Non-null keys, `payment_value >= 0`, valid payment type |
| `order_reviews` | Non-null `review_id`, `review_score` is integer 1–5 |
| `products` | Non-null `product_id`, non-negative dimensions |
| `sellers` | Non-null `seller_id`, valid state code |
| `geolocation` | Non-null zip code, valid lat/lng ranges |
| `category_translations` | Non-null category name |

### Quarantine Tables

Rejected records are persisted in quarantine tables for investigation instead of being silently dropped:

| Quarantine Table | Rows | Rejection Reason |
|---|---|---|
| `_quarantine_order_reviews` | 4,548 | `invalid_review_score_or_null_key` (corrupted by Auto Loader rescue mode) |
| `_quarantine_orders` | 0 | — |
| `_quarantine_customers` | 0 | — |
| `_quarantine_order_items` | 0 | — |
| `_quarantine_order_payments` | 0 | — |
| `_quarantine_products` | 0 | — |
| `_quarantine_sellers` | 0 | — |
| `_quarantine_geolocation` | 0 | — |

Quarantine tables include a `rejection_reason` column and use the same MERGE upsert pattern as clean tables, making them idempotent.

---

## Performance Optimizations

### Liquid Clustering on Gold Fact Tables

Both Gold fact tables use **liquid clustering** to optimize dashboard query performance:

| Table | Cluster Columns | Rationale |
|---|---|---|
| `fact_order_items` | `(order_purchase_date, product_id)` | Dashboard filters by date range and product/category |
| `fact_orders` | `(order_purchase_date)` | Dashboard filters by date range |

**Why liquid clustering over partitioning?** The dataset is small (<1M rows per table). Partitioning would create too many tiny files and hurt performance. Liquid clustering continuously reorganizes data files to skip irrelevant rows during filtered queries without the overhead of partition management.

### Auto Loader Incremental Ingestion

Auto Loader with `trigger(availableNow=True)` processes only new files since the last run, making ingestion efficient and cost-effective for batch workloads.

### Silver MERGE Upsert

Silver uses MERGE instead of full overwrite, so only new/changed rows are written on re-runs — no full table rewrites.

---

## Idempotency & Re-runnability

The entire pipeline is designed to be safely re-runnable:

| Layer | Pattern | Idempotent? | Why |
|---|---|---|---|
| Ingestion (Bronze) | Auto Loader append + checkpoint | ✅ | Checkpoint tracks processed files; re-running only picks up new files |
| Silver | MERGE upsert by primary key | ✅ | Same input produces same output; updates are idempotent by PK |
| Gold | Full overwrite with `overwriteSchema` | ✅ | Static dataset; re-running produces identical tables |
| Quarantine | MERGE upsert by primary key | ✅ | Same pattern as Silver clean tables |

---

## How to Run

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- `ecommerce_data` catalog with `USE CATALOG` permission
- Serverless compute (auto-selected) or an all-purpose cluster
- Source CSV files uploaded to `/Volumes/ecommerce_data/raw/raw_files/<table>/`

### Execution Order

Run the notebooks in this order. Each can be run independently if the upstream layer is already populated.

1. **Ingestion** — `ingestion/ingestion`
   - Creates schemas and UC volume
   - Runs Auto Loader for all 9 source tables
   - Output: 9 Bronze Delta tables

2. **Silver Pipeline** — `silver/silver_pipeline`
   - Reads Bronze tables, transforms, validates, and writes Silver + quarantine tables
   - Output: 9 clean Silver tables + 8 quarantine tables

3. **Gold Pipeline** — `gold/gold_pipeline`
   - Reads Silver tables, builds dimensional model and aggregates
   - Applies liquid clustering and OPTIMIZE
   - Output: 11 Gold tables (5 dimensions, 2 facts, 4 aggregates)

4. **Dashboard** — Open `E-Commerce Sales Analytics` in the Databricks workspace
   - All datasets query Gold tables directly
   - Use the date range filter to explore different time periods

### Re-running

- **Add new source files** → Re-run ingestion notebook only (Auto Loader picks up new files)
- **Re-process Silver** → Re-run silver_pipeline (MERGE upserts only new/changed rows)
- **Rebuild Gold** → Re-run gold_pipeline (full overwrite, ~2 minutes on serverless)
- **Full refresh** → Run all three notebooks in order

---

## Future Enhancements

| Enhancement | Priority | Description |
|---|---|---|
| Convert to Lakeflow Spark Declarative Pipelines | Medium | Replace notebooks with SDP streaming tables + expectations for automated scheduling |
| Gold MERGE upsert | Low | Switch Gold from overwrite to MERGE if the dataset becomes incremental |
| Foreign key constraints | Low | Add `ALTER TABLE ... ADD CONSTRAINT` for referential integrity enforcement |
| Dashboard metric views | Medium | Consolidate Executive Overview datasets into a single metric view for faster refresh |
| Automated job scheduling | Medium | Chain all three notebooks into a Databricks job with task dependencies |
| Data quality alerts | Low | Set up SQL alerts on quarantine table row counts to detect upstream issues |

---

## Dataset

This project uses the [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) by Olist Store, containing 100K orders from 2016–2018 across Brazilian marketplaces. The dataset includes orders, items, payments, reviews, products, sellers, customers, geolocation, and category translations — 9 tables total.
