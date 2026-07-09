# Databricks Notebook: Ingestion & Medallion Pipeline Code

This document contains the production-ready code cells you can copy and paste directly into a **Databricks Notebook** (configured with Python/SQL) to run this pipeline end-to-end inside your Databricks Workspace.

---

## Cell 1: Setup Mock CDC Data in DBFS
* **Language**: Python
* **Description**: Simulates the extraction step by creating a mock JSON CDC batch file directly inside Databricks File System (DBFS).

```python
# Create DBFS directories
dbutils.fs.mkdirs("dbfs:/FileStore/cdc_buffer/")
dbutils.fs.mkdirs("dbfs:/FileStore/bronze/customers/")
dbutils.fs.mkdirs("dbfs:/FileStore/bronze/orders/")

# Write mock customer mutations JSON payload
mock_customers_json = """[
    {"_row_sequence_id": 1, "entity_type": "customers", "_change_type": "INSERT", "_commit_timestamp": "2026-07-09 10:00:00", "customer_id": "cust-001", "first_name": "Bob", "last_name": "Smith", "email": "bob.smith@example.com", "state": "IL"},
    {"_row_sequence_id": 2, "entity_type": "customers", "_change_type": "INSERT", "_commit_timestamp": "2026-07-09 10:05:00", "customer_id": "cust-002", "first_name": "Alice", "last_name": "Green", "email": "alice.g@example.com", "state": "TX"},
    {"_row_sequence_id": 3, "entity_type": "customers", "_change_type": "UPDATE_AFTER", "_commit_timestamp": "2026-07-09 12:00:00", "customer_id": "cust-001", "first_name": "Bob", "last_name": "Smith", "email": "bob.smith@example.com", "state": "CA"}
]"""

dbutils.fs.put("dbfs:/FileStore/cdc_buffer/cdc_batch_test.json", mock_customers_json, overwrite=True)
print("Successfully generated mock raw CDC data inside dbfs:/FileStore/cdc_buffer/ !")
```

---

## Cell 2: Ingest Raw Data to Bronze Delta Table
* **Language**: Python
* **Description**: Read raw JSON files dynamically and write them to a Bronze Delta table partitioned by date.

```python
from pyspark.sql.functions import current_timestamp, lit

# 1. Read JSON cdc feed from DBFS
raw_df = spark.read.json("dbfs:/FileStore/cdc_buffer/cdc_batch_test.json")

# 2. Add Bronze Ingestion metadata headers
bronze_df = raw_df.withColumn("_bronze_ingested_at", current_timestamp()) \
                  .withColumn("_bronze_batch_id", lit("databricks-batch-101"))

# 3. Write to Bronze Delta Table
(bronze_df.write
  .format("delta")
  .mode("append")
  .partitionBy("entity_type")
  .saveAsTable("raw_bronze_cdc"))

print("Bronze ingestion complete. Delta table 'raw_bronze_cdc' created and updated.")
display(spark.sql("SELECT * FROM raw_bronze_cdc"))
```

---

## Cell 3: Build Silver Conformed History (SCD Type 2)
* **Language**: SQL
* **Description**: Compile historical changes and execute a Slowly Changing Dimension (SCD Type 2) merge using ANSI Spark SQL.

```sql
-- 1. Create target Silver delta table structure
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_sk STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    state STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT,
    is_deleted INT
)
USING DELTA;

-- 2. Build history view using LEAD window function
CREATE OR REPLACE TEMPORARY VIEW v_customers_history AS
WITH history_calc AS (
    SELECT
        customer_id,
        first_name,
        SUBSTR(last_name, 1, 1) || '.' as last_name, -- PII Masking
        CASE WHEN email LIKE '%@%' THEN SUBSTR(email, 1, 2) || '***@' || SUBSTR(email, INSTR(email, '@') + 1) ELSE '***' END as email, -- PII Masking
        UPPER(TRIM(state)) as state,
        CAST(_commit_timestamp as TIMESTAMP) as valid_from,
        CAST(LEAD(_commit_timestamp, 1, '9999-12-31 23:59:59') OVER (
            PARTITION BY customer_id ORDER BY _commit_timestamp ASC, _row_sequence_id ASC
        ) as TIMESTAMP) as valid_to,
        _change_type
    FROM raw_bronze_cdc
    WHERE entity_type = 'customers' AND _change_type != 'UPDATE_BEFORE'
)
SELECT
    customer_id || '_' || DATE_FORMAT(valid_from, 'yyyyMMddHHmmss') as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    state,
    valid_from,
    valid_to,
    CASE WHEN valid_to = '9999-12-31 23:59:59' AND _change_type != 'DELETE' THEN 1 ELSE 0 END as is_current,
    CASE WHEN _change_type = 'DELETE' THEN 1 ELSE 0 END as is_deleted
FROM history_calc;

-- 3. Overwrite Silver target table with history
INSERT OVERWRITE silver_customers SELECT * FROM v_customers_history;

SELECT * FROM silver_customers ORDER BY customer_id, valid_from;
```

---

## Cell 4: Build Gold Analytical Star Schema Dimension
* **Language**: SQL
* **Description**: Build the dimension table containing standard fallback rows (`customer_sk = '-1'`) to resolve late-arriving records.

```sql
-- 1. Create target Gold Dimension
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    state STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
)
USING DELTA;

-- 2. Load active conformed dimensions
INSERT OVERWRITE dim_customers
SELECT 
    '-1' as customer_sk,
    'unknown' as customer_id,
    'Unknown' as first_name,
    'Customer' as last_name,
    'unknown@example.com' as email,
    'UNKNOWN' as state,
    CAST('1970-01-01 00:00:00' as TIMESTAMP) as valid_from,
    CAST('9999-12-31 23:59:59' as TIMESTAMP) as valid_to,
    1 as is_current
UNION ALL
SELECT 
    customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    state,
    valid_from,
    valid_to,
    is_current
FROM silver_customers
WHERE is_deleted = 0;

SELECT * FROM dim_customers;
```

---

## Cell 5: Create Database Indexes (Optimize Query Speed)
* **Language**: SQL
* **Description**: Optimizes Delta table layout. Note that Delta Lake manages database indexing automatically using **Z-Ordering** clustering instead of SQLite post-hooks!

```sql
-- Delta Lake optimizes performance by organizing files on disk by keys (Z-Ordering)
OPTIMIZE silver_customers ZORDER BY (customer_id, valid_from);
OPTIMIZE dim_customers ZORDER BY (customer_sk);
```
