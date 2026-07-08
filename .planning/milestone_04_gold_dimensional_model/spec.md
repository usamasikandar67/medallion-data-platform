# Milestone 4 Spec: Gold Dimensional Model (Star Schema)

## Objective
Design and implement the Gold layer analytical tables representing a Kimball Star Schema. Build the dimension tables (e.g. `dim_customers`) utilizing the historical records from Silver, and order facts (e.g. `fact_orders`) mapped to the correct surrogate key versions of dimensions corresponding to the transaction time.

---

## Business Problem
Analysts, business intelligence dashboards, and reporting applications require fast, structured query surfaces. Querying the Silver tables directly is computationally expensive and complex because analysts would have to manually join transaction dates to SCD Type 2 time ranges (`valid_from` to `valid_to`). The Gold layer pre-calculates these relationships, producing denormalized star-schema dimension and fact tables, allowing users to query data using simple, high-performance joins.

---

## Functional Requirements
1. **Dimension Design (`dim_customers`)**:
   - Extract records from `silver_customers`.
   - Expose the surrogate key `customer_sk` as the primary join key.
   - Maintain historical attributes (`first_name`, `last_name`, `email`, `state`).
   - Retain `valid_from`, `valid_to`, and `is_current` columns to allow historical context.
   - Add a default "unknown" row (e.g., surrogate key = `-1` or `MD5('unknown')`) to handle orders associated with deleted or missing customer accounts.
2. **Fact Design (`fact_orders`)**:
   - Extract records from `silver_orders`.
   - Resolve the correct `customer_sk` by performing a historical join: join `silver_orders.customer_id = silver_customers.customer_id` and match the transaction date (`silver_orders.created_at`) to the SCD Type 2 range (`silver_orders.created_at BETWEEN silver_customers.valid_from AND silver_customers.valid_to`).
   - Fall back to the "unknown" customer surrogate key if no matching customer range is found.
   - Store metrics: `order_amount` and flags for status changes.
3. **Date Dimension (`dim_date`)**:
   - Generate a static Date Dimension table containing fields: `date_key` (e.g. YYYYMMDD), `calendar_date`, `day_of_week`, `month`, `quarter`, `year`.

---

## Non-functional Requirements
- **Query Latency**: Star schema queries joining fact and dimensions should return results in < 500ms for typical aggregate queries.
- **Data Consistency**: Ensure referential integrity between fact table dimension keys and dimension table primary keys (no orphaned orders).
- **Storage Optimization**: Columnar representation (Parquet) or indexing (SQL indexes) on all dimension keys in the fact table.

---

## Inputs
- **Silver Tables**:
  - `silver_customers` (with SCD Type 2 tracking columns).
  - `silver_orders` (cleaned orders history).

---

## Outputs
- **Gold Tables**:
  - `dim_customers`
  - `dim_date`
  - `fact_orders`

---

## Architecture Decisions
- **Surrogate Key Mapping (Historical Alignment)**: To enforce Kimball's "point-in-time" correctness, the fact table must link to the specific dimension surrogate key version active when the event occurred, rather than pointing to the natural key or the current active version.
- **Table vs. View**: Dimensions will be loaded as tables to optimize read speed, while complex business transformations that update frequently can be exposed as thin SQL Views on top of the Gold tables.

---

## Dependencies
- Language runtimes: Python / SQL.
- Storage engine: SQL Database (SQLite / PostgreSQL) or Delta Lake.

---

## Acceptance Criteria
- [ ] `dim_customers` includes all historical versions of customers with their specific surrogate keys.
- [ ] `fact_orders` successfully maps each order to the correct historical `customer_sk` based on the order's creation timestamp.
- [ ] Joining `fact_orders` to `dim_customers` using `customer_sk` yields correct customer states at the time of order creation.
- [ ] Orders with invalid customer IDs default to the "Unknown" customer dimension row without crashing the execution.
- [ ] static `dim_date` is populated with records spanning the required business date range.

---

## Risks
- **Dimension Processing Lag**: If an order arrives but the customer's creation event has not yet been processed (late-arriving dimension), the order will link to the "Unknown" surrogate key.
  - *Mitigation*: Build a reconciliation run that re-processes facts matching "Unknown" keys once late-arriving dimensions are loaded.

---

## Deliverables
1. Table creation DDL scripts (`scripts/create_gold_tables.sql`).
2. Transformation and loading script (`scripts/load_gold.py`).

---

## Future Improvements
- Implement out-of-the-box late-arriving dimension handlers that automatically insert placeholders in the dimension table when a new fact key is detected before the dimension arrives.
