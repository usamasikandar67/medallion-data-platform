# Milestone 1 Spec: Operational Database Simulation & Change Data Capture (CDC)

## Objective
Establish a reliable source transactional (OLTP) database simulator and configure Change Data Capture (CDC) using either Delta Change Data Feed (CDF) or Snowflake Streams. This setup will serve as the source engine that continuously generates realistic events (inserts, updates, deletes) to downstream medallion ingestion layers.

---

## Business Problem
In order to build and test an enterprise-grade data platform without depending on expensive or restricted production resources, the development team requires a simulated source that mimics real-world database mutations. Additionally, standard batch ingestion patterns lose the granular transactional history (e.g., how many times an order status changed). Real-time or near-real-time CDC is required to capture every state change, enabling accurate historical analysis, auditability, and regulatory compliance.

---

## Functional Requirements
1. **Simulation Workload Engine**:
   - Programmatically spin up a source relational database instance (e.g., SQLite or PostgreSQL).
   - Generate structured transactions modeling a business domain (e.g., E-commerce orders, customer updates, inventory changes).
   - Randomly apply `INSERT` (new orders/customers), `UPDATE` (order status changes, customer address revisions), and `DELETE` (cart removal, membership cancellations) mutations.
2. **CDC Capture Execution**:
   - Extract raw database modifications containing both state data and transactional metadata.
   - For Delta Lake: Enable Delta Change Data Feed (CDF) property on source-side table targets.
   - For Snowflake: Create source tables and bind Snowflake Streams to capture DML changes.
   - Every captured transaction record must include:
     - All user data fields.
     - `_change_type`: `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, or `DELETE`.
     - `_commit_timestamp`: DateTime of transactional commit in source.
     - `_row_sequence_id`: Incrementing integer/UUID to uniquely order updates within a transaction.

---

## Non-functional Requirements
- **Frequency**: Simulator runs at configurable intervals (e.g., every 5 seconds or continuous loop) to mimic continuous transactional traffic.
- **Reliability**: Simulator must handle errors gracefully (e.g., DB locks) and auto-reconnect without crashing.
- **Portability**: Code must be runnable locally (Docker container or direct python scripts) without complex installation.
- **Determinism**: Workload simulator should support seed-based randomization to reproduce identical sequences of events during tests.

---

## Inputs
- **Entity Schemas**:
  - `customers`: `customer_id` (PK), `first_name`, `last_name`, `email`, `state`, `created_at`, `updated_at`.
  - `orders`: `order_id` (PK), `customer_id` (FK), `order_amount`, `order_status` (Pending, Shipped, Delivered, Cancelled), `created_at`, `updated_at`.

---

## Outputs
- **CDC Payloads**: Raw tabular streams containing modified entity rows paired with CDC headers.
- **Simulation Log**: A local transaction log tracking the number of records mutated during each simulator cycle.

---

## Architecture Decisions
- **Source Database Selection**: SQLite is selected for local developer ease-of-use and lightweight footprint, but the code must be structured using standard SQL dialects to easily swap in PostgreSQL.
- **CDC Mechanism**: File-based simulated CDC (writing events to a JSON/CSV buffer mimicking Kafka Connect or direct stream outputs) to simplify local workspace execution. For cloud deployment, this translates directly to Spark Structured Streaming reading Delta CDF or Snowflake Task reading Snowflake Streams.

---

## Dependencies
- Language runtimes: Node.js or Python.
- Target library: Database connector driver (e.g., sqlite3 or psycopg2).
- Data formatting: JSON/CSV libraries.

---

## Acceptance Criteria
- [ ] Running the simulator generates continuous database mutations in the source tables.
- [ ] Every mutation is immediately captured by the CDC simulator and written to the output change feed directory.
- [ ] Output feed records possess standard CDC columns (`_change_type`, `_commit_timestamp`, `_row_sequence_id`).
- [ ] The simulation logs reflect the actions taken (e.g., "Inserted 5 orders, updated 2 customer addresses").

---

## Risks
- **Concurrency Locks**: High frequency simulator updates on SQLite might trigger database locked errors (`SQLITE_BUSY`).
  - *Mitigation*: Adjust write connection timeouts or use SQLite WAL (Write-Ahead Logging) mode.
- **Out of Order Delivery**: In distributed environments, change events might arrive out of sequence.
  - *Mitigation*: The inclusion of `_commit_timestamp` and `_row_sequence_id` is mandatory to guarantee downstream deterministic ordering.

---

## Deliverables
1. Transaction Simulator Code (Python/Node.js script).
2. CDC Extractor Script (capturing changes and outputting them to a change-feed directory).
3. Configuration parameters (`.env.example`).

---

## Future Improvements
- Integrate Kafka or AWS Kinesis to model a real-time streaming queue instead of file-buffered micro-batches.
- Support schema mutation (DDL changes) and test auto-migration of CDC payloads.
