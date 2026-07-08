# Goal
Initialize a dbt project directory, migrate existing SQL and python transformations into dbt models, write documentation, and configure schemas and custom SQL tests to enforce data quality constraints.

# Implementation Steps
1. Initialize the dbt project: Run `dbt init transformations --skip-profile-setup`.
2. Configure `transformations/dbt_project.yml` with project variables and model structure configurations (materializations).
3. Create `transformations/profiles.yml` setting up local database paths using environment variables:
   ```yaml
   transformations:
     outputs:
       dev:
         type: sqlite
         threads: 1
         database: 'warehouse'
         schema: 'main'
         schemas_and_databases:
           main: "data/warehouse.db"
     target: dev
   ```
4. Define sources in `transformations/models/sources.yml` referencing Bronze directories/tables.
5. Create dbt models in `transformations/models/staging/`:
   - `stg_customers.sql`: Reads customer events from Bronze.
   - `stg_orders.sql`: Reads order events from Bronze.
6. Create dbt models in `transformations/models/marts/`:
   - `silver_customers.sql`: Implements conformed customer table using incremental merge logic.
   - `silver_orders.sql`: Implements conformed orders table.
   - `dim_customers.sql`: Generates customer analytical dimensions.
   - `fact_orders.sql`: Generates transactional facts resolving keys.
7. Configure `transformations/models/schema.yml` to define all model columns and test suites:
   - Unique/not_null assertions for primary keys.
   - Values validation assertions.
   - Reference integrity validations.
8. Create a custom test in `transformations/tests/assert_order_amount_non_negative.sql` validating order values.

# Validation Checklist
- [ ] Run `dbt debug` from inside `transformations/` and confirm successful connection.
- [ ] Run `dbt build` (which compiles, runs, and tests the models) and verify all steps complete successfully.
- [ ] Verify that model assets (`silver_customers`, `dim_customers`, `fact_orders`) are created/updated in `data/warehouse.db`.
- [ ] Artificially insert a record with a negative order amount, run `dbt test`, and confirm that the test `assert_order_amount_non_negative` fails.
- [ ] Run `dbt docs generate` and confirm that docs and lineage graphs generate without errors.

# Rollback Plan
- Delete the `transformations/` folder.
- Revert modifications to profile environments and local database schemas.
- Clean database elements built by dbt.

# Expected Git Commit Message
feat(dbt): integrate dbt models and configure data quality tests
