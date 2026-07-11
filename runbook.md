# Runbook: Medallion Operations

## 1. Deploying the Databricks Workflow
To deploy the orchestration pipeline to Databricks:
```bash
# Ensure Databricks CLI is configured
databricks jobs create --json @orchestration/databricks_workflow.json
```

## 2. Running the Pipeline
Execute the job via the Databricks UI or CLI:
```bash
databricks jobs run-now <JOB_ID>
```

## 3. Investigating Failures
1. Check the `workspace.medallion.pipeline_audit_log` table:
   ```sql
   SELECT * FROM workspace.medallion.pipeline_audit_log WHERE status = 'FAILED' ORDER BY start_time DESC LIMIT 5;
   ```
2. Navigate to the Databricks Jobs UI and inspect the Spark UI for the specific Task (`2_Silver_Processing` or `3_Gold_Processing`).

## 4. Handling Quarantined Records
If records are sent to Quarantine during the Silver process:
1. Identify the reason:
   ```sql
   SELECT _quarantine_reason, COUNT(*) FROM workspace.silver.quarantine_customers GROUP BY 1;
   ```
2. Fix the source data in the OLTP or CDC generator.
3. The next pipeline run will automatically pull the updated, clean CDC events and `MERGE` them into the active Silver tables.
