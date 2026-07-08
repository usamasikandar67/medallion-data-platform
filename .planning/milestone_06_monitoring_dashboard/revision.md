# Revision Log: Milestone 6 (Monitoring & Dashboard)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial spec layout for run logging, alerting, and analytical dashboards. |

---

## Design Decisions
- **Decision 1 (Log Structure)**: Chose to log metrics at both the job level (start/stop) and partition level (number of rows written per date partition) to allow administrators to trace skew in batch sizes.
- **Decision 2 (Dashboard Format)**: To maintain a simple local developer environment, the dashboard will render a static, responsive HTML file (`index.html`) displaying interactive Chart.js graphs populated from the SQLite analytical views via a Python-generated JSON file.

---

## Open Questions
- **Q1**: How do we monitor late-arriving dimension frequency?
  - *Current Resolution*: Build a view `vw_unresolved_orders` that counts rows in `fact_orders` where `customer_sk = -1`. The dashboard script will display a warning if this metric exceeds 5% of total orders.

---

## Review Notes
- **Browser Compatibility**: Ensure the HTML dashboard is responsive and runs on standard browsers without requiring an active web server (opening direct file URI).
