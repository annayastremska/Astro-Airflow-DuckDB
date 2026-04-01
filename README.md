# 📞 Hourly Support Call Enrichment Pipeline

An Apache Airflow pipeline that runs every hour, incrementally enriching support call logs with telephony metadata and LLM-generated summaries — landing clean, analytics-ready data into DuckDB.

---

## Overview

Support teams need near-real-time visibility into call quality and context. This pipeline bridges raw MySQL call logs with a mock telephony API (JSON files) to produce a fully enriched analytical table for reporting, QA, and monitoring.

```
MySQL (calls + employees)
        │
        ▼
[ detect_new_calls ]  ──── watermark tracking (DuckDB or Airflow Variable)
        │
        ▼
[ load_telephony_details ]  ──── JSON mock API (per call_id)
        │
        ▼
[ transform_and_load_duckdb ]  ──── join + upsert → support_call_enriched
```

---

## Architecture

### Sources

| Source | Type | Description |
|---|---|---|
| MySQL | Database | `employees` and `calls` tables (~50 employees, growing call records) |
| Telephony JSON | Mock API | One JSON file per `call_id` with duration and LLM-style summary |

### Output

| Target | Table | Description |
|---|---|---|
| DuckDB | `support_call_enriched` | Joined, validated, deduplicated enriched calls |

---

## DAG Structure

### `detect_new_calls`
- Queries MySQL for calls where `call_time > last_loaded_call_time`
- Reads/writes the watermark from DuckDB metadata table or Airflow Variable
- Pushes a list of new `call_id`s via **XCom**

### `load_telephony_details`
- For each new `call_id`, reads the corresponding JSON file
- Validates required fields: `call_id`, `duration_sec`, `short_description`
- Handles missing or malformed JSON gracefully (logs and skips)
- Pushes parsed records via **XCom** or staging file

### `transform_and_load_duckdb`
- Joins `calls`, `employees`, and telephony records
- Upserts into `support_call_enriched` using `call_id` as primary key
- Updates the watermark **only after a successful load**

---

## Data Schema

### MySQL — `employees`
| Field | Type | Notes |
|---|---|---|
| `employee_id` | INT | Primary key |
| `full_name` | VARCHAR | |
| `team` | VARCHAR | |
| `role` | VARCHAR | |
| `hire_date` | DATE | |

### MySQL — `calls`
| Field | Type | Notes |
|---|---|---|
| `call_id` | INT | Primary key |
| `employee_id` | INT | Foreign key → `employees` |
| `call_time` | DATETIME | Used for watermark filtering |
| `phone` | VARCHAR | |
| `direction` | ENUM | `inbound` / `outbound` |
| `status` | VARCHAR | e.g. `completed`, `missed` |

### Telephony JSON (per `call_id`)
```json
{
  "call_id": 42,
  "duration_sec": 183,
  "short_description": "Customer reported login issue; agent guided through password reset successfully."
}
```

### DuckDB — `support_call_enriched`
Merged result of all three sources, keyed on `call_id`. Includes all fields from `calls`, `employees`, and the telephony JSON payload.

---

## Setup

### Prerequisites
- Apache Airflow 2.x
- MySQL (with the support call centre database)
- DuckDB
- Python dependencies: `mysql-connector-python`, `duckdb`, `apache-airflow`

### Airflow Connection

Configure a MySQL connection in the Airflow UI (no hardcoded credentials):

```
Conn ID:   mysql_support_calls
Conn Type: MySQL
Host:      <your-mysql-host>
Schema:    <your-database>
Login:     <username>
Password:  <password>
Port:      3306
```

### Airflow Variable (optional watermark bootstrap)

```
Key:   last_loaded_call_time
Value: 2024-01-01 00:00:00
```

### Environment

```bash
# Install dependencies
pip install apache-airflow mysql-connector-python duckdb

# Place DAG file
cp dags/support_call_enrichment.py $AIRFLOW_HOME/dags/

# Place JSON mock files (one per call_id)
mkdir -p /data/telephony_mock/
# e.g. /data/telephony_mock/call_42.json
```

---

## Running the Pipeline

```bash
# Trigger manually
airflow dags trigger support_call_enrichment

# Backfill (DAG is backfill-friendly)
airflow dags backfill support_call_enrichment \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

---

## Key Design Decisions

**Idempotency** — Re-running the DAG never duplicates data. DuckDB upserts on `call_id` make every run safe to repeat.

**Watermark handling** — The high-water mark (`last_loaded_call_time`) is only advanced after a fully successful load, preventing data gaps on partial failures.

**Graceful JSON errors** — Missing or invalid telephony files are logged and skipped. Observability metrics track rejected file counts per run.

**Airflow Connections** — MySQL credentials are never hardcoded. All access goes through the Airflow Connection store.

**Hooks over raw connections** — The DAG uses `MySqlHook` and equivalent patterns rather than raw `mysql.connector` calls.

---

## Data Quality Checks

| Check | Rule |
|---|---|
| Duration validity | `duration_sec >= 0` |
| Employee integrity | `employee_id` must exist in `employees` |
| Deduplication | `call_id` uniqueness enforced at upsert |
| JSON schema | All three required fields must be present and non-null |

---

## Observability

Each run logs:
- Number of new calls detected
- Number of telephony JSON files successfully parsed
- Number of files rejected (missing / invalid)
- Rows inserted/updated in DuckDB

---

## Retry Strategy

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}
```

---

## Schedule

```python
schedule_interval = "@hourly"
```

The DAG uses `catchup=True` and a deterministic `start_date` to support safe backfills.

---

## Project Structure

```
.
├── dags/
│   └── support_call_enrichment.py   # Main DAG definition
├── scripts/
│   ├── generate_employees.py        # Seeds ~50 employee records into MySQL
│   ├── generate_calls.py            # Seeds call records (with time progression)
│   └── generate_telephony_json.py   # Generates mock JSON files per call_id
├── data/
│   └── telephony_mock/              # JSON files: call_<id>.json
├── duckdb/
│   └── support_calls.duckdb         # Output database
└── README.md
```

---

## Evaluation Checklist

| Criterion | Status |
|---|---|
| Hourly schedule + clear task structure | ✅ |
| Airflow Connection for MySQL (no hardcoded credentials) | ✅ |
| Incremental load with watermark | ✅ |
| JSON loading, validation, error handling | ✅ |
| Correct joins, no duplicates or dropped rows | ✅ |
| DuckDB upsert on `call_id` | ✅ |
| Data quality checks | ✅ |
| Observability (row counts, rejected files) | ✅ |
| Backfill-friendly (`catchup`, `start_date`) | ✅ |
| Retry + alert strategy | ✅ |
