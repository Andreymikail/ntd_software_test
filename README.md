# Battery14 ETL Pipeline

Serverless ETL pipeline built on AWS that ingests **battery14_df.csv** and a **degrees.csv** lookup, applies filtering and transformation rules, writes the result to S3 as Parquet, and registers an **Apache Iceberg** table in the Glue Data Catalog.

---

## Architecture

```
EventBridge (cron)
      │
      ▼
 Lambda (start_etl)          ← starts State Machine with runtime params
      │
      ▼
Step Functions State Machine
      │
      ├─► Glue Job 1: etl_job       (Filter → Transform → Parquet → S3)
      │
      └─► Glue Job 2: iceberg_job   (Parquet → Iceberg Table in Glue Catalog)
```

### AWS Services Used

| Service | Role |
|---|---|
| **EventBridge** | Cron schedule — triggers Lambda daily at 06:00 UTC |
| **Lambda** | Starts the Step Functions State Machine; can inject runtime params |
| **Step Functions** | Orchestrates Glue jobs sequentially with retry/error handling |
| **Glue Jobs (PySpark)** | Data processing (ETL) and Iceberg table creation |
| **S3** | Raw data storage, Glue scripts, processed Parquet, Iceberg warehouse |
| **Glue Data Catalog** | Iceberg table metadata |
| **AWS SAM** | Infrastructure-as-code — defines and deploys all resources |

---

## Repository Structure

```
etl-project/
├── template.yaml                        # SAM IaC template
├── statemachine/
│   └── etl_pipeline.asl.json           # Step Functions ASL definition
├── lambda/
│   └── start_etl/
│       ├── handler.py                  # Lambda — starts State Machine
│       └── requirements.txt
├── glue/
│   ├── etl_job/
│   │   └── etl_job.py                  # Glue PySpark ETL script
│   └── iceberg_job/
│       └── iceberg_job.py              # Glue PySpark Iceberg script
└── scripts/
    ├── degrees.csv                     # Education level lookup table
    └── deploy.sh                       # One-shot deploy helper
```

---

## Data Description

### battery14_df.csv

Survey / cognitive battery dataset. Relevant columns:

| Column | Type | Description |
|---|---|---|
| `user_id` | int | Participant ID |
| `age` | float | Age in years |
| `gender` | str | `m` / `f` |
| `education_level` | float | Numeric education code (see degrees lookup) |
| `country` | str | ISO country code |
| `raw_score` | float | Raw test score |
| `grand_index` | float | Composite index |

### degrees.csv (lookup)

| `education_level` | `description` |
|---|---|
| 1 | Some high school |
| 2 | High school diploma/GED |
| 3 | Some college |
| 4 | College degree |
| 5 | Professional degree |
| 6 | Master's degree |
| **7** | **Ph.D.** |
| 8 | Associate's degree |
| 99 | Other |

---

## ETL Logic

### Stage 1 — ETL Glue Job (`etl_job.py`)

**Filter** (all conditions must be true):

| Field | Condition |
|---|---|
| `gender` | `== 'f'` (Female) |
| `age` | `> 30` |
| `country` | `== 'US'` |
| `education_level` | `> 6` (above Master's → effectively Ph.D. only) |
| `raw_score` | `> 300` |

**Transform:**

1. Replace `'m'` → `'Male'`, `'f'` → `'Female'` in the `gender` column.
2. Left-join with `degrees.csv` on `education_level` to add `degree_description`.

**Output:** Snappy-compressed Parquet written to:
```
s3://battery14-processed-data-<env>/processed/battery14_filtered/
```

### Stage 2 — Iceberg Glue Job (`iceberg_job.py`)

Reads the Parquet output and creates/replaces an Iceberg table:

```
glue_catalog.battery14_catalog.battery14_filtered
```

Warehouse location:
```
s3://battery14-processed-data-<env>/iceberg/battery14_filtered/
```

---

## Prerequisites

- AWS CLI configured (`aws configure`)
- AWS SAM CLI installed (`pip install aws-sam-cli`)
- Python 3.12+
- Sufficient IAM permissions to create Lambda, Glue, Step Functions, S3, EventBridge, and IAM resources

---

## Deployment

### Quick deploy (dev)

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh --env dev --region us-east-1
```

### Manual step-by-step

```bash
# 1. Build
sam build

# 2. Deploy infrastructure
sam deploy \
  --stack-name battery14-etl-dev \
  --region us-east-1 \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment=dev \
  --resolve-s3

# 3. Upload Glue scripts
aws s3 cp glue/etl_job/etl_job.py         s3://battery14-glue-scripts-dev/scripts/etl_job.py
aws s3 cp glue/iceberg_job/iceberg_job.py s3://battery14-glue-scripts-dev/scripts/iceberg_job.py

# 4. Upload raw data
aws s3 cp scripts/degrees.csv       s3://battery14-source-data-dev/raw/degrees.csv
aws s3 cp <your-battery-file>.csv   s3://battery14-source-data-dev/raw/battery14_df.csv
```

---

## Testing

### Trigger the pipeline manually

```bash
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name battery14-etl-dev \
  --query 'Stacks[0].Outputs[?OutputKey==`StateMachineArn`].OutputValue' \
  --output text)

aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input '{
    "etl_job_name": "battery14-etl-job-dev",
    "iceberg_job_name": "battery14-iceberg-job-dev",
    "source_bucket": "battery14-source-data-dev",
    "destination_bucket": "battery14-processed-data-dev",
    "battery_file_key": "raw/battery14_df.csv",
    "degrees_file_key": "raw/degrees.csv",
    "output_prefix": "processed/battery14_filtered",
    "glue_database": "battery14_catalog",
    "iceberg_table": "battery14_filtered"
  }'
```

### Validate the Iceberg table

**Option A — PySpark (Glue notebook or local Spark):**

```python
spark.sql("""
    SELECT
        user_id, age, gender, degree_description,
        country, raw_score, grand_index
    FROM glue_catalog.battery14_catalog.battery14_filtered
    LIMIT 10
""").show(truncate=False)

spark.sql("""
    SELECT COUNT(*) AS total_rows
    FROM glue_catalog.battery14_catalog.battery14_filtered
""").show()
```

**Option B — AWS Athena:**

```sql
-- Count rows
SELECT COUNT(*) FROM battery14_catalog.battery14_filtered;

-- Inspect sample
SELECT user_id, age, gender, degree_description, country, raw_score
FROM battery14_catalog.battery14_filtered
LIMIT 10;

-- Verify filter correctness
SELECT DISTINCT gender, country, degree_description
FROM battery14_catalog.battery14_filtered;
```

---

## Monitoring

| Resource | Where to look |
|---|---|
| Lambda logs | CloudWatch → `/aws/lambda/battery14-start-etl-<env>` |
| State Machine executions | Step Functions Console → `battery14-etl-statemachine-<env>` |
| Glue job logs | CloudWatch → `/aws-glue/jobs/output` |
| State Machine logs | CloudWatch → `/aws/states/battery14-etl-<env>` |

---

## Tear-down

```bash
aws cloudformation delete-stack --stack-name battery14-etl-dev

# Delete S3 buckets (must be empty first or use --force)
aws s3 rb s3://battery14-source-data-dev      --force
aws s3 rb s3://battery14-processed-data-dev   --force
aws s3 rb s3://battery14-glue-scripts-dev     --force
```
