#!/usr/bin/env bash
# scripts/deploy.sh
# ─────────────────────────────────────────────────────────────
# One-shot deploy script for the Battery14 ETL pipeline.
# Uploads Glue scripts + data files to S3, then runs sam deploy.
#
# Usage:
#   ./scripts/deploy.sh [--env dev|staging|prod] [--region us-east-1]
# ─────────────────────────────────────────────────────────────

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────
ENVIRONMENT="${ENVIRONMENT:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="battery14-etl-${ENVIRONMENT}"

SOURCE_BUCKET="battery14-source-data-${ENVIRONMENT}"
SCRIPTS_BUCKET="battery14-glue-scripts-${ENVIRONMENT}"

# ── Parse args ────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)      ENVIRONMENT="$2"; shift 2 ;;
    --region)   AWS_REGION="$2";  shift 2 ;;
    *)          echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "═══════════════════════════════════════════════"
echo " Battery14 ETL Deploy"
echo " Environment : ${ENVIRONMENT}"
echo " Region      : ${AWS_REGION}"
echo " Stack       : ${STACK_NAME}"
echo "═══════════════════════════════════════════════"

# ── 1. Build SAM application ──────────────────────────────────
echo "[1/5] Building SAM application ..."
sam build --template template.yaml

# ── 2. Deploy SAM stack ───────────────────────────────────────
echo "[2/5] Deploying SAM stack ..."
sam deploy \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    Environment="${ENVIRONMENT}" \
    SourceBucketName="battery14-source-data" \
    DestinationBucketName="battery14-processed-data" \
    GlueScriptsBucketName="battery14-glue-scripts" \
  --resolve-s3 \
  --no-confirm-changeset

# ── 3. Upload Glue scripts ────────────────────────────────────
echo "[3/5] Uploading Glue job scripts to s3://${SCRIPTS_BUCKET}/scripts/ ..."
aws s3 cp glue/etl_job/etl_job.py         "s3://${SCRIPTS_BUCKET}/scripts/etl_job.py"     --region "${AWS_REGION}"
aws s3 cp glue/iceberg_job/iceberg_job.py "s3://${SCRIPTS_BUCKET}/scripts/iceberg_job.py" --region "${AWS_REGION}"

# ── 4. Upload raw data files ──────────────────────────────────
echo "[4/5] Uploading raw data files to s3://${SOURCE_BUCKET}/raw/ ..."
aws s3 cp scripts/degrees.csv "s3://${SOURCE_BUCKET}/raw/degrees.csv" --region "${AWS_REGION}"

# Upload battery14_df.csv if present locally
if [[ -f "scripts/battery14_df.csv" ]]; then
  aws s3 cp scripts/battery14_df.csv "s3://${SOURCE_BUCKET}/raw/battery14_df.csv" --region "${AWS_REGION}"
else
  echo "  ⚠  scripts/battery14_df.csv not found — upload it manually:"
  echo "     aws s3 cp <your-file> s3://${SOURCE_BUCKET}/raw/battery14_df.csv"
fi

# ── 5. Done ───────────────────────────────────────────────────
echo "[5/5] Deploy complete!"
echo ""
echo "Next steps:"
echo "  • Verify the EventBridge schedule in the AWS Console."
echo "  • Trigger a test execution:"
echo "      aws stepfunctions start-execution \\"
echo "        --state-machine-arn \$(aws cloudformation describe-stacks \\"
echo "          --stack-name ${STACK_NAME} --query 'Stacks[0].Outputs[?OutputKey==\`StateMachineArn\`].OutputValue' --output text) \\"
echo "        --input '{\"etl_job_name\":\"battery14-etl-job-${ENVIRONMENT}\",\"iceberg_job_name\":\"battery14-iceberg-job-${ENVIRONMENT}\",\"source_bucket\":\"${SOURCE_BUCKET}\",\"destination_bucket\":\"battery14-processed-data-${ENVIRONMENT}\",\"battery_file_key\":\"raw/battery14_df.csv\",\"degrees_file_key\":\"raw/degrees.csv\",\"output_prefix\":\"processed/battery14_filtered\",\"glue_database\":\"battery14_catalog\",\"iceberg_table\":\"battery14_filtered\"}'"
echo ""
echo "  • Query the Iceberg table in Athena:"
echo "      SELECT * FROM battery14_catalog.battery14_filtered LIMIT 10;"
