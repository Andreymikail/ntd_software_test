"""
lambda/start_etl/handler.py
────────────────────────────────────────────────────────────────
Lambda triggered by EventBridge on a cron schedule.
Starts the Battery14 ETL Step Functions State Machine with the
required execution parameters.

Environment variables (injected by SAM template):
  STATE_MACHINE_ARN   – ARN of the ETL State Machine
  ENVIRONMENT         – dev | staging | prod
"""

from __future__ import annotations

import json
import logging
import os
import time

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sfn_client = boto3.client("stepfunctions")

# ── Config resolved from env ──────────────────────────────────
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

SOURCE_BUCKET = os.environ.get("SOURCE_BUCKET", f"battery14-source-data-{ENVIRONMENT}")
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET", f"battery14-processed-data-{ENVIRONMENT}")
GLUE_SCRIPTS_BUCKET = os.environ.get("GLUE_SCRIPTS_BUCKET", f"battery14-glue-scripts-{ENVIRONMENT}")


def build_execution_input() -> dict:
    """
    Build the JSON payload that the State Machine will receive as input.
    The Lambda can modify these parameters dynamically if needed
    (e.g., date-partitioned S3 paths, feature flags).
    """
    return {
        # ── Job names (resolved by SAM DefinitionSubstitutions) ──
        "etl_job_name": f"battery14-etl-job-{ENVIRONMENT}",
        "iceberg_job_name": f"battery14-iceberg-job-{ENVIRONMENT}",

        # ── S3 locations ─────────────────────────────────────────
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "battery_file_key": "raw/battery14_df.csv",
        "degrees_file_key": "raw/degrees.csv",
        "output_prefix": "processed/battery14_filtered",

        # ── Iceberg / Glue Catalog ────────────────────────────────
        "glue_database": "battery14_catalog",
        "iceberg_table": "battery14_filtered",
    }


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point — triggered by EventBridge scheduled rule.

    Args:
        event:   EventBridge scheduled event (content unused).
        context: Lambda context object.

    Returns:
        dict with executionArn and startDate.
    """
    execution_name = f"battery14-etl-{int(time.time())}"
    execution_input = build_execution_input()

    logger.info(
        "Starting ETL State Machine | name=%s | input=%s",
        execution_name,
        json.dumps(execution_input),
    )

    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(execution_input),
    )

    logger.info(
        "State Machine started | executionArn=%s",
        response["executionArn"],
    )

    return {
        "statusCode": 200,
        "executionArn": response["executionArn"],
        "startDate": response["startDate"].isoformat(),
    }
