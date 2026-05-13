"""
glue/iceberg_job/iceberg_job.py
────────────────────────────────────────────────────────────────
Glue Iceberg Job (PySpark / GlueVersion 4.0)

Reads the Parquet output from the ETL job and creates (or replaces)
an Apache Iceberg table registered in the Glue Data Catalog.

Job parameters (passed via Step Functions / Glue defaults):
  --DESTINATION_BUCKET  S3 bucket with processed Parquet data
  --PROCESSED_PREFIX    S3 prefix where Parquet files live
  --GLUE_DATABASE       Glue Data Catalog database name
  --ICEBERG_TABLE       Target Iceberg table name
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ── Glue bootstrap ───────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DESTINATION_BUCKET",
        "PROCESSED_PREFIX",
        "GLUE_DATABASE",
        "ICEBERG_TABLE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ── Resolve parameters ────────────────────────────────────────
DESTINATION_BUCKET = args["DESTINATION_BUCKET"]
PROCESSED_PREFIX = args["PROCESSED_PREFIX"]
GLUE_DATABASE = args["GLUE_DATABASE"]
ICEBERG_TABLE = args["ICEBERG_TABLE"]

PARQUET_PATH = f"s3://{DESTINATION_BUCKET}/{PROCESSED_PREFIX}"
ICEBERG_WAREHOUSE = f"s3://{DESTINATION_BUCKET}/iceberg/"
FULL_TABLE_NAME = f"glue_catalog.{GLUE_DATABASE}.{ICEBERG_TABLE}"

logger.info(f"Parquet source  : {PARQUET_PATH}")
logger.info(f"Iceberg warehouse: {ICEBERG_WAREHOUSE}")
logger.info(f"Target table    : {FULL_TABLE_NAME}")

# ── Configure Iceberg / Spark ─────────────────────────────────
spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO",
)

# ── 1. Read processed Parquet ────────────────────────────────
logger.info("Reading processed Parquet data ...")
parquet_df = spark.read.parquet(PARQUET_PATH)
logger.info(f"Rows read: {parquet_df.count()}")
parquet_df.printSchema()

# ── 2. Register as a temporary view ──────────────────────────
parquet_df.createOrReplaceTempView("battery14_temp")

# ── 3. Create Glue Data Catalog database if not exists ───────
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}")

# ── 4. Create or replace Iceberg table ───────────────────────
logger.info(f"Creating/replacing Iceberg table: {FULL_TABLE_NAME} ...")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FULL_TABLE_NAME}
    USING iceberg
    LOCATION '{ICEBERG_WAREHOUSE}{ICEBERG_TABLE}/'
    TBLPROPERTIES (
        'table_type'            = 'ICEBERG',
        'format'                = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.metadata.compression-codec' = 'gzip'
    )
    AS SELECT * FROM battery14_temp
""")

logger.info("Iceberg table created/replaced successfully.")

# ── 5. Validate: count rows via SQL ──────────────────────────
count_result = spark.sql(f"SELECT COUNT(*) AS row_count FROM {FULL_TABLE_NAME}")
count_result.show()

row_count = count_result.collect()[0]["row_count"]
logger.info(f"Iceberg table row count: {row_count}")

# ── 6. Quick validation query ────────────────────────────────
logger.info("Sample rows from the Iceberg table:")
spark.sql(f"""
    SELECT
        user_id,
        age,
        gender,
        degree_description,
        country,
        raw_score,
        grand_index
    FROM {FULL_TABLE_NAME}
    LIMIT 10
""").show(truncate=False)

logger.info("Iceberg job completed successfully.")
job.commit()
