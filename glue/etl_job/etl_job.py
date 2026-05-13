"""
glue/etl_job/etl_job.py
────────────────────────────────────────────────────────────────
Glue ETL Job (PySpark / GlueVersion 4.0)

Steps
─────
1. Read battery14_df.csv and degrees.csv from S3.
2. Filter: females > 30 years old, country == "US",
           education_level > 6 (Master's degree),
           raw_score > 300.
3. Transform:
   a. Replace gender codes: 'm' → 'Male', 'f' → 'Female'.
   b. Join with degrees to get the human-readable degree name.
4. Write the resulting DataFrame as Parquet to S3.

Job parameters (passed via Step Functions / Glue defaults):
  --SOURCE_BUCKET       S3 bucket that holds raw data
  --DESTINATION_BUCKET  S3 bucket for processed output
  --BATTERY_FILE_KEY    S3 key for battery14_df.csv
  --DEGREES_FILE_KEY    S3 key for degrees.csv
  --OUTPUT_PREFIX       S3 prefix for the Parquet output
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Glue bootstrap ───────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "DESTINATION_BUCKET",
        "BATTERY_FILE_KEY",
        "DEGREES_FILE_KEY",
        "OUTPUT_PREFIX",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ── Resolve parameters ────────────────────────────────────────
SOURCE_BUCKET = args["SOURCE_BUCKET"]
DESTINATION_BUCKET = args["DESTINATION_BUCKET"]
BATTERY_PATH = f"s3://{SOURCE_BUCKET}/{args['BATTERY_FILE_KEY']}"
DEGREES_PATH = f"s3://{SOURCE_BUCKET}/{args['DEGREES_FILE_KEY']}"
OUTPUT_PATH = f"s3://{DESTINATION_BUCKET}/{args['OUTPUT_PREFIX']}"

logger.info(f"Battery data path : {BATTERY_PATH}")
logger.info(f"Degrees data path : {DEGREES_PATH}")
logger.info(f"Output path       : {OUTPUT_PATH}")

# ── 1. Read raw data ──────────────────────────────────────────
logger.info("Reading battery14_df.csv ...")
battery_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(BATTERY_PATH)
)

logger.info("Reading degrees.csv ...")
degrees_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(DEGREES_PATH)
)

logger.info(f"Raw battery rows: {battery_df.count()}")
logger.info(f"Degrees rows    : {degrees_df.count()}")

# ── 2. Filter ─────────────────────────────────────────────────
# Criteria:
#   - female (gender == 'f')
#   - over 30 years old (age > 30)
#   - US residents (country == 'US')
#   - education_level > 6  (above Master's degree → Ph.D. = 7)
#   - raw_score > 300
logger.info("Applying filters ...")
filtered_df = battery_df.filter(
    (F.col("gender") == "f")
    & (F.col("age") > 30)
    & (F.col("country") == "US")
    & (F.col("education_level") > 6)
    & (F.col("raw_score") > 300)
)

logger.info(f"Rows after filter: {filtered_df.count()}")

# ── 3a. Transform — expand gender codes ───────────────────────
logger.info("Transforming gender codes ...")
transformed_df = filtered_df.withColumn(
    "gender",
    F.when(F.col("gender") == "m", "Male")
     .when(F.col("gender") == "f", "Female")
     .otherwise(F.col("gender"))
)

# ── 3b. Transform — join with degrees lookup ──────────────────
logger.info("Joining with degrees lookup ...")
# Cast education_level to int for join (CSV infers as double)
transformed_df = transformed_df.withColumn(
    "education_level", F.col("education_level").cast("int")
)
degrees_df = degrees_df.withColumn(
    "education_level", F.col("education_level").cast("int")
)

result_df = transformed_df.join(
    degrees_df,
    on="education_level",
    how="left",
).withColumnRenamed("description", "degree_description")

logger.info(f"Final row count: {result_df.count()}")
logger.info("Schema:")
result_df.printSchema()

# ── 4. Write to S3 as Parquet ─────────────────────────────────
logger.info(f"Writing Parquet to {OUTPUT_PATH} ...")
(
    result_df
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(OUTPUT_PATH)
)

logger.info("ETL job completed successfully.")
job.commit()
