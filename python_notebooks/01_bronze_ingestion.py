# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Raw Data Ingestion
# MAGIC Read raw CSV files from Chicago and Dallas and persist them as Delta tables (no transformations).
# MAGIC
# MAGIC **Medallion Architecture - Bronze**: Raw data as-is from source + lineage columns.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------


# COMMAND ----------

import re
from pyspark.sql.functions import lit, current_timestamp, md5, concat_ws, col

def sanitize_columns(df):
    """Replace spaces and special characters in column names with underscores."""
    for old_name in df.columns:
        new_name = re.sub(r'[^a-zA-Z0-9_]', '_', old_name.strip()).strip('_')
        new_name = re.sub(r'_+', '_', new_name)  # collapse multiple underscores
        df = df.withColumnRenamed(old_name, new_name)
    return df

def get_etl_job_id():
    """Get the current notebook run ID, or 'interactive' if running manually."""
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return "interactive"

ETL_JOB_ID = get_etl_job_id()

def add_bronze_lineage(df, source_name):
    """Add lineage/audit columns to Bronze layer DataFrame."""
    return (
        df
        .withColumn("data_source_name", lit(source_name))
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("etl_job_id", lit(ETL_JOB_ID))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Chicago Food Inspections

# COMMAND ----------

# RAW_CHICAGO_PATH is available from %run ./00_setup_config

df_chicago_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(RAW_CHICAGO_PATH)
)

print(f"Chicago raw record count: {df_chicago_raw.count()}")
print(f"Chicago raw column count: {len(df_chicago_raw.columns)}")

# COMMAND ----------

df_chicago_raw.printSchema()

# COMMAND ----------

display(df_chicago_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanitize Column Names, Add Lineage & Write Chicago Bronze Delta Table

# COMMAND ----------

df_chicago_raw = sanitize_columns(df_chicago_raw)
df_chicago_raw = add_bronze_lineage(df_chicago_raw, "Chicago_OpenData_FoodInspections")
print("Chicago columns after sanitization + lineage:")
print(df_chicago_raw.columns)

# COMMAND ----------

(
    df_chicago_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .option("path", f"{BRONZE_PATH}/chicago_inspections").saveAsTable(f"{BRONZE_SCHEMA}.chicago_inspections")
)

print("Bronze table bronze.chicago_inspections created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Dallas Food Inspections

# COMMAND ----------

# RAW_DALLAS_PATH is available from %run ./00_setup_config

df_dallas_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(RAW_DALLAS_PATH)
)

print(f"Dallas raw record count: {df_dallas_raw.count()}")
print(f"Dallas raw column count: {len(df_dallas_raw.columns)}")

# COMMAND ----------

df_dallas_raw.printSchema()

# COMMAND ----------

display(df_dallas_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanitize Column Names, Add Lineage & Write Dallas Bronze Delta Table

# COMMAND ----------

df_dallas_raw = sanitize_columns(df_dallas_raw)
df_dallas_raw = add_bronze_lineage(df_dallas_raw, "Dallas_OpenData_FoodInspections")
print("Dallas columns after sanitization + lineage:")
print(df_dallas_raw.columns)

# COMMAND ----------

(
    df_dallas_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .option("path", f"{BRONZE_PATH}/dallas_inspections").saveAsTable(f"{BRONZE_SCHEMA}.dallas_inspections")
)

print("Bronze table bronze.dallas_inspections created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Bronze Tables

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'Chicago' AS source, COUNT(*) AS row_count FROM {BRONZE_SCHEMA}.chicago_inspections
    UNION ALL
    SELECT 'Dallas' AS source, COUNT(*) AS row_count FROM {BRONZE_SCHEMA}.dallas_inspections
"""))

# COMMAND ----------

# Verify lineage columns are present
display(
    spark.table(f"{BRONZE_SCHEMA}.chicago_inspections")
    .select("data_source_name", "ingest_timestamp", "etl_job_id")
    .limit(5)
)
