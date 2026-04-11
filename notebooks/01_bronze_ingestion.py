# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Raw Data Ingestion
# MAGIC Read raw CSV files from Chicago and Dallas and persist them as Delta tables (no transformations).
# MAGIC
# MAGIC **Medallion Architecture - Bronze**: Raw data as-is from source.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

import re

def sanitize_columns(df):
    """Replace spaces and special characters in column names with underscores."""
    for old_name in df.columns:
        new_name = re.sub(r'[^a-zA-Z0-9_]', '_', old_name.strip()).strip('_')
        new_name = re.sub(r'_+', '_', new_name)  # collapse multiple underscores
        df = df.withColumnRenamed(old_name, new_name)
    return df

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
# MAGIC ### Sanitize Column Names & Write Chicago Bronze Delta Table

# COMMAND ----------

df_chicago_raw = sanitize_columns(df_chicago_raw)
print("Chicago columns after sanitization:")
print(df_chicago_raw.columns)

# COMMAND ----------

(
    df_chicago_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.bronze_chicago_inspections")
)

print("Bronze table 'bronze_chicago_inspections' created successfully.")

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
# MAGIC ### Sanitize Column Names & Write Dallas Bronze Delta Table

# COMMAND ----------

df_dallas_raw = sanitize_columns(df_dallas_raw)
print("Dallas columns after sanitization:")
print(df_dallas_raw.columns)

# COMMAND ----------

(
    df_dallas_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.bronze_dallas_inspections")
)

print("Bronze table 'bronze_dallas_inspections' created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Bronze Tables

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {DATABASE_NAME} LIKE 'bronze_*'"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'Chicago' AS source, COUNT(*) AS row_count FROM {DATABASE_NAME}.bronze_chicago_inspections
    UNION ALL
    SELECT 'Dallas' AS source, COUNT(*) AS row_count FROM {DATABASE_NAME}.bronze_dallas_inspections
"""))
