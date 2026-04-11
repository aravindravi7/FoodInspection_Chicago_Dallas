# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Raw Data Ingestion
# MAGIC Read raw CSV files from Chicago and Dallas and persist them as Delta tables (no transformations).
# MAGIC
# MAGIC **Medallion Architecture - Bronze**: Raw data as-is from source.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql("USE food_inspection")

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
# MAGIC ### Write Chicago Bronze Delta Table

# COMMAND ----------

# BRONZE_PATH is available from %run ./00_setup_config

(
    df_chicago_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("food_inspection.bronze_chicago_inspections")
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
# MAGIC ### Write Dallas Bronze Delta Table

# COMMAND ----------

(
    df_dallas_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("food_inspection.bronze_dallas_inspections")
)

print("Bronze table 'bronze_dallas_inspections' created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN food_inspection LIKE 'bronze_*';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Chicago' AS source, COUNT(*) AS row_count FROM food_inspection.bronze_chicago_inspections
# MAGIC UNION ALL
# MAGIC SELECT 'Dallas' AS source, COUNT(*) AS row_count FROM food_inspection.bronze_dallas_inspections;
