# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup & Configuration
# MAGIC This notebook sets up the database, defines paths, and installs required libraries for the Food Inspection project.
# MAGIC
# MAGIC All file paths are parameterized using widgets — no hardcoded values.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters

# COMMAND ----------

# Define widgets with default values (configurable at runtime)
dbutils.widgets.text("volume_path", "/Volumes/workspace/food_inspection/raw_data", "Volume Path")
dbutils.widgets.text("chicago_file", "Food_Inspections_20260411.csv", "Chicago CSV Filename")
dbutils.widgets.text("dallas_file", "Restaurant_and_Food_Establishment_Inspections_(October_2016_to_January_2024)_20260411.csv", "Dallas CSV Filename")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema")

# COMMAND ----------

# Read widget values
VOLUME_PATH = dbutils.widgets.get("volume_path")
CHICAGO_FILE = dbutils.widgets.get("chicago_file")
DALLAS_FILE = dbutils.widgets.get("dallas_file")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

# Explicit medallion storage paths
BRONZE_PATH = "/Volumes/workspace/bronze/food_inspection"
SILVER_PATH = "/Volumes/workspace/silver/food_inspection"
GOLD_PATH = "/Volumes/workspace/gold/food_inspection"

# Construct full paths
RAW_CHICAGO_PATH = f"{VOLUME_PATH}/{CHICAGO_FILE}"
RAW_DALLAS_PATH = f"{VOLUME_PATH}/{DALLAS_FILE}"

print(f"Raw Path         : {VOLUME_PATH}")
print(f"Raw Chicago Path : {RAW_CHICAGO_PATH}")
print(f"Raw Dallas Path  : {RAW_DALLAS_PATH}")
print(f"Bronze Schema    : {BRONZE_SCHEMA}")
print(f"Silver Schema    : {SILVER_SCHEMA}")
print(f"Gold Schema      : {GOLD_SCHEMA}")
print(f"Bronze Path      : {BRONZE_PATH}")
print(f"Silver Path      : {SILVER_PATH}")
print(f"Gold Path        : {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Medallion Schemas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
print(f"Schemas ready: {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Raw Data Files Exist

# COMMAND ----------

try:
    files = dbutils.fs.ls(VOLUME_PATH)
    print("Raw data files in Volume:")
    for f in files:
        print(f"  {f.name} ({f.size / 1024 / 1024:.2f} MB)")
except Exception as e:
    print(f"Raw data not found at {VOLUME_PATH}. Please upload the CSVs.")

# COMMAND ----------

# All variables are available as Python variables in any notebook that runs: %run ./00_setup_config
print("Configuration ready. Medallion schemas and paths are available via %run.")
