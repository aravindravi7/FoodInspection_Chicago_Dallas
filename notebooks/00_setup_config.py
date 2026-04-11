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
dbutils.widgets.text("database_name", "food_inspection", "Database Name")

# COMMAND ----------

# Read widget values
VOLUME_PATH = dbutils.widgets.get("volume_path")
CHICAGO_FILE = dbutils.widgets.get("chicago_file")
DALLAS_FILE = dbutils.widgets.get("dallas_file")
DATABASE_NAME = dbutils.widgets.get("database_name")

# Construct full paths
RAW_CHICAGO_PATH = f"{VOLUME_PATH}/{CHICAGO_FILE}"
RAW_DALLAS_PATH = f"{VOLUME_PATH}/{DALLAS_FILE}"

print(f"Volume Path      : {VOLUME_PATH}")
print(f"Raw Chicago Path : {RAW_CHICAGO_PATH}")
print(f"Raw Dallas Path  : {RAW_DALLAS_PATH}")
print(f"Database Name    : {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")
print(f"Database '{DATABASE_NAME}' is ready.")

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

# All variables (VOLUME_PATH, RAW_CHICAGO_PATH, RAW_DALLAS_PATH, DATABASE_NAME)
# are available as Python variables in any notebook that runs: %run ./00_setup_config
print("Configuration ready. All path variables are available via %run.")
