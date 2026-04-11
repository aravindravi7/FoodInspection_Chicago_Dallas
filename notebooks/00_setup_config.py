# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup & Configuration
# MAGIC This notebook sets up the database, defines paths, and installs required libraries for the Food Inspection project.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS food_inspection")
spark.sql("USE food_inspection")
print("Database 'food_inspection' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Paths

# COMMAND ----------

# Raw data paths (Unity Catalog Volumes)
VOLUME_PATH = "/Volumes/workspace/food_inspection/raw_data"

RAW_CHICAGO_PATH = f"{VOLUME_PATH}/Food_Inspections_20260411.csv"
RAW_DALLAS_PATH = f"{VOLUME_PATH}/Restaurant_and_Food_Establishment_Inspections_(October_2016_to_January_2024)_20260411.csv"

print(f"Raw Chicago Path : {RAW_CHICAGO_PATH}")
print(f"Raw Dallas Path  : {RAW_DALLAS_PATH}")

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

# All variables (VOLUME_PATH, RAW_CHICAGO_PATH, RAW_DALLAS_PATH)
# are available as Python variables in any notebook that runs: %run ./00_setup_config
print("Configuration ready. All path variables are available via %run.")
