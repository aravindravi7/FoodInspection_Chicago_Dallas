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

# Raw data paths (update if your workspace path differs)
BASE_PATH = "/Workspace/Users/ravi.ara@northeastern.edu/FoodInspection_FinalProject"

RAW_CHICAGO_PATH = f"{BASE_PATH}/raw_data/chicago/"
RAW_DALLAS_PATH = f"{BASE_PATH}/raw_data/dallas/"

# Delta table storage paths
BRONZE_PATH = f"{BASE_PATH}/bronze/delta/"
SILVER_PATH = f"{BASE_PATH}/silver/delta/"
GOLD_PATH = f"{BASE_PATH}/gold/delta/"

print(f"Raw Chicago Path : {RAW_CHICAGO_PATH}")
print(f"Raw Dallas Path  : {RAW_DALLAS_PATH}")
print(f"Bronze Path      : {BRONZE_PATH}")
print(f"Silver Path      : {SILVER_PATH}")
print(f"Gold Path        : {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Raw Data Files Exist

# COMMAND ----------

try:
    chicago_files = dbutils.fs.ls(RAW_CHICAGO_PATH)
    print("Chicago raw data files:")
    for f in chicago_files:
        print(f"  {f.name} ({f.size / 1024 / 1024:.2f} MB)")
except Exception as e:
    print(f"Chicago raw data not found at {RAW_CHICAGO_PATH}. Please upload the CSV.")

print()

try:
    dallas_files = dbutils.fs.ls(RAW_DALLAS_PATH)
    print("Dallas raw data files:")
    for f in dallas_files:
        print(f"  {f.name} ({f.size / 1024 / 1024:.2f} MB)")
except Exception as e:
    print(f"Dallas raw data not found at {RAW_DALLAS_PATH}. Please upload the CSV.")

# COMMAND ----------

# All variables (BASE_PATH, RAW_CHICAGO_PATH, RAW_DALLAS_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH)
# are available as Python variables in any notebook that runs: %run ./00_setup_config
print("Configuration ready. All path variables are available via %run.")
