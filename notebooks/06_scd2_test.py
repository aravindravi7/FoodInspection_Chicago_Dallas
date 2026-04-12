# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - SCD Type 2 Testing
# MAGIC
# MAGIC This notebook performs comprehensive SCD2 testing on `dim_restaurant` by modifying the raw source CSV and re-running the pipeline.
# MAGIC
# MAGIC **Test Scenarios:**
# MAGIC 1. **UPDATE** — Change a tracked attribute (facility_type) for an existing restaurant
# MAGIC 2. **INSERT** — Add a brand new restaurant to the source data
# MAGIC 3. **MULTI-ATTRIBUTE UPDATE** — Change multiple tracked attributes at once (risk_category + aka_name)
# MAGIC 4. **NO-CHANGE** — Re-run pipeline without modifications, verify no new history
# MAGIC
# MAGIC **How it works:**
# MAGIC - Modify the raw CSV in Volumes → Re-run pipeline (01 → 03 → 04) → Validate SCD2

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

from pyspark.sql.functions import col, lit
import csv
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STEP 1: Capture BEFORE State
# MAGIC Run these queries BEFORE modifying the raw file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Overall dim_restaurant state

# COMMAND ----------

display(spark.sql(f"""
    SELECT is_current, COUNT(*) AS row_count
    FROM {DATABASE_NAME}.dim_restaurant
    GROUP BY is_current
    ORDER BY is_current DESC
"""))

total_before = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant").collect()[0]["cnt"]
print(f"Total rows BEFORE: {total_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Pick test restaurants and show their current state

# COMMAND ----------

# Test Restaurant 1: For UPDATE test (change facility_type)
# Test Restaurant 2: For MULTI-ATTRIBUTE UPDATE test (change risk + aka_name)
# We'll pick specific restaurants that are easy to find in the CSV

test_restaurants = spark.sql(f"""
    SELECT restaurant_name, license_number, facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = True
      AND source_city = 'Chicago'
      AND facility_type IS NOT NULL
      AND risk_category IS NOT NULL
      AND license_number IS NOT NULL
    ORDER BY restaurant_name
    LIMIT 5
""")

display(test_restaurants)

# Save the names for reference
test_rows = test_restaurants.collect()
TEST_REST_1 = test_rows[0]["restaurant_name"]
TEST_LICENSE_1 = test_rows[0]["license_number"]
TEST_OLD_FACILITY_1 = test_rows[0]["facility_type"]

TEST_REST_2 = test_rows[1]["restaurant_name"]
TEST_LICENSE_2 = test_rows[1]["license_number"]
TEST_OLD_RISK_2 = test_rows[1]["risk_category"]
TEST_OLD_AKA_2 = test_rows[1]["aka_name"]

print(f"\n=== TEST 1 (UPDATE facility_type) ===")
print(f"Restaurant: {TEST_REST_1} | License: {TEST_LICENSE_1} | Current facility_type: {TEST_OLD_FACILITY_1}")

print(f"\n=== TEST 2 (MULTI-ATTRIBUTE UPDATE) ===")
print(f"Restaurant: {TEST_REST_2} | License: {TEST_LICENSE_2}")
print(f"  Current risk_category: {TEST_OLD_RISK_2}")
print(f"  Current aka_name: {TEST_OLD_AKA_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STEP 2: Modify the Raw CSV
# MAGIC This reads the Chicago CSV, applies test changes, and writes it back to the Volume.

# COMMAND ----------

# Read the raw CSV into a DataFrame
raw_chicago_path = f"{VOLUME_PATH}/{CHICAGO_FILE}"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_chicago_path)

print(f"Raw CSV rows: {df_raw.count()}")
print(f"Raw CSV columns: {len(df_raw.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 TEST 1: UPDATE — Change facility_type for an existing restaurant

# COMMAND ----------

# Change facility_type from current value to "SCD2_TEST_BAKERY"
from pyspark.sql.functions import when

df_modified = df_raw.withColumn(
    "Facility Type",
    when(
        (col("DBA Name") == TEST_REST_1) & (col("License #") == TEST_LICENSE_1),
        lit("SCD2_TEST_BAKERY")
    ).otherwise(col("Facility Type"))
)

# Verify the change
changed_count_1 = df_modified.filter(
    (col("DBA Name") == TEST_REST_1) & (col("License #") == TEST_LICENSE_1)
).select("Facility Type").distinct().collect()
print(f"TEST 1: Changed '{TEST_REST_1}' (License: {TEST_LICENSE_1}) facility_type to 'SCD2_TEST_BAKERY'")
print(f"  Verification: {changed_count_1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 TEST 2: MULTI-ATTRIBUTE UPDATE — Change risk_category + AKA Name

# COMMAND ----------

df_modified = df_modified.withColumn(
    "Risk",
    when(
        (col("DBA Name") == TEST_REST_2) & (col("License #") == TEST_LICENSE_2),
        lit("Risk 3 (Low)")
    ).otherwise(col("Risk"))
).withColumn(
    "AKA Name",
    when(
        (col("DBA Name") == TEST_REST_2) & (col("License #") == TEST_LICENSE_2),
        lit("SCD2_TEST_AKA_NAME")
    ).otherwise(col("AKA Name"))
)

changed_count_2 = df_modified.filter(
    (col("DBA Name") == TEST_REST_2) & (col("License #") == TEST_LICENSE_2)
).select("Risk", "AKA Name").distinct().collect()
print(f"TEST 2: Changed '{TEST_REST_2}' (License: {TEST_LICENSE_2})")
print(f"  risk_category -> 'Risk 3 (Low)', aka_name -> 'SCD2_TEST_AKA_NAME'")
print(f"  Verification: {changed_count_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 TEST 3: INSERT — Add a brand new restaurant

# COMMAND ----------

# Create a new restaurant row by duplicating an existing row and changing key fields
# This avoids schema/type mismatch issues
from pyspark.sql.functions import when, lit

new_restaurant = (
    df_modified.limit(1)
    .withColumn("Inspection ID", lit(9999999))
    .withColumn("DBA Name", lit("SCD2_TEST_NEW_RESTAURANT"))
    .withColumn("AKA Name", lit("SCD2 TEST NEW"))
    .withColumn("License #", lit(9999999))
    .withColumn("Facility Type", lit("Test Facility"))
    .withColumn("Risk", lit("Risk 2 (Medium)"))
    .withColumn("Address", lit("123 TEST STREET"))
    .withColumn("City", lit("CHICAGO"))
    .withColumn("State", lit("IL"))
    .withColumn("Zip", lit("60601"))
    .withColumn("Inspection Type", lit("Canvass"))
    .withColumn("Results", lit("Pass"))
    .withColumn("Violations", lit("1. TEST VIOLATION - Comments: SCD2 test violation"))
)

df_modified = df_modified.unionByName(new_restaurant)

print(f"TEST 3: Added new restaurant 'SCD2_TEST_NEW_RESTAURANT' (License: 9999999)")
print(f"Modified CSV rows: {df_modified.count()} (was {df_raw.count()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Write modified CSV back to Volume (backup original first)

# COMMAND ----------

# Backup original file
backup_path = f"{VOLUME_PATH}/backup_{CHICAGO_FILE}"
dbutils.fs.cp(raw_chicago_path, backup_path)
print(f"Original backed up to: {backup_path}")

# Write modified CSV back (overwrite the original)
(
    df_modified.coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(f"{VOLUME_PATH}/temp_chicago_modified")
)

# Move the single CSV file to replace the original
import glob
temp_files = dbutils.fs.ls(f"{VOLUME_PATH}/temp_chicago_modified")
csv_file = [f.path for f in temp_files if f.name.endswith(".csv")][0]
dbutils.fs.cp(csv_file, raw_chicago_path)
dbutils.fs.rm(f"{VOLUME_PATH}/temp_chicago_modified", recurse=True)

print(f"Modified CSV written to: {raw_chicago_path}")
print("Changes applied:")
print(f"  1. UPDATE: '{TEST_REST_1}' facility_type -> 'SCD2_TEST_BAKERY'")
print(f"  2. MULTI-UPDATE: '{TEST_REST_2}' risk -> 'Risk 3 (Low)', aka -> 'SCD2_TEST_AKA_NAME'")
print(f"  3. INSERT: 'SCD2_TEST_NEW_RESTAURANT' (new restaurant)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STEP 3: Re-run the Pipeline
# MAGIC
# MAGIC **Go to Workflows → Jobs → Food_Inspection_ETL_Pipeline → Run Now**
# MAGIC
# MAGIC Wait for the pipeline to complete, then continue to Step 4 below.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STEP 4: Validate SCD2 Results (Run AFTER pipeline completes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Overall state — should now have expired rows

# COMMAND ----------

display(spark.sql(f"""
    SELECT is_current, COUNT(*) AS row_count
    FROM {DATABASE_NAME}.dim_restaurant
    GROUP BY is_current
    ORDER BY is_current DESC
"""))

total_after = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant").collect()[0]["cnt"]
current_after = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant WHERE is_current = True").collect()[0]["cnt"]
expired_after = total_after - current_after
print(f"Total rows AFTER: {total_after}")
print(f"Current: {current_after} | Expired: {expired_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 TEST 1 Validation: UPDATE — facility_type changed
# MAGIC Should show 2 rows:
# MAGIC - Old row: `is_current = False`, `facility_type = '{original value}'`
# MAGIC - New row: `is_current = True`, `facility_type = 'SCD2_TEST_BAKERY'`

# COMMAND ----------

print(f"=== TEST 1: {TEST_REST_1} (License: {TEST_LICENSE_1}) ===")
display(spark.sql(f"""
    SELECT restaurant_name, license_number, facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE restaurant_name = '{TEST_REST_1.replace("'", "''")}'
      AND license_number = '{TEST_LICENSE_1}'
    ORDER BY effective_start_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 TEST 2 Validation: MULTI-ATTRIBUTE UPDATE — risk + aka_name changed
# MAGIC Should show 2 rows:
# MAGIC - Old row: `is_current = False`, original risk and aka_name
# MAGIC - New row: `is_current = True`, `risk = 'Risk 3 (Low)'`, `aka_name = 'SCD2_TEST_AKA_NAME'`

# COMMAND ----------

print(f"=== TEST 2: {TEST_REST_2} (License: {TEST_LICENSE_2}) ===")
display(spark.sql(f"""
    SELECT restaurant_name, license_number, facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE restaurant_name = '{TEST_REST_2.replace("'", "''")}'
      AND license_number = '{TEST_LICENSE_2}'
    ORDER BY effective_start_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 TEST 3 Validation: INSERT — New restaurant added
# MAGIC Should show 1 row with `is_current = True`

# COMMAND ----------

print("=== TEST 3: SCD2_TEST_NEW_RESTAURANT ===")
display(spark.sql(f"""
    SELECT restaurant_name, license_number, facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE restaurant_name = 'SCD2_TEST_NEW_RESTAURANT'
    ORDER BY effective_start_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Integrity Checks

# COMMAND ----------

# Check 1: Every business key has exactly 1 current row
multi_current = spark.sql(f"""
    SELECT restaurant_name, source_city, license_number, COUNT(*) AS current_count
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = True
    GROUP BY restaurant_name, source_city, license_number
    HAVING COUNT(*) > 1
""")

if multi_current.count() == 0:
    print("CHECK 1 PASSED: Every restaurant has exactly 1 current row")
else:
    print(f"CHECK 1 FAILED: {multi_current.count()} restaurants have multiple current rows")
    display(multi_current)

# COMMAND ----------

# Check 2: Expired rows have valid end dates
bad_expired = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = False AND effective_end_date = '9999-12-31'
""").collect()[0]["cnt"]

if bad_expired == 0:
    print("CHECK 2 PASSED: All expired rows have valid end dates")
else:
    print(f"CHECK 2 FAILED: {bad_expired} expired rows have end_date = 9999-12-31")

# COMMAND ----------

# Check 3: Current rows have end_date = 9999-12-31
bad_current = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = True AND effective_end_date != '9999-12-31'
""").collect()[0]["cnt"]

if bad_current == 0:
    print("CHECK 3 PASSED: All current rows have end_date = 9999-12-31")
else:
    print(f"CHECK 3 FAILED: {bad_current} current rows have wrong end date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Delta Table History — Shows MERGE operations

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {DATABASE_NAME}.dim_restaurant"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STEP 5: Restore Original CSV
# MAGIC Run this to restore the original raw file after testing.

# COMMAND ----------

# Restore original CSV from backup
backup_path = f"{VOLUME_PATH}/backup_{CHICAGO_FILE}"
dbutils.fs.cp(backup_path, raw_chicago_path)
dbutils.fs.rm(backup_path)
print(f"Original CSV restored from backup.")
print("Raw data is back to its original state.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STEP 6 (Optional): NO-CHANGE TEST
# MAGIC Re-run the pipeline again WITHOUT any modifications.
# MAGIC Then run the cell below — the counts should not change.

# COMMAND ----------

# Run this AFTER re-running pipeline with no changes
total_no_change = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant").collect()[0]["cnt"]
expired_no_change = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant WHERE is_current = False").collect()[0]["cnt"]

print(f"Total rows after no-change re-run: {total_no_change}")
print(f"Expired rows: {expired_no_change}")

if total_no_change == total_after and expired_no_change == expired_after:
    print("\nNO-CHANGE TEST PASSED: Pipeline re-run with same data did not create new history")
else:
    print(f"\nNO-CHANGE TEST NOTE: Row counts changed (was {total_after} total, {expired_after} expired)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary
# MAGIC
# MAGIC | Test | Scenario | Expected Result | How to Verify |
# MAGIC |------|----------|----------------|---------------|
# MAGIC | 1 | UPDATE single attribute | Old row expired, new row with changed `facility_type` | Section 4.2 |
# MAGIC | 2 | UPDATE multiple attributes | Old row expired, new row with changed `risk_category` + `aka_name` | Section 4.3 |
# MAGIC | 3 | INSERT new restaurant | New row appears with `is_current = True` | Section 4.4 |
# MAGIC | 4 | NO-CHANGE re-run | No new rows, no new history | Section 4.6 (Step 6) |
# MAGIC
# MAGIC ### SCD2 Implementation:
# MAGIC - **Method**: Delta Lake MERGE in Notebook 04 (append mode, never overwrite)
# MAGIC - **Business Key**: `restaurant_name + source_city + license_number`
# MAGIC - **Tracked Attributes**: `facility_type`, `risk_category`, `aka_name`
# MAGIC - **SCD2 Columns**: `effective_start_date`, `effective_end_date`, `is_current`
