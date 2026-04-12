# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - SCD Type 2 Validation Test
# MAGIC
# MAGIC This notebook validates the SCD Type 2 implementation on `dim_restaurant` by simulating a pipeline re-run with changed data.
# MAGIC
# MAGIC **Test Plan:**
# MAGIC 1. Show current state of dim_restaurant (all rows are `is_current = True`)
# MAGIC 2. Pick a test restaurant and simulate a change in the Silver table
# MAGIC 3. Re-run notebook 04 (Gold load) which includes the SCD2 merge
# MAGIC 4. Verify: old row expired (`is_current = False`), new row inserted (`is_current = True`)
# MAGIC 5. Revert the Silver table back to original
# MAGIC
# MAGIC **SCD2 Details:**
# MAGIC - **Table**: `dim_restaurant`
# MAGIC - **Business Key**: `restaurant_name` + `source_city` + `license_number`
# MAGIC - **Tracked Attributes**: `facility_type`, `risk_category`, `aka_name`
# MAGIC - **SCD2 Columns**: `effective_start_date`, `effective_end_date`, `is_current`
# MAGIC - **Method**: Delta Lake MERGE (2-step: expire old, insert new)

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

from pyspark.sql.functions import col, lit, current_date
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Current State of dim_restaurant (Before Change)
# MAGIC All rows should be `is_current = True` from the initial load.

# COMMAND ----------

display(spark.sql(f"""
    SELECT is_current, COUNT(*) AS row_count
    FROM {DATABASE_NAME}.dim_restaurant
    GROUP BY is_current
    ORDER BY is_current
"""))

total = spark.table(f"{DATABASE_NAME}.dim_restaurant").count()
print(f"Total dim_restaurant rows: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select a Test Restaurant

# COMMAND ----------

test_restaurant = spark.sql(f"""
    SELECT * FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = True AND facility_type IS NOT NULL AND source_city = 'Chicago'
    LIMIT 1
""").collect()[0]

test_name = test_restaurant["restaurant_name"]
test_city = test_restaurant["source_city"]
test_license = test_restaurant["license_number"] or ""
test_old_facility = test_restaurant["facility_type"]

print(f"Test restaurant: {test_name}")
print(f"Source city: {test_city}")
print(f"License: {test_license}")
print(f"Current facility_type: {test_old_facility}")

# COMMAND ----------

# Show current state of this restaurant in dim_restaurant
print(f"=== dim_restaurant rows for '{test_name}' BEFORE change ===")
display(spark.sql(f"""
    SELECT restaurant_name, facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE restaurant_name = '{test_name.replace("'", "''")}' AND source_city = '{test_city}'
    ORDER BY effective_start_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulate a Change in Silver Table
# MAGIC We update the `facility_type` for our test restaurant in the Silver table to simulate a real-world data change (e.g., a restaurant changed its type from "Restaurant" to "Bakery").

# COMMAND ----------

# Update Silver table to simulate change
silver_table = DeltaTable.forName(spark, f"{DATABASE_NAME}.silver_chicago_inspections")
silver_table.update(
    condition=f"DBA_Name = '{test_name.replace(chr(39), chr(39)+chr(39))}'",
    set={"Facility_Type": lit("CHANGED_FOR_SCD2_TEST")}
)

print(f"✓ Updated '{test_name}' facility_type from '{test_old_facility}' to 'CHANGED_FOR_SCD2_TEST' in Silver table")

# Verify the change in Silver
display(spark.sql(f"""
    SELECT DBA_Name, Facility_Type, source_city
    FROM {DATABASE_NAME}.silver_chicago_inspections
    WHERE DBA_Name = '{test_name.replace("'", "''")}'
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Re-run Gold Pipeline (Notebook 04)
# MAGIC This triggers the SCD2 merge in notebook 04 which will:
# MAGIC - Detect that `facility_type` changed for our test restaurant
# MAGIC - Expire the old row (`is_current = False`, `effective_end_date = today`)
# MAGIC - Insert a new row with the updated value (`is_current = True`)

# COMMAND ----------

# MAGIC %run ./04_gold_dim_load

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify SCD2: Old Row Expired, New Row Inserted

# COMMAND ----------

# Show the test restaurant - should now have 2 rows: one expired, one current
print(f"=== dim_restaurant rows for '{test_name}' AFTER change ===")
display(spark.sql(f"""
    SELECT restaurant_name, facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE restaurant_name = '{test_name.replace("'", "''")}' AND source_city = '{test_city}'
    ORDER BY effective_start_date
"""))

# COMMAND ----------

# Overall SCD2 state - should show both current and expired rows
display(spark.sql(f"""
    SELECT is_current, COUNT(*) AS row_count
    FROM {DATABASE_NAME}.dim_restaurant
    GROUP BY is_current
    ORDER BY is_current
"""))

total_after = spark.table(f"{DATABASE_NAME}.dim_restaurant").count()
print(f"Total dim_restaurant rows after SCD2: {total_after} (was {total})")
print(f"New rows added (expired + new current): {total_after - total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Revert Silver Table Back to Original
# MAGIC Restore the original `facility_type` to keep data clean.

# COMMAND ----------

silver_table.update(
    condition=f"DBA_Name = '{test_name.replace(chr(39), chr(39)+chr(39))}'",
    set={"Facility_Type": lit(test_old_facility)}
)

print(f"✓ Reverted '{test_name}' facility_type back to '{test_old_facility}' in Silver table")

# Verify revert
display(spark.sql(f"""
    SELECT DBA_Name, Facility_Type, source_city
    FROM {DATABASE_NAME}.silver_chicago_inspections
    WHERE DBA_Name = '{test_name.replace("'", "''")}'
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. SCD2 Implementation Summary
# MAGIC
# MAGIC ### How It Works (in Notebook 04):
# MAGIC 1. **Staging data** is built from Silver tables, deduplicated per business key (latest values via window function)
# MAGIC 2. **First run**: If `dim_restaurant` doesn't exist, creates it with initial load (all `is_current = True`)
# MAGIC 3. **Subsequent runs**: Delta MERGE compares staging vs current rows:
# MAGIC    - If `facility_type`, `risk_category`, or `aka_name` changed → expire old row, insert new current row
# MAGIC    - If brand new restaurant → insert as current row
# MAGIC    - If unchanged → no action
# MAGIC
# MAGIC ### Test Results:
# MAGIC - **Before change**: Test restaurant had 1 row (`is_current = True`)
# MAGIC - **After simulated change**: Test restaurant has 2 rows:
# MAGIC   - Old row: `is_current = False`, `effective_end_date = today`
# MAGIC   - New row: `is_current = True`, `effective_end_date = 9999-12-31`, `facility_type = CHANGED_FOR_SCD2_TEST`
# MAGIC - This confirms SCD Type 2 is working correctly.
