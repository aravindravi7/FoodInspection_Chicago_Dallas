# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - SCD Type 2 Validation
# MAGIC
# MAGIC This notebook validates the SCD Type 2 implementation on `dim_restaurant`.
# MAGIC
# MAGIC **SCD2 merge logic lives in Notebook 04** (part of the regular Gold pipeline).
# MAGIC
# MAGIC **How to test:**
# MAGIC 1. Run the pipeline (01 → 03 → 04) via Databricks Jobs — initial load
# MAGIC 2. Modify source data (raw CSV in Volumes OR Bronze table)
# MAGIC 3. Re-run the pipeline — SCD2 detects the change
# MAGIC 4. Run this notebook to validate results
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overall dim_restaurant State
# MAGIC Shows count of current vs expired rows. After initial load, all rows are `is_current = True`.
# MAGIC After a pipeline re-run with changed data, some rows will be `is_current = False` (expired).

# COMMAND ----------

display(spark.sql(f"""
    SELECT is_current, COUNT(*) AS row_count
    FROM {DATABASE_NAME}.dim_restaurant
    GROUP BY is_current
    ORDER BY is_current DESC
"""))

total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant").collect()[0]["cnt"]
current = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_restaurant WHERE is_current = True").collect()[0]["cnt"]
expired = total - current
print(f"Total rows: {total} | Current: {current} | Expired (historical): {expired}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Restaurants with SCD2 History
# MAGIC These are restaurants where a tracked attribute changed between pipeline runs.
# MAGIC Each should have at least 2 rows: one expired (old values) and one current (new values).

# COMMAND ----------

display(spark.sql(f"""
    SELECT restaurant_name, source_city, license_number,
           facility_type, risk_category, aka_name,
           effective_start_date, effective_end_date, is_current
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE restaurant_name IN (
        SELECT restaurant_name
        FROM {DATABASE_NAME}.dim_restaurant
        GROUP BY restaurant_name, source_city
        HAVING COUNT(*) > 1
    )
    ORDER BY restaurant_name, source_city, effective_start_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD2 Change Detail
# MAGIC Shows what specifically changed for restaurants with history.

# COMMAND ----------

display(spark.sql(f"""
    WITH history AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY restaurant_name, source_city ORDER BY effective_start_date DESC) AS rn
        FROM {DATABASE_NAME}.dim_restaurant
        WHERE restaurant_name IN (
            SELECT restaurant_name
            FROM {DATABASE_NAME}.dim_restaurant
            GROUP BY restaurant_name, source_city
            HAVING COUNT(*) > 1
        )
    )
    SELECT
        h_new.restaurant_name,
        h_new.source_city,
        h_old.facility_type AS old_facility_type,
        h_new.facility_type AS new_facility_type,
        h_old.risk_category AS old_risk_category,
        h_new.risk_category AS new_risk_category,
        h_old.aka_name AS old_aka_name,
        h_new.aka_name AS new_aka_name,
        h_old.effective_start_date AS old_start,
        h_old.effective_end_date AS old_end,
        h_new.effective_start_date AS new_start,
        h_new.effective_end_date AS new_end
    FROM history h_new
    JOIN history h_old
        ON h_new.restaurant_name = h_old.restaurant_name
        AND h_new.source_city = h_old.source_city
        AND h_new.rn = 1 AND h_old.rn = 2
    ORDER BY h_new.restaurant_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate SCD2 Integrity
# MAGIC Checks:
# MAGIC - Every restaurant has exactly 1 current row
# MAGIC - Expired rows have a valid end date (not 9999-12-31)
# MAGIC - Current rows have end date = 9999-12-31

# COMMAND ----------

# Check 1: Every restaurant+city combo should have exactly 1 current row
multi_current = spark.sql(f"""
    SELECT restaurant_name, source_city, COUNT(*) AS current_count
    FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = True
    GROUP BY restaurant_name, source_city
    HAVING COUNT(*) > 1
""")

if multi_current.count() == 0:
    print("✓ CHECK PASSED: Every restaurant has exactly 1 current row")
else:
    print(f"✗ CHECK FAILED: {multi_current.count()} restaurants have multiple current rows")
    display(multi_current)

# COMMAND ----------

# Check 2: All expired rows should have end_date != 9999-12-31
bad_expired = spark.sql(f"""
    SELECT * FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = False AND effective_end_date = '9999-12-31'
""")

if bad_expired.count() == 0:
    print("✓ CHECK PASSED: All expired rows have a valid end date")
else:
    print(f"✗ CHECK FAILED: {bad_expired.count()} expired rows still have end_date = 9999-12-31")

# COMMAND ----------

# Check 3: All current rows should have end_date = 9999-12-31
bad_current = spark.sql(f"""
    SELECT * FROM {DATABASE_NAME}.dim_restaurant
    WHERE is_current = True AND effective_end_date != '9999-12-31'
""")

if bad_current.count() == 0:
    print("✓ CHECK PASSED: All current rows have end_date = 9999-12-31")
else:
    print(f"✗ CHECK FAILED: {bad_current.count()} current rows have wrong end date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta Table History
# MAGIC Shows the version history of the dim_restaurant Delta table — each MERGE operation creates a new version.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {DATABASE_NAME}.dim_restaurant"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SCD2 Implementation Documentation
# MAGIC
# MAGIC ### Architecture:
# MAGIC - SCD2 merge logic is built into **Notebook 04** (Gold load pipeline)
# MAGIC - On first run: `dim_restaurant` is created with initial load (all `is_current = True`)
# MAGIC - On subsequent runs: Delta MERGE detects changes and applies SCD2
# MAGIC
# MAGIC ### How it works:
# MAGIC 1. **Staging data** is built from Silver tables, deduplicated per business key using window functions (latest inspection values)
# MAGIC 2. **MERGE Step 1**: Compare staging vs current dim rows — if tracked attributes changed, expire old row (`is_current = False`, `effective_end_date = today`)
# MAGIC 3. **MERGE Step 2**: Insert new current row for changed/new restaurants (`is_current = True`, `effective_start_date = today`, `effective_end_date = 9999-12-31`)
# MAGIC
# MAGIC ### Testing Approach:
# MAGIC - **Test via Raw CSV**: Modify the source CSV in Volumes → Run full pipeline (01 → 03 → 04) → SCD2 detects change
# MAGIC - **Test via Bronze**: Modify Bronze Delta table directly → Run pipeline (03 → 04) → SCD2 detects change
# MAGIC - **Validation**: Run this notebook (05) to verify expired/current rows and integrity checks
# MAGIC - **Pipeline**: Set up in Databricks Jobs (01 → 03 → 04), validated via SQL Query Editor
