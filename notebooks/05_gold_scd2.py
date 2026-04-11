# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold Layer: SCD Type 2 Implementation
# MAGIC Implement Slowly Changing Dimension Type 2 on `dim_restaurant`.
# MAGIC
# MAGIC **Tracked attributes**: restaurant_name, facility_type, risk_category
# MAGIC
# MAGIC When any tracked attribute changes, the old row is expired and a new current row is inserted.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql("USE food_inspection")

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_date, when, coalesce
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Current dim_restaurant State

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM food_inspection.dim_restaurant LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_current, COUNT(*) FROM food_inspection.dim_restaurant GROUP BY is_current;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SCD2 Merge Logic
# MAGIC
# MAGIC This cell implements the SCD2 merge. When re-running the pipeline with updated source data:
# MAGIC - **New restaurants** are inserted as current rows.
# MAGIC - **Changed restaurants** (name, facility_type, or risk_category changed): old row is expired, new row inserted.
# MAGIC - **Unchanged restaurants**: no action.

# COMMAND ----------

def apply_scd2_restaurant():
    """
    Apply SCD Type 2 merge on dim_restaurant.
    Source: latest data from silver Chicago + Dallas inspections.
    Target: food_inspection.dim_restaurant (Delta table).
    """

    # Build staging data from silver tables (latest state of restaurants)
    df_chi = spark.table("food_inspection.silver_chicago_inspections")
    df_dal = spark.table("food_inspection.silver_dallas_inspections")

    staging_chi = (
        df_chi.select(
            col("DBA Name").alias("restaurant_name"),
            col("AKA Name").alias("aka_name"),
            col("License #").cast("string").alias("license_number"),
            col("Facility Type").alias("facility_type"),
            col("Risk").alias("risk_category"),
            col("source_city")
        ).distinct()
    )

    staging_dal = (
        df_dal.select(
            col("Restaurant Name").alias("restaurant_name"),
            lit(None).cast("string").alias("aka_name"),
            lit(None).cast("string").alias("license_number"),
            lit(None).cast("string").alias("facility_type"),
            lit(None).cast("string").alias("risk_category"),
            col("source_city")
        ).distinct()
    )

    staging = staging_chi.unionByName(staging_dal).distinct()
    staging.createOrReplaceTempView("staging_restaurant")

    # Get the Delta table reference
    target = DeltaTable.forName(spark, "food_inspection.dim_restaurant")

    # Step 1: Expire changed rows (update end date and is_current flag)
    # We identify changes by comparing tracked attributes
    target.alias("target").merge(
        staging.alias("source"),
        """
        target.restaurant_name = source.restaurant_name
        AND target.source_city = source.source_city
        AND COALESCE(target.license_number, '') = COALESCE(source.license_number, '')
        AND target.is_current = True
        """
    ).whenMatchedUpdate(
        condition="""
            COALESCE(target.facility_type, '') != COALESCE(source.facility_type, '')
            OR COALESCE(target.risk_category, '') != COALESCE(source.risk_category, '')
            OR COALESCE(target.aka_name, '') != COALESCE(source.aka_name, '')
        """,
        set={
            "effective_end_date": current_date(),
            "is_current": lit(False)
        }
    ).execute()

    print("Step 1 complete: Expired changed rows.")

    # Step 2: Insert new current rows for changed records + brand new records
    # Find records that are either new or were just expired
    expired_or_new = spark.sql("""
        SELECT s.*
        FROM staging_restaurant s
        LEFT JOIN food_inspection.dim_restaurant t
        ON s.restaurant_name = t.restaurant_name
           AND s.source_city = t.source_city
           AND COALESCE(s.license_number, '') = COALESCE(t.license_number, '')
           AND t.is_current = True
        WHERE t.restaurant_key IS NULL
    """)

    if expired_or_new.count() > 0:
        # Generate new surrogate keys (starting after max existing key)
        from pyspark.sql.functions import monotonically_increasing_id

        max_key = spark.sql("SELECT COALESCE(MAX(restaurant_key), 0) AS max_key FROM food_inspection.dim_restaurant").collect()[0]["max_key"]

        new_rows = (
            expired_or_new
            .withColumn("restaurant_key", monotonically_increasing_id() + max_key + 1)
            .withColumn("effective_start_date", current_date())
            .withColumn("effective_end_date", lit("9999-12-31").cast("date"))
            .withColumn("is_current", lit(True))
        )

        # Append new rows
        (
            new_rows.write
            .format("delta")
            .mode("append")
            .saveAsTable("food_inspection.dim_restaurant")
        )

        print(f"Step 2 complete: Inserted {new_rows.count()} new/updated rows.")
    else:
        print("Step 2 complete: No new or changed records found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute SCD2 Merge

# COMMAND ----------

apply_scd2_restaurant()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify SCD2 Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current vs expired rows
# MAGIC SELECT
# MAGIC   is_current,
# MAGIC   COUNT(*) AS row_count
# MAGIC FROM food_inspection.dim_restaurant
# MAGIC GROUP BY is_current;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show any restaurants with history (both current and expired rows)
# MAGIC SELECT *
# MAGIC FROM food_inspection.dim_restaurant
# MAGIC WHERE restaurant_name IN (
# MAGIC   SELECT restaurant_name
# MAGIC   FROM food_inspection.dim_restaurant
# MAGIC   GROUP BY restaurant_name, source_city
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY restaurant_name, effective_start_date
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SCD2 Documentation
# MAGIC
# MAGIC ### Implementation Details:
# MAGIC - **Table**: `dim_restaurant`
# MAGIC - **Tracked Attributes**: `facility_type`, `risk_category`, `aka_name`
# MAGIC - **Business Key**: `restaurant_name` + `source_city` + `license_number`
# MAGIC - **SCD2 Columns**: `effective_start_date`, `effective_end_date`, `is_current`
# MAGIC - **Method**: Delta Lake MERGE (2-step: expire old, insert new)
# MAGIC
# MAGIC ### How it works:
# MAGIC 1. Compare staging (Silver) data against current dim_restaurant rows
# MAGIC 2. If tracked attributes changed: set `is_current = False`, `effective_end_date = today`
# MAGIC 3. Insert new row with `is_current = True`, `effective_start_date = today`, `effective_end_date = 9999-12-31`
# MAGIC 4. Brand new restaurants are also inserted as current rows
