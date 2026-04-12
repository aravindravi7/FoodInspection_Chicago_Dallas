# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Layer: Load Dimensional Model
# MAGIC Read from Silver zone (cleansed) and load the star schema (Gold zone).
# MAGIC
# MAGIC **Medallion Architecture - Gold**: Business-level dimensional model (dims & facts).

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, upper, concat, concat_ws,
    monotonically_increasing_id, row_number, dense_rank,
    year, month, dayofmonth, dayofweek, quarter, date_format,
    min as spark_min, max as spark_max, current_date, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# COMMAND ----------

def get_etl_job_id():
    """Get the current notebook path, or 'interactive' if unavailable."""
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    except Exception:
        return "interactive"

ETL_JOB_ID = get_etl_job_id()

def add_gold_lineage(df):
    """Add Gold layer lineage columns: created_at, updated_at, etl_job_id."""
    return (
        df
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .withColumn("etl_job_id", lit(ETL_JOB_ID))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver Tables

# COMMAND ----------

df_chi_insp = spark.table(f"{DATABASE_NAME}.silver_chicago_inspections")
df_dal_insp = spark.table(f"{DATABASE_NAME}.silver_dallas_inspections")
df_chi_viol = spark.table(f"{DATABASE_NAME}.silver_chicago_violations")
df_dal_viol = spark.table(f"{DATABASE_NAME}.silver_dallas_violations")

print(f"Chicago inspections: {df_chi_insp.count()}")
print(f"Dallas inspections:  {df_dal_insp.count()}")
print(f"Chicago violations:  {df_chi_viol.count()}")
print(f"Dallas violations:   {df_dal_viol.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. dim_date

# COMMAND ----------

# Get date range from both datasets
chi_dates = df_chi_insp.select(
    spark_min("Inspection_Date").alias("min_date"),
    spark_max("Inspection_Date").alias("max_date")
).collect()[0]

dal_dates = df_dal_insp.select(
    spark_min("Inspection_Date").alias("min_date"),
    spark_max("Inspection_Date").alias("max_date")
).collect()[0]

min_date = min(chi_dates["min_date"], dal_dates["min_date"])
max_date = max(chi_dates["max_date"], dal_dates["max_date"])

print(f"Date range: {min_date} to {max_date}")

# COMMAND ----------

# Generate date dimension
df_dim_date = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{min_date}'),
            to_date('{max_date}'),
            interval 1 day
        )) AS full_date
    """)
    .withColumn("date_key", (year(col("full_date")) * 10000 + month(col("full_date")) * 100 + dayofmonth(col("full_date"))).cast(IntegerType()))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("year", year(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
)

df_dim_date = add_gold_lineage(df_dim_date)

(
    df_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.dim_date")
)

print(f"dim_date: {df_dim_date.count()} rows")
display(df_dim_date.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. dim_restaurant

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Build staging data (deduplicated per business key)
# MAGIC Business key: `restaurant_name + source_city + license_number`
# MAGIC
# MAGIC Tracked SCD2 attributes: `facility_type`, `risk_category`, `aka_name`

# COMMAND ----------

# Chicago restaurants - deduplicate by business key, keep latest values
# Add Inspection_ID as tiebreaker for rows with same date
w_chi = Window.partitionBy("DBA_Name", "source_city", "License").orderBy(col("Inspection_Date").desc(), col("Inspection_ID").desc())
df_chi_restaurants = (
    df_chi_insp
    .withColumn("_rn", row_number().over(w_chi))
    .filter(col("_rn") == 1)
    .select(
        col("DBA_Name").alias("restaurant_name"),
        col("AKA_Name").alias("aka_name"),
        col("License").cast("string").alias("license_number"),
        col("Facility_Type").alias("facility_type"),
        col("Risk").alias("risk_category"),
        col("source_city")
    )
    .dropDuplicates(["restaurant_name", "source_city", "license_number"])  # safety net
)

# Dallas restaurants - deduplicate by business key
w_dal = Window.partitionBy("Restaurant_Name", "source_city").orderBy(col("Inspection_Date").desc(), col("Inspection_ID").desc())
df_dal_restaurants = (
    df_dal_insp
    .withColumn("_rn", row_number().over(w_dal))
    .filter(col("_rn") == 1)
    .select(
        col("Restaurant_Name").alias("restaurant_name"),
        lit(None).cast("string").alias("aka_name"),
        lit(None).cast("string").alias("license_number"),
        lit(None).cast("string").alias("facility_type"),
        lit(None).cast("string").alias("risk_category"),
        col("source_city")
    )
    .dropDuplicates(["restaurant_name", "source_city", "license_number"])  # safety net
)

# Union staging data
df_staging_restaurant = df_chi_restaurants.unionByName(df_dal_restaurants)

df_staging_restaurant.createOrReplaceTempView("staging_restaurant")
print(f"Staging restaurants: {df_staging_restaurant.count()} (Chicago: {df_chi_restaurants.count()}, Dallas: {df_dal_restaurants.count()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 SCD Type 2 Merge on dim_restaurant
# MAGIC - First run: creates the table with initial load
# MAGIC - Subsequent runs: expires changed rows, inserts new current rows

# COMMAND ----------

table_name = f"{DATABASE_NAME}.dim_restaurant"

# Check if dim_restaurant exists as a Delta table
table_exists = False
try:
    if spark.catalog.tableExists(table_name):
        # Verify it's actually a Delta table
        table_format = spark.sql(f"DESCRIBE DETAIL {table_name}").select("format").collect()[0][0]
        if table_format == "delta":
            target = DeltaTable.forName(spark, table_name)
            table_exists = True
            print("dim_restaurant exists as Delta — applying SCD Type 2 merge...")
        else:
            print(f"dim_restaurant exists but is {table_format} format — dropping and recreating as Delta...")
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
except Exception as e:
    print(f"Table check: {e} — will create fresh.")
    table_exists = False

if table_exists:

    # Step 1: Expire changed rows
    target.alias("t").merge(
        df_staging_restaurant.alias("s"),
        """
        t.restaurant_name = s.restaurant_name
        AND t.source_city = s.source_city
        AND COALESCE(t.license_number, '') = COALESCE(s.license_number, '')
        AND t.is_current = True
        """
    ).whenMatchedUpdate(
        condition="""
            COALESCE(t.facility_type, '') != COALESCE(s.facility_type, '')
            OR COALESCE(t.risk_category, '') != COALESCE(s.risk_category, '')
            OR COALESCE(t.aka_name, '') != COALESCE(s.aka_name, '')
        """,
        set={
            "effective_end_date": current_date(),
            "is_current": lit(False),
            "updated_at": current_timestamp()
        }
    ).execute()

    print("Step 1: Expired changed rows.")

    # Step 2: Insert new current rows for changed + brand new records
    new_or_changed = spark.sql(f"""
        SELECT s.*
        FROM staging_restaurant s
        LEFT JOIN {table_name} t
        ON s.restaurant_name = t.restaurant_name
           AND s.source_city = t.source_city
           AND COALESCE(s.license_number, '') = COALESCE(t.license_number, '')
           AND t.is_current = True
        WHERE t.restaurant_key IS NULL
    """)

    if new_or_changed.count() > 0:
        max_key = spark.sql(f"SELECT COALESCE(MAX(restaurant_key), 0) AS mk FROM {table_name}").collect()[0]["mk"]

        new_rows = (
            new_or_changed
            .withColumn("restaurant_key", monotonically_increasing_id() + max_key + 1)
            .withColumn("effective_start_date", current_date())
            .withColumn("effective_end_date", lit("9999-12-31").cast("date"))
            .withColumn("is_current", lit(True))
        )
        new_rows = add_gold_lineage(new_rows)
        new_rows.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"Step 2: Inserted {new_rows.count()} new/updated rows.")
    else:
        print("Step 2: No new or changed records.")

else:
    print("dim_restaurant does not exist — creating initial load with APPEND mode...")

    df_dim_restaurant = (
        df_staging_restaurant
        .withColumn("restaurant_key", monotonically_increasing_id())
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit("9999-12-31").cast("date"))
        .withColumn("is_current", lit(True))
    )
    df_dim_restaurant = add_gold_lineage(df_dim_restaurant)

    # Use append mode — saveAsTable creates the Delta table if it doesn't exist
    # Never use overwrite for SCD2 tables to preserve history on re-runs
    (
        df_dim_restaurant.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )
    print(f"Initial load (append): {df_dim_restaurant.count()} rows")

# COMMAND ----------

# Show SCD2 state
display(spark.sql(f"""
    SELECT is_current, COUNT(*) AS row_count
    FROM {DATABASE_NAME}.dim_restaurant
    GROUP BY is_current
"""))

dim_restaurant = spark.table(f"{DATABASE_NAME}.dim_restaurant")
print(f"dim_restaurant total: {dim_restaurant.count()} rows")
display(dim_restaurant.filter(col("is_current") == True).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. dim_location

# COMMAND ----------

# Chicago locations
df_chi_locations = (
    df_chi_insp.select(
        col("Address").alias("address"),
        col("City").alias("city"),
        col("State").alias("state"),
        col("Zip").alias("zip"),
        col("Latitude").cast("double").alias("latitude"),
        col("Longitude").cast("double").alias("longitude"),
        col("source_city")
    ).distinct()
)

# Dallas locations
df_dal_locations = (
    df_dal_insp.select(
        col("Street_Address").alias("address"),
        col("City").alias("city"),
        col("State").alias("state"),
        col("Zip_Code").alias("zip"),
        col("Latitude").cast("double").alias("latitude"),
        col("Longitude").cast("double").alias("longitude"),
        col("source_city")
    ).distinct()
)

# Union and assign surrogate keys
df_dim_location = (
    df_chi_locations.unionByName(df_dal_locations)
    .distinct()
    .withColumn("location_key", monotonically_increasing_id())
)

df_dim_location = add_gold_lineage(df_dim_location)

(
    df_dim_location.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.dim_location")
)

print(f"dim_location: {df_dim_location.count()} rows")
display(df_dim_location.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. dim_inspection_type

# COMMAND ----------

# Distinct inspection types from both cities
df_chi_types = df_chi_insp.select(col("Inspection_Type").alias("inspection_type_name"), col("source_city")).distinct()
df_dal_types = df_dal_insp.select(col("Inspection_Type").alias("inspection_type_name"), col("source_city")).distinct()

df_dim_inspection_type = (
    df_chi_types.unionByName(df_dal_types)
    .distinct()
    .withColumn("inspection_type_key", monotonically_increasing_id())
)

df_dim_inspection_type = add_gold_lineage(df_dim_inspection_type)

(
    df_dim_inspection_type.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.dim_inspection_type")
)

print(f"dim_inspection_type: {df_dim_inspection_type.count()} rows")
display(df_dim_inspection_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. dim_violation
# MAGIC Stores both Chicago and Dallas violation codes (they don't need to match).

# COMMAND ----------

# Chicago distinct violations
df_chi_viol_dim = (
    df_chi_viol.select(
        col("violation_code"),
        col("violation_description"),
        lit(None).cast(IntegerType()).alias("violation_points"),
        col("source_city")
    ).distinct()
)

# Dallas distinct violations
df_dal_viol_dim = (
    df_dal_viol.select(
        col("violation_code"),
        col("violation_description"),
        col("violation_points"),
        col("source_city")
    ).distinct()
)

# Union and assign surrogate keys
df_dim_violation = (
    df_chi_viol_dim.unionByName(df_dal_viol_dim)
    .distinct()
    .withColumn("violation_key", monotonically_increasing_id())
)

df_dim_violation = add_gold_lineage(df_dim_violation)

(
    df_dim_violation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.dim_violation")
)

print(f"dim_violation: {df_dim_violation.count()} rows")
display(df_dim_violation.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. fact_inspection

# COMMAND ----------

# Reload dims to get surrogate keys (filter dim_restaurant for current rows only)
dim_restaurant = spark.table(f"{DATABASE_NAME}.dim_restaurant").filter(col("is_current") == True)
dim_location = spark.table(f"{DATABASE_NAME}.dim_location")
dim_inspection_type = spark.table(f"{DATABASE_NAME}.dim_inspection_type")
dim_date = spark.table(f"{DATABASE_NAME}.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Chicago Fact

# COMMAND ----------

df_chi_fact = (
    df_chi_insp
    .join(
        dim_restaurant.select("restaurant_key", "restaurant_name", "license_number", "source_city"),
        (df_chi_insp["DBA_Name"] == dim_restaurant["restaurant_name"]) &
        (df_chi_insp["License"].cast("string") == dim_restaurant["license_number"]) &
        (df_chi_insp["source_city"] == dim_restaurant["source_city"]),
        "left"
    )
    .join(
        dim_location,
        (df_chi_insp["Address"] == dim_location["address"]) &
        (df_chi_insp["Zip"] == dim_location["zip"]) &
        (df_chi_insp["source_city"] == dim_location["source_city"]),
        "left"
    )
    .join(
        dim_inspection_type,
        (df_chi_insp["Inspection_Type"] == dim_inspection_type["inspection_type_name"]) &
        (df_chi_insp["source_city"] == dim_inspection_type["source_city"]),
        "left"
    )
    .join(
        dim_date,
        df_chi_insp["Inspection_Date"] == dim_date["full_date"],
        "left"
    )
    .select(
        df_chi_insp["Inspection_ID"].alias("inspection_id"),
        col("restaurant_key"),
        col("location_key"),
        col("inspection_type_key"),
        col("date_key"),
        df_chi_insp["Results"].alias("inspection_result"),
        df_chi_insp["Inspection_Score"].alias("inspection_score"),
        df_chi_insp["source_city"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Dallas Fact

# COMMAND ----------

df_dal_fact = (
    df_dal_insp
    .join(
        dim_restaurant.filter(col("is_current") == True).select("restaurant_key", "restaurant_name", "source_city"),
        (df_dal_insp["Restaurant_Name"] == dim_restaurant["restaurant_name"]) &
        (df_dal_insp["source_city"] == dim_restaurant["source_city"]),
        "left"
    )
    .join(
        dim_location,
        (df_dal_insp["Street_Address"] == dim_location["address"]) &
        (df_dal_insp["Zip_Code"] == dim_location["zip"]) &
        (df_dal_insp["source_city"] == dim_location["source_city"]),
        "left"
    )
    .join(
        dim_inspection_type,
        (df_dal_insp["Inspection_Type"] == dim_inspection_type["inspection_type_name"]) &
        (df_dal_insp["source_city"] == dim_inspection_type["source_city"]),
        "left"
    )
    .join(
        dim_date,
        df_dal_insp["Inspection_Date"] == dim_date["full_date"],
        "left"
    )
    .select(
        df_dal_insp["Inspection_ID"].alias("inspection_id"),
        col("restaurant_key"),
        col("location_key"),
        col("inspection_type_key"),
        col("date_key"),
        # Derive inspection result from score for Dallas
        when(df_dal_insp["Inspection_Score"] >= 90, lit("Pass"))
        .when(df_dal_insp["Inspection_Score"] >= 80, lit("Pass w/ Conditions"))
        .when(df_dal_insp["Inspection_Score"] >= 70, lit("Fail"))
        .when(df_dal_insp["Inspection_Score"] < 70, lit("Fail"))
        .otherwise(lit(None).cast("string"))
        .alias("inspection_result"),
        df_dal_insp["Inspection_Score"].alias("inspection_score"),
        df_dal_insp["source_city"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Union & Write Fact Table

# COMMAND ----------

df_fact_inspection = (
    df_chi_fact.unionByName(df_dal_fact)
    .withColumn("inspection_key", monotonically_increasing_id())
)

df_fact_inspection = add_gold_lineage(df_fact_inspection)

(
    df_fact_inspection.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.fact_inspection")
)

print(f"fact_inspection: {df_fact_inspection.count()} rows")
display(df_fact_inspection.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. bridge_inspection_violation

# COMMAND ----------

# Reload fact and violation dim
fact_inspection = spark.table(f"{DATABASE_NAME}.fact_inspection")
dim_violation = spark.table(f"{DATABASE_NAME}.dim_violation")

# COMMAND ----------

# Chicago bridge
df_chi_bridge = (
    df_chi_viol
    .join(
        fact_inspection.select("inspection_key", "inspection_id", "source_city"),
        (df_chi_viol["Inspection_ID"] == fact_inspection["inspection_id"]) &
        (df_chi_viol["source_city"] == fact_inspection["source_city"]),
        "inner"
    )
    .join(
        dim_violation,
        (df_chi_viol["violation_code"] == dim_violation["violation_code"]) &
        (df_chi_viol["source_city"] == dim_violation["source_city"]),
        "inner"
    )
    .select(
        fact_inspection["inspection_key"],
        dim_violation["violation_key"],
        df_chi_viol["violation_comments"]
    )
)

# Dallas bridge
df_dal_bridge = (
    df_dal_viol
    .join(
        fact_inspection.select("inspection_key", "inspection_id", "source_city"),
        (df_dal_viol["Inspection_ID"] == fact_inspection["inspection_id"]) &
        (df_dal_viol["source_city"] == fact_inspection["source_city"]),
        "inner"
    )
    .join(
        dim_violation,
        (df_dal_viol["violation_code"] == dim_violation["violation_code"]) &
        (df_dal_viol["source_city"] == dim_violation["source_city"]),
        "inner"
    )
    .select(
        fact_inspection["inspection_key"],
        dim_violation["violation_key"],
        df_dal_viol["violation_comments"]
    )
)

# Union and write
df_bridge = df_chi_bridge.unionByName(df_dal_bridge)

df_bridge = add_gold_lineage(df_bridge)

(
    df_bridge.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.bridge_inspection_violation")
)

print(f"bridge_inspection_violation: {df_bridge.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Verify Gold Layer

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM {DATABASE_NAME}.dim_date
    UNION ALL SELECT 'dim_restaurant', COUNT(*) FROM {DATABASE_NAME}.dim_restaurant
    UNION ALL SELECT 'dim_location', COUNT(*) FROM {DATABASE_NAME}.dim_location
    UNION ALL SELECT 'dim_inspection_type', COUNT(*) FROM {DATABASE_NAME}.dim_inspection_type
    UNION ALL SELECT 'dim_violation', COUNT(*) FROM {DATABASE_NAME}.dim_violation
    UNION ALL SELECT 'fact_inspection', COUNT(*) FROM {DATABASE_NAME}.fact_inspection
    UNION ALL SELECT 'bridge_inspection_violation', COUNT(*) FROM {DATABASE_NAME}.bridge_inspection_violation
"""))
