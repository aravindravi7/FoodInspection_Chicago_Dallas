# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Layer: Data Cleansing & Validation
# MAGIC Apply validation rules as expectations, drop bad rows, standardize schemas.
# MAGIC
# MAGIC **Medallion Architecture - Silver**: Cleansed, validated, standardized data.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql("USE food_inspection")

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, regexp_replace, regexp_extract,
    to_date, split, explode, posexplode, array, struct, expr,
    monotonically_increasing_id, concat_ws, coalesce, length,
    count, sum as spark_sum, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Tables

# COMMAND ----------

df_chicago_bronze = spark.table("food_inspection.bronze_chicago_inspections")
df_dallas_bronze = spark.table("food_inspection.bronze_dallas_inspections")

print(f"Chicago Bronze count: {df_chicago_bronze.count()}")
print(f"Dallas Bronze count: {df_dallas_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Chicago Silver Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Basic Cleansing & Standardization

# COMMAND ----------

df_chicago_clean = (
    df_chicago_bronze
    # Trim whitespace from string columns
    .withColumn("DBA Name", trim(col("DBA Name")))
    .withColumn("AKA Name", trim(col("AKA Name")))
    .withColumn("Address", trim(col("Address")))
    .withColumn("City", trim(upper(col("City"))))
    .withColumn("State", trim(upper(col("State"))))
    .withColumn("Results", trim(col("Results")))
    .withColumn("Inspection Type", trim(col("Inspection Type")))
    .withColumn("Facility Type", trim(col("Facility Type")))
    .withColumn("Risk", trim(col("Risk")))
    # Cast Zip to string and pad to 5 digits
    .withColumn("Zip", regexp_replace(col("Zip").cast("string"), "\\.0$", ""))
    .withColumn("Zip", when(length(col("Zip")) == 4, concat_ws("", lit("0"), col("Zip"))).otherwise(col("Zip")))
    # Parse Inspection Date
    .withColumn("Inspection Date", to_date(col("Inspection Date"), "MM/dd/yyyy"))
    # Add source city
    .withColumn("source_city", lit("Chicago"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Derive Inspection Score from Results (Chicago)

# COMMAND ----------

df_chicago_clean = df_chicago_clean.withColumn(
    "Inspection Score",
    when(col("Results") == "Pass", 90)
    .when(col("Results") == "Pass w/ Conditions", 80)
    .when(col("Results") == "Fail", 70)
    .when(col("Results") == "No Entry", 0)
    .otherwise(lit(None).cast(IntegerType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Apply Validation Rules (Expectations) - Chicago

# COMMAND ----------

chicago_before = df_chicago_clean.count()

# Rule 1: Restaurant Name cannot be null
df_chicago_clean = df_chicago_clean.filter(col("DBA Name").isNotNull() & (col("DBA Name") != ""))

# Rule 2: Inspection Date cannot be null
df_chicago_clean = df_chicago_clean.filter(col("Inspection Date").isNotNull())

# Rule 3: Inspection Type cannot be null
df_chicago_clean = df_chicago_clean.filter(col("Inspection Type").isNotNull() & (col("Inspection Type") != ""))

# Rule 4: Zip codes cannot be null and must be valid format (5 digits)
df_chicago_clean = df_chicago_clean.filter(
    col("Zip").isNotNull() & col("Zip").rlike("^\\d{5}$")
)

# Rule 5: Results cannot be null
df_chicago_clean = df_chicago_clean.filter(col("Results").isNotNull() & (col("Results") != ""))

# Rule 6: Every inspection must have at least 1 violation
df_chicago_clean = df_chicago_clean.filter(col("Violations").isNotNull() & (col("Violations") != ""))

# Rule 7: Result cannot be PASS if any violation contains Urgent/Critical
df_chicago_clean = df_chicago_clean.filter(
    ~(
        (upper(col("Results")) == "PASS") &
        (col("Violations").rlike("(?i)(urgent|critical)"))
    )
)

chicago_after = df_chicago_clean.count()
print(f"Chicago: {chicago_before} -> {chicago_after} rows ({chicago_before - chicago_after} dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Parse Chicago Violations into Rows
# MAGIC Chicago violations are pipe-delimited. Each violation has format: `CODE. DESCRIPTION - Comments: TEXT`

# COMMAND ----------

# Split violations by pipe delimiter and explode into rows
df_chicago_violations = (
    df_chicago_clean
    .withColumn("violation_array", split(col("Violations"), "\\|"))
    .withColumn("violation_raw", explode(col("violation_array")))
    .withColumn("violation_raw", trim(col("violation_raw")))
    .filter(col("violation_raw") != "")
)

# Extract violation code and description from the raw text
# Format: "CODE. DESCRIPTION - Comments: COMMENT_TEXT"
df_chicago_violations = (
    df_chicago_violations
    .withColumn("violation_code", regexp_extract(col("violation_raw"), r"^(\d+)\.", 1))
    .withColumn("violation_description",
        trim(regexp_extract(col("violation_raw"), r"^\d+\.\s*(.+?)(?:\s*-\s*Comments:.*)?$", 1))
    )
    .withColumn("violation_comments",
        trim(regexp_extract(col("violation_raw"), r"-\s*Comments:\s*(.*)", 1))
    )
    .withColumn("violation_points", lit(None).cast(IntegerType()))  # Chicago doesn't have points per violation
)

# Deduplicate violations per inspection
df_chicago_violations = df_chicago_violations.dropDuplicates(["Inspection ID", "violation_code"])

# COMMAND ----------

display(
    df_chicago_violations
    .select("Inspection ID", "violation_code", "violation_description", "violation_comments")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Write Chicago Silver Tables

# COMMAND ----------

# Silver - Chicago inspections (one row per inspection)
df_chicago_silver = df_chicago_clean.drop("Violations")  # violations stored separately

(
    df_chicago_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("food_inspection.silver_chicago_inspections")
)
print(f"Silver Chicago inspections: {df_chicago_silver.count()} rows")

# COMMAND ----------

# Silver - Chicago violations (one row per violation per inspection)
(
    df_chicago_violations
    .select(
        "Inspection ID", "violation_code", "violation_description",
        "violation_comments", "violation_points", "source_city"
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("food_inspection.silver_chicago_violations")
)
print("Silver Chicago violations table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Dallas Silver Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Basic Cleansing & Standardization

# COMMAND ----------

df_dallas_clean = (
    df_dallas_bronze
    .withColumn("Restaurant Name", trim(col("Restaurant Name")))
    .withColumn("Street Address", trim(col("Street Address")))
    .withColumn("Inspection Type", trim(col("Inspection Type")))
    # Standardize Zip to 5 digits
    .withColumn("Zip Code", regexp_replace(col("Zip Code").cast("string"), "\\.0$", ""))
    .withColumn("Zip Code", when(length(col("Zip Code")) == 4, concat_ws("", lit("0"), col("Zip Code"))).otherwise(col("Zip Code")))
    # Parse Inspection Date
    .withColumn("Inspection Date", to_date(col("Inspection Date"), "MM/dd/yyyy"))
    # Cast score to integer
    .withColumn("Inspection Score", col("Inspection Score").cast(IntegerType()))
    # Parse Lat/Long from combined field
    .withColumn("Latitude",
        regexp_extract(col("Lat Long Location"), r"\(([^,]+),", 1).cast(DoubleType())
    )
    .withColumn("Longitude",
        regexp_extract(col("Lat Long Location"), r",\s*([^)]+)\)", 1).cast(DoubleType())
    )
    # Add hardcoded city/state and source
    .withColumn("City", lit("DALLAS"))
    .withColumn("State", lit("TX"))
    .withColumn("source_city", lit("Dallas"))
    # Generate a unique inspection ID for Dallas (it doesn't have one)
    .withColumn("Inspection ID", monotonically_increasing_id())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Apply Validation Rules (Expectations) - Dallas

# COMMAND ----------

dallas_before = df_dallas_clean.count()

# Rule 1: Restaurant Name cannot be null
df_dallas_clean = df_dallas_clean.filter(col("Restaurant Name").isNotNull() & (col("Restaurant Name") != ""))

# Rule 2: Inspection Date cannot be null
df_dallas_clean = df_dallas_clean.filter(col("Inspection Date").isNotNull())

# Rule 3: Inspection Type cannot be null
df_dallas_clean = df_dallas_clean.filter(col("Inspection Type").isNotNull() & (col("Inspection Type") != ""))

# Rule 4: Zip codes cannot be null and must be valid (5 digits)
df_dallas_clean = df_dallas_clean.filter(
    col("Zip Code").isNotNull() & col("Zip Code").rlike("^\\d{5}$")
)

# Rule 5: Violation score cannot be more than 100
df_dallas_clean = df_dallas_clean.filter(
    col("Inspection Score").isNull() | (col("Inspection Score") <= 100)
)

# Count violations per row for validation
violation_desc_cols = [c for c in df_dallas_clean.columns if c.startswith("Violation Description")]
df_dallas_clean = df_dallas_clean.withColumn(
    "violation_count",
    sum([when(col(c).isNotNull() & (col(c) != ""), lit(1)).otherwise(lit(0)) for c in violation_desc_cols])
)

# Rule 6: Every inspection must have at least 1 violation
df_dallas_clean = df_dallas_clean.filter(col("violation_count") >= 1)

# Rule 7: If score >= 90, cannot have more than 3 violations
df_dallas_clean = df_dallas_clean.filter(
    ~((col("Inspection Score") >= 90) & (col("violation_count") > 3))
)

dallas_after = df_dallas_clean.count()
print(f"Dallas: {dallas_before} -> {dallas_after} rows ({dallas_before - dallas_after} dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Unpivot Dallas Violations (Wide to Long)
# MAGIC Convert 25 violation column groups into individual rows.

# COMMAND ----------

# Build arrays of structs for each violation slot (1-25)
violation_structs = []
for i in range(1, 26):
    desc_col = f"Violation Description - {i}"
    pts_col = f"Violation Points - {i}"
    detail_col = f"Violation Detail - {i}"
    memo_col = f"Violation Memo - {i}"

    # Check if columns exist
    if desc_col in df_dallas_clean.columns:
        violation_structs.append(
            struct(
                lit(str(i)).alias("violation_num"),
                col(f"`{desc_col}`").alias("violation_description"),
                col(f"`{pts_col}`").cast(IntegerType()).alias("violation_points"),
                col(f"`{detail_col}`").alias("violation_detail"),
                col(f"`{memo_col}`").alias("violation_comments")
            )
        )

# Create array of violation structs and explode
df_dallas_violations = (
    df_dallas_clean
    .withColumn("violations_array", array(*violation_structs))
    .withColumn("violation", explode(col("violations_array")))
    # Only keep non-null violations
    .filter(col("violation.violation_description").isNotNull() & (col("violation.violation_description") != ""))
    .select(
        "Inspection ID",
        col("violation.violation_num").alias("violation_num"),
        col("violation.violation_description").alias("violation_description_raw"),
        col("violation.violation_points").alias("violation_points"),
        col("violation.violation_detail").alias("violation_detail"),
        col("violation.violation_comments").alias("violation_comments"),
        "source_city"
    )
)

# Extract violation code from description (format: "*CODE Description text")
df_dallas_violations = (
    df_dallas_violations
    .withColumn("violation_code",
        regexp_extract(col("violation_description_raw"), r"\*?(\d+)\s", 1)
    )
    .withColumn("violation_description",
        trim(regexp_extract(col("violation_description_raw"), r"\*?\d+\s+(.*)", 1))
    )
)

# Deduplicate violations per inspection
df_dallas_violations = df_dallas_violations.dropDuplicates(["Inspection ID", "violation_code"])

# COMMAND ----------

display(
    df_dallas_violations
    .select("Inspection ID", "violation_code", "violation_description", "violation_points", "violation_comments")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Write Dallas Silver Tables

# COMMAND ----------

# Silver - Dallas inspections (one row per inspection)
# Drop the wide violation columns
violation_cols_to_drop = [c for c in df_dallas_clean.columns if c.startswith("Violation ")]
violation_cols_to_drop += ["Lat Long Location", "Inspection Month", "Inspection Year", "violation_count"]
# Also drop individual street components (we keep Street Address)
violation_cols_to_drop += ["Street Number", "Street Name", "Street Direction", "Street Type", "Street Unit"]

df_dallas_silver = df_dallas_clean.drop(*violation_cols_to_drop)

(
    df_dallas_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("food_inspection.silver_dallas_inspections")
)
print(f"Silver Dallas inspections: {df_dallas_silver.count()} rows")

# COMMAND ----------

# Silver - Dallas violations (one row per violation per inspection)
(
    df_dallas_violations
    .select(
        "Inspection ID", "violation_code", "violation_description",
        "violation_comments", "violation_points", "source_city"
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("food_inspection.silver_dallas_violations")
)
print("Silver Dallas violations table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Validation Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Chicago Inspections' AS table_name, COUNT(*) AS row_count FROM food_inspection.silver_chicago_inspections
# MAGIC UNION ALL
# MAGIC SELECT 'Chicago Violations', COUNT(*) FROM food_inspection.silver_chicago_violations
# MAGIC UNION ALL
# MAGIC SELECT 'Dallas Inspections', COUNT(*) FROM food_inspection.silver_dallas_inspections
# MAGIC UNION ALL
# MAGIC SELECT 'Dallas Violations', COUNT(*) FROM food_inspection.silver_dallas_violations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cleansing Steps Documentation
# MAGIC
# MAGIC ### Chicago Cleansing:
# MAGIC 1. Trimmed whitespace from all string columns
# MAGIC 2. Standardized City/State to uppercase
# MAGIC 3. Padded Zip codes to 5 digits, validated format
# MAGIC 4. Parsed Inspection Date to date type
# MAGIC 5. Derived Inspection Score from Results (Pass=90, Pass w/ Conditions=80, Fail=70, No Entry=0)
# MAGIC 6. Dropped rows: null Restaurant Name, Inspection Date, Type, Zip, Results
# MAGIC 7. Dropped rows: invalid zip format
# MAGIC 8. Dropped rows: no violations
# MAGIC 9. Dropped rows: Result=PASS but violations contain Urgent/Critical
# MAGIC 10. Parsed pipe-delimited violations into individual rows
# MAGIC 11. Extracted violation code and description from unstructured text
# MAGIC 12. Deduplicated violations per inspection
# MAGIC
# MAGIC ### Dallas Cleansing:
# MAGIC 1. Trimmed whitespace from string columns
# MAGIC 2. Standardized Zip codes to 5 digits, validated format
# MAGIC 3. Parsed Inspection Date to date type
# MAGIC 4. Parsed Lat/Long from combined location field
# MAGIC 5. Added City="DALLAS", State="TX"
# MAGIC 6. Generated unique Inspection IDs
# MAGIC 7. Dropped rows: null Restaurant Name, Inspection Date, Type, Zip
# MAGIC 8. Dropped rows: Inspection Score > 100
# MAGIC 9. Dropped rows: no violations
# MAGIC 10. Dropped rows: score >= 90 with > 3 violations
# MAGIC 11. Unpivoted 25 wide violation columns into individual rows
# MAGIC 12. Extracted violation codes from description text
# MAGIC 13. Deduplicated violations per inspection
