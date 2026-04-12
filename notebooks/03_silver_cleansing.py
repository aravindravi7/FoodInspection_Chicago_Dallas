# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Layer: Data Cleansing & Validation
# MAGIC Apply validation rules as expectations, drop bad rows, standardize schemas.
# MAGIC
# MAGIC **Medallion Architecture - Silver**: Cleansed, validated, standardized data.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, regexp_replace, regexp_extract,
    to_date, split, explode, posexplode, array, struct, expr,
    monotonically_increasing_id, concat_ws, coalesce, length,
    count, sum as spark_sum, row_number, current_timestamp, md5, sha2
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType

# COMMAND ----------

def add_silver_lineage(df, key_columns):
    """Add Silver layer lineage columns: updated_at and durability_key (hash of business key columns)."""
    return (
        df
        .withColumn("updated_at", current_timestamp())
        .withColumn("durability_key", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in key_columns]), 256))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Tables

# COMMAND ----------

df_chicago_bronze = spark.table(f"{DATABASE_NAME}.bronze_chicago_inspections")
df_dallas_bronze = spark.table(f"{DATABASE_NAME}.bronze_dallas_inspections")

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
    # Trim whitespace and remove escaped/stray quotes from name columns
    .withColumn("DBA_Name", trim(regexp_replace(col("DBA_Name"), '"', '')))
    .withColumn("AKA_Name", trim(regexp_replace(col("AKA_Name"), '"', '')))
    .withColumn("Address", trim(col("Address")))
    .withColumn("City", trim(upper(col("City"))))
    .withColumn("State", trim(upper(col("State"))))
    .withColumn("Results", trim(col("Results")))
    .withColumn("Inspection_Type", trim(col("Inspection_Type")))
    .withColumn("Facility_Type", trim(col("Facility_Type")))
    .withColumn("Risk", trim(col("Risk")))
    # Cast Zip to string and pad to 5 digits
    .withColumn("Zip", regexp_replace(col("Zip").cast("string"), "\\.0$", ""))
    .withColumn("Zip", when(length(col("Zip")) == 4, concat_ws("", lit("0"), col("Zip"))).otherwise(col("Zip")))
    # Parse Inspection Date
    .withColumn("Inspection_Date", to_date(col("Inspection_Date"), "MM/dd/yyyy"))
    # Cast Latitude/Longitude to double (use try_cast to handle bad data like ' RODENTS' → null)
    .withColumn("Latitude", expr("try_cast(Latitude as double)"))
    .withColumn("Longitude", expr("try_cast(Longitude as double)"))
    # Add source city
    .withColumn("source_city", lit("Chicago"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Derive Inspection Score from Results (Chicago)

# COMMAND ----------

df_chicago_clean = df_chicago_clean.withColumn(
    "Inspection_Score",
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
df_chicago_clean = df_chicago_clean.filter(col("DBA_Name").isNotNull() & (col("DBA_Name") != ""))

# Rule 2: Inspection Date cannot be null
df_chicago_clean = df_chicago_clean.filter(col("Inspection_Date").isNotNull())

# Rule 3: Inspection Type cannot be null
df_chicago_clean = df_chicago_clean.filter(col("Inspection_Type").isNotNull() & (col("Inspection_Type") != ""))

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

# Deduplicate violations per inspection (use code + description as key to avoid
# collapsing different violations when code extraction returns blank)
df_chicago_violations = df_chicago_violations.dropDuplicates(["Inspection_ID", "violation_code", "violation_description"])

# COMMAND ----------

display(
    df_chicago_violations
    .select("Inspection_ID", "violation_code", "violation_description", "violation_comments")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Write Chicago Silver Tables

# COMMAND ----------

# Silver - Chicago inspections (one row per inspection)
df_chicago_silver = df_chicago_clean.drop("Violations")  # violations stored separately
# Add lineage: updated_at + durability_key (hash of business key)
df_chicago_silver = add_silver_lineage(df_chicago_silver, ["Inspection_ID", "DBA_Name", "Inspection_Date"])

(
    df_chicago_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.silver_chicago_inspections")
)
print(f"Silver Chicago inspections: {df_chicago_silver.count()} rows")

# COMMAND ----------

# Silver - Chicago violations (one row per violation per inspection)
df_chicago_violations_out = (
    df_chicago_violations
    .select(
        "Inspection_ID", "violation_code", "violation_description",
        "violation_comments", "violation_points", "source_city"
    )
)
df_chicago_violations_out = add_silver_lineage(df_chicago_violations_out, ["Inspection_ID", "violation_code"])

(
    df_chicago_violations_out
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.silver_chicago_violations")
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
    .withColumn("Restaurant_Name", trim(col("Restaurant_Name")))
    .withColumn("Street_Address", trim(col("Street_Address")))
    .withColumn("Inspection_Type", trim(col("Inspection_Type")))
    # Standardize Zip to 5 digits (extract first 5 digits from ZIP+4 like '75228-3007' or '752435219')
    .withColumn("Zip_Code", regexp_replace(col("Zip_Code").cast("string"), "\\.0$", ""))
    .withColumn("Zip_Code", regexp_extract(col("Zip_Code"), r"^(\d{5})", 1))
    .withColumn("Zip_Code", when(col("Zip_Code") == "", lit(None)).otherwise(col("Zip_Code")))
    .withColumn("Zip_Code", when(length(col("Zip_Code")) == 4, concat_ws("", lit("0"), col("Zip_Code"))).otherwise(col("Zip_Code")))
    # Parse Inspection Date
    .withColumn("Inspection_Date", to_date(col("Inspection_Date"), "MM/dd/yyyy"))
    # Cast score to integer (use try_cast for ANSI mode safety)
    .withColumn("Inspection_Score", expr("try_cast(Inspection_Score as int)"))
    # Parse Lat/Long from combined field, then try_cast for safety
    .withColumn("Latitude", regexp_extract(col("Lat_Long_Location"), r"\(([^,]+),", 1))
    .withColumn("Latitude", expr("try_cast(Latitude as double)"))
    .withColumn("Longitude", regexp_extract(col("Lat_Long_Location"), r",\s*([^)]+)\)", 1))
    .withColumn("Longitude", expr("try_cast(Longitude as double)"))
    # Add hardcoded city/state and source
    .withColumn("City", lit("DALLAS"))
    .withColumn("State", lit("TX"))
    .withColumn("source_city", lit("Dallas"))
    # Derive inspection result from score (to match Chicago's Results field)
    .withColumn("Results",
        when(col("Inspection_Score") >= 90, lit("Pass"))
        .when(col("Inspection_Score") >= 80, lit("Pass w/ Conditions"))
        .when(col("Inspection_Score") >= 70, lit("Fail"))
        .when(col("Inspection_Score") < 70, lit("Fail"))
        .otherwise(lit(None).cast("string"))
    )
    # Generate a unique inspection ID for Dallas (it doesn't have one)
    .withColumn("Inspection_ID", monotonically_increasing_id())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Apply Validation Rules (Expectations) - Dallas

# COMMAND ----------

dallas_before = df_dallas_clean.count()

# Rule 1: Restaurant Name cannot be null
df_dallas_clean = df_dallas_clean.filter(col("Restaurant_Name").isNotNull() & (col("Restaurant_Name") != ""))

# Rule 2: Inspection Date cannot be null
df_dallas_clean = df_dallas_clean.filter(col("Inspection_Date").isNotNull())

# Rule 3: Inspection Type cannot be null
df_dallas_clean = df_dallas_clean.filter(col("Inspection_Type").isNotNull() & (col("Inspection_Type") != ""))

# Rule 4: Zip codes cannot be null and must be valid (5 digits)
df_dallas_clean = df_dallas_clean.filter(
    col("Zip_Code").isNotNull() & col("Zip_Code").rlike("^\\d{5}$")
)

# Rule 5: Violation score must be between 0 and 100 (negative scores like -26 found in profiling are invalid)
df_dallas_clean = df_dallas_clean.filter(
    col("Inspection_Score").isNull() | ((col("Inspection_Score") >= 0) & (col("Inspection_Score") <= 100))
)

# Count violations per row for validation
violation_desc_cols = [c for c in df_dallas_clean.columns if c.startswith("Violation_Description")]
df_dallas_clean = df_dallas_clean.withColumn(
    "violation_count",
    sum([when(col(c).isNotNull() & (col(c) != ""), lit(1)).otherwise(lit(0)) for c in violation_desc_cols])
)

# Rule 6: Every inspection must have at least 1 violation
df_dallas_clean = df_dallas_clean.filter(col("violation_count") >= 1)

# Rule 7: If score >= 90, cannot have more than 3 violations
df_dallas_clean = df_dallas_clean.filter(
    ~((col("Inspection_Score") >= 90) & (col("violation_count") > 3))
)

dallas_after = df_dallas_clean.count()
print(f"Dallas: {dallas_before} -> {dallas_after} rows ({dallas_before - dallas_after} dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Unpivot Dallas Violations (Wide to Long)
# MAGIC Convert 25 violation column groups into individual rows.

# COMMAND ----------

# Build arrays of structs for each violation slot (1-25)
# Column names after sanitization: Violation_Description_1, Violation_Points_1, etc.
violation_structs = []
for i in range(1, 26):
    desc_col = f"Violation_Description_{i}"
    pts_col = f"Violation_Points_{i}"
    detail_col = f"Violation_Detail_{i}"
    memo_col = f"Violation_Memo_{i}"

    # Check if columns exist
    if desc_col in df_dallas_clean.columns:
        violation_structs.append(
            struct(
                lit(str(i)).alias("violation_num"),
                col(desc_col).alias("violation_description"),
                col(pts_col).cast(IntegerType()).alias("violation_points"),
                col(detail_col).alias("violation_detail"),
                col(memo_col).alias("violation_comments")
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
        "Inspection_ID",
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

# Deduplicate violations per inspection (use code + description as key to avoid
# collapsing different violations when code extraction returns blank)
df_dallas_violations = df_dallas_violations.dropDuplicates(["Inspection_ID", "violation_code", "violation_description"])

# COMMAND ----------

display(
    df_dallas_violations
    .select("Inspection_ID", "violation_code", "violation_description", "violation_points", "violation_comments")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Write Dallas Silver Tables

# COMMAND ----------

# Silver - Dallas inspections (one row per inspection)
# Drop the wide violation columns
violation_cols_to_drop = [c for c in df_dallas_clean.columns if c.startswith("Violation_")]
violation_cols_to_drop += ["Lat_Long_Location", "Inspection_Month", "Inspection_Year", "violation_count"]
# Also drop individual street components (we keep Street_Address)
violation_cols_to_drop += ["Street_Number", "Street_Name", "Street_Direction", "Street_Type", "Street_Unit"]

df_dallas_silver = df_dallas_clean.drop(*violation_cols_to_drop)
# Add lineage: updated_at + durability_key
df_dallas_silver = add_silver_lineage(df_dallas_silver, ["Inspection_ID", "Restaurant_Name", "Inspection_Date"])

(
    df_dallas_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.silver_dallas_inspections")
)
print(f"Silver Dallas inspections: {df_dallas_silver.count()} rows")

# COMMAND ----------

# Silver - Dallas violations (one row per violation per inspection)
df_dallas_violations_out = (
    df_dallas_violations
    .select(
        "Inspection_ID", "violation_code", "violation_description",
        "violation_comments", "violation_points", "source_city"
    )
)
df_dallas_violations_out = add_silver_lineage(df_dallas_violations_out, ["Inspection_ID", "violation_code"])

(
    df_dallas_violations_out
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{DATABASE_NAME}.silver_dallas_violations")
)
print("Silver Dallas violations table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Validation Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'Chicago Inspections' AS table_name, COUNT(*) AS row_count FROM {DATABASE_NAME}.silver_chicago_inspections
    UNION ALL
    SELECT 'Chicago Violations', COUNT(*) FROM {DATABASE_NAME}.silver_chicago_violations
    UNION ALL
    SELECT 'Dallas Inspections', COUNT(*) FROM {DATABASE_NAME}.silver_dallas_inspections
    UNION ALL
    SELECT 'Dallas Violations', COUNT(*) FROM {DATABASE_NAME}.silver_dallas_violations
"""))

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
# MAGIC 7. Derived Inspection Result from Score (>=90=Pass, 80-89=Pass w/ Conditions, <80=Fail)
# MAGIC 8. Dropped rows: null Restaurant Name, Inspection Date, Type, Zip
# MAGIC 9. Dropped rows: Inspection Score < 0 or > 100 (profiling revealed negative scores e.g. -26, treated as invalid data entry)
# MAGIC 10. Dropped rows: no violations
# MAGIC 11. Dropped rows: score >= 90 with > 3 violations
# MAGIC 12. Unpivoted 25 wide violation columns into individual rows
# MAGIC 13. Extracted violation codes from description text
# MAGIC 14. Deduplicated violations per inspection
