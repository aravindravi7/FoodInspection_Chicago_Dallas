# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Data Profiling with DQX
# MAGIC **Part 1 of the assignment**: Profile the data and document findings.
# MAGIC
# MAGIC Goal: Understand structure, quality, characteristics, and find common attributes for merging.

# COMMAND ----------

# MAGIC %run ./00_setup_config

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Tables

# COMMAND ----------

df_chicago = spark.table(f"{DATABASE_NAME}.bronze_chicago_inspections")
df_dallas = spark.table(f"{DATABASE_NAME}.bronze_dallas_inspections")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chicago Data Profiling

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Schema Overview

# COMMAND ----------

print(f"Chicago - Rows: {df_chicago.count()}, Columns: {len(df_chicago.columns)}")
print("\nColumns:")
for c in df_chicago.columns:
    print(f"  - {c}")

# COMMAND ----------

df_chicago.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Null Analysis - Chicago

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan, round as spark_round, countDistinct, min, max, avg

# Null counts and percentages for Chicago
chicago_total = df_chicago.count()

null_analysis_chicago = df_chicago.select([
    count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
    for c in df_chicago.columns
])

print("=== CHICAGO: Null/Empty Counts ===")
display(null_analysis_chicago)

# COMMAND ----------

# Null percentage
null_pct_chicago = df_chicago.select([
    spark_round((count(when(col(c).isNull() | (col(c) == ""), c)) / chicago_total * 100), 2).alias(c)
    for c in df_chicago.columns
])

print("=== CHICAGO: Null/Empty Percentage ===")
display(null_pct_chicago)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Distinct Value Counts - Chicago

# COMMAND ----------

distinct_counts_chicago = df_chicago.select([
    countDistinct(col(c)).alias(c) for c in df_chicago.columns
])

print("=== CHICAGO: Distinct Value Counts ===")
display(distinct_counts_chicago)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Key Column Distributions - Chicago

# COMMAND ----------

# Inspection Results distribution
print("=== Chicago: Inspection Results Distribution ===")
display(
    df_chicago.groupBy("Results")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# Inspection Type distribution
print("=== Chicago: Inspection Type Distribution ===")
display(
    df_chicago.groupBy("Inspection_Type")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# Risk distribution
print("=== Chicago: Risk Category Distribution ===")
display(
    df_chicago.groupBy("Risk")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# Facility Type distribution (top 20)
print("=== Chicago: Top 20 Facility Types ===")
display(
    df_chicago.groupBy("Facility_Type")
    .count()
    .orderBy(col("count").desc())
    .limit(20)
)

# COMMAND ----------

# Zip code distribution (top 20)
print("=== Chicago: Top 20 Zip Codes ===")
display(
    df_chicago.groupBy("Zip")
    .count()
    .orderBy(col("count").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Chicago Violations Analysis
# MAGIC Note: Chicago violations are stored as a single pipe-delimited string.

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim, length, regexp_extract

# Sample violations to understand structure
display(
    df_chicago.select("Inspection_ID", "Violations")
    .filter(col("Violations").isNotNull())
    .limit(5)
)

# COMMAND ----------

# Count inspections with no violations
no_violations_chicago = df_chicago.filter(col("Violations").isNull() | (col("Violations") == "")).count()
print(f"Chicago inspections with NO violations: {no_violations_chicago} ({no_violations_chicago/chicago_total*100:.2f}%)")
print(f"Chicago inspections WITH violations: {chicago_total - no_violations_chicago}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dallas Data Profiling

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Schema Overview

# COMMAND ----------

print(f"Dallas - Rows: {df_dallas.count()}, Columns: {len(df_dallas.columns)}")
print("\nColumns:")
for c in df_dallas.columns:
    print(f"  - {c}")

# COMMAND ----------

df_dallas.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Null Analysis - Dallas

# COMMAND ----------

dallas_total = df_dallas.count()

null_analysis_dallas = df_dallas.select([
    count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
    for c in df_dallas.columns
])

print("=== DALLAS: Null/Empty Counts (key columns) ===")
display(null_analysis_dallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Key Column Distributions - Dallas

# COMMAND ----------

# Inspection Type distribution
print("=== Dallas: Inspection Type Distribution ===")
display(
    df_dallas.groupBy("Inspection_Type")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

# Inspection Score statistics
print("=== Dallas: Inspection Score Statistics ===")
display(
    df_dallas.select(
        min("Inspection_Score").alias("min_score"),
        max("Inspection_Score").alias("max_score"),
        avg("Inspection_Score").alias("avg_score"),
        count(when(col("Inspection_Score") > 100, True)).alias("scores_above_100"),
        count(when(col("Inspection_Score").isNull(), True)).alias("null_scores")
    )
)

# COMMAND ----------

# Score distribution
print("=== Dallas: Score Distribution (binned) ===")
display(
    df_dallas.withColumn("score_bin",
        when(col("Inspection_Score") >= 90, "90-100 (Excellent)")
        .when(col("Inspection_Score") >= 80, "80-89 (Good)")
        .when(col("Inspection_Score") >= 70, "70-79 (Fair)")
        .when(col("Inspection_Score") >= 0, "0-69 (Poor)")
        .otherwise("No Score")
    )
    .groupBy("score_bin")
    .count()
    .orderBy("score_bin")
)

# COMMAND ----------

# Zip code distribution (top 20)
print("=== Dallas: Top 20 Zip Codes ===")
display(
    df_dallas.groupBy("Zip_Code")
    .count()
    .orderBy(col("count").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Dallas Violations Analysis
# MAGIC Note: Dallas has 25 violation groups stored as wide columns (Violation_Description_1 through 25).

# COMMAND ----------

# Count how many violations each inspection has
from pyspark.sql.functions import lit

violation_desc_cols = [c for c in df_dallas.columns if c.startswith("Violation_Description")]

df_dallas_violation_count = df_dallas.withColumn(
    "violation_count",
    sum([when(col(c).isNotNull() & (col(c) != ""), lit(1)).otherwise(lit(0)) for c in violation_desc_cols])
)

print("=== Dallas: Violations Per Inspection Distribution ===")
display(
    df_dallas_violation_count.groupBy("violation_count")
    .count()
    .orderBy("violation_count")
)

# COMMAND ----------

# Inspections with zero violations
no_violations_dallas = df_dallas_violation_count.filter(col("violation_count") == 0).count()
print(f"Dallas inspections with NO violations: {no_violations_dallas} ({no_violations_dallas/dallas_total*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Dallas - Validate score >= 90 with > 3 violations (rule check)

# COMMAND ----------

high_score_many_violations = (
    df_dallas_violation_count
    .filter((col("Inspection_Score") >= 90) & (col("violation_count") > 3))
    .count()
)
print(f"Dallas records with score >= 90 AND > 3 violations: {high_score_many_violations}")
print("These rows will be flagged/dropped in Silver layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Visual Data Summarization (Built-in Profiling)
# MAGIC These generate rich visual profiling reports directly in the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Chicago - Visual Summary

# COMMAND ----------

dbutils.data.summarize(df_chicago)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Dallas - Visual Summary

# COMMAND ----------

dbutils.data.summarize(df_dallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DQX Profiling

# COMMAND ----------

from databricks.labs.dqx.profiler import DQProfiler

profiler = DQProfiler(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 DQX Profile - Chicago

# COMMAND ----------

chicago_profile = profiler.profile(df_chicago)
display(chicago_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 DQX Profile - Dallas

# COMMAND ----------

dallas_profile = profiler.profile(df_dallas)
display(dallas_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Common Attributes Mapping (for merging)
# MAGIC
# MAGIC | Concept | Chicago Column | Dallas Column | Notes |
# MAGIC |---|---|---|---|
# MAGIC | Restaurant Name | `DBA_Name` | `Restaurant_Name` | Direct mapping |
# MAGIC | Also Known As | `AKA_Name` | N/A | Chicago only |
# MAGIC | License # | `License` | N/A | Chicago only |
# MAGIC | Facility Type | `Facility_Type` | N/A | Chicago only |
# MAGIC | Risk Category | `Risk` | N/A | Chicago only |
# MAGIC | Inspection Date | `Inspection_Date` | `Inspection_Date` | Direct mapping |
# MAGIC | Inspection Type | `Inspection_Type` | `Inspection_Type` | Values may differ |
# MAGIC | Inspection Result | `Results` | Derived from Score | Chicago has text, Dallas needs derivation |
# MAGIC | Inspection Score | Derived from Results | `Inspection_Score` | Dallas has numeric, Chicago needs derivation |
# MAGIC | Address | `Address` | `Street_Address` | Direct mapping |
# MAGIC | City | `City` | Hardcode "DALLAS" | Dallas dataset doesn't have city column |
# MAGIC | State | `State` | Hardcode "TX" | Dallas dataset doesn't have state column |
# MAGIC | Zip | `Zip` | `Zip_Code` | Direct mapping |
# MAGIC | Latitude | `Latitude` | Parse from `Lat_Long_Location` | Dallas combines lat/long |
# MAGIC | Longitude | `Longitude` | Parse from `Lat_Long_Location` | Dallas combines lat/long |
# MAGIC | Violations | Single pipe-delimited string | 25 wide column groups | Need to standardize into rows |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Summary
# MAGIC
# MAGIC ### Chicago Issues Found:
# MAGIC - Violations stored as unstructured pipe-delimited text (needs parsing)
# MAGIC - No numeric inspection score (needs derivation from Results)
# MAGIC - Potential null values in key fields (to be validated in Silver)
# MAGIC
# MAGIC ### Dallas Issues Found:
# MAGIC - 114 columns with wide violation format (needs unpivoting to rows)
# MAGIC - Missing city and state columns (hardcode Dallas, TX)
# MAGIC - Lat/Long combined in single field (needs parsing)
# MAGIC - Scores potentially > 100 (invalid, to be dropped in Silver)
# MAGIC - High-score inspections with too many violations (to be dropped in Silver)
# MAGIC - No explicit inspection result text (needs derivation from score)
