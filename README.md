# 🍽️ Food Inspection Analytics Platform — Chicago & Dallas

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)

> A production-style data engineering and analytics platform that ingests, profiles, cleanses, and transforms **357,000+ food inspection records** from Chicago and Dallas into a unified dimensional model — powering interactive BI dashboards that surface public health insights across two major US cities.

---

## 📊 At a Glance

| Metric | Value |
|---|---|
| Total Inspections | 357,080 |
| Unique Restaurants | 36,877 |
| Cities Covered | Chicago (2010–2025), Dallas (2016–2024) |
| Gold Layer Tables | 7 (1 fact, 5 dims, 1 bridge) |
| Violations Tracked | 1M+ violation instances |
| Pipeline Notebooks | 7 |
| Dashboard Views | 3 dashboards, 12+ visualizations |

---

## 🧭 What This Project Does

Food safety is a critical public health concern — yet the data that regulators collect is often siloed, inconsistently structured, and difficult to analyze across jurisdictions. This platform solves that by:

1. **Ingesting** raw inspection data from two cities with fundamentally different schemas
2. **Profiling** the raw data to understand quality issues before any transformation
3. **Cleansing and validating** the data using business rules defined for each city
4. **Transforming** it into a unified star schema optimized for analytics
5. **Tracking history** of restaurant changes using SCD Type 2
6. **Visualizing** insights through interactive Tableau dashboards

The result: a single analytics platform where you can compare inspection outcomes, violation patterns, risk distributions, and trends across Chicago and Dallas — even though their source data looks nothing alike.

---

## 🏗️ Architecture Overview

This project follows the **Medallion Architecture** — a layered data design pattern where data is progressively refined from raw to analytics-ready.

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│   Chicago Open Data Portal          Dallas Open Data Portal      │
│   (308K+ records, 17 columns)       (79K+ records, 100+ cols)   │
└────────────────────┬────────────────────────────┬────────────────┘
                     │                            │
                     ▼                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  (Notebook 01)                                  │
│  Raw data as-is from source CSV files                            │
│  • Column name sanitization (Delta compatibility)                │
│  • Lineage columns added: source, timestamp, job ID             │
│  • No transformations — preserve original values                 │
│  Tables: bronze_chicago_inspections, bronze_dallas_inspections   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER  (Notebooks 02 + 03)                            │
│  Profiled, cleansed, validated, standardized data                │
│  • Data quality profiling using Databricks Labs DQX              │
│  • 14 validation rules (7 Chicago, 7 Dallas) — bad rows dropped  │
│  • Schema standardization across both cities                     │
│  • Violation parsing: pipe-delimited text → structured rows      │
│  • Wide violation columns (Dallas) → long format                 │
│  Tables: silver_chicago_inspections, silver_chicago_violations,  │
│          silver_dallas_inspections,  silver_dallas_violations    │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER  (Notebook 04)                                    │
│  Star schema dimensional model — analytics-ready                 │
│  • 5 dimension tables + 1 fact table + 1 bridge table            │
│  • SCD Type 2 on dim_restaurant (history tracking)               │
│  • Surrogate keys, foreign keys, lineage columns                 │
│  Tables: dim_date, dim_restaurant (SCD2), dim_location,          │
│          dim_inspection_type, dim_violation,                     │
│          fact_inspection, bridge_inspection_violation            │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  📊 BI DASHBOARDS  (Tableau Public)                              │
│  Dashboard 1: Inspection Overview (KPIs, trends, results)        │
│  Dashboard 2: Risk, Facility & Score Analysis                    │
│  Dashboard 3: Violations, Trends & Geography                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Compute | Databricks (Serverless) | Notebook execution, job orchestration |
| Storage | Delta Lake | ACID-compliant table storage with versioning |
| Processing | PySpark | Distributed data transformation |
| Data Quality | Databricks Labs DQX | Profiling and quality validation |
| Orchestration | Databricks Jobs | Pipeline automation with parameterized widgets |
| Version Control | GitHub + Databricks Repos | Code management and CI/CD |
| BI | Tableau Public | Interactive dashboards |
| Format | CSV → Delta Parquet | Raw ingestion to optimized columnar storage |

---

## 📁 Data Sources

### Chicago Food Inspections
- **Source**: Chicago Open Data Portal
- **Records**: 308,431 raw inspections
- **Time Range**: 2010–2025
- **Schema**: 17 columns including Facility Type, Risk Category (1/2/3), text-based violations (pipe-delimited), Results (Pass/Fail/Pass w/ Conditions)
- **Unique to Chicago**: Risk categories, facility types, license numbers, pipe-delimited violation text

### Dallas Restaurant Inspections
- **Source**: Dallas Open Data Portal
- **Records**: 78,984 raw inspections
- **Time Range**: 2016–2024
- **Schema**: 100+ columns — numeric inspection score (0–100), 25 structured violation column groups (Description, Points, Detail, Memo per slot)
- **Unique to Dallas**: Numeric scores, violation points, structured violation slots

> **Key Challenge**: These two datasets use fundamentally different inspection frameworks. Chicago uses pass/fail categorization with risk tiers; Dallas uses a numeric scoring system. A major goal of this project was unifying them under a common analytical model while respecting each city's schema.

---

## 🔬 Data Profiling (Notebook 02)

Before any cleansing, comprehensive profiling was performed on both raw Bronze datasets using **Databricks Labs DQX**.

<details>
<summary><b>Chicago Profiling Findings</b></summary>

| Column | Null Count | Null % | Distinct Values |
|---|---|---|---|
| Inspection_ID | 0 | 0% | 308,431 |
| DBA_Name | 0 | 0% | 34,626 |
| AKA_Name | 2,420 | 0.78% | 32,950 |
| Facility_Type | 5,323 | 1.73% | 520 |
| Violations | 86,503 | **28.05%** | 220,306 |
| Latitude / Longitude | 1,051 | 0.34% | 18,813 |
| Zip | 42 | 0.01% | 132 |

**Key Findings:**
- 28% of Chicago inspections have no violation text (valid — e.g., "Out of Business" result)
- Latitude/Longitude fields had text values from CSV multiline parsing artifacts (~20 rows)
- 520 distinct facility types — many are free-text variations (e.g., "TAVERN" vs "Tavern")
- Inspection Type had 100+ values due to inconsistent data entry (e.g., "CANVASS" vs "Canvass")
- 13 restaurant names contain intentional double quotes (e.g., `ANGELS "R" US`) — legitimate names

</details>

<details>
<summary><b>Dallas Profiling Findings</b></summary>

| Metric | Value |
|---|---|
| Total Records | 78,984 |
| Null Restaurant Names | 11 (0.01%) |
| Null Lat/Long | 8,638 (10.94%) |
| Score Range | -26 to 100 |
| Average Score | 90.86 |
| Score Distribution (Excellent 90-100) | 50,243 (63.6%) |

**Score Distribution:**
| Range | Label | Count |
|---|---|---|
| 90–100 | Excellent | 50,243 |
| 80–89 | Good | 25,564 |
| 70–79 | Fair | 2,664 |
| 0–69 | Poor | 507 |

**Key Findings:**
- 6 rows with negative scores (-26, -15, -13, -5, -5, -3) — data entry errors
- 66.97% null Street_Direction (expected — not all addresses have directions)
- Dallas has 3 inspection types: Routine (78,019), Follow-up (935), Complaint (30)
- No Risk Category or Facility Type fields — only available in Chicago

</details>

---

## 🧹 Data Cleansing & Business Rules (Notebook 03)

### Chicago Validation Rules
| Rule | Description | Action |
|---|---|---|
| R1 | Restaurant Name cannot be null | Drop row |
| R2 | Inspection Date cannot be null | Drop row |
| R3 | Inspection Type cannot be null | Drop row |
| R4 | Zip code must be 5-digit format | Drop row |
| R5 | Results cannot be null | Drop row |
| R6 | Every inspection must have at least 1 violation | Drop row |
| R7 | Result cannot be PASS if violations contain Urgent/Critical terms | Drop row |

### Dallas Validation Rules
| Rule | Description | Action |
|---|---|---|
| R1 | Restaurant Name cannot be null | Drop row |
| R2 | Inspection Date cannot be null | Drop row |
| R3 | Inspection Type cannot be null | Drop row |
| R4 | Zip code must be 5-digit format | Drop row |
| R5 | Inspection Score must be between 0 and 100 | Drop row |
| R6 | Every inspection must have at least 1 violation | Drop row |
| R7 | Score ≥ 90 cannot have more than 3 violations | Drop row |

### Key Transformations

| Transformation | Column | Detail |
|---|---|---|
| Score derivation | Chicago `inspection_score` | Pass=90, Pass w/ Conditions=80, Fail=70, No Entry=0 |
| Result derivation | Dallas `inspection_result` | ≥90=Pass, 80-89=Pass w/ Conditions, <80=Fail |
| ZIP+4 handling | Dallas `Zip_Code` | Extract first 5 digits from e.g. `75228-3007` |
| try_cast safety | Chicago/Dallas Lat/Long | Handles text artifacts → NULL instead of error |
| Quote removal | Chicago `DBA_Name`, `AKA_Name` | Remove escaped CSV double quotes |
| Violation parsing | Chicago `Violations` | Pipe-delimited text → individual rows |
| Violation unpivot | Dallas 25 violation columns | Wide → long format |
| Violation dedup | Both cities | `dropDuplicates([inspection_id, violation_code, violation_description])` |
| City/State | Dallas | Hardcoded `DALLAS` / `TX` (not in source) |

---

## 🗃️ Dimensional Model (Star Schema)

The Gold layer implements **Kimball's star schema** methodology:

```
                        ┌─────────────────┐
                        │  dim_restaurant  │
                        │  (SCD Type 2)   │
                        │  restaurant_key  │
                        │  effective_dates │
                        │  is_current      │
                        └────────┬─────────┘
                                 │
   ┌──────────┐    ┌─────────────▼──────────┐    ┌──────────────────┐
   │ dim_date │◄───┤    fact_inspection      ├───►│   dim_location   │
   │ date_key │    │    inspection_key (PK)  │    │  location_key    │
   │ full_date│    │    restaurant_key (FK)  │    │  address, zip    │
   │ year     │    │    location_key (FK)    │    │  latitude, long  │
   │ month    │    │    date_key (FK)        │    └──────────────────┘
   │ quarter  │    │    insp_type_key (FK)   │
   └──────────┘    │    inspection_result    │    ┌──────────────────┐
                   │    inspection_score     ├───►│dim_inspection_type│
                   └─────────────┬───────────┘    │ inspection_type  │
                                 │               └──────────────────┘
                                 ▼
                   ┌─────────────────────────┐
                   │ bridge_inspection_       │
                   │ violation               │
                   │ inspection_key (FK)     │
                   │ violation_key (FK)      │
                   └─────────────┬───────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │  dim_violation   │
                        │  violation_key   │
                        │  violation_code  │
                        │  violation_desc  │
                        │  violation_pts   │
                        └─────────────────┘
```

### Table Summary

| Table | Type | Rows (approx) | Description |
|---|---|---|---|
| `fact_inspection` | Fact | 357,080 | One row per inspection |
| `dim_restaurant` | SCD Type 2 | 45,550 | Restaurant master with history |
| `dim_date` | Fixed Dimension | ~5,500 | Calendar from 2010–2025 |
| `dim_location` | Fixed Dimension | ~35,000 | Unique address + coordinates |
| `dim_inspection_type` | Fixed Dimension | ~15 | Inspection categories |
| `dim_violation` | Fixed Dimension | ~990 | Violation codes + descriptions |
| `bridge_inspection_violation` | Bridge | 1M+ | M:M: inspections ↔ violations |

---

## 🔄 SCD Type 2 — Historical Restaurant Tracking

### What is SCD Type 2?

Slowly Changing Dimension Type 2 (SCD2) is a technique for tracking **historical changes** to dimension data. Instead of overwriting a changed record, the old record is **expired** and a new **current** record is inserted — preserving full history.

**Use case here**: A restaurant that changes from a `Restaurant` to a `Bakery` (facility type change) needs both records preserved so historical inspection data remains accurate.

### Implementation

```python
# Step 1: Expire changed rows
target.merge(staging, on=business_key & is_current=True)
  .whenMatchedUpdate(condition=attribute_changed, set={
      "effective_end_date": current_date(),
      "is_current": False
  })

# Step 2: Insert new current rows (changed + brand new)
new_rows = staging LEFT ANTI JOIN target ON (business_key AND is_current=True)
new_rows.write.mode("append")  # Never overwrite — preserve history
```

### SCD2 Columns on dim_restaurant

| Column | Description |
|---|---|
| `effective_start_date` | Date when this version became active |
| `effective_end_date` | Date when this version was superseded (`9999-12-31` = currently active) |
| `is_current` | Boolean flag — `True` for the active version |
| `restaurant_key` | Surrogate key (new key generated for each version) |

### Tracked Attributes
Changes to any of these trigger a new version:
- `facility_type` (e.g., Restaurant → Bakery)
- `risk_category` (e.g., Risk 1 High → Risk 2 Medium)
- `aka_name` (doing-business-as name change)

### Test Results

Three scenarios were tested and verified:

| Test | Scenario | Result |
|---|---|---|
| Test 1 | Change `facility_type` | Old row expired, new row inserted with correct dates ✅ |
| Test 2 | Change `risk_category` + `aka_name` simultaneously | Both attributes tracked, 2 expired rows ✅ |
| Test 3 | Insert brand new restaurant | New row inserted with `is_current=True` ✅ |

**Final state after testing**: 45,548 current rows + 2 expired rows — confirmed via Delta table history (`DESCRIBE HISTORY`).

---

## 📊 BI Dashboards

Three dashboards built in Tableau Public covering all required analytical dimensions:

### Dashboard 1: Inspection Overview
- **KPI Cards**: 357,080 total inspections | 36,877 unique restaurants | 51.6% overall pass rate
- **Results by City** (stacked bar): Chicago vs Dallas pass/fail breakdown
- **Inspections Over Time** (line chart): Volume trends 2010–2025

### Dashboard 2: Risk, Facility & Score Analysis
- **Chicago Risk Distribution**: Risk 1 (High): 172K | Risk 2 (Medium): 38K | Risk 3 (Low): 12K
- **Chicago Top 10 Facility Types**: Restaurants dominate with 150K+ inspections
- **Dallas Score Distribution** (histogram): Majority score 90–95 (passing)
- **Inspections by Type**: Routine/Canvass dominate both cities

### Dashboard 3: Violations, Trends & Geography
- **Top 15 Violation Codes**: Violation 39 most common in Dallas (2.2M instances)
- **Pass Rate Trend**: Dallas consistently 85–93% vs Chicago 55–75%
- **Most Inspected Businesses**: Subway, Dunkin Donuts, McDonald's top the list
- **Inspection Locations Map**: Geographic distribution in Chicago and Dallas metro areas

### Key Insights

| Insight | Finding |
|---|---|
| Pass rate gap | Dallas (~90%) significantly outperforms Chicago (~65%) |
| Chicago 2019 dip | Chicago pass rate dropped to ~47% — lowest in 15 years |
| Dallas consistency | Average inspection score stable at ~89–90 across all years |
| COVID impact | Both cities show inspection volume drop in 2020 |
| Risk concentration | 70%+ of Chicago inspections are Risk 1 (High) |
| Chain dominance | Subway has 3,600+ inspections — most of any restaurant |

---

## 📂 Project Structure

```
FoodInspection_Chicago_Dallas/
│
├── notebooks/                          # Databricks PySpark notebooks
│   ├── 00_setup_config.py              # Widget parameters, DB setup, DQX install
│   ├── 01_bronze_ingestion.py          # Raw CSV → Bronze Delta tables
│   ├── 02_data_profiling_dqx.py        # DQX profiling on Bronze layer
│   ├── 03_silver_cleansing.py          # Validation rules, transformations → Silver
│   ├── 04_gold_dim_load.py             # Star schema construction → Gold layer
│   ├── 05_gold_scd2.py                 # SCD2 validation queries
│   └── 06_scd2_test.py                 # SCD2 test scenarios (3 tests)
│
├── data/
│   ├── metadata/                       # Source data dictionaries
│   ├── tableau_data_export/            # CSV exports for Tableau
│   │   ├── fact_inspections_denormalized.csv
│   │   ├── summary_results_by_year.csv
│   │   ├── summary_risk_facility.csv
│   │   └── summary_top_violations.csv
│   └── results_export/
│       ├── profiling_exports/          # DQX profiling results (Chicago + Dallas)
│       └── gold_tables/                # Gold layer sample exports
│
├── dimensional_model/
│   └── Dimensional_Model.png           # Star schema diagram
│
├── docs/
│   ├── Source_to_Target_Mapping.xlsx   # Source-to-target field mappings
│   └── generate_mapping_doc.py         # Script to regenerate mapping doc
│
├── screenshots/                        # 65+ execution screenshots
│   └── SQL Data Inspection/            # Data inspection query results
│
├── tableau/
│   └── dashboards/                     # Dashboard screenshots
│
├── DOCUMENTATION.docx                  # Full project documentation (Word)
├── DOCUMENTATION.md                    # Markdown version
└── README.md                           # This file
```

---

## 🚀 How to Run

### Prerequisites

- Databricks workspace (Community Edition or above)
- Databricks Unity Catalog or Hive Metastore
- Databricks Volume for file storage
- GitHub account (for Repos integration)

### Step 1: Upload Raw Data

Upload the source CSVs to your Databricks Volume:
```
/Volumes/<catalog>/food_inspection/raw_data/
  ├── Food_Inspections_<date>.csv
  └── Restaurant_and_Food_Establishment_Inspections_<date>.csv
```

### Step 2: Connect Repo

In Databricks: **Repos → Add Repo → GitHub URL** → link this repository.

### Step 3: Configure the Pipeline Job

Create a Databricks Job with 3 tasks in sequence:

```
01_bronze_ingestion → 03_silver_cleansing → 04_gold_dim_load
```

Set job-level parameters:
```json
{
  "chicago_file": "Food_Inspections_<date>.csv",
  "dallas_file": "Restaurant_and_Food_Establishment_Inspections_<date>.csv",
  "database_name": "food_inspection",
  "volume_path": "/Volumes/<catalog>/food_inspection/raw_data"
}
```

### Step 4: Run the Pipeline

Click **Run Now** in Databricks Jobs. The full pipeline completes in ~4 minutes (Serverless).

### Step 5: Validate

Run Notebook 05 to validate the Gold layer and SCD2 integrity:
```python
# Expected output:
# is_current=True:  45,548 rows
# is_current=False: 2 rows (from SCD2 test)
# ✓ CHECK PASSED: Every restaurant has exactly 1 current row
# ✓ CHECK PASSED: All expired rows have a valid end date
# ✓ CHECK PASSED: All current rows have end_date = 9999-12-31
```

### Step 6: Connect to Tableau

Export Gold layer data using the SQL queries in `data/tableau_data_export/` and load CSVs into Tableau Public.

---

## ⚠️ Known Limitations & Design Notes

| Item | Note |
|---|---|
| Dallas Inspection IDs | Generated via `monotonically_increasing_id()` — non-deterministic across reruns. Hash-based IDs from business columns would be more robust for production. |
| Dallas Risk & Facility | Dallas source data does not capture risk category or facility type — NULL values are intentional, not missing. |
| SCD2 dating | `effective_end_date` is inclusive (row was valid through that date). Both expire and insert use `current_date()` — standard for batch SCD2. |
| Bronze column names | Column names sanitized for Delta compatibility (e.g., `License #` → `License`). Data values are preserved as-is. |
| 2026 data | Partial year (data pulled in April 2026) — exclude from trend analysis for accurate YoY comparison. |
| Lat/Long quality | ~1,000 Chicago rows have null coordinates due to CSV multiline parsing artifacts. Handled via `try_cast`. |

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
