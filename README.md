# 🍔 Food Aggregator Data Engineering Project with Snowflake

An end-to-end **Data Engineering project** simulating a food aggregator data platform using **Snowflake and SQL**.

The project demonstrates how raw transactional data can be ingested, transformed, and modeled into a cloud data warehouse using a **Bronze → Silver → Gold architecture**.

## 🏗️ Architecture

```text
CSV Files
   ↓
Snowflake Stage
   ↓
🥉 Bronze
   ↓
🥈 Silver
   ↓
🥇 Gold
   ↓
Fact & Dimension Tables
```

### 🥉 Bronze Layer

* Raw data ingestion
* Snowflake Stages
* `COPY INTO`
* Audit metadata

### 🥈 Silver Layer

* Data cleansing and transformation
* Incremental processing
* Snowflake Streams
* `MERGE`

### 🥇 Gold Layer

* Fact & Dimension tables
* Surrogate keys
* Analytical data model
* **SCD Type 2** for historical tracking

## 📊 Data Model

### Dimensions

* Customer
* Restaurant
* Location
* Customer Address
* Menu
* Delivery Agent
* Delivery
* Date
* Login Audit

### Fact

* Order Item Fact

## 🔄 SCD Type 2

SCD Type 2 is used to preserve historical changes in dimension records instead of overwriting previous values.

```text
Old Record → current_flag = FALSE
New Record → current_flag = TRUE
```

## ⚙️ Technologies

* Snowflake
* SQL
* Snowflake Streams
* MERGE
* SCD Type 2
* Data Warehousing
* Dimensional Modeling
* ETL / ELT
* Recursive CTEs
