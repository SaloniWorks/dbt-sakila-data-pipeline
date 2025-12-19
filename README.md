# Sakila Database ELT Pipeline (Snowflake, dbt Cloud, SQL) 

This repository contains dbt models that transform Sakila source data into a star schema in Snowflake. 
The project focuses on dimensional modeling, incremental fact loading, and data quality testing using dbt Cloud.


## Architecture

The data pipeline follows a layered architecture separating ingestion and transformation responsibilities:

- **Source**: Sakila SQLite database
- **Ingestion**: Python-based extraction and load process (external to this repository)
- **Warehouse**: Snowflake
- **Transformation**: dbt Cloud
- **Output**: Star schema (fact and dimension tables) for analytics

This repository focuses on the transformation and modeling layer implemented using dbt Cloud.


## Data Model

The warehouse is modeled using a star schema to support analytical queries on rental activities.
<img width="869" height="452" alt="image" src="https://github.com/user-attachments/assets/d87e8c3b-29b3-45df-874f-5c371efc2669" />

### Fact Table
- **rental_fact**  
  Grain: one row per `rental_id` (rental transaction).  
  Measures include rental duration, rental price, late fees, and total amount paid.

### Dimension Tables
- **customer_dim**  
  Customer attributes and location context (city and country), joined to the fact
  table using a surrogate key.

- **film_dim**  
  Film attributes including title, rating, language, and rental rate.

- **store_dim**  
  Store attributes and store location information.

- **date_dim**  
  Calendar dimension at daily grain, including weekday/weekend indicators
  and U.S. holiday flags.


## Repository Contents

This repository contains the dbt transformation layer for the Sakila warehouse:

- **models/**: dbt models organized into staging and mart layers (dimensions and fact tables)
- **snapshots/**: dbt snapshots used to track source-level changes and support Slowly Changing Dimension (Type 2) modeling in selected dimension tables
- **schema.yml** files: model documentation and data quality tests (unique, not_null, relationships, accepted values, and expression-based tests)
- **packages.yml**: dbt package dependencies (e.g., `dbt_utils`)

> Note: Source extraction and loading (SQLite → Snowflake via Python) is external to this repository.


## How to Run (dbt Cloud)

Transformations in this project are executed using **dbt Cloud**.

Typical workflow:
1. Connect the dbt Cloud project to the Snowflake warehouse
2. Run the dbt job to build models (staging, dimensions, and fact tables)
3. Execute dbt tests as part of the job to validate data quality

The dbt Cloud job is configured to run model builds and tests against the target
Snowflake environment.


## Testing & Data Quality

Data quality is enforced using dbt tests defined in schema files. Tests are executed
as part of the dbt Cloud job and validate both structural integrity and business logic
across the warehouse.

Tests implemented in this project include:
- **Primary key tests**: `not_null` and `unique` on surrogate keys
- **Referential integrity tests**:
  - Between fact and dimension tables
  - Within dimension models to validate joins to reference data (e.g., city, country, language)
- **Domain tests**: accepted values for flags and categorical fields 
- **Range and logic checks**: expression-based tests for numeric measures and date attributes
- **Incremental safety checks**: ensuring uniqueness and consistency after incremental merges
- **SCD integrity checks**: validation of surrogate key uniqueness and temporal consistency
  for Type 2 dimensions (e.g., one current record per business key)


## Notes & Assumptions

- Source data originates from the Sakila SQLite database and is loaded into Snowflake
  via an external Python-based ingestion process.
- The `rental_fact` table is built incrementally using a merge strategy to support
  efficient updates as new rental data becomes available.
- The `date_dim` is derived from observed rental dates and enriched with U.S. federal
  holiday indicators for the years 2024 and 2025.
- Dimension tables are modeled as Slowly Changing Dimensions (Type 2) to preserve
  historical attribute changes; snapshot metadata is used internally to support this behavior.


