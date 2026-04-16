Enterprise Analytics Hub: ERP Merger ELT Pipeline
Project Overview
This project simulates a complex, real-world Data Engineering and Business Intelligence scenario: the corporate merger between NovaTech Global Industries (US) and AeroForge Dynamics (EU). The objective was to design and implement a scalable ELT (Extract, Load, Transform) pipeline to consolidate 100,000 unstandardized transactional records from two distinct legacy ERP systems into a single, analytics-ready Data Mart.

Architecture & Tech Stack

Language: Python (Pandas, Numpy, Boto3)

Data Lake: AWS S3

Data Warehouse: Snowflake

Data Transformation & Testing: dbt Cloud (Data Build Tool)

Version Control: GitHub

Phase 1: Data Extraction & Loading (ELT)
To replicate a highly chaotic corporate data environment, a Python script was developed to generate synthetic ERP data containing common business anomalies: mismatched date formats, unstructured JSON metadata, mixed currencies, and duplicate records.

The raw dataset was programmatically uploaded to an AWS S3 bucket acting as the initial Data Lake.

The data was then ingested into Snowflake using the COPY INTO command.

Technical Challenge Solved: Configured the ingestion File Format with FIELD_OPTIONALLY_ENCLOSED_BY = '"' to prevent the default CSV parser from incorrectly splitting columns due to internal commas within the JSON payload.

Phase 2: Data Transformation (dbt Staging)
A staging model (stg_global_transactions) was built to act as the primary cleaning layer, enforcing strict business rules and standardizing the data.

Date Normalization: Utilized CASE statements to harmonize US formats (MM/DD/YYYY), European formats (DD-MM-YYYY), and Unix Timestamps into a standard YYYY-MM-DD structure.

Currency Conversion: Applied dynamic exchange rates to convert USD and GBP transactions into a unified EUR amount (AMOUNT_EUR).

Semi-Structured Data Flattening: Extracted specific attributes (category, warranty_years) directly from the JSON VARIANT column using Snowflake's dot/colon notation.

Data Deduplication: Implemented Window Functions (QUALIFY ROW_NUMBER() OVER(PARTITION BY TRANSACTION_ID ORDER BY NORMALIZED_DATE DESC) = 1) to eliminate duplicate transactions, ensuring only the most recent entry per invoice was retained.

Phase 3: Automated Data Quality & Governance
To ensure the integrity of downstream BI dashboards, automated testing was configured within dbt (_models.yml).

Enforced unique and not_null constraints on the primary key (TRANSACTION_ID).

Applied not_null checks on financial metrics to prevent empty revenue values.

Implemented accepted_values testing to ensure the source system column strictly contained approved categorical data, leveraging the latest dbt 2.0 configuration standards.

Phase 4: Business Intelligence Data Mart (Fact Modeling)
The final output is a clean, optimized Fact Table (fct_global_sales) designed for direct consumption by BI tools like Power BI or Tableau.

Extracted chronological dimensions (SALES_MONTH) for trend analysis.

Applied financial business logic to calculate ESTIMATED_PROFIT_EUR based on dynamic product category margins.

Implemented automatic CUSTOMER_SEGMENT logic to categorize clients into VIP, Mid-Market, and Small Business tiers based on transaction volume.

Business Value
This pipeline bridges the gap between operational ERP systems and strategic decision-making. By automating the cleaning, deduplication, and financial standardization of legacy data, the business can now instantly visualize global sales performance across newly merged entities without manual Excel interventions.
•	Applied financial business logic to calculate ESTIMATED_PROFIT_EUR based on dynamic product category margins.
•	Implemented automatic CUSTOMER_SEGMENT logic to categorize clients into VIP, Mid-Market, and Small Business tiers based on transaction volume.
