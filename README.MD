**# 🏦 Banking Data Lakehouse**

A data engineering project that simulates a modern banking data platform, focusing on data ingestion, distributed processing, data quality, Medallion architecture, analytical data modeling, dbt transformations, and Lakehouse technologies.

The project uses completely synthetic banking data to simulate a real-world environment containing customers, accounts, transactions, cards, merchants, loans, and loan payments.

\> 🚧 **\*\*Project Status:\*\*** Active Development — End-to-End Data Engineering Pipeline Operational

\---

**# 🎯 Project Goal**

The main goal of this project is to build an end-to-end banking data platform capable of:

\* Generating realistic synthetic banking data

\* Ingesting and processing large datasets

\* Storing data in a Data Lake architecture

\* Processing large datasets with Apache Spark

\* Implementing Bronze, Silver, and Gold layers

\* Applying data quality and business rules

\* Detecting and isolating invalid records

\* Building analytical data models

\* Implementing transformation workflows with dbt

\* Applying automated data tests

\* Documenting analytical data models

\* Preparing the platform for a Lakehouse architecture

\* Supporting analytics and machine learning workloads

\* Orchestrating the complete pipeline

The project is designed to demonstrate practical Data Engineering concepts rather than simply building a traditional database.

\---

**# 🏗️ Architecture**

The project follows a Medallion-style Data Lake architecture.

The current implementation includes a complete Spark-based ETL pipeline, data quality validation, Gold analytical models, Apache Iceberg tables on MinIO/S3, and Airflow orchestration.

\`\`\`text

                         DATA SOURCES

                    Synthetic Banking Data

                              │

                              ▼

                         INGESTION

                           Python

                              │

                              ▼

                       🥉 BRONZE

                    Raw / Partitioned

                              │

                              ▼

                        Apache Spark

                              │

                    ┌─────────┴─────────┐

                    │                   │

                    ▼                   ▼

                 SILVER            QUARANTINE

              Clean / Trusted      Invalid Data

                    │

                    ▼

                 🥇 GOLD

             Analytical Models

                    │

                    ▼

                   dbt

                    │

          ┌─────────┼─────────┐

          ▼         ▼         ▼

      STAGING  INTERMEDIATE  MARTS

                              │

                    ┌─────────┴─────────┐

                    ▼                   ▼

                   BI                   ML

             Future Lakehouse Layer

                    │

                    ▼

               Apache Iceberg

                    │

                    ▼

                 MinIO / S3

                    │

                    ▼

                 Airflow

\`\`\`

\---

**# 🛠️ Technology Stack**

**## Data Generation**

\* Python

\* Faker

\* NumPy

\* Pandas

\* PyArrow

**## Data Processing**

\* Apache Spark

\* PySpark

**## Storage**

\* Apache Parquet

\* MinIO / S3 *\*(planned)\**

**## Lakehouse**

\* Apache Iceberg *\*(planned)\**

**## Transformation**

\* dbt Core

\* dbt-duckdb

\* DuckDB

**## Orchestration**

\* Apache Airflow *\*(planned)\**

**## Infrastructure**

\* Docker

\* Docker Compose *\*(planned)\**

**## Analytics**

\* BigQuery *\*(planned)\**

\* Looker Studio / Metabase *\*(planned)\**

**## Machine Learning**

\* Python

\* Scikit-learn *\*(planned)\**

\---

**# 📊 Data Domain**

The simulated banking environment contains the following entities:

\`\`\`text

                         CUSTOMER

                        /        \\

                       /          \\

                      ▼            ▼

                  ACCOUNT         LOAN

                 /      \           │

                /        \          ▼

               ▼          ▼   LOAN_PAYMENT

             CARD     TRANSACTION

                          │

                          ▼

                       MERCHANT

\`\`\`

**## Customers**

Contains customer information and segmentation.

\`\`\`text

customer_id

first_name

last_name

birth_date

document

email

phone

city

state

customer_segment

created_at

\`\`\`

Customer segments:

\`\`\`text

BASIC

STANDARD

PREMIUM

\`\`\`

**## Accounts**

Represents customer bank accounts.

\`\`\`text

account_id

customer_id

account_type

branch

account_number

status

opened_at

closed_at

\`\`\`

Account types:

\`\`\`text

CHECKING

SAVINGS

\`\`\`

**## Transactions**

The largest dataset in the project.

\`\`\`text

transaction_id

account_id

transaction_type

amount

currency

status

transaction_timestamp

merchant_id

description

\`\`\`

Transaction types include:

\`\`\`text

PIX

CARD_PURCHASE

TRANSFER

DEPOSIT

WITHDRAWAL

TED

DOC

\`\`\`

Business rules include:

\* Every transaction must belong to an existing account.

\* Transaction amounts must be greater than zero.

\* Currency must be BRL.

\* \`CARD_PURCHASE\` transactions must have a valid \`merchant_id\`.

\* Other transaction types may have a NULL \`merchant_id\`.

\* Transaction IDs must be unique.

**## Merchants**

Represents establishments where card transactions can occur.

\`\`\`text

merchant_id

merchant_name

merchant_category

city

state

created_at

\`\`\`

Merchant categories include:

\`\`\`text

SUPERMARKET

RESTAURANT

GAS_STATION

PHARMACY

CLOTHING

ELECTRONICS

ENTERTAINMENT

TRAVEL

SERVICES

OTHER

\`\`\`

**## Cards**

Represents debit and credit cards associated with bank accounts.

\`\`\`text

card_id

account_id

card_type

brand

status

credit_limit

issued_at

expires_at

\`\`\`

Business rules include:

\* Savings accounts can only have debit cards.

\* Credit cards must have a credit limit.

\* Debit cards do not have a credit limit.

\* Cards must belong to existing accounts.

**## Loans**

Represents customer loans.

\`\`\`text

loan_id

customer_id

loan_type

principal_amount

interest_rate

term_months

status

created_at

\`\`\`

Loan types:

\`\`\`text

PERSONAL

PAYROLL

AUTO

\`\`\`

Loan statuses:

\`\`\`text

ACTIVE

PAID

DEFAULTED

CANCELLED

\`\`\`

**## Loan Payments**

Represents individual loan installments.

\`\`\`text

payment_id

loan_id

payment_number

due_date

payment_date

amount

status

\`\`\`

Payment statuses:

\`\`\`text

PAID

PENDING

LATE

\`\`\`

\---

**# 📦 Synthetic Dataset**

The current project generates:

\| Dataset       |   Records |

\| ------------- | --------: |

\| Customers     |   100,000 |

\| Accounts      |   150,025 |

\| Merchants     |    10,000 |

\| Transactions  | 5,000,000 |

\| Cards         |   120,000 |

\| Loans         |    30,000 |

\| Loan Payments |   740,448 |

All data is completely synthetic and generated locally.

Generated datasets are intentionally excluded from version control because of their size.

\---

**# 📁 Project Structure**

\`\`\`text

banking-data-lakehouse/

│

├── data/

│   ├── generated/

│   ├── bronze/

│   ├── silver/

│   ├── gold/

│   └── quarantine/

│

├── scripts/

│   ├── generate_customers.py

│   ├── generate_accounts.py

│   ├── generate_transactions.py

│   ├── generate_merchants.py

│   ├── generate_cards.py

│   ├── generate_loans.py

│   ├── generate_loan_payments.py

│   ├── profile_data.py

│   ├── build_bronze.py

│   ├── profile_silver_data.py

│   └── profile_gold_data.py

│

├── spark/

│   ├── silver/

│   │   ├── build_customers_silver.py

│   │   ├── build_accounts_silver.py

│   │   ├── build_transactions.py

│   │   ├── build_merchants_silver.py

│   │   ├── build_cards_silver.py

│   │   ├── build_loans_silver.py

│   │   └── build_loan_payments_silver.py

│   │

│   └── gold/

│       ├── build_daily_transaction_summary.py

│       ├── build_customer_transaction_metrics.py

│       ├── build_merchant_performance.py

│       ├── build_loan_portfolio.py

│       └── build_customer_360.py

│

├── dbt/

│   └── banking_dbt/

│       ├── models/

│       │   ├── staging/

│       │   │   ├── stg_customers.sql

│       │   │   ├── stg_accounts.sql

│       │   │   ├── stg_transactions.sql

│       │   │   ├── stg_merchants.sql

│       │   │   ├── stg_cards.sql

│       │   │   ├── stg_loans.sql

│       │   │   ├── stg_loan_payments.sql

│       │   │   └── staging.yml

│       │   │

│       │   ├── intermediate/

│       │   │   ├── int_customer_transactions.sql

│       │   │   ├── int_merchant_transactions.sql

│       │   │   ├── int_loan_payments.sql

│       │   │   ├── int_customer_loans.sql

│       │   │   └── intermediate.yml

│       │   │

│       │   └── marts/

│       │       ├── fct_daily_transactions.sql

│       │       ├── fct_merchant_performance.sql

│       │       ├── fct_loan_portfolio.sql

│       │       ├── fct_customer_transactions.sql

│       │       ├── customer_360.sql

│       │       └── marts.yml

│       │

│       ├── dbt_project.yml

│       └── packages.yml

│

├── tests/

├── pipeline.py

├── requirements.txt

├── .gitignore

└── README.md

\`\`\`

\---

**# 🥉 Bronze Layer**

The Bronze layer represents the raw data ingestion layer.

Data is stored as Apache Parquet and partitioned by year and month based on the relevant timestamp/date column.

Example:

\`\`\`text

data/bronze/transactions/

├── year=2024/

│   ├── month=01/

│   ├── month=02/

│   └── ...

├── year=2025/

│   └── ...

└── year=2026/

    ├── month=01/

    ├── month=02/

    └── ...

\`\`\`

The Bronze ingestion process was designed to handle large datasets without loading the entire dataset into memory.

Data is processed in batches using PyArrow.

This is particularly important for the 5 million transaction dataset.

\---

**# 🥈 Silver Layer**

The Silver layer is implemented using Apache Spark.

Its purpose is to transform Bronze data into clean, validated, and trusted datasets.

Current transformations include:

\* String standardization

\* Text trimming

\* Categorical normalization

\* Duplicate detection

\* Primary key validation

\* Foreign key validation

\* Business rule validation

\* Schema/type normalization

\* Partitioned output

\* Invalid record quarantine

The processing flow is:

\`\`\`text

BRONZE

   │

   ▼

Apache Spark

   │

   ├── Standardization

   ├── Validation

   ├── Deduplication

   ├── Business Rules

   │

   ├───────────────┐

   ▼               ▼

SILVER        QUARANTINE

\`\`\`

The Silver layer currently contains validated datasets for:

\* Customers

\* Accounts

\* Merchants

\* Transactions

\* Cards

\* Loans

\* Loan Payments

\---

**# 🔎 Data Quality**

Data quality is treated as a core part of the pipeline.

Validation is performed during Silver processing and through dedicated QA scripts.

**## Primary Key Validation**

Each Silver dataset is validated for:

\* NULL primary keys

\* Duplicate primary keys

Current results:

\`\`\`text

Dataset                     Records     Duplicates     NULL PK

\----------------------------------------------------------------

customers                   100,000              0           0

accounts                    150,025              0           0

merchants                    10,000              0           0

transactions              5,000,000              0           0

cards                       120,000              0           0

loans                        30,000              0           0

loan_payments               740,448              0           0

\`\`\`

**## Referential Integrity**

Foreign key relationships are validated between related datasets.

Examples:

\`\`\`text

transactions.account_id

        ↓

accounts.account_id

\`\`\`

\`\`\`text

transactions.merchant_id

        ↓

merchants.merchant_id

\`\`\`

\`\`\`text

cards.account_id

        ↓

accounts.account_id

\`\`\`

\`\`\`text

loans.customer_id

        ↓

customers.customer_id

\`\`\`

\`\`\`text

loan_payments.loan_id

        ↓

loans.loan_id

\`\`\`

The Silver QA process currently reports no invalid foreign key relationships.

\---

**# 🥇 Gold Layer**

The Gold layer contains business-oriented analytical datasets generated from the validated Silver layer.

Current Gold models:

\`\`\`text

data/gold/

├── daily_transaction_summary/

├── customer_transaction_metrics/

├── merchant_performance/

├── loan_portfolio/

└── customer_360/

\`\`\`

**## Daily Transaction Summary**

Aggregates transactions by:

\`\`\`text

transaction_date

transaction_type

status

\`\`\`

Metrics include:

\`\`\`text

transaction_count

total_amount

average_amount

minimum_amount

maximum_amount

\`\`\`

This model supports time-series transaction analysis.

**## Customer Transaction Metrics**

Provides transaction-level metrics aggregated by customer.

Metrics include customer segmentation, transaction volume, transaction amounts, transaction types, status metrics, and transaction timestamps.

**## Merchant Performance**

Provides performance metrics for merchants, including transaction volume, transaction amounts, transaction status, and merchant attributes.

**## Loan Portfolio**

Provides a consolidated view of the loan portfolio and payment behavior.

Metrics include:

\`\`\`text

loan_id

customer_id

loan_type

principal_amount

interest_rate

term_months

loan_status

total_payments

paid_payments

pending_payments

late_payments

total_paid_amount

remaining_amount

\`\`\`

\> \`remaining_amount\` is currently a simplified analytical metric calculated from principal amount minus total paid amount. It does not represent a real amortization balance.

**## Customer 360**

The Customer 360 model consolidates information from multiple banking domains into a single customer-level analytical dataset.

It combines:

\`\`\`text

CUSTOMERS

   │

   ├── ACCOUNTS

   ├── TRANSACTIONS

   ├── CARDS

   ├── LOANS

   └── LOAN PAYMENTS

\`\`\`

The model contains customer-level metrics related to:

\* Accounts

\* Transactions

\* Cards

\* Loans

\* Loan payments

\* Defaulted loans

\* Late payments

\* Total amounts

This model will serve as an important foundation for future analytics and machine learning workloads.

\---

**# 🧪 Gold Quality Assurance**

The Gold layer has a dedicated QA process.

Validation checks include:

\* Primary key uniqueness

\* NULL primary keys

\* Composite key uniqueness

\* Record count reconciliation

\* Referential integrity

\* Metric consistency

\* Negative metric detection

\* Aggregation reconciliation against Silver

Examples:

\`\`\`text

Silver Transactions

        ↓

5,000,000 records

        ↓

Gold transaction_count

        ↓

5,000,000 records

\`\`\`

\`\`\`text

Silver Customers

        ↓

100,000 records

        ↓

Customer 360

        ↓

100,000 customers

\`\`\`

The current Gold QA stage has been successfully executed.

\---

**# 🔄 dbt Transformation Layer**

After the Spark-based ETL pipeline, dbt is used as an analytical transformation and testing layer.

The dbt project uses **\*\*DuckDB\*\*** to query the local Parquet-based Silver data.

The dbt architecture follows:

\`\`\`text

Silver Parquet

      │

      ▼

   STAGING

      │

      ▼

 INTERMEDIATE

      │

      ▼

    MARTS

\`\`\`

**## Staging**

The staging layer exposes the Silver datasets through dbt models.

Current staging models:

\`\`\`text

stg_customers

stg_accounts

stg_transactions

stg_merchants

stg_cards

stg_loans

stg_loan_payments

\`\`\`

Staging models are materialized as views.

**## Intermediate**

The intermediate layer contains reusable transformations and joins between the staging models.

Current models:

\`\`\`text

int_customer_transactions

int_merchant_transactions

int_loan_payments

int_customer_loans

\`\`\`

These models enrich and combine data before it reaches the analytical marts.

Intermediate models are materialized as tables.

**## Marts**

The marts layer contains business-oriented analytical models designed for downstream consumption.

Current models:

\`\`\`text

fct_daily_transactions

fct_merchant_performance

fct_loan_portfolio

fct_customer_transactions

customer_360

\`\`\`

Marts are materialized as tables.

The marts provide datasets suitable for:

\* BI dashboards

\* Analytical queries

\* Business metrics

\* Machine learning feature engineering

**## dbt Data Quality**

The dbt project includes tests and documentation for:

\* Primary key uniqueness

\* NULL checks

\* Model-level documentation

\* Column descriptions

\* Analytical model validation

The current dbt test suite executes successfully across the Staging, Intermediate, and Mart layers.

\---

**# 🔄 End-to-End Pipeline**

The project currently supports the following processing flow:

\`\`\`text

Synthetic Data

      │

      ▼

Data Profiling

      │

      ▼

🥉 Bronze

      │

      ▼

Apache Spark

      │

      ▼

🥈 Silver

      │

      ├── Data Quality

      ├── Referential Integrity

      ├── Business Rules

      └── Quarantine

      │

      ▼

🥇 Gold

      │

      ▼

Gold QA

      │

      ▼

dbt

      │

      ├── Staging

      ├── Intermediate

      └── Marts

      │

      ▼

dbt Tests

\`\`\`

The complete ETL, Lakehouse, and orchestration pipeline is currently operational.

\---

**# 🔄 Incremental Processing**

A major objective of the project is to avoid reprocessing the entire dataset whenever new data arrives.

Instead of:

\`\`\`text

10,000,000 existing records

        \+

50,000 new records

        ↓

Process all 10,050,000 records

\`\`\`

The target architecture is:

\`\`\`text

Existing Data

     │

     ▼

Last Processed Timestamp

     │

     ▼

Only New Records

     │

     ▼

Incremental Processing

\`\`\`

Incremental processing will be implemented in a future phase using Lakehouse table capabilities and orchestration.

\---

**# 🚀 Roadmap**

**## Phase 1 — Synthetic Data**

\* [x] Define banking domain

\* [x] Generate customers

\* [x] Generate accounts

\* [x] Generate transactions

\* [x] Generate merchants

\* [x] Generate cards

\* [x] Generate loans

\* [x] Generate loan payments

\* [x] Perform initial data profiling

**## Phase 2 — Data Lake / Bronze**

\* [x] Create Bronze layer

\* [x] Store data as Parquet

\* [x] Implement date partitioning

\* [x] Implement batch-based ingestion

\* [x] Avoid full dataset loading during Bronze processing

\* [ ] Improve partition strategy

\* [ ] Add metadata management

**## Phase 3 — Silver Layer**

\* [x] Introduce PySpark

\* [x] Read Bronze data

\* [x] Standardize data

\* [x] Deduplicate records

\* [x] Validate primary keys

\* [x] Validate foreign keys

\* [x] Validate business rules

\* [x] Implement quarantine processing

\* [x] Create Silver datasets

\* [x] Create Silver QA

\* [x] Validate Silver referential integrity

**## Phase 4 — Gold Layer**

\* [x] Create analytical models

\* [x] Daily transaction summary

\* [x] Customer transaction metrics

\* [x] Merchant performance

\* [x] Loan portfolio

\* [x] Customer 360

\* [x] Implement Gold QA

\* [x] Validate aggregation consistency

**## Phase 5 — Transformation with dbt**

\* [x] Introduce dbt Core

\* [x] Configure DuckDB

\* [x] Create staging models

\* [x] Create intermediate models

\* [x] Create analytical marts

\* [x] Add dbt tests

\* [x] Add model documentation

\* [ ] Implement lineage visualization

\* [ ] Implement incremental models

**## Phase 6 — Lakehouse

* [x] Introduce Apache Iceberg
* [x] Implement Iceberg table management
* [x] Create Gold tables in Iceberg
* [x] Validate Iceberg data against Gold Parquet
* [x] Implement partitioning for large analytical tables
* [ ] Schema evolution
* [ ] Time travel
* [ ] Upserts / MERGE operations

## Phase 7 — Object Storage

* [x] Introduce MinIO
* [x] Configure S3-compatible storage
* [x] Move Bronze, Silver, and Gold storage to MinIO
* [x] Configure `banking` bucket structure
* [x] Integrate Spark with MinIO through S3A
* [x] Integrate Iceberg with MinIO
* [ ] Complete dbt execution directly against the Lakehouse layer

## Phase 8 — Orchestration

* [x] Introduce Apache Airflow
* [x] Create the `banking_data_pipeline` DAG
* [x] Define task dependencies
* [x] Integrate Airflow Worker with Spark through Docker
* [x] Orchestrate Generate → Bronze → Silver → QA → Gold → QA → Iceberg
* [ ] Add production-grade retries and alerting
* [ ] Add pipeline monitoring
* [ ] Implement incremental execution

## Phase 9 — Infrastructure

* [x] Dockerize the platform
* [x] Create Docker Compose environment
* [x] Configure MinIO
* [x] Configure Spark
* [x] Configure Airflow, PostgreSQL, and Redis
* [x] Create reproducible local environment
* [ ] Centralize environment/resource profiles

## Phase 10 — Machine Learning

*

\* [ ] Customer segmentation

\* [ ] Loan default prediction

\* [ ] Feature engineering

\* [ ] Model training

\* [ ] Model evaluation

\* [ ] Integrate ML outputs with Gold/Lakehouse layer

\---

**# 🧪 Running the Project**

Create a virtual environment:

\`\`\`bash

python -m venv .venv

\`\`\`

Activate it:

\`\`\`bash

source .venv/bin/activate

\`\`\`

Install dependencies:

\`\`\`bash

pip install -r requirements.txt

\`\`\`

Generate the datasets:

\`\`\`bash

python scripts/generate_customers.py

python scripts/generate_accounts.py

python scripts/generate_merchants.py

python scripts/generate_transactions.py

python scripts/generate_cards.py

python scripts/generate_loans.py

python scripts/generate_loan_payments.py

\`\`\`

Run initial data profiling:

\`\`\`bash

python scripts/profile_data.py

\`\`\`

Build the Bronze layer:

\`\`\`bash

python scripts/build_bronze.py

\`\`\`

Run the Spark pipeline:

\`\`\`bash

python pipeline.py

\`\`\`

Run Silver QA:

\`\`\`bash

spark-submit scripts/profile_silver_data.py

\`\`\`

Run Gold QA:

\`\`\`bash

spark-submit scripts/profile_gold_data.py

\`\`\`

Run dbt:

\`\`\`bash

cd dbt/banking_dbt

dbt debug

dbt run

dbt test

\`\`\`

\---

**# 🔐 Data Privacy**

This project does **\*\*not\*\*** use real banking or customer data.

All customers, transactions, accounts, merchants, cards, loans, and payments are synthetically generated for educational and portfolio purposes.

No real personal or financial information is stored in the repository.

Local analytical artifacts such as the DuckDB database are also excluded from version control.

\---

**# 📚 Engineering Concepts Demonstrated**

This project demonstrates practical knowledge of:

\* ETL / ELT

\* Data Lake Architecture

\* Medallion Architecture

\* Lakehouse Architecture

\* Apache Spark

\* PySpark

\* Apache Parquet

\* Partitioning

\* Batch Processing

\* Distributed Data Processing

\* Data Quality

\* Data Validation

\* Referential Integrity

\* Business Rule Validation

\* Data Quarantine

\* Data Modeling

\* Analytical Modeling

\* Data Warehousing

\* dbt

\* SQL Transformations

\* dbt Testing

\* Data Documentation

\* Pipeline Design

\* Incremental Processing

\* Data Governance

\* Scalable Data Processing

\---

**# 📌 Current Project Status

The end-to-end Data Engineering pipeline is currently operational.

```text
Synthetic Data
      ↓
Data Profiling
      ↓
🥉 Bronze
      ↓
🥈 Silver
      ↓
Silver QA
      ↓
🥇 Gold
      ↓
Gold QA
      ↓
Apache Iceberg
      ↓
MinIO / S3
      ↓
Apache Airflow
      ↓
Machine Learning
```

Current implementation includes:

* 100,000 synthetic customers
* 150,025 accounts
* 10,000 merchants
* 5,000,000 transactions
* 120,000 cards
* 30,000 loans
* 740,448 loan payments
* Partitioned Parquet storage
* Batch-based Bronze ingestion
* PySpark Silver transformations
* Data quality validation
* Referential integrity validation
* Business rule validation
* Invalid record quarantine
* Five analytical Gold models
* Silver QA
* Gold QA
* dbt Staging layer
* dbt Intermediate layer
* Five dbt analytical marts
* dbt tests and documentation
* MinIO S3-compatible object storage
* Apache Iceberg Lakehouse tables
* Iceberg validation against Gold Parquet
* Apache Airflow orchestration
* Docker Compose infrastructure
* End-to-end execution through a single Airflow DAG

### Current Airflow Pipeline

```text
banking_data_pipeline

generate_data
      ↓
build_bronze
      ↓
build_silver
      ↓
silver_qa
      ↓
build_gold
      ↓
gold_qa
      ↓
build_iceberg
```

### Current Lakehouse Tables

The Gold analytical datasets are materialized as Iceberg tables in the `banking` namespace:

```text
iceberg.banking.customer_360
iceberg.banking.daily_transaction_summary
iceberg.banking.customer_transaction_metrics
iceberg.banking.merchant_performance
iceberg.banking.loan_portfolio
```

The Iceberg tables were validated against the corresponding Gold Parquet datasets using record counts, distinct keys, and relevant aggregate metrics.

### Next Major Milestone

The Data Engineering foundation is complete enough to move into the Machine Learning phase.

The next stage is:

```text
Apache Iceberg
      ↓
Feature Engineering
      ↓
ML Training Dataset
      ↓
Model Training
      ↓
Model Evaluation
      ↓
Predictions
      ↓
Lakehouse
```

The initial ML problem will be selected based on the available banking data, with loan default prediction and customer segmentation as candidate workloads.