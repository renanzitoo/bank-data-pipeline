# 🏦 Banking Data Lakehouse

A data engineering project that simulates a modern banking data platform, focusing on data ingestion, distributed processing, data quality, Medallion architecture, analytical data modeling, and Lakehouse technologies.

The project uses completely synthetic banking data to simulate a real-world environment containing customers, accounts, transactions, cards, merchants, loans, and loan payments.

> 🚧 **Project Status:** Active Development — Core ETL Pipeline Completed

---

# 🎯 Project Goal

The main goal of this project is to build an end-to-end banking data platform capable of:

* Generating realistic synthetic banking data
* Ingesting and processing large datasets
* Storing data in a Data Lake architecture
* Processing large datasets with Apache Spark
* Implementing Bronze, Silver, and Gold layers
* Applying data quality and business rules
* Detecting and isolating invalid records
* Building analytical data models
* Supporting incremental data processing
* Preparing the platform for a Lakehouse architecture
* Providing data for analytics and machine learning
* Orchestrating the complete pipeline

The project is designed to demonstrate practical Data Engineering concepts rather than simply building a traditional database.

---

# 🏗️ Architecture

The project follows a Medallion-style Data Lake architecture, with the Lakehouse components being introduced in subsequent phases.

```text
                         DATA SOURCES
                              │
                    ┌─────────┴─────────┐
                    │                   │
                   CSV                 APIs
                    │                   │
                    └─────────┬─────────┘
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
               Valid Data         Invalid Data
                    │                   │
                    ▼                   ▼
                🥈 SILVER          QUARANTINE
             Clean / Trusted
                    │
                    ▼
                  🥇 GOLD
             Analytical Models
                    │
              ┌─────┴─────┐
              ▼           ▼
             BI           ML

              Future Lakehouse Components
                       │
                       ▼
                 Apache Iceberg
                       │
                       ▼
                    MinIO/S3
                       │
                       ▼
                    Airflow
```

---

# 🛠️ Technology Stack

## Data Generation

* Python
* Faker
* NumPy
* Pandas
* PyArrow

## Data Processing

* Apache Spark
* PySpark

## Storage

* Apache Parquet
* MinIO / S3 *(planned)*

## Lakehouse

* Apache Iceberg *(planned)*

## Transformation

* dbt *(planned)*

## Orchestration

* Apache Airflow *(planned)*

## Infrastructure

* Docker
* Docker Compose *(planned)*

## Analytics

* BigQuery *(planned)*
* Looker Studio / Metabase *(planned)*

---

# 📊 Data Domain

The simulated banking environment contains the following entities:

```text
                         CUSTOMER
                        /        \
                       /          \
                      ▼            ▼
                  ACCOUNT         LOAN
                 /      \           │
                /        \          ▼
               ▼          ▼   LOAN_PAYMENT
             CARD     TRANSACTION
                          │
                          ▼
                       MERCHANT
```

## Customers

Contains customer information and segmentation.

```text
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
```

Customer segments:

```text
BASIC
STANDARD
PREMIUM
```

---

## Accounts

Represents customer bank accounts.

```text
account_id
customer_id
account_type
branch
account_number
status
opened_at
closed_at
```

Account types:

```text
CHECKING
SAVINGS
```

---

## Transactions

The largest dataset in the project.

```text
transaction_id
account_id
transaction_type
amount
currency
status
transaction_timestamp
merchant_id
description
```

Transaction types include:

```text
PIX
CARD_PURCHASE
TRANSFER
DEPOSIT
WITHDRAWAL
TED
DOC
```

Business rules include:

* Every transaction must belong to an existing account.
* Transaction amounts must be greater than zero.
* Currency must be BRL.
* `CARD_PURCHASE` transactions must have a valid `merchant_id`.
* Other transaction types may have a NULL `merchant_id`.
* Transaction IDs must be unique.

---

## Merchants

Represents establishments where card transactions can occur.

```text
merchant_id
merchant_name
merchant_category
city
state
created_at
```

Merchant categories include:

```text
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
```

---

## Cards

Represents debit and credit cards associated with bank accounts.

```text
card_id
account_id
card_type
brand
status
credit_limit
issued_at
expires_at
```

Business rules include:

* Savings accounts can only have debit cards.
* Credit cards must have a credit limit.
* Debit cards do not have a credit limit.
* Cards must belong to existing accounts.

---

## Loans

Represents customer loans.

```text
loan_id
customer_id
loan_type
principal_amount
interest_rate
term_months
status
created_at
```

Loan types:

```text
PERSONAL
PAYROLL
AUTO
```

Loan statuses:

```text
ACTIVE
PAID
DEFAULTED
CANCELLED
```

---

## Loan Payments

Represents individual loan installments.

```text
payment_id
loan_id
payment_number
due_date
payment_date
amount
status
```

Payment statuses:

```text
PAID
PENDING
LATE
```

---

# 📦 Synthetic Dataset

The current project generates:

| Dataset       |   Records |
| ------------- | --------: |
| Customers     |   100,000 |
| Accounts      |   150,025 |
| Merchants     |    10,000 |
| Transactions  | 5,000,000 |
| Cards         |   120,000 |
| Loans         |    30,000 |
| Loan Payments |   740,448 |

All data is completely synthetic and generated locally.

Generated datasets are intentionally excluded from version control because of their size.

---

# 📁 Project Structure

```text
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
├── tests/
│
├── pipeline.py
├── requirements.txt
├── .gitignore
└── README.md
```

---

# 🥉 Bronze Layer

The Bronze layer represents the raw data ingestion layer.

Data is stored as Apache Parquet and partitioned by year and month based on the relevant timestamp/date column.

Example:

```text
data/bronze/transactions/

├── year=2024/
│   ├── month=01/
│   ├── month=02/
│   └── ...
│
├── year=2025/
│   └── ...
│
└── year=2026/
    ├── month=01/
    ├── month=02/
    └── ...
```

The Bronze ingestion process was designed to handle large datasets without loading the entire dataset into memory.

Data is processed in batches using PyArrow.

This is particularly important for the 5 million transaction dataset.

---

# 🥈 Silver Layer

The Silver layer is implemented using Apache Spark.

Its purpose is to transform Bronze data into clean, validated, and trusted datasets.

Current transformations include:

* String standardization
* Text trimming
* Categorical normalization
* Duplicate detection
* Primary key validation
* Foreign key validation
* Business rule validation
* Schema/type normalization
* Partitioned output
* Invalid record quarantine

The processing flow is:

```text
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
SILVER         QUARANTINE
```

The Silver layer currently contains validated datasets for:

* Customers
* Accounts
* Merchants
* Transactions
* Cards
* Loans
* Loan Payments

---

# 🔎 Data Quality

Data quality is treated as a core part of the pipeline.

Validation is performed both during Silver processing and through dedicated QA scripts.

## Primary Key Validation

Each Silver dataset is validated for:

* NULL primary keys
* Duplicate primary keys

Current results:

```text
Dataset                     Records     Duplicates     NULL PK
----------------------------------------------------------------
customers                   100,000              0           0
accounts                    150,025              0           0
merchants                    10,000              0           0
transactions              5,000,000              0           0
cards                       120,000              0           0
loans                        30,000              0           0
loan_payments               740,448              0           0
```

## Referential Integrity

Foreign key relationships are validated between related datasets.

Examples:

```text
transactions.account_id
        ↓
accounts.account_id
```

```text
transactions.merchant_id
        ↓
merchants.merchant_id
```

```text
cards.account_id
        ↓
accounts.account_id
```

```text
loans.customer_id
        ↓
customers.customer_id
```

```text
loan_payments.loan_id
        ↓
loans.loan_id
```

The Silver QA process currently reports no invalid foreign key relationships.

---

# 🥇 Gold Layer

The Gold layer contains business-oriented analytical datasets generated from the validated Silver layer.

Current Gold models:

```text
data/gold/

├── daily_transaction_summary/
├── customer_transaction_metrics/
├── merchant_performance/
├── loan_portfolio/
└── customer_360/
```

---

## Daily Transaction Summary

Aggregates transactions by:

```text
transaction_date
transaction_type
status
```

Metrics include:

```text
transaction_count
total_amount
average_amount
minimum_amount
maximum_amount
```

This model is designed to support time-series transaction analysis.

---

## Customer Transaction Metrics

Provides transaction-level metrics aggregated by customer.

Metrics include:

```text
customer_id
customer_segment
transaction_count
total_transaction_amount
average_transaction_amount
pix_count
card_purchase_count
transfer_count
deposit_count
withdrawal_count
completed_transactions
failed_transactions
pending_transactions
first_transaction_at
last_transaction_at
```

---

## Merchant Performance

Provides performance metrics for merchants.

Metrics include:

```text
merchant_id
merchant_name
merchant_category
city
state
transaction_count
total_transaction_amount
average_transaction_amount
completed_transactions
failed_transactions
pending_transactions
unique_accounts
```

This model can be used to analyze merchant performance and transaction volume.

---

## Loan Portfolio

Provides a consolidated view of the loan portfolio and payment behavior.

Metrics include:

```text
loan_id
customer_id
loan_type
principal_amount
interest_rate
term_months
loan_status
created_at
total_payments
paid_payments
pending_payments
late_payments
total_paid_amount
remaining_amount
```

> `remaining_amount` is currently a simplified analytical metric calculated from principal amount minus total paid amount. It does not represent a real amortization balance.

---

## Customer 360

The Customer 360 model consolidates information from multiple banking domains into a single customer-level analytical dataset.

It combines:

```text
CUSTOMERS
   │
   ├── ACCOUNTS
   ├── TRANSACTIONS
   ├── CARDS
   ├── LOANS
   └── LOAN PAYMENTS
```

Metrics include:

```text
customer_id
customer_segment
total_accounts
active_accounts
total_transactions
total_transaction_amount
average_transaction_amount
total_cards
credit_cards
total_loans
total_loan_amount
active_loans
defaulted_loans
total_loan_payments
paid_payments
late_payments
total_paid_amount
```

This model will serve as an important foundation for future analytics and machine learning workloads.

---

# 🧪 Gold Quality Assurance

The Gold layer has a dedicated QA process.

The validation checks include:

* Primary key uniqueness
* NULL primary keys
* Composite key uniqueness
* Record count reconciliation
* Referential integrity
* Metric consistency
* Negative metric detection
* Aggregation reconciliation against Silver

Examples of reconciliation checks:

```text
Silver Transactions
        ↓
5,000,000 records
        ↓
Gold transaction_count
        ↓
5,000,000 records
```

```text
Silver Customers
        ↓
100,000 records
        ↓
Customer 360
        ↓
100,000 customers
```

The current Gold QA stage has been successfully executed.

---

# 🔄 End-to-End Pipeline

The project currently supports the following processing flow:

```text
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
```

The core ETL pipeline from raw synthetic data to validated analytical datasets is currently operational.

---

# 🔄 Incremental Processing

A major objective of the project is to avoid reprocessing the entire dataset whenever new data arrives.

Instead of:

```text
10,000,000 existing records
        +
50,000 new records
        ↓
Process all 10,050,000 records
```

The target architecture is:

```text
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
```

Incremental processing will be implemented in a future phase using Lakehouse table capabilities and orchestration.

---

# 🚀 Roadmap

## Phase 1 — Synthetic Data

* [x] Define banking domain
* [x] Generate customers
* [x] Generate accounts
* [x] Generate transactions
* [x] Generate merchants
* [x] Generate cards
* [x] Generate loans
* [x] Generate loan payments
* [x] Perform initial data profiling

## Phase 2 — Data Lake / Bronze

* [x] Create Bronze layer
* [x] Store data as Parquet
* [x] Implement date partitioning
* [x] Implement batch-based ingestion
* [x] Avoid full dataset loading during Bronze processing
* [ ] Improve partition strategy
* [ ] Add metadata management

## Phase 3 — Silver Layer

* [x] Introduce PySpark
* [x] Read Bronze data
* [x] Standardize data
* [x] Deduplicate records
* [x] Validate primary keys
* [x] Validate foreign keys
* [x] Validate business rules
* [x] Implement quarantine processing
* [x] Create Silver datasets
* [x] Create Silver QA
* [x] Validate Silver referential integrity

## Phase 4 — Gold Layer

* [x] Create analytical models
* [x] Daily transaction summary
* [x] Customer transaction metrics
* [x] Merchant performance
* [x] Loan portfolio
* [x] Customer 360
* [x] Implement Gold QA
* [x] Validate aggregation consistency

## Phase 5 — Transformation with dbt

* [ ] Introduce dbt
* [ ] Create staging models
* [ ] Create intermediate models
* [ ] Create analytical models
* [ ] Add dbt tests
* [ ] Add model documentation
* [ ] Implement lineage
* [ ] Implement incremental models

## Phase 6 — Lakehouse

* [ ] Introduce Apache Iceberg
* [ ] Implement table management
* [ ] Schema evolution
* [ ] Time travel
* [ ] Partition optimization
* [ ] Upserts / MERGE operations

## Phase 7 — Object Storage

* [ ] Introduce MinIO
* [ ] S3-compatible storage
* [ ] Move Data Lake storage to object storage
* [ ] Configure bucket structure
* [ ] Integrate Spark with object storage

## Phase 8 — Orchestration

* [ ] Introduce Apache Airflow
* [ ] Create DAGs
* [ ] Define task dependencies
* [ ] Add retries
* [ ] Add pipeline monitoring
* [ ] Implement incremental execution

## Phase 9 — Infrastructure

* [ ] Dockerize the entire platform
* [ ] Create Docker Compose environment
* [ ] Configure services
* [ ] Create reproducible local environment

## Phase 10 — Analytics

* [ ] Banking dashboard
* [ ] Transaction analytics
* [ ] Customer segmentation
* [ ] Merchant analysis
* [ ] Loan portfolio analysis
* [ ] Risk indicators
* [ ] Connect BI platform

## Phase 11 — Machine Learning

* [ ] Customer segmentation
* [ ] Loan default prediction
* [ ] Feature engineering
* [ ] Model training
* [ ] Model evaluation
* [ ] Integrate ML outputs with Gold layer

---

# 🧪 Running the Project

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Generate the datasets:

```bash
python scripts/generate_customers.py
python scripts/generate_accounts.py
python scripts/generate_merchants.py
python scripts/generate_transactions.py
python scripts/generate_cards.py
python scripts/generate_loans.py
python scripts/generate_loan_payments.py
```

Run initial data profiling:

```bash
python scripts/profile_data.py
```

Build the Bronze layer:

```bash
python scripts/build_bronze.py
```

Run the Silver pipeline:

```bash
python pipeline.py
```

Run Silver QA:

```bash
spark-submit scripts/profile_silver_data.py
```

Run Gold QA:

```bash
spark-submit scripts/profile_gold_data.py
```

---

# 🔐 Data Privacy

This project does **not** use real banking or customer data.

All customers, transactions, accounts, merchants, cards, loans, and payments are synthetically generated for educational and portfolio purposes.

No real personal or financial information is stored in the repository.

---

# 📚 Engineering Concepts Demonstrated

This project demonstrates practical knowledge of:

* ETL / ELT
* Data Lake Architecture
* Medallion Architecture
* Lakehouse Architecture
* Apache Spark
* PySpark
* Apache Parquet
* Partitioning
* Batch Processing
* Distributed Data Processing
* Data Quality
* Data Validation
* Referential Integrity
* Business Rule Validation
* Data Quarantine
* Data Modeling
* Analytical Modeling
* Dimensional Modeling
* Data Warehousing
* Pipeline Design
* Incremental Processing
* Data Governance
* Scalable Data Processing

---

# 📌 Current Project Status

The core Data Engineering pipeline is currently operational.

```text
Synthetic Data
      ↓
Data Profiling
      ↓
🥉 Bronze
      ↓
Apache Spark
      ↓
🥈 Silver
      ↓
Silver QA
      ↓
🥇 Gold
      ↓
Gold QA
      ↓
✅ CORE PIPELINE COMPLETED
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

### Next Major Milestone

The next major milestone is **introducing dbt and evolving the project toward a complete Lakehouse architecture**, followed by MinIO/S3, Apache Iceberg, Airflow, Docker, BI, and Machine Learning.
