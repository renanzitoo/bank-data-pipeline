# 🏦 Banking Data Lakehouse

A data engineering project that simulates a modern banking data platform, focusing on data ingestion, distributed processing, data quality, lakehouse architecture, and analytical data modeling.

The project uses synthetic banking data to simulate a real-world environment containing customers, accounts, transactions, cards, merchants, loans, and loan payments.

> 🚧 **Project Status:** In Development

---

## 🎯 Project Goal

The main goal of this project is to build an end-to-end banking data platform capable of:

* Generating realistic synthetic banking data
* Ingesting data from different sources
* Storing raw data in a Data Lake
* Processing large datasets with Apache Spark
* Implementing Bronze, Silver, and Gold data layers
* Applying data quality and business rules
* Supporting incremental data processing
* Building analytical data models
* Orchestrating pipelines with Apache Airflow
* Using a Lakehouse table format
* Providing data for analytics and dashboards

The project is designed to demonstrate practical Data Engineering concepts rather than simply building a database.

---

# 🏗️ Architecture

The planned architecture follows a Medallion-style Data Lakehouse architecture:

```text
                    DATA SOURCES
                         │
             ┌───────────┴───────────┐
             │                       │
            CSV                    APIs
             │                       │
             └───────────┬───────────┘
                         ▼
                    INGESTION
                       Python
                         │
                         ▼
                     🥉 BRONZE
                  Raw / Immutable
                         │
                         ▼
                     Apache Spark
                         │
              ┌──────────┴──────────┐
              │                     │
           Valid Data          Invalid Data
              │                     │
              ▼                     ▼
          🥈 SILVER             QUARANTINE
       Clean / Trusted
              │
              ▼
             dbt
              │
              ▼
           🥇 GOLD
      Analytical Models
              │
        ┌─────┴─────┐
        ▼           ▼
       BI           ML
```

---

# 🛠️ Technology Stack

### Data Generation

* Python
* Faker
* NumPy
* Pandas
* PyArrow

### Data Processing

* Apache Spark
* PySpark

### Storage

* Apache Parquet
* MinIO / S3 *(planned)*

### Lakehouse

* Apache Iceberg *(planned)*

### Transformation

* dbt *(planned)*

### Orchestration

* Apache Airflow *(planned)*

### Infrastructure

* Docker
* Docker Compose *(planned)*

### Analytics

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
                   ACCOUNT        LOAN
                  /      \          │
                 /        \         ▼
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
* Cards can only be associated with existing accounts.

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

The project currently generates approximately:

| Dataset       |               Records |
| ------------- | --------------------: |
| Customers     |               100,000 |
| Accounts      |              ~150,000 |
| Merchants     |                10,000 |
| Transactions  |             5,000,000 |
| Cards         |               120,000 |
| Loans         |                30,000 |
| Loan Payments | Hundreds of thousands |

The data is completely synthetic and is generated locally.

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
│   └── gold/
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
│   └── build_bronze.py
│
├── spark/
│   └── silver/
│       └── build_silver.py
│
├── tests/
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

# 🥉 Bronze Layer

The Bronze layer stores data with minimal transformation.

The current implementation uses Apache Parquet and partitions datasets by date.

For example:

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

This allows downstream processing engines to take advantage of partition pruning.

---

# 🥈 Silver Layer

The Silver layer is currently being implemented with Apache Spark.

The purpose of this layer is to transform raw data into clean and trusted datasets.

Current transformations include:

* Standardizing string values
* Trimming text fields
* Normalizing categorical values
* Removing duplicate records
* Preserving partitioning

Example:

```text
BRONZE
  │
  ▼
Apache Spark
  │
  ├── Standardization
  ├── Deduplication
  └── Transformation
  │
  ▼
SILVER
```

Data quality rules will be added as the Silver layer evolves.

---

# 🔎 Data Quality

Data quality is treated as a core part of the pipeline.

Examples of planned rules:

```text
transaction_id
    → NOT NULL
    → UNIQUE

account_id
    → NOT NULL
    → Must exist in accounts

amount
    → Must be greater than zero

currency
    → Must be BRL

transaction_type
    → Must belong to the supported domain

status
    → Must belong to the supported domain

CARD_PURCHASE
    → merchant_id must exist

Other transaction types
    → merchant_id can be NULL
```

Invalid records will eventually be separated into a quarantine layer instead of simply being deleted.

---

# 📈 Analytical Layer

The Gold layer will provide business-oriented datasets for analytical workloads.

Planned fact tables:

```text
fact_transactions
fact_loans
fact_loan_payments
```

Planned dimensions:

```text
dim_customer
dim_account
dim_card
dim_merchant
dim_date
```

Example analytical questions:

* What is the total transaction volume?
* What is the average transaction value?
* Which transaction types are most common?
* What percentage of transactions are PIX?
* Which customer segments generate the most revenue?
* What is the total loan portfolio?
* What is the default rate?
* Which merchant categories generate the highest transaction volume?
* How does transaction volume change over time?

---

# 🔄 Incremental Processing

A major objective of the project is to avoid reprocessing the entire dataset whenever new data arrives.

Instead of:

```text
10,000,000 existing records
        +
50,000 new records

→ Process all 10,050,000 records
```

the target architecture is:

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

This will be implemented in a later stage.

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

## Phase 2 — Data Lake

* [x] Create Bronze layer
* [x] Store data as Parquet
* [x] Implement partitioning
* [ ] Improve partition strategy
* [ ] Add metadata management

## Phase 3 — Silver Layer

* [x] Introduce PySpark
* [x] Read Bronze data
* [x] Basic transformations
* [x] Deduplication
* [ ] Data quality framework
* [ ] Invalid-record quarantine
* [ ] Schema validation

## Phase 4 — Lakehouse

* [ ] Apache Iceberg
* [ ] Table management
* [ ] Schema evolution
* [ ] Time travel
* [ ] Partition optimization

## Phase 5 — Orchestration

* [ ] Apache Airflow
* [ ] DAG design
* [ ] Dependency management
* [ ] Retry strategies
* [ ] Pipeline monitoring

## Phase 6 — Transformation

* [ ] dbt
* [ ] Staging models
* [ ] Intermediate models
* [ ] Gold models
* [ ] dbt tests

## Phase 7 — Cloud

* [ ] Object storage
* [ ] Cloud Data Warehouse
* [ ] Cloud-based orchestration
* [ ] CI/CD

## Phase 8 — Analytics

* [ ] Banking dashboard
* [ ] Transaction analytics
* [ ] Customer segmentation
* [ ] Loan portfolio analysis
* [ ] Risk indicators

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

Run data profiling:

```bash
python scripts/profile_data.py
```

Build the Bronze layer:

```bash
python scripts/build_bronze.py
```

Run the Spark Silver pipeline:

```bash
spark-submit spark/silver/build_silver.py
```

---

# 🔐 Data Privacy

This project does **not** use real banking or customer data.

All customers, transactions, accounts, merchants, cards, and loans are synthetically generated for educational and portfolio purposes.

No real personal or financial information is stored in the repository.

---

# 📚 Engineering Concepts Demonstrated

This project is designed to demonstrate practical knowledge of:

* ETL / ELT
* Data Lake Architecture
* Lakehouse Architecture
* Medallion Architecture
* Apache Spark
* PySpark
* Apache Parquet
* Partitioning
* Batch Processing
* Incremental Processing
* Data Quality
* Data Validation
* Data Modeling
* Dimensional Modeling
* Data Warehousing
* Pipeline Orchestration
* Distributed Data Processing
* Cloud Data Engineering
* Data Governance

---

# 📌 Project Status

The project is currently under active development.

The current implementation covers:

```text
Synthetic Data
      ↓
Data Profiling
      ↓
Bronze Layer
      ↓
PySpark
      ↓
Silver Layer
```

The next major milestone is implementing robust **Data Quality and Quarantine processing in the Silver layer**, followed by the Lakehouse architecture.
