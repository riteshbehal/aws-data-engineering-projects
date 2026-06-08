# Build a Lakehouse for E-Commerce Transactions using Delta Lake on AWS

## Overview

This project demonstrates how to build a modern Lakehouse architecture on AWS using Delta Lake.

The solution combines Amazon S3 for storage, Apache Spark for processing, Delta Lake for ACID-compliant tables, AWS Glue for cataloging metadata, and Amazon Athena for analytics. The pipeline supports both initial data ingestion and incremental updates through Delta Lake merge (upsert) operations.

Unlike a traditional data warehouse, this architecture stores data directly in Amazon S3 while providing reliable transactional capabilities and analytical querying.

---

## Solution Architecture

### Step 1: Raw Data Ingestion

Transactional datasets are uploaded into Amazon S3 raw zones.

The raw zone acts as the landing area for incoming files.

### Step 2: Delta Lake Processing

Apache Spark reads the raw datasets and converts them into Delta Lake tables.

The processing logic:

* Reads CSV datasets
* Applies transformations
* Creates Delta tables
* Partitions data where required
* Supports merge (upsert) operations

### Step 3: Incremental Updates

When updated datasets arrive, Delta Lake performs merge operations using primary keys.

This enables:

* Updates to existing records
* Inserts of new records
* ACID-compliant transactions

### Step 4: Data Archival

After successful processing, raw files are moved into an archive folder.

This prevents duplicate processing and preserves historical input files.

### Step 5: Metadata Cataloging

AWS Glue Crawlers scan the Delta tables stored in S3 and automatically create metadata definitions inside the Glue Data Catalog.

### Step 6: Query with Athena

Amazon Athena uses the Glue Data Catalog to query Delta Lake tables directly from S3 using standard SQL.

### Step 7: Redshift Integration

The cataloged datasets can also be accessed through Redshift using the AWS Data Catalog.

---

## Architecture Flow

```text
Raw CSV Files
      ↓
Amazon S3 Raw Zone
      ↓
Apache Spark
      ↓
Delta Lake Tables
      ↓
Amazon S3 Lakehouse
      ↓
AWS Glue Crawlers
      ↓
Glue Data Catalog
      ↓
Amazon Athena
      ↓
Analytics & Reporting

      ↓
Amazon Redshift (Optional Access Layer)
```

---

## Project Structure

```text
build-lakehouse-using-delta-lake/
├── Lab_1_Build_a_Lakehouse_for_E-commerce_Transactions_using_Delta_Lake_on_AWS.pdf
│
├── Resources/
│   │
│   ├── data/
│   │   ├── products.csv
│   │   ├── orders_2024-06-06.csv
│   │   ├── orders_2024-06-06-Updated.csv
│   │   ├── orders_2024-06-07.csv
│   │   ├── order_items_2024-06-06.csv
│   │   └── order_items_2024-06-07.csv
│   │
│   ├── jars/
│   │   └── delta-core_2.12-1.0.0.jar
│   │
│   ├── spark-transactional-delta-lake.py
│   ├── local-docker-development.sh
│   └── Dockerfile
│
└── index.md
```

---

## Input Datasets

### Products Dataset

Contains product catalog information.

Typical attributes include:

* Product ID
* Product Name
* Product Category
* Product Details

### Orders Dataset

Contains order-level transaction information.

Typical attributes include:

* Order ID
* Customer Details
* Order Timestamp
* Total Amount

### Order Items Dataset

Contains item-level order information.

Typical attributes include:

* Order ID
* Product ID
* Quantity
* Item Amount

### Updated Orders Dataset

Used to demonstrate Delta Lake merge (upsert) functionality by updating existing records while preserving transactional consistency.

---

## Key Services Used

### Amazon S3

Stores both raw input files and Delta Lake tables.

Acts as the storage layer for the Lakehouse architecture.

### Apache Spark

Processes large datasets and performs transformations.

Handles Delta Lake table creation and merge operations.

### Delta Lake

Adds transactional capabilities to data stored in S3.

Provides:
* ACID transactions
* Upserts (MERGE)
* Schema enforcement
* Data versioning

### AWS Glue

Runs Spark jobs and manages metadata discovery.x

### AWS Glue Crawlers

Automatically detect schemas and create catalog tables.

### AWS Glue Data Catalog

Stores metadata about Delta Lake tables for querying services.

### Amazon Athena

Allows SQL queries directly against Delta Lake data stored in S3.

### Amazon Redshift

Can access cataloged datasets for additional analytics use cases.

### Docker

Provides a local environment for developing and testing Spark jobs.

### IAM

Manages permissions across AWS services.

### CloudWatch

Provides logging and monitoring for processing jobs.

---

## Key Learning Outcomes

After completing this project, you will understand how to:

* Build a Lakehouse architecture on AWS
* Use Delta Lake with Apache Spark
* Store transactional data in Amazon S3
* Implement ACID-compliant data processing
* Perform Delta Lake merge (upsert) operations
* Create metadata catalogs using Glue Crawlers
* Query Delta tables using Athena
* Integrate Delta Lake with Redshift
* Manage incremental data processing workflows
* Develop and test Spark workloads using Docker

---

## Project Result

By the end of the project, raw e-commerce transaction data is transformed into Delta Lake tables stored in Amazon S3. The datasets support ACID transactions, incremental updates, metadata cataloging, and SQL analytics through Athena, creating a modern cloud-native Lakehouse platform.
