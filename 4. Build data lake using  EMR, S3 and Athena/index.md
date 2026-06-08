# AWS Data Lake Pipeline using EMR, Spark, Athena and Step Functions

## Project Overview

This project demonstrates how to build a modern Data Lake on AWS for processing large-scale rental marketplace data.

The solution uses Amazon S3 as the storage layer, Apache Spark running on Amazon EMR for data processing, AWS Glue Crawlers for metadata discovery, Amazon Athena for querying data, and AWS Step Functions for workflow orchestration.

The pipeline processes raw rental marketplace datasets, generates analytical metrics using Spark, stores the results in a Data Lake, and makes them available for serverless querying.

---

## Business Scenario

Imagine a company that operates a rental marketplace platform similar to Airbnb.

Every day, the platform generates large volumes of data related to:

* Rental transactions
* Users
* Locations
* Vehicles

The business wants to analyze this data to understand customer behavior, location performance, transaction trends, and vehicle utilization.

Instead of loading everything into a traditional data warehouse, the company decides to build a Data Lake where data can be processed at scale and queried directly from storage.

The solution must:

* Process large datasets efficiently
* Store transformed data in a central Data Lake
* Automatically discover schemas
* Enable SQL-based analytics
* Automate the entire workflow

---

## Project Flow

### 1. Store Raw Data in Amazon S3

The pipeline starts with rental marketplace datasets stored in Amazon S3.

Datasets include:

* Locations
* Users
* Vehicles
* Rental Transactions

---

### 2. Process Data Using Spark on EMR

Apache Spark jobs running on Amazon EMR perform large-scale data transformations.
The processed output is written back to Amazon S3.

---

### 3. Build the Data Lake

The transformed datasets are stored in Amazon S3, which acts as the Data Lake storage layer.
This allows data to be stored cheaply while remaining available for analytics.

---

### 4. Discover Schemas Using Glue Crawlers

Glue Crawlers scan the processed files and automatically create metadata tables in the Glue Data Catalog.
This removes the need to manually define schemas.

---

### 5. Query Data Using Athena

Amazon Athena uses the Glue Data Catalog to query data directly from S3.
Business users can run SQL queries without managing databases or servers.

---

### 6. Automate with Step Functions

AWS Step Functions orchestrates the complete workflow.
This removes the need for manual execution.

---

### 7. Run Using EMR Serverless

The same Spark workloads can also be executed using EMR Serverless.

Benefits include:
* No cluster management
* Automatic scaling
* Pay-per-use pricing
* Reduced operational overhead

---

## Architecture

```text
Raw Datasets
      │
      ▼
 Amazon S3
      │
      ▼
 Apache Spark on EMR
      │
      ▼
 Processed Metrics
      │
      ▼
 Amazon S3 Data Lake
      │
      ▼
 AWS Glue Crawlers
      │
      ▼
 Glue Data Catalog
      │
      ▼
 Amazon Athena
      │
      ▼
 Business Analytics

      │
      ▼
 AWS Step Functions
```

# Project Structure

```text
4. Build data lake using  EMR, S3 and Athena/
│
│── Lab_1_Big_Data_Processing_using_EMR_Spark_Athena_Step_Functions.pdf
│
├── resources/
│   ├── emr-serverless-trust-policy.json
│   ├── execution-policy-step-functions.json
│   ├── local-docker-development.sh
│   ├── locations.csv
│   ├── rental_transactions.csv
│   ├── spark-agg1.py
│   ├── spark-agg2.py
│   ├── step-functions-emr.json
│   ├── users.csv
│   └── vehicles.csv
│
└── index.md
```

---

## Services Overview 

### Amazon S3
Stores raw input data and processed output data.

### Amazon EMR
Runs Spark jobs for large-scale data processing.

### Apache Spark
Performs the transformations and metric calculations.

### AWS Glue Crawlers
Discover the schema of the output files.

### AWS Glue Data Catalog
Stores table metadata for Athena.

### Amazon Athena
Queries the transformed data directly from S3.

### AWS Step Functions
Coordinates the workflow and controls job execution.

### IAM
Manages permissions between AWS services.

### CloudWatch
Provides logs and monitoring.

---

## Data Sets Used

- Vehicles
- Users
- Locations
- Rental transactions

---

## Metrics Produced

The Spark jobs generate analytics such as:

- Transaction metrics
- User metrics
- Location performance metrics
- Vehicle performance metrics

---

## Learning Outcomes

After completing this project, you will understand how to:

- build a data lake on AWS
- process big data using Spark
- run workloads on Amazon EMR
- store transformed data in S3
- use Glue Crawlers and Athena
- automate processing with Step Functions
- manage EMR cluster lifecycle efficiently

---

## Project Result

By the end of the project, raw rental marketplace data is transformed into analytics-ready datasets in S3, cataloged automatically, and queried through Athena, with the whole workflow managed by Step Functions.