# AWS Data Engineering Project: Spotify Data Pipeline using Amazon Redshift Serverless and Apache Airflow (MWAA)

## Project Overview

This project demonstrates how to build a modern cloud-based data engineering pipeline on AWS using:

* Amazon Redshift Serverless
* AWS Managed Workflows for Apache Airflow (MWAA)
* Amazon S3
* IAM Roles and Policies

The pipeline processes Spotify streaming datasets stored in Amazon S3, performs data validation and transformation through Airflow workflows, and loads analytical data into Amazon Redshift Serverless for reporting and business intelligence.

---

## Architecture

```text
                +-------------------+
                |   Spotify Data    |
                | (CSV Files in S3) |
                +---------+---------+
                          |
                          v
                +-------------------+
                |  Apache Airflow   |
                |      (MWAA)       |
                +---------+---------+
                          |
          +---------------+---------------+
          |                               |
          v                               v
 +----------------+             +----------------+
 | Data Validation|             | Data Transform |
 +----------------+             +----------------+
          |                               |
          +---------------+---------------+
                          |
                          v
                +-------------------+
                | Amazon Redshift   |
                |    Serverless     |
                +---------+---------+
                          |
                          v
                +-------------------+
                | Reporting Tables  |
                +-------------------+
```

---

# Learning Objectives

By completing this project, students will learn:

* How Amazon Redshift Serverless works
* Difference between Workgroups and Namespaces
* How to configure networking and security for Redshift
* How to deploy and manage AWS Managed Airflow (MWAA)
* How Airflow DAGs orchestrate ETL pipelines
* How to integrate Airflow with Amazon S3 and Redshift
* How to build an end-to-end data engineering workflow on AWS

---

# Project Components

## 1. Amazon Redshift Serverless

Redshift Serverless is used as the analytical data warehouse.

### Key Configurations

* Workgroup Creation
* Namespace Creation
* IAM Role Association
* Public Accessibility Configuration
* Security Group Configuration
* Query Editor v2 Access

### Why Redshift Serverless?

* No cluster management
* Automatic scaling
* Pay-per-use pricing
* High-performance analytics

---

## 2. AWS Managed Workflows for Apache Airflow (MWAA)

MWAA provides a managed Airflow environment for workflow orchestration.

### Components Configured

* Airflow Environment
* S3 Bucket for DAG Storage
* requirements.txt Dependencies
* VPC Networking
* Security Groups
* IAM Execution Role

### Why MWAA?

* Managed Airflow service
* No infrastructure maintenance
* Native AWS integration
* Enterprise-grade orchestration

---

## 3. Amazon S3 Data Lake

Two S3 buckets are used:

### Airflow Bucket

Stores:

* DAG files
* requirements.txt
* Workflow assets

### Data Bucket

Stores:

```text
spotify_data/
│
├── songs.csv
├── users.csv
│
└── streams/
      ├── streams_1.csv
      ├── streams_2.csv
      └── streams_3.csv
```

---

# Project Workflow

## Phase 1: Create Redshift Serverless

### Tasks

1. Create Workgroup
2. Create Namespace
3. Configure Admin User
4. Create IAM Role
5. Enable Public Access
6. Configure Port 5439 Access
7. Verify Connectivity

### Outcome

A fully functional Redshift Serverless data warehouse ready to receive analytical data.

---

## Phase 2: Create MWAA Environment

### Tasks

1. Create Airflow S3 Bucket
2. Create DAG Folder
3. Create Requirements Folder
4. Upload requirements.txt
5. Create MWAA Environment
6. Configure VPC and Networking
7. Configure Worker Scaling
8. Create Execution Role

### Outcome

A managed Airflow environment capable of executing ETL workflows.

---

## Phase 3: Build Analytics Layer

### Create Database

```sql
CREATE DATABASE songs_db;
```

### Create Schema

```sql
CREATE SCHEMA reporting_schema;
```

### Create Reporting Tables

* genre_level_kpis
* temp_genre_level_kpis
* only_kpis
* temp_only_kpis

### Purpose

These tables store transformed Spotify analytics and KPI calculations.

---

## Phase 4: Deploy Airflow DAG

### Steps

1. Upload DAG to S3 dags folder
2. Configure Airflow connection
3. Connect Airflow to Redshift
4. Trigger DAG execution
5. Monitor workflow execution

### Redshift Connection

```text
Connection ID : redshift_default
Connection Type : Redshift
Port : 5439
Database : songs_db
```

---

## ETL Pipeline Logic

### Extract

Airflow reads:

* songs.csv
* users.csv
* stream files

from Amazon S3.

### Transform

Airflow performs:

* Data validation
* KPI calculations
* Genre aggregations
* Business metric generation

### Load

Processed data is loaded into:

```text
reporting_schema.genre_level_kpis
reporting_schema.only_kpis
```

inside Amazon Redshift.

---

# Data Validation

After execution, validate data using:

```sql
SELECT * FROM genre_level_kpis LIMIT 10;

SELECT * FROM only_kpis LIMIT 10;
```

Expected Result:

* Records successfully loaded
* KPI metrics generated
* No workflow failures

---

# Archival Process

After successful processing:

```text
spotify_data/
│
└── streams/
      │
      └── archived/
```

Processed stream files are automatically moved to the archived folder to prevent duplicate processing.

---

# Skills Gained

After completing this project, students will have hands-on experience with:

* Data Warehousing
* ETL Pipelines
* Workflow Orchestration
* Amazon Redshift Serverless
* AWS MWAA
* Amazon S3
* IAM Security
* Cloud Data Engineering
* SQL Analytics
* Production Data Pipeline Design

---

# Real-World Use Cases

The architecture used in this project is similar to:

* Music Streaming Analytics
* Customer Behavior Analytics
* Sales Reporting Pipelines
* Data Warehouse Automation
* Marketing KPI Dashboards
* Business Intelligence Platforms

---

# Final Outcome

Successfully building this project demonstrates how modern data engineering solutions are implemented on AWS using serverless analytics and workflow orchestration services. The solution automates data ingestion, transformation, validation, loading, and archival while following cloud-native best practices.
