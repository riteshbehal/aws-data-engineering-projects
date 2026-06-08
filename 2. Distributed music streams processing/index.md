# Distributed Music Streams Processing on AWS

## Overview

Modern music streaming platforms generate listening events continuously throughout the day. Unlike traditional batch analytics systems, new data can arrive at unpredictable intervals — every few minutes, every hour, or at completely random times.

In this project, we simulate a real-world data engineering scenario where streaming data from a music platform is continuously delivered to Amazon S3. The objective is to process newly arrived data as quickly as possible, calculate business metrics, and make those metrics available to downstream applications without waiting for a scheduled batch process.

This is **not a traditional batch ETL or BI reporting pipeline**. The challenge is that data arrival times are unknown, while business requirements demand that analytics become available shortly after new files arrive.

To solve this problem, the project combines:

* **Amazon S3** as the storage layer
* **Apache Airflow (MWAA)** for orchestration
* **AWS Glue Spark Jobs** for distributed data processing
* **AWS Glue Python Shell Jobs** for lightweight data ingestion
* **Amazon DynamoDB** for serving analytics to downstream applications

---

## Business Scenario

A music streaming company receives listening activity data from its platform backend. These files are continuously uploaded to Amazon S3 and contain information about:

* Songs
* Users
* Listening events (streams)

Whenever new data arrives, the platform must:

1. Validate incoming datasets.
2. Calculate song-level performance metrics.
3. Store processed results for application consumption.
4. Archive processed files to avoid duplicate processing.

Because file arrivals are unpredictable, the entire workflow must be automated and event-driven rather than relying on manual execution.

---

## Solution Architecture

The workflow is orchestrated using Apache Airflow.

### Step 1: Data Validation

The Airflow DAG continuously checks whether the required datasets are available in Amazon S3.

Before processing begins, the workflow validates that:

* Required files exist
* Required columns are present
* Input data is suitable for transformation

If validation fails, processing is skipped.

---

### Step 2: Spark-Based Metric Calculation

After validation succeeds, Airflow triggers an AWS Glue Spark job.

The Spark job:

* Reads streaming activity files from S3
* Performs distributed transformations
* Calculates business KPIs
* Identifies top-performing songs and genres
* Writes transformed output back to S3

Spark is responsible for the heavy lifting because it is optimized for large-scale distributed data processing.

---

### Step 3: DynamoDB Ingestion

Once Spark processing completes, Airflow triggers a second AWS Glue job running a lightweight Python script.

This job:

* Reads the Spark output from S3
* Performs any final column mapping or reshaping
* Inserts or updates records in DynamoDB

Although Spark could write directly to DynamoDB, separating these responsibilities reduces cost significantly.

Running Spark jobs on AWS Glue is comparatively expensive because billing is based on cluster resources and execution time. A lightweight Python Shell job is much more cost-effective for simple ingestion operations.

---

### Step 4: File Archival

After successful processing:

* Input stream files are moved to an archive folder
* Duplicate processing is prevented
* Historical raw data remains available for auditing

---

## Architecture Flow

```text
Incoming Music Stream Files
            │
            ▼
        Amazon S3
            │
            ▼
      Apache Airflow
            │
            ▼
   Validate Incoming Data
            │
            ▼
 AWS Glue Spark ETL Job
            │
            ▼
     KPI Output in S3
            │
            ▼
 AWS Glue Python Job
            │
            ▼
       DynamoDB
            │
            ▼
 Downstream Applications

            │
            ▼
     Archive Raw Files
```

# Project Structure

```text
Distributed music streams processing/
│
│── Lab_1_Deploy_Spark_and_Python_Jobs_on_AWS_Glue.pdf
│── Lab_2_Orchestrate_AWS_Glue_Jobs_Using_Apache_Airflow.pdf
│
├── resources/
│   ├── dag-glue-workflow.py
│   ├── glue-dynamo.py
│   ├── glue-pyspark.py
│   ├── local-docker-development.sh
│   ├── songs.csv
│   ├── streams1.csv
│   ├── streams2.csv
│   ├── users.csv
│
└── index.md
```
# Input Datasets

### songs.csv

Contains song metadata:

```text
track_id
track_name
artists
track_genre
```

### users.csv

Contains user information.

### streams1.csv & streams2.csv

Contains listening events:

```text
user_id
track_id
listen_time
```

Upload:

```text
dag-glue-workflow.py
```

to:

```text
s3://<airflow-bucket>/dags/
```

Enable the DAG from Airflow UI.

---

## Key Learning Outcomes

This project demonstrates:

* Event-driven data processing
* Workflow orchestration using Apache Airflow
* Distributed processing with Apache Spark
* AWS Glue Spark and Python Shell jobs
* Data lake architecture using Amazon S3
* DynamoDB data ingestion patterns
* Cost optimization by separating heavy processing from lightweight ingestion
* End-to-end cloud-native data engineering workflows

---
