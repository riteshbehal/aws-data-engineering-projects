# AWS Rental Apartments Data Pipeline

## Overview

This project demonstrates an end-to-end AWS batch data pipeline for a rental apartment analytics use case.

The workflow extracts data from Amazon Aurora MySQL, stages it in Amazon S3, loads it into Amazon Redshift, transforms it into a star-schema process layer, and orchestrates the full pipeline with AWS Step Functions and Amazon EventBridge.

---

## Project Flow

### 1. Set up Aurora MySQL
- Create the source database
- Load rental apartment data

### 2. Extract data to S3
- Use a Glue Python Shell job
- Support full and incremental loads
- Store extracted files in S3

### 3. Load Redshift raw layer
- Create Redshift database and raw schema
- Copy extracted data into the raw zone

### 4. Build the curated layer
- Create the process schema
- Load dimensional and fact tables
- Apply star schema modeling

### 5. Build the presentation layer
- Create reporting tables
- Store final business metrics

### 6. Orchestrate the workflow
- Use Step Functions to run all Glue jobs in sequence

### 7. Schedule the workflow
- Use EventBridge to run the pipeline automatically

---

## Architecture

```text
Aurora MySQL
   ↓
AWS Glue Python Shell
   ↓
Amazon S3
   ↓
AWS Glue Python Shell
   ↓
Amazon Redshift Raw Layer
   ↓
AWS Glue Python Shell
   ↓
Amazon Redshift Curated Layer
   ↓
AWS Glue Python Shell
   ↓
Amazon Redshift Presentation Layer
   ↓
AWS Step Functions
   ↓
Amazon EventBridge
```

---

## Key Components

### Source Layer
- Amazon Aurora MySQL

### Landing Layer
- Amazon S3

### Warehouse Layer
- Amazon Redshift

### Orchestration
- AWS Step Functions
- Amazon EventBridge

### Supporting Services
- AWS Glue
- AWS Secrets Manager
- IAM
- DynamoDB
- CloudWatch Logs

---

## Main Data Layers

### Raw Layer
Stores source data in its original form.

### Curated Layer
Applies data modeling and warehouse transformations.

### Presentation Layer
Stores final business metrics for reporting and analytics.

---

## Main Tables

### Aurora MySQL
- `apartments`
- `apartment_attributes`
- `apartment_viewings`

### Redshift Raw Layer
- `raw_zone.apartments`
- `raw_zone.apartment_attributes`
- `raw_zone.apartment_viewings`

### Redshift Process Layer
- `process_zone.dim_apartments`
- `process_zone.dim_users`
- `process_zone.fact_apartment_viewings`

### DynamoDB
- `IncrementalLoadConfigurations`

---

## Services Overview
- Amazon Aurora MySQL – The source database. This is where the original rental apartment data is stored.
- Amazon S3 – The storage bucket. Data is first saved here in files before being loaded into Redshift.
- AWS Glue – The ETL service. It runs the jobs that extract, move, and transform data.
- AWS Glue Python Shell – A lighter Glue job type used for small scripts like extraction or loading data into Redshift.
- Amazon Redshift – The data warehouse. It stores cleaned and organized data for reporting and analytics.
- AWS Step Functions – The workflow manager. It runs the Glue jobs one after another in the correct order.
- Amazon EventBridge – The scheduler. It starts the whole workflow automatically at fixed intervals.
- AWS Secrets Manager – Stores database usernames and passwords safely.
- IAM – Controls permissions. It decides which AWS services can access others.
- Amazon CloudWatch – Logs and monitoring. It helps you see whether jobs ran successfully or failed.
- DynamoDB – A fast NoSQL database used here to track incremental load settings.

## Learning Outcomes

After completing this project, you will understand how to:

- build a cloud data warehouse pipeline on AWS
- extract data from Aurora MySQL
- stage data in Amazon S3
- load data into Redshift
- design raw, curated, and presentation layers
- automate jobs with Step Functions
- schedule workflows with EventBridge
- keep the pipeline simple and cost effective

---

## Project Result

By the end of the project, rental apartment data moves from the operational database into a fully automated AWS data warehouse pipeline that is ready for reporting and analytics.

