# CI/CD for AWS Services using GitHub Actions

## Overview

This project demonstrates how to implement CI/CD pipelines using GitHub Actions to automate the deployment of AWS resources.

The solution uses GitHub Actions as the CI/CD platform, AWS CLI for manual deployments, IAM roles with OpenID Connect for secure authentication, AWS Lambda for serverless compute, AWS Glue for ETL job deployment, Amazon ECS and ECR for containerized workloads, and Amazon S3 as the script storage layer.

The objective is to automate the build, test, and deployment of AWS services directly from GitHub repositories using event-driven workflows.

---

## Business Scenario

Imagine a data engineering team that maintains multiple AWS services including Lambda functions, Glue ETL jobs, and containerized ECS applications.

The team regularly pushes code changes and needs those changes to be tested and deployed automatically without manual intervention.

The current challenges include:

- Manual deployments are error-prone and time-consuming
- No automated testing before deployment
- No consistent deployment process across environments
- Credentials management is insecure and hard to audit

The management team wants an automated, secure, and auditable deployment pipeline.

The solution should provide:

1. Automated deployment of Lambda functions on every code push.
2. Automated deployment of Glue ETL jobs with S3 script upload.
3. Containerized ECS task deployments via Docker and ECR.
4. Secure AWS authentication using IAM roles and OIDC.
5. Deployment validation through GitHub Actions logs and AWS Console.

---

## Solution Architecture

### Step 1: Store Code in GitHub

Application code, ETL scripts, Dockerfiles, and workflow YAML files are committed to a GitHub repository.

### Step 2: Trigger Workflow on Push

A push to the `main` branch triggers the GitHub Actions workflow defined in `.github/workflows/workflow.yml`.

### Step 3: Authenticate with AWS

The workflow assumes an IAM role using OpenID Connect (OIDC), eliminating the need for long-lived static credentials stored as secrets.

### Step 4: Run Tests

Unit tests are executed before any deployment step to catch issues early and maintain code quality.

### Step 5: Deploy AWS Resources

Depending on the workflow, the pipeline deploys one or more of the following:

- Zips and deploys a Lambda function using AWS CLI
- Uploads a Glue script to S3 and creates or updates the Glue job
- Builds a Docker image, pushes it to ECR, and registers an ECS task definition

### Step 6: Validate Deployment

Deployment success is verified through GitHub Actions logs and confirmed directly in the AWS Console.

---

## Architecture Flow

```text
   Code Push to GitHub
           │
           ▼
  GitHub Actions Trigger
           │
           ▼
  AWS OIDC Authentication
           │
           ▼
     Run Unit Tests
           │
           ▼
  ┌────────┴─────────┬──────────────────┐
  ▼                  ▼                  ▼
Lambda            Glue Job           ECS Task
Deployment        Deployment         Deployment
  │                  │                  │
  ▼                  ▼                  ▼
Zip + Deploy     S3 Upload +       Docker Build +
via CLI          CLI Create        ECR Push +
  │                  │             Task Register
  └────────┬─────────┴──────────────────┘
           ▼
   Deployment Validation
   (GitHub Logs + AWS Console)
```

---

## Input Files

### Lambda Deployment

Contains the serverless function code including:

- `lambda_function.py` — core handler logic
- `test_lambda.py` — unit tests

### Glue Deployment

Contains the ETL script and test files including:

- `mysql_extraction.py` — PythonShell Glue script
- Test scripts for validation

### ECS Deployment

Contains the containerized application files including:

- `app.py` — application code
- `requirements.txt` — Python dependencies
- `dockerfile` — container build instructions
- `task-definition.json` — ECS task definition
- `test_app.py` — unit tests

---

## Key Services Used

### GitHub Actions

Provides the CI/CD platform. Workflows are defined in YAML and triggered automatically by repository events such as code pushes and pull requests.

### AWS IAM (OIDC)

Manages secure, short-lived authentication between GitHub Actions and AWS using OpenID Connect, removing the need for static access keys.

### AWS Lambda

Hosts serverless functions deployed as zip packages via the AWS CLI inside the workflow.

### AWS Glue

Runs PythonShell ETL jobs with scripts stored in S3. Jobs are created or updated by the workflow on every push.

### Amazon S3

Stores Glue scripts uploaded by the CI/CD pipeline before job creation.

### Amazon ECS

Hosts containerized applications as task definitions. New task revisions are registered automatically by the deployment workflow.

### Amazon ECR

Stores Docker images built and pushed by the GitHub Actions pipeline for use by ECS.

### AWS CLI

Executes deployment commands inside the GitHub Actions runner environment for Lambda and Glue resources.

### IAM Roles

Separate roles are created for each service (Lambda, Glue, ECS) with least-privilege permissions and trust policies scoped to the specific GitHub repository.

### CloudWatch

Monitors Glue job runs, ECS task execution, and Lambda function invocations across all deployed resources.

---

## Deployments Automated

### Lambda Function Deployment

Automatically packages, tests, and deploys a Python Lambda function on every push to `main`.

### Glue ETL Job Deployment

Automatically uploads the ETL script to S3 and creates or updates the Glue job definition.

### ECS Task Deployment

Automatically builds a Docker image, pushes it to ECR, and registers a new ECS task definition revision.

---

## Key Learning Outcomes

After completing this project, you will understand how to:

- Deploy Lambda functions using AWS CLI
- Deploy Glue jobs using AWS CLI and S3
- Configure GitHub Actions workflow YAML files
- Automate deployments using CI/CD pipelines
- Deploy ECS tasks via a GitHub Actions pipeline
- Use IAM roles with OIDC for secure GitHub-to-AWS authentication
- Run unit tests as part of a deployment pipeline
- Validate deployments using GitHub Actions logs and the AWS Console
- Design reusable CI/CD workflows for multiple AWS services

---

## Project Result

By the end of this project, Lambda functions, Glue ETL jobs, and ECS containerized tasks are all deployed automatically whenever code is pushed to the GitHub repository. Each pipeline runs unit tests, authenticates securely via IAM OIDC, deploys the target resource using AWS CLI, and produces a verifiable audit trail through GitHub Actions logs and the AWS Console.