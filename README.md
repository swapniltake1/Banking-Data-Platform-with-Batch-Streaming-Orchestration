# Cloud-Native-Banking-Data-Platform-with-Batch-Streaming-Orchestration

## Overview
This project implements a comprehensive banking data platform using Databricks, featuring batch and streaming data processing, layered architecture (Bronze, Silver, Gold), and orchestration.

## Architecture
```
Raw Data (Kafka/Files) -> Bronze Layer (Raw Ingestion) -> Silver Layer (Cleansed Data) -> Gold Layer (Aggregated Analytics)
                                      |                        |
                                   Batch Jobs              DBT Transformations
                                      |
                                   Streaming Ingestion
```

## Components
- **Data Generation**: Simulates banking transactions.
- **Bronze Layer**: Raw data ingestion and storage.
- **Silver Layer**: Data cleansing and transformation using DBT.
- **Gold Layer**: Business-ready aggregates and analytics.
- **Streaming Ingestion**: Real-time data processing (supports Kafka for production).

## Setup
1. Configure Databricks workspace.
2. For Kafka streaming: Set up Kafka cluster and update configs/config.yaml with server details.
3. Set up secrets in config.yaml.
4. Run `python scripts/deploy.py dev` for deployment.

## Testing
Run `pytest tests/` for unit tests.
Run `dbt test` for data tests.

## CI/CD
Automated via GitHub Actions. Deploys on main branch push.

## Security
- Uses Databricks secrets for credentials.
- Data masking for sensitive fields.
- RBAC enforced via Databricks permissions.