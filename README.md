# Banking Data Platform with Batch, Streaming, and Orchestration

[![CI/CD](https://github.com/swapniltake1/Banking-Data-Platform-with-Batch-Streaming-Orchestration/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/swapniltake1/Banking-Data-Platform-with-Batch-Streaming-Orchestration/actions)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A comprehensive, enterprise-grade data engineering platform for banking transactions, built on Databricks. This project demonstrates batch processing, real-time streaming, data orchestration, and modern DevOps practices for scalable, secure, and compliant data pipelines.

## Table of Contents
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## Features

### Core Data Pipeline
- **Batch Processing**: ETL pipelines for historical data ingestion and transformation.
- **Real-Time Streaming**: Continuous data ingestion with support for Apache Kafka or simulated streams.
- **Multi-Layer Architecture**: Bronze (raw), Silver (cleansed), Gold (aggregated) layers for data maturity.
- **Data Quality**: Automated validation, schema enforcement, and anomaly detection.
- **ML-Powered Fraud Detection**: Machine learning models for advanced fraud detection using PySpark MLlib and MLflow.

### Enterprise Enhancements
- **CI/CD Pipeline**: Automated linting, testing, and deployment via GitHub Actions.
- **Testing Framework**: Unit tests for Python code and DBT data tests for transformations.
- **Monitoring & Logging**: Structured logging with Python's logging module and Databricks job metrics.
- **Security & Compliance**: Secrets management, data masking, encryption, and RBAC.
- **Multi-Environment Support**: Separate configs for dev, staging, and prod environments.
- **Scalability**: Partitioning, auto-scaling clusters, and optimized queries.
- **Containerization**: Docker support for portability outside Databricks.

### Additional Capabilities
- **Orchestration**: Databricks Jobs for workflow management and scheduling.
- **Data Cataloging**: Unity Catalog integration for metadata management.
- **Alerting**: Configurable notifications for pipeline failures.
- **Documentation**: Comprehensive guides and architecture diagrams.

## Tech Stack

### Data Processing
- **Databricks**: Unified analytics platform for big data processing.
- **PySpark**: Distributed data processing and ETL.
- **Delta Lake**: ACID transactions, time travel, and optimized storage.
- **DBT (Data Build Tool)**: SQL-based data transformations and testing.
- **MLflow**: Model lifecycle management for machine learning pipelines.

### Streaming
- **Apache Kafka**: Real-time data ingestion (optional, with simulated fallback).
- **Structured Streaming**: Spark's streaming API for continuous processing.

### Infrastructure & DevOps
- **GitHub Actions**: CI/CD automation.
- **Databricks CLI**: Command-line deployment and management.
- **Docker**: Containerization for local development.
- **Terraform**: Infrastructure as Code (IaC) for advanced deployments.

### Languages & Tools
- **Python**: Core scripting, data generation, and utilities.
- **SQL**: DBT transformations and queries.
- **YAML**: Configuration management.
- **Pytest**: Unit testing.
- **Black & Flake8**: Code formatting and linting.

### Security
- **Azure Key Vault / Databricks Secrets**: Secure credential storage.
- **RBAC**: Role-based access controls.

## Architecture

```
Raw Data Sources (Files/Kafka) → Databricks Ingestion
                                      ↓
Bronze Layer (Raw Storage in Delta) → Silver Layer (Cleansed via DBT)
                                      ↓
Gold Layer (Aggregated Analytics) → Dashboards/BI Tools
                                      ↓
Streaming Ingestion (Real-Time Processing)
```

### Data Flow
1. **Data Generation**: Simulate or ingest banking transactions.
2. **Bronze Layer**: Store raw data with minimal processing.
3. **Silver Layer**: Clean, transform, and enrich data using DBT models.
4. **Gold Layer**: Aggregate for business intelligence (e.g., fraud detection, reporting).
5. **Streaming**: Process real-time data concurrently.

### Key Components
- `notebooks/`: Databricks notebooks for each pipeline stage.
- `dbt/`: DBT models and tests for transformations.
- `src/`: Python utilities (e.g., data generators).
- `configs/`: Environment-specific configurations.
- `tests/`: Unit and integration tests.
- `scripts/`: Deployment and automation scripts.
- `.github/`: CI/CD workflows.

## Prerequisites

- **Databricks Account**: Workspace with admin access.
- **Cluster**: Databricks Runtime 13.3+ with PySpark and DBT support.
- **GitHub Repository**: For CI/CD (optional).
- **Kafka Cluster**: For real streaming (e.g., Confluent Cloud; optional for simulated mode).
- **Python 3.9+**: For local development.
- **Databricks CLI**: `pip install databricks-cli`.
- **Docker**: For containerized runs (optional).

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/swapniltake1/Banking-Data-Platform-with-Batch-Streaming-Orchestration.git
   cd Banking-Data-Platform-with-Batch-Streaming-Orchestration
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Databricks CLI**:
   ```bash
   databricks configure --token
   # Enter your Databricks host (e.g., https://your-workspace.cloud.databricks.com) and token.
   ```

4. **Set Up Secrets**:
   - In Databricks: Create a secrets scope and add tokens/keys.
   - Update `configs/config.yaml` with your values (e.g., Kafka servers, tokens).

5. **For Kafka (Optional)**:
   - Deploy Kafka and create topic `banking-transactions`.
   - Update `configs/config.yaml` with bootstrap servers.

## Usage

### Running Locally (Simulated)
1. **Generate Data**:
   ```bash
   python src/ingestion/data_generator.py
   ```

2. **Run Tests**:
   ```bash
   pytest tests/
   ```

3. **Build Docker Image**:
   ```bash
   docker build -t banking-platform .
   docker run banking-platform
   ```

### Running on Databricks

1. **Import Notebooks**:
   - Upload `notebooks/` to Databricks Workspace > Shared > Banking-Platform.

2. **Attach Cluster**:
   - Ensure libraries are installed: pandas, pyspark, dbt-databricks.

3. **Run Pipelines**:
   - Execute notebooks in order:
     - `01_data_generation.ipynb`: Generates sample transactions.
     - `02_bronze_layer.ipynb`: Ingests to bronze table.
     - `03_silver_layer.ipynb`: Runs DBT (`dbt run` in terminal).
     - `04_gold_layer.ipynb`: Aggregates data.
     - `05_streaming_ingestion.ipynb`: Starts streaming (uncomment Kafka code for real ingestion).

4. **Monitor**:
   - Check Databricks Jobs for runs and logs.
   - View data in Tables > banking.banking schema.

### Using Orchestration
- **Manual Job Creation**: In Databricks, create a job with the notebooks as tasks.
- **Automated Deployment**: Run `python scripts/deploy.py dev` (after CLI config).

### Streaming with Kafka
- Uncomment Kafka code in `05_streaming_ingestion.ipynb`.
- Ensure Kafka config in `config.yaml` is set.
- Producer example: Send JSON messages to `banking-transactions` topic.

## Testing

- **Unit Tests**: `pytest tests/` (validates data generator).
- **DBT Tests**: `dbt test` (schema and data quality checks).
- **Integration**: Run full pipeline and verify data flow.
- **CI/CD**: Automated on GitHub pushes.

## Deployment

- **Dev/Prod**: Use `scripts/deploy.py` with env arg (e.g., `prod`).
- **CI/CD**: Push to main branch triggers GitHub Actions for deployment.
- **IaC**: Extend with Terraform for cluster/job provisioning.

## Contributing

1. Fork the repo.
2. Create a feature branch: `git checkout -b feature/your-feature`.
3. Commit changes: `git commit -m "feat: add your feature"`.
4. Push and open a PR.
5. Ensure tests pass and follow code standards (Black, Flake8).

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.

---

For issues or questions, open a GitHub issue. This platform is designed for production banking data workloads with high reliability and compliance.