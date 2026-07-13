import pytest
import sys
import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pandas as pd

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("BankingPipelineTests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_transactions_df(spark):
    """Create a sample transactions DataFrame for testing."""
    data = [
        (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
        (2, 1002, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
        (3, 1003, 150000.00, "CREDIT", "UK", datetime(2026, 7, 1, 12, 0), 1),
        (4, 1001, 2000.00, "DEBIT", "India", datetime(2026, 7, 2, 9, 0), 0),
        (5, 1004, 300000.00, "DEBIT", "US", datetime(2026, 7, 2, 10, 0), 1),
    ]
    
    columns = ["txn_id", "account_id", "amount", "transaction_type", 
               "location", "timestamp", "is_fraud"]
    
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_transactions_pandas():
    """Create a sample transactions pandas DataFrame for testing."""
    return pd.DataFrame({
        "txn_id": [1, 2, 3, 4, 5],
        "account_id": [1001, 1002, 1003, 1001, 1004],
        "amount": [5000.00, 15000.00, 150000.00, 2000.00, 300000.00],
        "transaction_type": ["CREDIT", "DEBIT", "CREDIT", "DEBIT", "DEBIT"],
        "location": ["India", "US", "UK", "India", "US"],
        "timestamp": [
            datetime(2026, 7, 1, 10, 0),
            datetime(2026, 7, 1, 11, 0),
            datetime(2026, 7, 1, 12, 0),
            datetime(2026, 7, 2, 9, 0),
            datetime(2026, 7, 2, 10, 0),
        ],
        "is_fraud": [0, 0, 1, 0, 1]
    })


@pytest.fixture
def test_catalog():
    """Return test catalog name."""
    return "hive_metastore"


@pytest.fixture
def test_schema():
    """Return test schema name."""
    return "test_banking"


@pytest.fixture
def cleanup_tables(spark, test_catalog, test_schema):
    """Cleanup test tables after tests."""
    yield
    
    # Drop test tables
    tables_to_drop = [
        "bronze_transactions",
        "silver_transactions",
        "gold_daily_summary",
        "gold_fraud_analysis"
    ]
    
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {test_catalog}.{test_schema}.{table}")
        except:
            pass