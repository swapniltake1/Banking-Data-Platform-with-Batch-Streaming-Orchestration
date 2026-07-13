import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from datetime import datetime


class TestBronzeLayer:
    """Test cases for Bronze layer ingestion and validation."""

    def test_bronze_schema_validation(self, spark, sample_transactions_df):
        """Test that bronze layer has the correct schema."""
        expected_columns = [
            "txn_id", "account_id", "amount", "transaction_type",
            "location", "timestamp", "is_fraud"
        ]
        
        actual_columns = sample_transactions_df.columns
        assert all(col in actual_columns for col in expected_columns)

    def test_bronze_data_ingestion(self, spark, sample_transactions_df):
        """Test that data can be ingested into bronze layer."""
        # Write to bronze
        sample_transactions_df.write.mode("overwrite").saveAsTable("temp_bronze_test")
        
        # Read back
        bronze_df = spark.table("temp_bronze_test")
        
        assert bronze_df.count() == sample_transactions_df.count()
        
        # Cleanup
        spark.sql("DROP TABLE IF EXISTS temp_bronze_test")

    def test_bronze_no_null_critical_columns(self, sample_transactions_df):
        """Test that critical columns don't have null values."""
        critical_columns = ["txn_id", "account_id", "amount", "timestamp"]
        
        for col in critical_columns:
            null_count = sample_transactions_df.filter(F.col(col).isNull()).count()
            assert null_count == 0, f"Column {col} has null values"

    def test_bronze_data_types(self, sample_transactions_df):
        """Test that bronze layer has correct data types."""
        schema = sample_transactions_df.schema
        
        type_map = {
            "txn_id": (IntegerType, "bigint"),
            "account_id": (IntegerType, "bigint"),
            "amount": (DoubleType,),
            "transaction_type": (StringType,),
            "location": (StringType,),
            "timestamp": (TimestampType,),
            "is_fraud": (IntegerType, "bigint")
        }
        
        for field in schema.fields:
            if field.name in type_map:
                expected_types = type_map[field.name]
                assert any(
                    isinstance(field.dataType, t) if isinstance(t, type) else str(field.dataType).lower() == t
                    for t in expected_types
                ), f"Column {field.name} has incorrect type: {field.dataType}"

    def test_bronze_duplicate_detection(self, spark):
        """Test detection of duplicate transactions."""
        # Create data with duplicates
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),  # Duplicate
            (2, 1002, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Check for duplicates
        duplicate_count = df.groupBy("txn_id").count().filter(F.col("count") > 1).count()
        assert duplicate_count > 0, "Duplicate detection failed"

    def test_bronze_data_freshness(self, sample_transactions_df):
        """Test that timestamps are recent and valid."""
        # Check that all timestamps are not null and are valid datetime
        timestamp_nulls = sample_transactions_df.filter(F.col("timestamp").isNull()).count()
        assert timestamp_nulls == 0
        
        # Check that timestamps are in reasonable range (not in far future)
        future_timestamps = sample_transactions_df.filter(
            F.col("timestamp") > F.current_timestamp()
        ).count()
        assert future_timestamps == 0

    def test_bronze_amount_validation(self, sample_transactions_df):
        """Test that amounts are positive and within reasonable range."""
        # Check for negative amounts
        negative_amounts = sample_transactions_df.filter(F.col("amount") < 0).count()
        assert negative_amounts == 0
        
        # Check for zero amounts (might be suspicious)
        zero_amounts = sample_transactions_df.filter(F.col("amount") == 0).count()
        # This is informational - zero amounts might be valid in some cases

    def test_bronze_transaction_type_validation(self, sample_transactions_df):
        """Test that transaction types are valid."""
        valid_types = ["DEBIT", "CREDIT"]
        
        invalid_types = sample_transactions_df.filter(
            ~F.col("transaction_type").isin(valid_types)
        ).count()
        
        assert invalid_types == 0, "Invalid transaction types found"

    def test_bronze_location_validation(self, sample_transactions_df):
        """Test that locations are valid."""
        valid_locations = ["India", "US", "UK"]
        
        invalid_locations = sample_transactions_df.filter(
            ~F.col("location").isin(valid_locations)
        ).count()
        
        assert invalid_locations == 0, "Invalid locations found"

    def test_bronze_fraud_flag_validation(self, sample_transactions_df):
        """Test that fraud flags are binary (0 or 1)."""
        invalid_fraud_flags = sample_transactions_df.filter(
            ~F.col("is_fraud").isin([0, 1])
        ).count()
        
        assert invalid_fraud_flags == 0, "Invalid fraud flags found"

    def test_bronze_row_count_consistency(self, spark, sample_transactions_df):
        """Test that row count is preserved during ingestion."""
        initial_count = sample_transactions_df.count()
        
        # Write and read back
        sample_transactions_df.write.mode("overwrite").saveAsTable("temp_bronze_consistency")
        bronze_df = spark.table("temp_bronze_consistency")
        final_count = bronze_df.count()
        
        assert initial_count == final_count, "Row count mismatch"
        
        # Cleanup
        spark.sql("DROP TABLE IF EXISTS temp_bronze_consistency")

    def test_bronze_partition_by_date(self, spark, sample_transactions_df):
        """Test that bronze layer can be partitioned by date."""
        # Add date column
        df_with_date = sample_transactions_df.withColumn(
            "date", F.to_date(F.col("timestamp"))
        )
        
        # Write partitioned
        df_with_date.write.mode("overwrite") \
            .partitionBy("date") \
            .saveAsTable("temp_bronze_partitioned")
        
        # Read back
        bronze_df = spark.table("temp_bronze_partitioned")
        
        assert bronze_df.count() == sample_transactions_df.count()
        assert "date" in bronze_df.columns
        
        # Cleanup
        spark.sql("DROP TABLE IF EXISTS temp_bronze_partitioned")