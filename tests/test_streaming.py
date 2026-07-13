import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from datetime import datetime
import time


class TestStreaming:
    """Test cases for streaming ingestion functionality."""

    @pytest.fixture
    def streaming_schema(self):
        """Define schema for streaming data."""
        return StructType([
            StructField("txn_id", IntegerType(), False),
            StructField("account_id", IntegerType(), False),
            StructField("amount", DoubleType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("location", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("is_fraud", IntegerType(), False)
        ])

    def test_streaming_schema_definition(self, streaming_schema):
        """Test that streaming schema is properly defined."""
        assert streaming_schema is not None
        assert len(streaming_schema.fields) == 7
        
        field_names = [field.name for field in streaming_schema.fields]
        required_fields = ["txn_id", "account_id", "amount", "transaction_type",
                          "location", "timestamp", "is_fraud"]
        assert all(field in field_names for field in required_fields)

    def test_streaming_dataframe_creation(self, spark, sample_transactions_df):
        """Test creation of streaming DataFrame from batch data."""
        # Write batch data to a temp location
        temp_path = "/tmp/test_streaming_source"
        sample_transactions_df.write.mode("overwrite").parquet(temp_path)
        
        # Create streaming DataFrame
        streaming_df = spark.readStream.parquet(temp_path)
        
        assert streaming_df.isStreaming
        assert set(streaming_df.columns) == set(sample_transactions_df.columns)

    def test_streaming_read_write(self, spark, sample_transactions_df):
        """Test basic streaming read and write operations."""
        source_path = "/tmp/test_stream_source"
        target_path = "/tmp/test_stream_target"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # Read as stream
        streaming_df = spark.readStream.parquet(source_path)
        
        # Write to target (with checkpoint)
        query = streaming_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/test_checkpoint") \
            .option("path", target_path) \
            .outputMode("append") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        # Verify data was written
        result_df = spark.read.parquet(target_path)
        assert result_df.count() == sample_transactions_df.count()

    def test_streaming_transformations(self, spark, sample_transactions_df):
        """Test streaming transformations."""
        source_path = "/tmp/test_stream_transform"
        target_path = "/tmp/test_stream_result"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # Read as stream and apply transformations
        streaming_df = spark.readStream.parquet(source_path)
        
        transformed_df = streaming_df \
            .filter(F.col("amount") > 1000) \
            .withColumn("date", F.to_date(F.col("timestamp"))) \
            .withColumn("high_value", F.when(F.col("amount") > 100000, 1).otherwise(0))
        
        # Write transformed stream
        query = transformed_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/test_transform_checkpoint") \
            .option("path", target_path) \
            .outputMode("append") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        # Verify transformations
        result_df = spark.read.parquet(target_path)
        assert all(row.amount > 1000 for row in result_df.collect())
        assert "date" in result_df.columns
        assert "high_value" in result_df.columns

    def test_streaming_aggregations(self, spark, sample_transactions_df):
        """Test streaming aggregations with watermarking."""
        source_path = "/tmp/test_stream_agg"
        target_path = "/tmp/test_stream_agg_result"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # Read as stream with watermark
        streaming_df = spark.readStream.parquet(source_path)
        
        # Apply aggregation with watermark
        aggregated_df = streaming_df \
            .withWatermark("timestamp", "1 hour") \
            .groupBy(
                F.window(F.col("timestamp"), "1 hour"),
                F.col("location")
            ).agg(
                F.count("*").alias("txn_count"),
                F.sum("amount").alias("total_amount")
            )
        
        # Write aggregated stream
        query = aggregated_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/test_agg_checkpoint") \
            .option("path", target_path) \
            .outputMode("append") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        # Verify aggregations
        result_df = spark.read.parquet(target_path)
        assert "window" in result_df.columns
        assert "location" in result_df.columns
        assert "txn_count" in result_df.columns

    def test_streaming_fraud_detection(self, spark, sample_transactions_df):
        """Test real-time fraud detection in streaming."""
        source_path = "/tmp/test_stream_fraud"
        target_path = "/tmp/test_stream_fraud_result"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # Read as stream
        streaming_df = spark.readStream.parquet(source_path)
        
        # Apply fraud detection logic
        fraud_df = streaming_df \
            .filter(F.col("is_fraud") == 1) \
            .withColumn("alert_level",
                F.when(F.col("amount") > 100000, "CRITICAL")
                 .when(F.col("amount") > 50000, "HIGH")
                 .otherwise("MEDIUM")
            )
        
        # Write fraud alerts
        query = fraud_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/test_fraud_checkpoint") \
            .option("path", target_path) \
            .outputMode("append") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        # Verify fraud detection
        result_df = spark.read.parquet(target_path)
        if result_df.count() > 0:
            assert all(row.is_fraud == 1 for row in result_df.collect())
            assert "alert_level" in result_df.columns

    def test_streaming_deduplication(self, spark):
        """Test streaming deduplication logic."""
        source_path = "/tmp/test_stream_dedup"
        target_path = "/tmp/test_stream_dedup_result"
        
        # Create data with duplicates
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (2, 1002, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        df.write.mode("overwrite").parquet(source_path)
        
        # Read as stream and deduplicate
        streaming_df = spark.readStream.parquet(source_path)
        
        deduped_df = streaming_df \
            .withWatermark("timestamp", "1 hour") \
            .dropDuplicates(["txn_id"])
        
        query = deduped_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/test_dedup_checkpoint") \
            .option("path", target_path) \
            .outputMode("append") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        result_df = spark.read.parquet(target_path)
        assert result_df.count() == 2  # Should have only 2 unique transactions

    def test_streaming_output_modes(self, spark, sample_transactions_df):
        """Test different streaming output modes."""
        source_path = "/tmp/test_stream_modes"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # Test append mode
        streaming_df = spark.readStream.parquet(source_path)
        assert streaming_df.isStreaming
        
        # Note: Complete and Update modes require aggregations
        # Append mode works with any transformation

    def test_streaming_checkpoint_recovery(self, spark, sample_transactions_df):
        """Test streaming checkpoint and recovery."""
        source_path = "/tmp/test_stream_checkpoint_source"
        target_path = "/tmp/test_stream_checkpoint_target"
        checkpoint_path = "/tmp/test_stream_checkpoint_location"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # First streaming query
        streaming_df = spark.readStream.parquet(source_path)
        
        query = streaming_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", target_path) \
            .outputMode("append") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        # Verify checkpoint directory exists
        import os
        assert os.path.exists(checkpoint_path.replace("file:", ""))

    def test_streaming_trigger_modes(self, spark, sample_transactions_df):
        """Test different streaming trigger modes."""
        source_path = "/tmp/test_stream_trigger"
        
        # Write source data
        sample_transactions_df.write.mode("overwrite").parquet(source_path)
        
        # Test trigger once
        streaming_df = spark.readStream.parquet(source_path)
        
        query = streaming_df.writeStream \
            .format("memory") \
            .queryName("test_trigger") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        # Verify data was processed
        result_df = spark.sql("SELECT * FROM test_trigger")
        assert result_df.count() > 0

    def test_streaming_error_handling(self, spark):
        """Test streaming error handling for bad records."""
        source_path = "/tmp/test_stream_errors"
        
        # Create data with potential issues
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (2, None, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        df.write.mode("overwrite").parquet(source_path)
        
        # Read with filtering
        streaming_df = spark.readStream.parquet(source_path)
        
        # Filter out bad records
        clean_df = streaming_df.filter(F.col("account_id").isNotNull())
        
        query = clean_df.writeStream \
            .format("memory") \
            .queryName("test_errors") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination(timeout=30)
        
        result_df = spark.sql("SELECT * FROM test_errors")
        assert all(row.account_id is not None for row in result_df.collect())