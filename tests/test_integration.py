import pytest
from pyspark.sql import functions as F
from datetime import datetime
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))
from ingestion.data_generator import generate_transactions


class TestIntegration:
    """Integration tests for the entire pipeline."""

    def test_end_to_end_batch_pipeline(self, spark):
        """Test complete batch processing pipeline from generation to gold."""
        # Step 1: Generate data
        pandas_df = generate_transactions(n=100)
        assert len(pandas_df) == 100
        
        # Step 2: Bronze layer - Ingest raw data
        bronze_df = spark.createDataFrame(pandas_df)
        bronze_df.write.mode("overwrite").saveAsTable("test_bronze_integration")
        
        bronze_read = spark.table("test_bronze_integration")
        assert bronze_read.count() == 100
        
        # Step 3: Silver layer - Clean and transform
        silver_df = bronze_read \
            .dropDuplicates(["txn_id"]) \
            .filter(
                (F.col("account_id").isNotNull()) &
                (F.col("amount") > 0)
            ) \
            .withColumn("date", F.to_date(F.col("timestamp"))) \
            .withColumn("amount_category",
                F.when(F.col("amount") < 10000, "Low")
                 .when(F.col("amount") < 100000, "Medium")
                 .otherwise("High")
            )
        
        silver_df.write.mode("overwrite").saveAsTable("test_silver_integration")
        silver_read = spark.table("test_silver_integration")
        assert "date" in silver_read.columns
        assert "amount_category" in silver_read.columns
        
        # Step 4: Gold layer - Business aggregations
        gold_df = silver_read.groupBy("date").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count")
        )
        
        gold_df.write.mode("overwrite").saveAsTable("test_gold_integration")
        gold_read = spark.table("test_gold_integration")
        assert gold_read.count() > 0
        
        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_bronze_integration")
        spark.sql("DROP TABLE IF EXISTS test_silver_integration")
        spark.sql("DROP TABLE IF EXISTS test_gold_integration")

    def test_data_quality_across_layers(self, spark):
        """Test data quality is maintained across all layers."""
        # Generate test data
        pandas_df = generate_transactions(n=50)
        bronze_df = spark.createDataFrame(pandas_df)
        
        # Bronze layer checks
        bronze_count = bronze_df.count()
        bronze_nulls = bronze_df.filter(F.col("txn_id").isNull()).count()
        assert bronze_nulls == 0
        
        # Silver layer - apply quality rules
        silver_df = bronze_df \
            .dropDuplicates(["txn_id"]) \
            .filter(F.col("amount") > 0)
        
        silver_count = silver_df.count()
        assert silver_count <= bronze_count
        
        # Gold layer - aggregations should maintain data integrity
        gold_df = silver_df.groupBy("location").agg(
            F.sum("amount").alias("total_amount")
        )
        
        # Total amount should match
        silver_total = silver_df.agg(F.sum("amount")).collect()[0][0]
        gold_total = gold_df.agg(F.sum("total_amount")).collect()[0][0]
        
        assert abs(silver_total - gold_total) < 0.01

    def test_fraud_detection_pipeline(self, spark):
        """Test fraud detection across pipeline layers."""
        # Create data with known fraud patterns
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (2, 1002, 250000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 1),
            (3, 1003, 300000.00, "CREDIT", "UK", datetime(2026, 7, 1, 12, 0), 1),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Silver: Add fraud indicators
        silver_df = df.withColumn(
            "high_risk",
            F.when(F.col("amount") > 100000, 1).otherwise(0)
        )
        
        # Gold: Fraud analytics
        fraud_summary = silver_df.agg(
            F.count("*").alias("total_txns"),
            F.sum(F.col("is_fraud")).alias("confirmed_fraud"),
            F.sum(F.col("high_risk")).alias("high_risk_txns")
        ).collect()[0]
        
        assert fraud_summary.confirmed_fraud == 2
        assert fraud_summary.high_risk_txns == 2

    def test_location_analytics_pipeline(self, spark):
        """Test location-based analytics through pipeline."""
        # Generate data
        pandas_df = generate_transactions(n=200)
        df = spark.createDataFrame(pandas_df)
        
        # Bronze to Silver
        silver_df = df.dropDuplicates(["txn_id"])
        
        # Gold: Location analytics
        location_analytics = silver_df.groupBy("location").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_volume"),
            F.avg("amount").alias("avg_amount"),
            F.countDistinct("account_id").alias("unique_accounts")
        )
        
        assert location_analytics.count() > 0
        assert all(col in location_analytics.columns for col in 
                   ["txn_count", "total_volume", "avg_amount", "unique_accounts"])

    def test_time_series_analytics(self, spark):
        """Test time-series analytics pipeline."""
        # Generate data
        pandas_df = generate_transactions(n=100)
        df = spark.createDataFrame(pandas_df)
        
        # Add time dimensions in silver
        silver_df = df \
            .withColumn("date", F.to_date(F.col("timestamp"))) \
            .withColumn("hour", F.hour(F.col("timestamp")))
        
        # Gold: Time series aggregations
        daily_metrics = silver_df.groupBy("date").agg(
            F.count("*").alias("daily_txns"),
            F.sum("amount").alias("daily_volume")
        ).orderBy("date")
        
        hourly_metrics = silver_df.groupBy("hour").agg(
            F.count("*").alias("hourly_txns"),
            F.avg("amount").alias("avg_hourly_amount")
        ).orderBy("hour")
        
        assert daily_metrics.count() > 0
        assert hourly_metrics.count() > 0

    def test_customer_360_view(self, spark):
        """Test customer 360 view generation."""
        # Generate data
        pandas_df = generate_transactions(n=150)
        df = spark.createDataFrame(pandas_df)
        
        # Build customer 360
        customer_360 = df.groupBy("account_id").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("lifetime_value"),
            F.avg("amount").alias("avg_transaction"),
            F.max("timestamp").alias("last_transaction_date"),
            F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("total_credits"),
            F.sum(F.when(F.col("transaction_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("total_debits"),
            F.sum(F.col("is_fraud")).alias("fraud_incidents")
        ).withColumn(
            "risk_level",
            F.when(F.col("fraud_incidents") > 0, "HIGH")
             .when(F.col("avg_transaction") > 100000, "MEDIUM")
             .otherwise("LOW")
        )
        
        assert customer_360.count() > 0
        assert all(col in customer_360.columns for col in 
                   ["total_transactions", "lifetime_value", "risk_level"])

    def test_pipeline_performance(self, spark):
        """Test pipeline performance with larger dataset."""
        import time
        
        # Generate larger dataset
        pandas_df = generate_transactions(n=1000)
        
        start_time = time.time()
        
        # Bronze
        bronze_df = spark.createDataFrame(pandas_df)
        bronze_count = bronze_df.count()
        
        # Silver
        silver_df = bronze_df.dropDuplicates(["txn_id"])
        silver_count = silver_df.count()
        
        # Gold
        gold_df = silver_df.groupBy("location").agg(F.count("*"))
        gold_count = gold_df.count()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should process 1000 records in reasonable time (< 10 seconds in local mode)
        assert duration < 10
        assert bronze_count == 1000
        assert silver_count <= 1000
        assert gold_count > 0

    def test_incremental_processing(self, spark):
        """Test incremental data processing."""
        # Initial batch
        batch1 = generate_transactions(n=50)
        df1 = spark.createDataFrame(batch1)
        df1.write.mode("overwrite").saveAsTable("test_incremental")
        
        initial_count = spark.table("test_incremental").count()
        assert initial_count == 50
        
        # Incremental batch
        batch2 = generate_transactions(n=30)
        # Adjust txn_ids to avoid collision
        batch2['txn_id'] = batch2['txn_id'] + 1000
        
        df2 = spark.createDataFrame(batch2)
        df2.write.mode("append").saveAsTable("test_incremental")
        
        final_count = spark.table("test_incremental").count()
        assert final_count == 80
        
        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_incremental")

    def test_data_lineage_tracking(self, spark):
        """Test data lineage and audit trail."""
        # Generate data with lineage tracking
        pandas_df = generate_transactions(n=50)
        df = spark.createDataFrame(pandas_df)
        
        # Bronze with metadata
        bronze_df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
                     .withColumn("source_layer", F.lit("bronze"))
        
        # Silver with metadata
        silver_df = bronze_df \
            .withColumn("processing_timestamp", F.current_timestamp()) \
            .withColumn("target_layer", F.lit("silver"))
        
        assert "ingestion_timestamp" in bronze_df.columns
        assert "source_layer" in bronze_df.columns
        assert "processing_timestamp" in silver_df.columns
        assert "target_layer" in silver_df.columns

    def test_error_recovery_pipeline(self, spark):
        """Test pipeline error recovery mechanisms."""
        # Create data with some bad records
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (2, None, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),  # Bad
            (3, 1003, -1000.00, "CREDIT", "UK", datetime(2026, 7, 1, 12, 0), 0),  # Bad
            (4, 1004, 20000.00, "DEBIT", "India", datetime(2026, 7, 1, 13, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Separate good and bad records
        good_records = df.filter(
            (F.col("account_id").isNotNull()) &
            (F.col("amount") > 0)
        )
        
        bad_records = df.filter(
            (F.col("account_id").isNull()) |
            (F.col("amount") <= 0)
        )
        
        assert good_records.count() == 2
        assert bad_records.count() == 2

    def test_multi_location_aggregation(self, spark):
        """Test aggregations across multiple dimensions."""
        # Generate data
        pandas_df = generate_transactions(n=100)
        df = spark.createDataFrame(pandas_df)
        
        # Multi-dimensional aggregation
        result = df \
            .withColumn("date", F.to_date(F.col("timestamp"))) \
            .groupBy("location", "transaction_type", "date").agg(
                F.count("*").alias("count"),
                F.sum("amount").alias("total")
            )
        
        assert result.count() > 0
        assert all(col in result.columns for col in ["location", "transaction_type", "date", "count", "total"])