import pytest
from pyspark.sql import functions as F
from datetime import datetime


class TestSilverLayer:
    """Test cases for Silver layer transformations and cleaning."""

    def test_silver_deduplication(self, spark):
        """Test that duplicates are removed in silver layer."""
        # Create data with duplicates
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (2, 1002, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
            (2, 1002, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Remove duplicates
        silver_df = df.dropDuplicates(["txn_id"])
        
        assert silver_df.count() == 2
        assert df.count() == 4

    def test_silver_data_quality_filters(self, spark, sample_transactions_df):
        """Test that invalid data is filtered in silver layer."""
        # Add some invalid records
        invalid_data = [
            (100, None, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),  # Null account_id
            (101, 1001, -1000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),  # Negative amount
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        invalid_df = spark.createDataFrame(invalid_data, columns)
        combined_df = sample_transactions_df.union(invalid_df)
        
        # Apply silver layer filters
        silver_df = combined_df.filter(
            (F.col("account_id").isNotNull()) &
            (F.col("amount") > 0)
        )
        
        # Should only have valid records
        assert silver_df.count() == sample_transactions_df.count()

    def test_silver_add_derived_columns(self, sample_transactions_df):
        """Test addition of derived columns in silver layer."""
        # Add derived columns
        silver_df = sample_transactions_df.withColumn(
            "date", F.to_date(F.col("timestamp"))
        ).withColumn(
            "hour", F.hour(F.col("timestamp"))
        ).withColumn(
            "amount_category",
            F.when(F.col("amount") < 10000, "Low")
             .when(F.col("amount") < 100000, "Medium")
             .otherwise("High")
        )
        
        assert "date" in silver_df.columns
        assert "hour" in silver_df.columns
        assert "amount_category" in silver_df.columns

    def test_silver_standardize_location(self, spark):
        """Test location standardization in silver layer."""
        data = [
            (1, 1001, 5000.00, "CREDIT", "india", datetime(2026, 7, 1, 10, 0), 0),
            (2, 1002, 15000.00, "DEBIT", "INDIA", datetime(2026, 7, 1, 11, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Standardize location
        silver_df = df.withColumn(
            "location_std", F.initcap(F.col("location"))
        )
        
        locations = [row.location_std for row in silver_df.collect()]
        assert all(loc == "India" for loc in locations)

    def test_silver_enrich_with_risk_score(self, sample_transactions_df):
        """Test adding risk score based on amount and location."""
        # Add risk score
        silver_df = sample_transactions_df.withColumn(
            "risk_score",
            F.when((F.col("amount") > 100000) & (F.col("location") == "US"), 3)
             .when((F.col("amount") > 100000), 2)
             .when((F.col("amount") > 50000), 1)
             .otherwise(0)
        )
        
        assert "risk_score" in silver_df.columns
        
        # Verify risk scores are calculated correctly
        high_risk = silver_df.filter(
            (F.col("amount") > 100000) & (F.col("location") == "US")
        ).select("risk_score").first()
        
        if high_risk:
            assert high_risk.risk_score == 3

    def test_silver_time_based_aggregations(self, sample_transactions_df):
        """Test time-based aggregations for silver layer."""
        # Add date and aggregate by date
        silver_df = sample_transactions_df.withColumn(
            "date", F.to_date(F.col("timestamp"))
        )
        
        daily_agg = silver_df.groupBy("date").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount")
        )
        
        assert daily_agg.count() > 0
        assert "txn_count" in daily_agg.columns
        assert "total_amount" in daily_agg.columns
        assert "avg_amount" in daily_agg.columns

    def test_silver_account_level_enrichment(self, sample_transactions_df):
        """Test account-level enrichment calculations."""
        # Calculate account-level statistics
        account_stats = sample_transactions_df.groupBy("account_id").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count")
        )
        
        # Join back to enrich silver data
        silver_df = sample_transactions_df.join(
            account_stats,
            on="account_id",
            how="left"
        )
        
        assert "txn_count" in silver_df.columns
        assert "total_amount" in silver_df.columns
        assert "fraud_count" in silver_df.columns

    def test_silver_null_handling(self, spark):
        """Test null value handling in silver layer."""
        data = [
            (1, 1001, 5000.00, "CREDIT", None, datetime(2026, 7, 1, 10, 0), 0),
            (2, 1002, 15000.00, None, "US", datetime(2026, 7, 1, 11, 0), 0),
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Fill nulls with defaults
        silver_df = df.fillna({
            "location": "Unknown",
            "transaction_type": "UNKNOWN"
        })
        
        null_locations = silver_df.filter(F.col("location").isNull()).count()
        null_types = silver_df.filter(F.col("transaction_type").isNull()).count()
        
        assert null_locations == 0
        assert null_types == 0

    def test_silver_outlier_detection(self, spark):
        """Test outlier detection in silver layer."""
        data = [
            (1, 1001, 5000.00, "CREDIT", "India", datetime(2026, 7, 1, 10, 0), 0),
            (2, 1002, 15000.00, "DEBIT", "US", datetime(2026, 7, 1, 11, 0), 0),
            (3, 1003, 10000000.00, "CREDIT", "UK", datetime(2026, 7, 1, 12, 0), 0),  # Outlier
        ]
        
        columns = ["txn_id", "account_id", "amount", "transaction_type", 
                   "location", "timestamp", "is_fraud"]
        
        df = spark.createDataFrame(data, columns)
        
        # Flag outliers (amounts > 1M)
        silver_df = df.withColumn(
            "is_outlier",
            F.when(F.col("amount") > 1000000, 1).otherwise(0)
        )
        
        outlier_count = silver_df.filter(F.col("is_outlier") == 1).count()
        assert outlier_count == 1

    def test_silver_currency_normalization(self, sample_transactions_df):
        """Test currency normalization (if applicable)."""
        # Add currency column and normalize to USD
        silver_df = sample_transactions_df.withColumn(
            "currency", F.lit("INR")
        ).withColumn(
            "amount_usd",
            F.when(F.col("currency") == "INR", F.col("amount") / 83.0)
             .otherwise(F.col("amount"))
        )
        
        assert "currency" in silver_df.columns
        assert "amount_usd" in silver_df.columns

    def test_silver_data_lineage_tracking(self, sample_transactions_df):
        """Test addition of processing metadata."""
        # Add processing metadata
        silver_df = sample_transactions_df.withColumn(
            "processed_timestamp", F.current_timestamp()
        ).withColumn(
            "processing_layer", F.lit("silver")
        )
        
        assert "processed_timestamp" in silver_df.columns
        assert "processing_layer" in silver_df.columns
        
        # Verify all records have processing layer set
        silver_records = silver_df.filter(F.col("processing_layer") == "silver").count()
        assert silver_records == sample_transactions_df.count()