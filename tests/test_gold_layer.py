import pytest
from pyspark.sql import functions as F
from datetime import datetime


class TestGoldLayer:
    """Test cases for Gold layer aggregations and business metrics."""

    def test_gold_daily_transaction_summary(self, sample_transactions_df):
        """Test daily transaction summary aggregation."""
        # Create daily summary
        gold_df = sample_transactions_df.withColumn(
            "date", F.to_date(F.col("timestamp"))
        ).groupBy("date").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count")
        )
        
        assert "total_transactions" in gold_df.columns
        assert "total_amount" in gold_df.columns
        assert "avg_amount" in gold_df.columns
        assert "fraud_count" in gold_df.columns
        assert gold_df.count() > 0

    def test_gold_location_based_analytics(self, sample_transactions_df):
        """Test location-based transaction analytics."""
        gold_df = sample_transactions_df.groupBy("location").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_volume"),
            F.avg("amount").alias("avg_transaction"),
            F.countDistinct("account_id").alias("unique_accounts")
        )
        
        assert gold_df.count() > 0
        assert "txn_count" in gold_df.columns
        assert "total_volume" in gold_df.columns
        assert "unique_accounts" in gold_df.columns

    def test_gold_fraud_analysis(self, sample_transactions_df):
        """Test fraud analysis metrics."""
        # Calculate fraud metrics
        total_txns = sample_transactions_df.count()
        fraud_txns = sample_transactions_df.filter(F.col("is_fraud") == 1).count()
        
        gold_df = sample_transactions_df.agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_transactions"),
            F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)).alias("fraud_amount"),
            F.sum("amount").alias("total_amount")
        ).withColumn(
            "fraud_rate",
            F.col("fraud_transactions") / F.col("total_transactions")
        ).withColumn(
            "fraud_amount_rate",
            F.col("fraud_amount") / F.col("total_amount")
        )
        
        assert "fraud_rate" in gold_df.columns
        assert "fraud_amount_rate" in gold_df.columns

    def test_gold_account_summary(self, sample_transactions_df):
        """Test account-level summary statistics."""
        gold_df = sample_transactions_df.groupBy("account_id").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("total_credits"),
            F.sum(F.when(F.col("transaction_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("total_debits")
        )
        
        assert all(col in gold_df.columns for col in 
                   ["txn_count", "total_amount", "avg_amount", "min_amount", "max_amount"])

    def test_gold_transaction_type_distribution(self, sample_transactions_df):
        """Test transaction type distribution metrics."""
        gold_df = sample_transactions_df.groupBy("transaction_type").agg(
            F.count("*").alias("count"),
            F.sum("amount").alias("total_amount")
        )
        
        # Calculate percentage
        total = sample_transactions_df.count()
        gold_df = gold_df.withColumn(
            "percentage",
            (F.col("count") / F.lit(total)) * 100
        )
        
        assert "percentage" in gold_df.columns
        # Sum of percentages should be ~100
        percentages_sum = gold_df.agg(F.sum("percentage")).collect()[0][0]
        assert 99.0 <= percentages_sum <= 101.0

    def test_gold_hourly_patterns(self, sample_transactions_df):
        """Test hourly transaction pattern analysis."""
        gold_df = sample_transactions_df.withColumn(
            "hour", F.hour(F.col("timestamp"))
        ).groupBy("hour").agg(
            F.count("*").alias("txn_count"),
            F.avg("amount").alias("avg_amount")
        ).orderBy("hour")
        
        assert "hour" in gold_df.columns
        assert gold_df.count() > 0

    def test_gold_high_value_transactions(self, sample_transactions_df):
        """Test identification and metrics for high-value transactions."""
        threshold = 100000
        
        gold_df = sample_transactions_df.filter(
            F.col("amount") > threshold
        ).withColumn(
            "risk_category", F.lit("HIGH_VALUE")
        )
        
        high_value_count = gold_df.count()
        total_count = sample_transactions_df.count()
        
        if high_value_count > 0:
            assert "risk_category" in gold_df.columns
            assert all(row.amount > threshold for row in gold_df.collect())

    def test_gold_customer_segmentation(self, sample_transactions_df):
        """Test customer segmentation based on transaction behavior."""
        # Segment customers by total transaction volume
        customer_summary = sample_transactions_df.groupBy("account_id").agg(
            F.sum("amount").alias("total_volume"),
            F.count("*").alias("txn_count")
        )
        
        gold_df = customer_summary.withColumn(
            "customer_segment",
            F.when(F.col("total_volume") > 200000, "Premium")
             .when(F.col("total_volume") > 50000, "Gold")
             .otherwise("Standard")
        )
        
        assert "customer_segment" in gold_df.columns
        segments = gold_df.select("customer_segment").distinct().count()
        assert segments > 0

    def test_gold_fraud_by_location(self, sample_transactions_df):
        """Test fraud analysis by location."""
        gold_df = sample_transactions_df.groupBy("location").agg(
            F.count("*").alias("total_txns"),
            F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_txns")
        ).withColumn(
            "fraud_rate",
            F.col("fraud_txns") / F.col("total_txns")
        )
        
        assert "fraud_rate" in gold_df.columns
        assert gold_df.count() > 0

    def test_gold_time_series_aggregation(self, sample_transactions_df):
        """Test time-series aggregation for trend analysis."""
        gold_df = sample_transactions_df.withColumn(
            "date", F.to_date(F.col("timestamp"))
        ).groupBy("date").agg(
            F.count("*").alias("daily_count"),
            F.sum("amount").alias("daily_volume")
        ).orderBy("date")
        
        # Add moving average (window function)
        from pyspark.sql.window import Window
        window_spec = Window.orderBy("date").rowsBetween(-2, 0)
        
        gold_df = gold_df.withColumn(
            "moving_avg_volume",
            F.avg("daily_volume").over(window_spec)
        )
        
        assert "moving_avg_volume" in gold_df.columns

    def test_gold_kpi_calculations(self, sample_transactions_df):
        """Test calculation of key performance indicators."""
        gold_df = sample_transactions_df.agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("average_transaction_value"),
            F.countDistinct("account_id").alias("active_customers"),
            F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_incidents")
        ).withColumn(
            "revenue_per_customer",
            F.col("total_revenue") / F.col("active_customers")
        )
        
        assert "revenue_per_customer" in gold_df.columns
        row = gold_df.collect()[0]
        assert row.total_transactions > 0
        assert row.active_customers > 0

    def test_gold_aggregation_correctness(self, sample_transactions_df):
        """Test that aggregations produce correct values."""
        # Manual calculation
        transactions = sample_transactions_df.collect()
        expected_sum = sum(row.amount for row in transactions)
        expected_count = len(transactions)
        
        # Gold layer aggregation
        gold_df = sample_transactions_df.agg(
            F.sum("amount").alias("total"),
            F.count("*").alias("count")
        )
        
        row = gold_df.collect()[0]
        
        assert abs(row.total - expected_sum) < 0.01  # Allow small floating point difference
        assert row.count == expected_count

    def test_gold_business_day_analysis(self, sample_transactions_df):
        """Test business day vs weekend analysis."""
        gold_df = sample_transactions_df.withColumn(
            "day_of_week", F.dayofweek(F.col("timestamp"))
        ).withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)
        ).groupBy("is_weekend").agg(
            F.count("*").alias("txn_count"),
            F.avg("amount").alias("avg_amount")
        )
        
        assert gold_df.count() > 0