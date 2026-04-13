from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


def transform_to_bronze(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("source_system", lit("banking_simulator"))
        .withColumn("record_status", lit("RAW"))
    )


def load_to_bronze():
    print("🔹 Starting Bronze Load")

    # Read from Unity Catalog table
    raw_df = spark.table("banking.banking.banking_raw_transactions")

    # Idempotency check

    # Transform
    bronze_df = transform_to_bronze(raw_df)

    # Write
    bronze_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("banking.banking.banking_bronze_transactions")

    print("✅ Bronze load completed")