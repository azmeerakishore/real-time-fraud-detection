```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "financial-transactions"

transaction_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("RealTimeFraudDetection")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    transactions = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(
            from_json(col("json_value"), transaction_schema).alias("transaction")
        )
        .select("transaction.*")
        .filter(col("amount").isNotNull())
    )

    scored_transactions = (
        transactions
        .withColumn(
            "risk_reason",
            when(col("isFraud") == 1, lit("Dataset fraud label"))
            .when(
                (col("type").isin("TRANSFER", "CASH_OUT"))
                & (col("amount") >= 200000),
                lit("High-value transfer or cash-out"),
            )
            .otherwise(lit("No rule triggered")),
        )
        .withColumn(
            "fraud_alert",
            when(
                (col("isFraud") == 1)
                | (
                    col("type").isin("TRANSFER", "CASH_OUT")
                    & (col("amount") >= 200000)
                ),
                lit(1),
            ).otherwise(lit(0)),
        )
    )

    query = (
        scored_transactions.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", "/tmp/spark-checkpoints/fraud-stream")
        .start()
    )

    print("Fraud flagging stream is running.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming...")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()