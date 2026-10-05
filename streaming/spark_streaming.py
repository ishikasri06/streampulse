from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    count,
    avg,
    max,
    when,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


# Create Spark session
spark = (
    SparkSession.builder
    .appName("StreamPulseLogAnalytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Schema of our Kafka log messages
log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("service", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("response_time_ms", IntegerType(), True),
])


# Read streaming data from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "application-logs")
    .option("startingOffsets", "latest")
    .load()
)


# Convert Kafka's binary value into a string
json_stream = raw_stream.select(
    col("value").cast("string").alias("json")
)


# Parse JSON into structured columns
logs = (
    json_stream
    .select(
        from_json(col("json"), log_schema).alias("data")
    )
    .select("data.*")
)


service_metrics = (
    logs
    .groupBy("service")
    .agg(
        count("*").alias("total_logs"),

        count(
            when(col("level") == "ERROR", True)
        ).alias("error_count"),

        count(
            when(col("level") == "WARN", True)
        ).alias("warn_count"),

        avg("response_time_ms").alias("avg_response_time"),

        max("response_time_ms").alias("max_response_time")
    )
)

service_metrics = service_metrics.withColumn(
    "event_time",
    current_timestamp()
)

query = (
    service_metrics.writeStream
    .outputMode("complete")
    .option("checkpointLocation", "/opt/spark/streaming/checkpoint")
    .foreachBatch(
        lambda batch_df, batch_id: (
            batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "streampulse")
            .option("table", "service_metrics")
            .mode("append")
            .save()
        )
    )
    .start()
)

query.awaitTermination()