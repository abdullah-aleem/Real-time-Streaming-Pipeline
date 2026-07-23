from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from delta import configure_spark_with_delta_pip


def main():
    SparkSession.builder.getOrCreate().stop()
    spark =( 
        SparkSession.builder.appName("KafkaStructuredDeltaStreamingConsuming")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ).getOrCreate()
    )

    
    # read stream from kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "user_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    # lets setup the expecting schema for the upstream data , hardcoded field values do not handle schema evaluation (ps learned it the hard way)
    schema = StructType(
        [
            StructField("user_id", IntegerType()),
            StructField("event_type", StringType()),
            StructField("timestamp", StringType()),
        ]
    )

    # kafka gives key/value as binary
    messages_df = kafka_df.selectExpr(
        "CAST(key as STRING) as key", "CAST(value as STRING) as value"
    )

    # parsing from string to json
    messages_df = messages_df.withColumn("parsed", from_json("value", schema)).select(
        "parsed.*"
    )

    # write stream to console
    query = (
        messages_df.writeStream.format("delta")
        .option("checkpointLocation", "C:/spark-checkpoints/kafka-stream")
        .option("path", "C:/delta/kafka-stream")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
