from pyspark.sql.functions import col, from_json, lit, avg, udf, window, concat
from pyspark.sql.types import (StructType, LongType, StringType, FloatType, \
                               IntegerType, ArrayType, TimestampType)
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.getOrCreate()
ssc = StreamingContext(spark, 5)

KAFKA_BROKERS = "broker:9091"
USER = "dataproc"
PASSWORD = "password"
JAAS_CONFIG = f"org.apache.kafka.common.security.scram.ScramLoginModule " \
              f"required username=\"{USER}\" password=\"{PASSWORD}\";"

# Define schema for raw data
schema = StructType() \
    .add("id", LongType(), False) \
    .add("timestamp", TimestampType(), False) \
    .add("location", StructType() \
        .add("id", LongType(), False) \
        .add("latitude", StringType(), False) \
        .add("longitude", StringType(), False) \
        .add("country", StringType(), False) \
        .add("indoor", IntegerType(), False) \
    ) \
    .add("sensor", StructType() \
        .add("id", StringType(), False)) \
    .add("sensordatavalues", ArrayType( \
        StructType() \
            .add("value_type", StringType())\
            .add("value", StringType())), \
    )

# Define method for extracting common metrics
def get_metric(data, metric):
    for el in data:
        if el.value_type == metric:
            try:
                x = float(el.value)
                return x
            except:
                return None
    return None

# Define user-defined-function for extracting metrics
getTemperature = udf(lambda z: get_metric(z, 'temperature'), FloatType())
getHumidity = udf(lambda z: get_metric(z, 'humidity'), FloatType())
getPressure = udf(lambda z: get_metric(z, 'pressure'), FloatType())
getP1 = udf(lambda z: get_metric(z, 'P1'), FloatType())
getP2 = udf(lambda z: get_metric(z, 'P2'), FloatType())

# Create new stream from kafka
df = spark.readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
    .option("subscribe", "raw") \
    .load() \
    .selectExpr("CAST(value AS STRING)")\
    .where(col("value").isNotNull())

# Extract inner fields from json value to columns
df2 = df \
    .select( \
        from_json(col("value").cast("string"), schema).alias("v")) \
    .select( \
        col("v.id").alias("id"), \
        col("v.timestamp").alias("timestamp"), \
        col("v.location.id").alias("location_id"), \
        col("v.location.latitude").alias("latitude"), \
        col("v.location.longitude").alias("longitude"), \
        col("v.location.country").alias("country"), \
        col("v.location.indoor").alias("indoor"), \
        col("v.sensor.id").alias("sensor_id"), \
        col("v.sensordatavalues").alias("value"), \
    )

# Streaming filter some values.
# For example, exclude indoor measurements.
df3 = df2 \
    .where(col("indoor") == lit("0"))

# Add new columns with air quality measurements
df4 = df3 \
    .withColumn("temperature", getTemperature(col("value"))) \
    .withColumn("humidity", getHumidity(col("value"))) \
    .withColumn("pressure", getPressure(col("value"))) \
    .withColumn("P1", getP1(col("value"))) \
    .withColumn("P2", getP2(col("value"))) \
    .drop("value", "sensor_id", "id")


# Group all streaming measurements by location and timestamp.
# Group them by 1 minutes and calculate avg metrics for air quality
# one-minute intervals.
# Also, group them with 5 minutes delay for rewriting previous metrics
# for last 5 minutes if they occurs.
w = df4\
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "1 minutes"), \
             col("location_id"), \
             col("latitude"), \
             col("longitude"), \
             col("country")) \
    .agg( \
        avg("temperature").alias("temperature"), \
        avg("humidity").alias("humidity"), \
        avg("pressure").alias("pressure"), \
        avg("P1").alias("P1"), \
        avg("P2").alias("P2")) \
    .withColumn("timestamp", col("window").end).drop("window")

# Create new DataFrame with only one column 'value'
# that contains all row as csv row delimited by comma
output = w.select(
    concat(
        col("timestamp"), lit(","), \
        col("location_id"), lit(","), \
        col("latitude"), lit(","), col("longitude"), lit(","), \
        col("country"), lit(","), \
        col("temperature"), lit(","), col("humidity"), lit(","), \
        col("pressure"), lit(",") , col("P1"), lit(","), col("P2") \
        ) \
    .alias("value") \
)

# Write output stream to kafka topic "combined"
output.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("checkpointLocation", f"hdfs:///tmp/checkpoints/air-quality") \
    .option("topic", "combined") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
    .start() \
    .awaitTermination()
