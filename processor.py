from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("SIEM_DataProcessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("ip", StringType()) \
    .add("url", StringType()) \
    .add("method", StringType()) \
    .add("status", IntegerType()) \
    .add("time", TimestampType())

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "web-logs").option("startingOffsets", "latest").load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

def process_batch(batch_df, batch_id):
    batch_df.write.format("csv").mode("append").option("header", "true").save("/app/ket_qua_output")
    print(f"[{batch_id}] Đã ghi thêm dữ liệu mới vào Data Lake...")

query = parsed_df.writeStream.outputMode("append").foreachBatch(process_batch).trigger(processingTime='3 seconds').option("checkpointLocation", "/app/my_checkpoint").start()

query.awaitTermination()