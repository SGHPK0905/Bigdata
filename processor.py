from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

print("⏳ Đang khởi động Hệ thống Giám sát An ninh mạng (SIEM)...")
spark = SparkSession.builder.appName("BruteForceDetection").getOrCreate()

# CHÌA KHÓA Ở ĐÂY: Đổi "WARN" thành "ERROR" để dọn rác màn hình
spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("ip", StringType()) \
    .add("url", StringType()) \
    .add("method", StringType()) \
    .add("status", IntegerType()) \
    .add("time", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 1. Lọc log Đăng nhập thất bại
failed_logins = parsed_df.filter((col("url") == "/login") & (col("status") == 401))

# 2. Gom nhóm theo IP và đếm
brute_force_stats = failed_logins.groupBy("ip").count()

# 3. Lọc ra các IP thuộc mạng Botnet (sai > 10 lần)
blacklist_ips = brute_force_stats.filter(col("count") > 10)

def process_alerts(batch_df, batch_id):
    hacker_count = batch_df.count()
    if hacker_count > 0:
        print(f"\n🚨 [BATCH {batch_id}] PHÁT HIỆN TẤN CÔNG BOTNET! DANH SÁCH {hacker_count} IP BỊ CHẶN:")
        batch_df.show(truncate=False)
        
        batch_df.write \
            .format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save("/app/ket_qua_output")
    else:
        # In dấu chấm nhỏ để biết hệ thống vẫn đang theo dõi mà không làm rác màn hình
        print(".", end="", flush=True)

print("🚀 SIEM đã sẵn sàng! Đang phân tích hàng triệu luồng truy cập...")
query = blacklist_ips.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_alerts) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()