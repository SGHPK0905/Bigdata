from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Khởi tạo Spark
print("⏳ Đang khởi động Spark...")
spark = SparkSession.builder.appName("WebLogAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN") # Ẩn bớt log rác

# 2. Định nghĩa khuôn dạng dữ liệu (Phải khớp với bên Producer gửi sang)
schema = StructType() \
    .add("ip", StringType()) \
    .add("url", StringType()) \
    .add("method", StringType()) \
    .add("status", IntegerType()) \
    .add("time", StringType())

# 3. Đọc dữ liệu từ Kafka (Kết nối nội bộ Docker)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Giải mã dữ liệu JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. XỬ LÝ: Đếm số lượng truy cập theo từng URL
traffic_report = parsed_df.groupBy("url").count()

# --- HÀM XỬ LÝ QUAN TRỌNG (Vừa in ra màn hình, vừa lưu file) ---
def process_batch(batch_df, batch_id):
    print(f"🔄 Đang xử lý Batch: {batch_id}")
    
    # A. Hiện bảng ra màn hình (Console)
    print("--- BẢNG THỐNG KÊ TRUY CẬP (REAL-TIME) ---")
    batch_df.show(truncate=False)
    
    # B. Lưu xuống file CSV (Ghi đè để luôn lấy số liệu mới nhất)
    print("💾 Đang lưu kết quả vào folder: /app/ket_qua_output")
    batch_df.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save("/app/ket_qua_output")


# 6. Kích hoạt Stream
print("🚀 Spark đã sẵn sàng! Đang lắng nghe Kafka (Cập nhật file mỗi 10s)...")

query = traffic_report.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='10 seconds') \
    .start()  # <--- THÊM DÒNG TRÊN VÀO TRƯỚC .start()

query.awaitTermination()