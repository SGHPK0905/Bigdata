import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

print("⏳ Đang tìm kiếm Kafka tại localhost:9092...")

# Vòng lặp thử kết nối (Retry Logic)
producer = None
for i in range(10): # Thử 10 lần
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("✅ Đã kết nối thành công với Kafka!")
        break # Kết nối được thì thoát vòng lặp
    except NoBrokersAvailable:
        print(f"⚠️ Chưa thấy Kafka (Lần thử {i+1}/10). Đang chờ 3 giây...")
        time.sleep(3)

if not producer:
    print("❌ Lỗi: Không thể kết nối tới Kafka sau 30 giây. Hãy kiểm tra lại Docker!")
    exit()

print("🚀 Bắt đầu giả lập lưu lượng truy cập Web...")

# Danh sách IP và URL giả lập
ips = ["192.168.1.1", "192.168.1.5", "10.0.0.2", "172.16.0.55"]
urls = ["/home", "/products", "/cart", "/login", "/checkout", "/contact"]
methods = ["GET", "POST"]

while True:
    try:
        log_entry = {
            "ip": random.choice(ips),
            "url": random.choice(urls),
            "method": random.choice(methods),
            "response_time": random.randint(50, 500),
            "status_code": random.choice([200, 200, 200, 404, 500]),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Gửi vào topic 'web-logs'
        producer.send('web-logs', value=log_entry)
        print(f"📡 Sent: {log_entry['ip']} -> {log_entry['url']}")
        
        time.sleep(1)
    except Exception as e:
        print(f"Lỗi khi gửi: {e}")
        time.sleep(1)