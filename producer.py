import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

print("⏳ Đang kết nối tới Kafka...")
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("✅ Kết nối thành công!")
        break
    except NoBrokersAvailable:
        time.sleep(3)

if not producer:
    print("❌ Lỗi kết nối Kafka.")
    exit()

print("🚀 Đang giả lập mạng Botnet tấn công Brute Force vào trang /login...")

urls = ["/home", "/products", "/contact", "/login", "/about", "/api/data"]

# Hàm sinh IP ngẫu nhiên toàn cầu (Giả lập Big Data)
def generate_random_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

# Tạo một nhóm 50 IP của Hacker (Botnet) để xoay vòng tấn công
botnet_ips = [generate_random_ip() for _ in range(50)]

while True:
    try:
        # Tỉ lệ 30% là mạng Botnet của Hacker tấn công, 70% là traffic toàn cầu
        if random.random() < 0.3:
            log = {
                "ip": random.choice(botnet_ips), # Hacker dùng IP trong mảng Botnet
                "url": "/login",
                "method": "POST",
                "status": 401, # 401: Unauthorized (Đăng nhập thất bại)
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            # Không in ra dòng này nữa để mô phỏng log thật chạy ngầm rất nhanh
        else:
            log = {
                "ip": generate_random_ip(), # Hàng triệu IP ngẫu nhiên toàn cầu
                "url": random.choice(urls),
                "method": "GET" if random.choice(urls) != "/login" else "POST",
                "status": random.choice([200, 200, 200, 404]), 
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        producer.send('web-logs', value=log)
        
        # In ra màn hình cho vui mắt (mô phỏng tốc độ cao)
        print(f"📡 Sinh log: {log['ip']} -> {log['url']} [{log['status']}]")
        
        # Giảm thời gian chờ xuống 0.05s để bơm dữ liệu cực nhanh (Big Data)
        time.sleep(0.05) 
        
    except Exception as e:
        print(f"Lỗi: {e}")
        