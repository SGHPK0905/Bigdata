import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

print("⏳ Khởi động hệ thống giả lập Traffic thực tế...")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

urls = ["/home", "/products", "/contact", "/login", "/api/data", "/about"]

def generate_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

print("🚀 Bắt đầu phát sóng mạng...")

attack_active = False
attack_end_time = 0
next_attack_time = time.time() + random.randint(10, 30) 
current_botnet = []

while True:
    try:
        current_time = time.time()

        if not attack_active and current_time >= next_attack_time:
            attack_active = True
            
            duration = random.randint(5, 20)
            attack_end_time = current_time + duration
            botnet_size = random.randint(5, 255)
            current_botnet = [generate_ip() for _ in range(botnet_size)]
            
            print(f"\n🔥 [BÁO ĐỘNG] BẮT ĐẦU TẤN CÔNG! Kéo dài: {duration}s | Quân số Botnet: {botnet_size} IPs")

        if attack_active and current_time > attack_end_time:
            attack_active = False
            
            next_attack_time = current_time + random.randint(15, 60)
            print(f"\n✅ [THÔNG BÁO] ĐÃ YÊN BÌNH. Chờ đợt sóng tiếp theo...")

        if attack_active:
            log = {
                "ip": random.choice(current_botnet),
                "url": "/login",
                "method": "POST",
                "status": 401,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
            sleep_time = random.uniform(0.005, 0.02) 
            
        else:
            log = {
                "ip": generate_ip(),
                "url": random.choices(urls, weights=[40, 30, 10, 5, 10, 5])[0], 
                "method": "GET" if random.choice([True, False]) else "POST",
                "status": random.choices([200, 404, 500, 401], weights=[85, 10, 2, 3])[0], 
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            sleep_time = random.uniform(0.05, 0.8)
            
        producer.send('web-logs', value=log)
        
        print(f"📡 [Log] {log['ip']} -> {log['url']} ({log['status']})")
        
        time.sleep(sleep_time)
        
    except Exception as e:
        pass