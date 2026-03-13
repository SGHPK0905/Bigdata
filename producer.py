import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

normal_urls = ["/home", "/products", "/contact", "/cart", "/checkout", "/about"]
vuln_urls = ["/admin", "/.env", "/config.php", "/wp-login.php", "/backup.zip"]

def generate_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

current_scenario = "NORMAL"
scenario_end_time = 0
target_ip = ""

while True:
    try:
        current_time = time.time()

        if current_time >= scenario_end_time:
            rand_val = random.random()
            if rand_val < 0.7:
                current_scenario = "NORMAL"
                scenario_end_time = current_time + random.randint(10, 30)
            elif rand_val < 0.8:
                current_scenario = "BRUTE_FORCE"
                target_ip = generate_ip()
                scenario_end_time = current_time + random.randint(5, 10)
            elif rand_val < 0.9:
                current_scenario = "VULN_SCAN"
                target_ip = generate_ip()
                scenario_end_time = current_time + random.randint(5, 10)
            else:
                current_scenario = "WEB_SCRAPER"
                target_ip = generate_ip()
                scenario_end_time = current_time + random.randint(5, 10)

        if current_scenario == "BRUTE_FORCE":
            log = {"ip": target_ip, "url": "/login", "method": "POST", "status": 401, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            sleep_time = random.uniform(0.01, 0.05)
        elif current_scenario == "VULN_SCAN":
            log = {"ip": target_ip, "url": random.choice(vuln_urls), "method": "GET", "status": 404, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            sleep_time = random.uniform(0.01, 0.05)
        elif current_scenario == "WEB_SCRAPER":
            log = {"ip": target_ip, "url": "/products", "method": "GET", "status": 200, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            sleep_time = random.uniform(0.005, 0.02)
        else:
            log = {
                "ip": generate_ip(),
                "url": random.choices(normal_urls, weights=[40, 30, 10, 10, 5, 5])[0], 
                "method": "GET",
                "status": random.choices([200, 404, 500], weights=[90, 8, 2])[0], 
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            sleep_time = random.uniform(0.05, 0.5)

        producer.send('web-logs', value=log)
        print(f"{log['ip']} - {log['status']} - {log['url']}")
        time.sleep(sleep_time)
        
    except:
        pass