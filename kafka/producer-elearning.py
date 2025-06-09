import requests
from confluent_kafka import Producer
import json
import time

producer_conf = {'bootstrap.servers':'localhost:9092'}
producer = Producer(producer_conf)

seen_data = set()

while True:
    try:
        response = requests.get("http://localhost:8000/jawaban")
        if response.status_code == 200:
            data_list = response.json().get("data",[])

            for data in data_list:
                key = f"{data['username']}_{data['soal']}_{data['created_at']}"
                if key not in seen_data:
                    seen_data.add(key)

                    producer.produce("elearning_topic", json.dumps(data).encode("utf-8"))
                    print("✅ Sent to Kafka:", data)
                
            producer.flush()

        else:
            print("❌ Gagal ambil data dari API:", response.status_code)
    except Exception as e:
        print("❌ Error:", e)

    time.sleep(5)