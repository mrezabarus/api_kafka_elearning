from confluent_kafka import Consumer
import psycopg2
import json

conn = psycopg2.connect("dbname=db_from_kafka user=postgres password=admin")
cur = conn.cursor()

conf = {
    'bootstrap.servers':'localhost:9092',
    'group.id': 'group_elearning',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

consumer.subscribe(['elearning_topic'])

print("Consumer siap, menunggu data...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error consumer:", msg.error())
            continue

        data_str = msg.value().decode('utf-8')
        data = json.loads(data_str)

        jawaban = data.get("jawaban",0)
        #print(jawaban)
        
        if data['jawaban'].strip().lower() == "benar":
            cur.execute(
                "INSERT into jawaban_user_benar (username, soal, jawaban, created_at) values (%s,%s,%s,%s)",
                (data["username"], data["soal"], data["jawaban"],data["created_at"])
            )
            conn.commit()
            print("Di Simpan ke table jawaban_user_benar", data)   
        else:
            cur.execute(
                "INSERT into jawaban_user_salah (username, soal, jawaban, created_at) values (%s,%s,%s,%s)",
                (data["username"], data["soal"], data["jawaban"],data["created_at"])
            )
            conn.commit()
            print("Di Simpan ke table jawaban_user_salah", data)   

except KeyboardInterrupt:
    print("Consumer stop")

finally:
    consumer.close()
    cur.close()
    conn.close