from fastapi import FastAPI, Query
from typing import Optional
from datetime import datetime
from faker import Faker
import random
import psycopg2
import psycopg2.extras

app = FastAPI()

fake = Faker()

conn = psycopg2.connect("dbname=db_from_kafka user=postgres password=admin")
cur = conn.cursor()

@app.post("/generate")
def generate_data():
    cursor = conn.cursor()
    jumlah_data = random.randint(1,100)
    insert_query = """
        INSERT into jawaban_user (username, soal, jawaban, created_at)
        VALUES (%s,%s,%s,%s)
    """

    usernames = ["alice", "bob", "charlie", "diana","brownlauren","jamie29","christophermartin","dodsonbryan","amy58","copelandmichael"]  # fixed list supaya pasti ada yang sama
    for _ in range(jumlah_data):
        username = random.choice(usernames)
        soal = fake.lexify(text="soal_???")
        jawaban = random.choice(["benar","salah"])
        waktu = datetime.utcnow()

        cursor.execute(insert_query, (username, soal, jawaban, waktu))

    conn.commit()
    cursor.close()
    return {"status":"Oke", "generated": jumlah_data}

@app.get("/jawaban")
def get_jawaban(username: Optional[str] = Query(None, description="Filter by username")):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if username:
        query = "SELECT username, soal, jawaban, created_at from jawaban_user where username = %s ORDER BY created_at DESC"
        cur.execute(query, (username,)) 
    else:
        query = "SELECT username, soal, jawaban, created_at from jawaban_user ORDER BY created_at DESC"
        cur.execute(query)
    
    rows = cur.fetchall()
    cur.close()

    results = []
    for row in rows:
        results.append({
            "username":row["username"],
            "soal": row["soal"],
            "jawaban": row["jawaban"],
            "created_at": row["created_at"].isoformat()
        })
    
    return {"data": results}