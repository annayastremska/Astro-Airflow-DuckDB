import os
import json
import random
from datetime import datetime
import mysql.connector

import os

OUTPUT_FOLDER = "C:/Users/Lenovo/Desktop/data_engineering/astro_airflow/include/telephony_data"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# MySQL connection (use your credentials)
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="anna262326",
    database="as_2"
)

cursor = conn.cursor()
cursor.execute("SELECT call_id FROM calls")
call_ids = [row[0] for row in cursor.fetchall()]

sample_descriptions = [
    "Customer called regarding billing issue. Resolved successfully.",
    "Technical issue reported. Escalated to level 2 support.",
    "General product inquiry handled.",
    "Service complaint registered and documented.",
    "Refund request processed."
]

generated = 0

for call_id in call_ids:
    file_path = os.path.join(OUTPUT_FOLDER, f"call_{call_id}.json")

    # 🔑 Incremental logic: only create if missing
    if os.path.exists(file_path):
        continue

    data = {
        "call_id": call_id,
        "duration_sec": random.randint(30, 900),
        "short_description": random.choice(sample_descriptions)
    }

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    generated += 1

print(f"Generated {generated} new telephony JSON files.")
