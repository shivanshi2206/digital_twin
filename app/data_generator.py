
import psycopg2
import random
from datetime import datetime, timedelta

conn = psycopg2.connect(host="timescaledb", dbname="sensordata", user="admin", password="admin")
cur = conn.cursor()

start_date = datetime.now() - timedelta(days=365)
for i in range(365*96):  # 15-min intervals for 1 year
    timestamp = start_date + timedelta(minutes=15*i)
    building = random.choice(["Building A", "Building B", "Building C"])
    temp = round(random.uniform(18, 30), 2)
    humidity = round(random.uniform(30, 70), 2)
    occupancy = random.randint(0, 50)
    cur.execute("INSERT INTO sensor_data (building, timestamp, temperature, humidity, occupancy) VALUES (%s,%s,%s,%s,%s)",
                (building, timestamp, temp, humidity, occupancy))
conn.commit()
cur.close()
