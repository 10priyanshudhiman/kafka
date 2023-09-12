"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import OperationalError

TOPIC='toll'
db_params = {
    "dbname": "tolldata",
    "user": "priyanshu",
    "password": "priyanshu",
    "host": "localhost",  # Replace with your PostgreSQL server's IP or hostname
    "port": "5432",       # Replace with the appropriate port number
}

print("Connecting to the database")
try:
    connection = psycopg2.connect(**db_params)
except OperationalError as e:
    # If an OperationalError exception is raised, it means the connection failed
    print("Connection error:", e)
    print("Not connected to the PostgreSQL database.")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka

    message = msg.value.decode("utf-8")
    print(message)
    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
print('close')
connection.close()
