# ZooKeeper is required for Kafka to work. Start the ZooKeeper server.
# cd kafka_2.12-2.8.0
# bin/zookeeper-server-start.sh config/zookeeper.properties

# This will start the Kafka message broker service.
# cd kafka_2.12-2.8.0
# bin/kafka-server-start.sh config/server.properties

# To create a topic named news, start a new terminal and run the command below.
# cd kafka_2.12-2.8.0
# bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092
# bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bankbranch

# You need a producer to send messages to Kafka. Run the command below to start a producer.
# bin/kafka-console-producer.sh --topic news --bootstrap-server localhost:9092

# You need a consumer to read messages from kafka.
#
# Open a new terminal.
#
# Run the command below to listen to the messages in the topic news.

# cd kafka_2.12-2.8.0
# bin/kafka-console-consumer.sh --topic news --from-beginning --bootstrap-server localhost:9092

# Produce and consume with message keys In this step, you will be using message keys to ensure that messages with the
# same key will be consumed in the same order as they were published. In the backend, messages with the same key will
# be published into the same partition and will always be consumed by the same consumer. As such, the original
# publication order is kept in the consumer side.

# Ok, we can now start a new producer and consumer, this time using message keys. You can start a new producer with
# the following message key commands:
#
# --property parse.key=true to make the producer parse message keys
#
# --property key.separator=: define the key separator to be the : character,
# so our message with key now looks like the following key-value pair example:
#
# 1:{"atmid": 1, "transid": 102}. Here the message key is 1, which also corresponds to the ATM id, and the value is
# the transaction JSON object, {"atmid": 1, "transid": 102}. Start a new producer with message key enabled:

# bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch --property parse.key=true --property key.separator=:
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --from-beginning --property print.key=true --property key.separator=:

# each topic partition maintains its own message queue, and new messages are enqueued (appended to the end of the queue)
# as they get published to the partition. Once consumed, the earliest messages will be dequeued and nno longer be available for consumption.

# Consumer Offset
# Topic partitions keeps published messages in a sequence, like a list.
# Message offset indicates a message’s position in the sequence. For example,
# the offset of an empty Partition 0 of bankbranch is 0, and if you publish the first message
# to the partition, its offset will be 1.
#
# By using offsets in the consumer, you can specify the starting position for message consumption, such as from the
# beginning to retrieve all messages, or from some later point to retrieve only the latest messages.

# Consumer Group
# In addition, we normally group related consumers together as a consumer group.
# For example, we may want to create a consumer for each ATM in the bank and manage all ATM related consumers
# together in a group.
#
# So let’s see how to create a consumer group, which is actually very easy with the --group argument.
#
# In the consumer terminal, stop the previous consumer if it is still running.
#
# Run the following command to create a new consumer within a consumer group called atm-app:

# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app

# After the consumer within the atm-app consumer group is started, you should not expect any messages to be consumed.
# This is because the offsets for both partitions have already reached to the end.
# In other words, all messages have already been consumed, and therefore dequeued, by previous consumers.

# Reset offset
# We can reset the index with the --reset-offsets argument.
#
# First let’s try resetting the offset to the earliest position (beginning) using --reset-offsets --to-earliest.
#
# Stop the previous consumer if it is still running, and run the following command to reset the offset:

# bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092  --topic bankbranch --group atm-app --reset-offsets --to-earliest --execute

# In fact, you can reset the offset to any position. For example, let’s reset the offset so that
# we only consume the last two messages.
#
# Stop the previous consumer
#
# Shift the offset to left by 2 using --reset-offsets --shift-by -2:

# bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092  --topic bankbranch --group atm-app --reset-offsets --shift-by -2 --execute

# If you run the consumer again, you should see that we consumed 4 messages, 2 for each partition:
#
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app

"""
Streaming data consumer
"""




"""
Top Traffic Simulator
"""
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'toll'

VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                 "car", "car", "car", "truck", "truck", "truck",
                 "truck", "van", "van")
for _ in range(100000):
    vehicle_id = randint(10000, 10000000)
    vehicle_type = choice(VEHICLE_TYPES)
    now = ctime(time())
    plaza_id = randint(4000, 4010)
    message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
    message = bytearray(message.encode("utf-8"))
    print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")
    producer.send(TOPIC, message)
    sleep(random() * 2)


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

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()

