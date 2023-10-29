import time
from time import sleep
from kafka import KafkaProducer
import random
import json
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-n", "--amount", dest="amount",
                    help="amount of events to produce")

args = parser.parse_args()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization of messages
)

# List of products for generating transaction events
products = [0, 1, 2, 3]

# Generate sample transaction data
def generate_transaction_data():
    transaction = {
        'transaction_id': random.randint(1, 10000),
        'product_id': random.choice(products),
        'amount': random.randint(1, 10),
        'timestamp': int(time.time())
    }
    return transaction

# Publish data to Kafka topic
def publish(topic, data):
    producer.send(topic, value=data)
    producer.flush()

# Publish dummy transaction events to Kafka topic
def produce_data(topic, num_events, delay):
    for _ in range(num_events):
        transaction = generate_transaction_data()
        publish(topic, transaction)
        sleep(delay)

# Main execution
if __name__ == '__main__':
    topic = 'transaction_topic_v3'
    print(f" amount = {args.amount}")
    num_events = 1
    if args.amount:
        num_events = args.amount # Number of transaction events to generate
    delay = 1  # Delay in seconds between each event publication

    produce_data(topic, num_events, delay)
    print(f"finished sending {num_events} events")
