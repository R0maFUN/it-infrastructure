##from kafka import KafkaProducer
##import json
##
##kafka_server_ip = '158.160.81.86'
##port = '9092'
##bootstrap_servers = "158.160.81.86:9092"
##producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
##
##transaction = {
##    'transaction_id': '123',
##    'product_id': '456',
##    'quantity': 3
##}
##
##producer.send('transaction_topic_v2', value=json.dumps(transaction).encode('utf-8'))

import time
from kafka import KafkaProducer
import random
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='158.160.81.86:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization of messages
)

# List of products for generating transaction events
products = ['Product A', 'Product B', 'Product C', 'Product D']

# Generate sample transaction data
def generate_transaction_data():
    transaction = {
        'transaction_id': random.randint(1, 1000),
        'product': random.choice(products),
        'amount': round(random.uniform(10, 1000), 2),
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
    topic = '<transaction_topic_v2>'
    num_events = 100  # Number of transaction events to generate
    delay = 1  # Delay in seconds between each event publication

    produce_data(topic, num_events, delay)
