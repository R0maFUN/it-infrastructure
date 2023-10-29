from time import sleep
from kafka import KafkaProducer
import random
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='<kafka_broker_hosts>',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization of messages
)

# List of products for generating inventory updates
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

# Generate sample inventory update data
def generate_inventory_data():
    inventory_update = {
        'product': random.choice(products),
        'quantity': random.randint(1, 100),
        'timestamp': int(time.time())
    }
    return inventory_update

# Publish data to Kafka topic
def publish(topic, data):
    producer.send(topic, value=data)
    producer.flush()

# Publish dummy transaction and inventory data to Kafka topic
def produce_data(topic, data_type, num_records, delay):
    for _ in range(num_records):
        if data_type == 'transaction':
            data = generate_transaction_data()
        elif data_type == 'inventory':
            data = generate_inventory_data()
        publish(topic, data)
        sleep(delay)

# Main execution
if __name__ == '__main__':
    topic = '<kafka_topic>'
    data_type = '<transaction_or_inventory>'
    num_records = 100  # Number of records to generate
    delay = 1  # Delay in seconds between each record publication

    produce_data(topic, data_type, num_records, delay)



from kafka import KafkaProducer
import json
from kafka.errors import KafkaError

bootstrap_servers = ['158.160.81.86:9092']

producer = KafkaProducer(bootstrap_servers=['158.160.81.86:9092'])

# Asynchronous by default
future = producer.send('transaction_topic_v3', b'raw_bytes')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError as ke:
    # Decide what to do if produce request failed...
    #log.exception()
    print("Finished with error: " + ke)
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)
 
