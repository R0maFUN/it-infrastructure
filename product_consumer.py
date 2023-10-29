from kafka import KafkaConsumer
import json

BOOTSTRAP_SERVERS = ['localhost:9092']
PRODUCT_TOPIC = 'product_topic'

consumer = KafkaConsumer(PRODUCT_TOPIC, bootstrap_servers='BOOTSTRAP_SERVERS')

for message in consumer:
    product_event = json.loads(message.value)
    product_id = product_event['product_id']
    quantity = product_event['quantity']

    print(f"Got product event {product_event}")
    # Update the database with the product event
    print(f"Updating product {product_id} with quantity {quantity}")
