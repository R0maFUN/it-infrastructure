from kafka import KafkaConsumer, KafkaProducer, ConsumerRebalanceListener
import json
import threading

BOOTSTRAP_SERVERS = ['localhost:9092']
consumer_group_id = 'transaction_consumer_group'
product_topic = 'product_topic'
transaction_topic = 'transaction_topic_v3'

consumer = KafkaConsumer(
    transaction_topic,
    bootstrap_servers=BOOTSTRAP_SERVERS
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialization of messages
)

class RebalanceListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked_partitions):
        # Add code to pause consuming, commit offsets, etc.
        consumer.pause()
        consumer.commit()
    
    def on_partitions_assigned(self, assigned_partitions):
        # Add code to resume consuming
        consumer.resume()

def register_kafka_listener(topic, listener):
    # Poll kafka
    def poll():
        # Initialize consumer instance with group ID and bootstrap servers
        # consumer = KafkaConsumer(topic,
        #                          bootstrap_servers=BOOTSTRAP_SERVERS,
        #                          group_id=consumer_group_id)

        # Subscribe to topics with rebalance listener
        #consumer.subscribe(topics=[topic],
        #                   listener=RebalanceListener())  # Add the rebalance listener here

        print("About to start polling for topic:", topic)
        consumer.poll(timeout_ms=6000)
        print("Started Polling for topic:", topic)
        for msg in consumer:
            print("Entered the loop\nKey: ", msg.key, " Value:", msg.value)
            kafka_listener(msg)

    print("About to register listener to topic:", topic)
    t1 = threading.Thread(target=poll)
    t1.start()
    print("Started a background thread")

def kafka_listener(msg):
    # print("Got msg:\n", data.value.decode("utf-8"))
    transaction = json.loads(msg.value)
    print("Got transaction: " + transaction)
    product_id = transaction['product_id']
    amount = transaction['amount']
  
    product_event = {
        'product_id': product_id,
        'quantity': -1
    }
    producer.send(product_topic, value=product_event)

register_kafka_listener(transaction_topic, kafka_listener)
