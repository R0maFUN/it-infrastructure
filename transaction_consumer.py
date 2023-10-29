##from kafka import KafkaConsumer
##import json
##
##bootstrap_servers = '158.160.81.86:9092'
##topic = 'transaction_topic'
##
### Create a KafkaConsumer with the desired topic and bootstrap servers
##consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
##
### Start consuming messages from the topic
##for message in consumer:
##    # Decode the message value assuming it is encoded as UTF-8
##    event = json.loads(message.value.decode('utf-8'))
##    
##    # Process the event
##    print(event)

from kafka import KafkaConsumer, ConsumerRebalanceListener
import threading

BOOTSTRAP_SERVERS = ['158.160.81.86:9092']
consumer_group_id = 'transaction_consumer_group'

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
        consumer = KafkaConsumer(topic,
                                 bootstrap_servers=BOOTSTRAP_SERVERS,
                                 group_id=consumer_group_id)

        # Subscribe to topics with rebalance listener
        consumer.subscribe(topics=[topic],
                           listener=RebalanceListener())  # Add the rebalance listener here

        print("About to start polling for topic:", topic)
        consumer.poll(timeout_ms=6000)
        print("Started Polling for topic:", topic)
        for msg in consumer:
            print("Entered the loop\nKey: ",msg.key," Value:", msg.value)
            kafka_listener(msg)

    print("About to register listener to topic:", topic)
    t1 = threading.Thread(target=poll)
    t1.start()
    print("Started a background thread")

def kafka_listener(data):
    print("Image Ratings:\n", data.value.decode("utf-8"))

register_kafka_listener('transaction_topic_v2', kafka_listener)
