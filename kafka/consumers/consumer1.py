from confluent_kafka import Consumer

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'grp',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(["transactions"])

while True:
    msg = consumer.poll(timeout=1.0)  # Waits for a message
    if msg is None:
        continue
    if msg.error():
        print(f"❌ Consumer error: {msg.error()}")
        continue
    
    print(f"✅ Received message: {msg.value().decode('utf-8')}")

consumer.close()
