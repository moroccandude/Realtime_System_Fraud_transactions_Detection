from confluent_kafka import Producer

broker="localhost:29092"
topic="transactions"
conf = {'bootstrap.servers': broker}
def delivery_report(err, msg):
    if err:
        print(f"❌ Message failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    # Create Producer instance
producer = Producer(**conf)
for i in range(4):
    producer.produce(topic,value="geuu",key=i,callback=delivery_report)
producer.poll(0)  