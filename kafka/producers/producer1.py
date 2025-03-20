from confluent_kafka import Producer
from requests import get
import json

def fetch_data(var:str)->dict[str,any]:
    try:
        request = get(f"http://localhost:8000/api/v1/{var}")
        response=json.loads(request.text)
        return response
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

broker="localhost:9092"
topic="transactions"
conf = {'bootstrap.servers': broker}


def delivery_report(err, msg):
    if err:
        print(f"❌ Message failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

  

def main():
    items=fetch_data("customer")
      # Create Producer instance
    producer = Producer(**conf)
    i=0
    for item in items:
        i+=1
        item=json.dumps(item).encode('utf-8')
        print(type(item))
        if i==5:
            break;
        producer.produce(topic,value=item,key=str(i),callback=delivery_report)
        producer.flush()  

if __name__=="__main__":
    main()