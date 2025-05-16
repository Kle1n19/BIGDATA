import json
import time
from kafka import KafkaProducer
import sseclient
import requests


def main():
    producer = KafkaProducer(bootstrap_servers="kafka-server:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    response = requests.get("https://stream.wikimedia.org/v2/stream/page-create", stream=True)
    client = sseclient.SSEClient(response)

    for event in client.events():
        if event.event == 'message':
            try:
                data = json.loads(event.data)
                producer.send("input", value=data)
            except Exception as e:
                print(e)

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(e)
            time.sleep(5)
