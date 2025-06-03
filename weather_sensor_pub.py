import Adafruit_DHT
from google.cloud import pubsub_v1
import time
import json
import os

# GCP Project & Pub/Sub topic info
project_id = "your-project-id"
topic_id = "weather-data"

# Set the path to your GCP service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/pi/key.json"

# Sensor Setup
sensor = Adafruit_DHT.DHT22
gpio = 4  # GPIO pin where DHT22 is connected

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def read_and_publish():
    humidity, temperature = Adafruit_DHT.read_retry(sensor, gpio)
    if humidity is not None and temperature is not None:
        message = {
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        data = json.dumps(message).encode("utf-8")
        publisher.publish(topic_path, data=data)
        print("Published:", message)
    else:
        print("Failed to read from sensor")

while True:
    read_and_publish()
    time.sleep(10)
