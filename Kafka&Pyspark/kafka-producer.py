import time
import json
import pandas as pd
from confluent_kafka import Producer

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'inventory-management'
}

# Create Producer instance
producer = Producer(conf)

# Function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Convert Timestamps to strings
def convert_timestamp(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError("Type not serializable")

file_path = '/home/growlt259/Desktop/Inventory_Prediction_Retail_Market.xlsx'
df = pd.read_excel(file_path)

# Iterate over DataFrame rows as dictionaries
for index, row in df.iterrows():
    # Convert DataFrame row to JSON
    message_json = json.dumps(row.to_dict(), default=convert_timestamp)
    
    # Produce the JSON message to the Kafka topic
    producer.produce('inventory-management', value=message_json, callback=delivery_report)
    producer.poll(0)
    time.sleep(0.003)

producer.flush()