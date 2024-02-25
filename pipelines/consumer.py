import sys
sys.path.append(r'D:\school\depro\financial-market-data-pipeline')  # Adjust the path based on the directory structure

import yaml
from etl import GetDataFromAPI, KafkaHandler
from datetime import datetime, timedelta
import boto3
import json

def main():
    from_date = datetime.now() - timedelta(days=7)
    to_date = datetime.now()

    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        api_key = config['api']['key']
        stocks = config['stocks']
        kafka_server = config['kafka']['bootstrap_server']
        kafka_topic = config['kafka']['topic']
        bucket_name = config['s3']['bucket_name']

    # produce and consume data in kafka
    kafka = KafkaHandler(kafka_server)
    messages = []
    consumer = kafka.create_consumer(topic=kafka_topic)
    
    for msg in consumer:
        print(msg.value)
        messages.append(msg.value)
        print(len(messages))
        with open('tests/test.json', 'w') as f:
            json.dump(messages, f)
        # break

if __name__ == "__main__":
    main()