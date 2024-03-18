import sys
sys.path.append(r'/mnt/e/school24/deproject/financial-market-data-pipeline')  # Adjust the path based on the directory structure

import yaml
from etl import GetDataFromAPI, KafkaHandler
from datetime import datetime, timedelta
import boto3
import json
import os

def main():

    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        kafka_server = config['kafka']['bootstrap_server']
        kafka_topic = config['kafka']['topic']
        bucket_name = config['s3']['bucket_name']

    # produce and consume data in kafka
    kafka = KafkaHandler(kafka_server)
    # messages = []
    consumer = kafka.create_consumer(topic=kafka_topic)
    
    #create an boto client
    s3 = boto3.client('s3')

    count = 0
    for msg in consumer:
        print(count)
        cate = msg.value.get('category')
        file_name = f"message_{cate}.json"

        with open(f'tests/{file_name}', 'w') as f:
            json.dump(msg.value, f)

        print("write done")
        # send to s3
        s3.upload_file(f'tests/{file_name}', bucket_name, 'raw_json/{}'.format(file_name))
        print(f"done upload {file_name}")
        # remove local file
        os.remove(f'tests/{file_name}')

        count+=1

if __name__ == "__main__":
    main()