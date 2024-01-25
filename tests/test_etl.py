import sys
sys.path.append(r'D:\school\depro\financial-market-data-pipeline')  # Adjust the path based on the directory structure

import yaml
import etl
import pandas as pd
import os
print(os.getcwd())

with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Accessing the API key
api_key = config['api']['key']

res = etl.get_data_from_finnhub(api_key)


js = res.get_company_profile("GOOGL")


kafka = etl.KafkaHandler()

kafka.produce_message('finance', js)


consumed_messages = kafka.consume_message(topic='finance')
print(consumed_messages)