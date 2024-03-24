import sys
sys.path.append(r'/mnt/e/school24/deproject/financial-market-data-pipeline')  # Adjust the path based on the directory structure

import yaml
import time
from etl import GetDataFromAPI, KafkaHandler
from datetime import datetime, timedelta
import boto3
import json

def split_2_smaller_chunk(large_dict_encoded, key, chunk_size = 1000000):
    """
    divide a large dictionary into smaller parts for sending to Kafka
    """

    #additional key
    additional_key = {"category": key + '_{}'}

    # Slice the dictionary into chunks
    chunks = []
    start_idx = 0
    start_key = 1

    while start_idx < len(large_dict_encoded):
        additional_key["category"] = additional_key["category"].format(start_key)
        additional_key_encode = ','.encode('utf-8') + json.dumps(additional_key).encode('utf-8')
        #size of addtional key
        additional_key_size = sys.getsizeof(additional_key_encode)

        end_idx = start_idx + chunk_size - additional_key_size  # Adjusted chunk size to fit additional string

        # Ensure end index doesn't exceed the length of the dictionary string
        if end_idx > len(large_dict_encoded):
            end_idx = len(large_dict_encoded)

        # Create a chunk and append the additional string
        chunk = large_dict_encoded[start_idx:end_idx] + additional_key_encode

        chunks.append(chunk)

        start_idx = end_idx
        start_key += 1

    return chunks

def main():
    from_date = datetime.now() - timedelta(days=7)
    to_date = datetime.now()

    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        api_key = config['api']['key']
        stocks = config['stocks']
        kafka_server = config['kafka']['bootstrap_server']
        kafka_topic = config['kafka']['topic']


    get_data = GetDataFromAPI(api_key)

    #get all us stock data
    all_us_stocks_raw = get_data.get_list_symbols('US')

    #convert stock format from a list of dicts to a dict of dicts, in order to send to producer at once
    all_us_stocks = {} 
    for stock in all_us_stocks_raw:
        single_stock = {stock['symbol']: stock}
        all_us_stocks.update(single_stock)

    # get stock detail
    company_profile = {"category": "company_info"}
    basic_financial = {"category": "basic_financial"}
    stock_price = {"category": "price"}
    company_news= {"category": "news"}
    for stock in stocks:
        print(stock)
        company_profile[stock] = get_data.get_company_profile(stock)
        basic_financial[stock] = get_data.get_basic_basic_financials(stock)
        stock_price[stock] = get_data.get_stock_price(stock)
        company_news[stock] = get_data.get_company_news(stock, from_date=from_date, to_date=to_date)

    finance_data = [company_profile, basic_financial , company_news , stock_price ] 

    kafka = KafkaHandler(kafka_server)
    
    # send all stock to kafka
    producer = kafka.create_producer(encode=False)

    all_us_stocks_chunks = split_2_smaller_chunk(json.dumps(all_us_stocks).encode('utf-8'), "stock_info")

    for us_stock in all_us_stocks_chunks:
        future = producer.send(kafka_topic, value=us_stock)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
        except Exception as e:
            print(f"Failed to send message: {e}")
    print('done sent all stock')
    # time.sleep(15)

    # send finance data to producer
    # for idx, data in enumerate(finance_data):
    #     print(idx)
    #     kafka.produce_message(topic=kafka_topic, message=data)
    #     time.sleep(10)

if __name__ == "__main__":
    main()