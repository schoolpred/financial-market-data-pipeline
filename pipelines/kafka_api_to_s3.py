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


    get_data = GetDataFromAPI(api_key)

    all_us_stocks = get_data.get_list_symbols('US')

    company_profile = {}
    basic_financial = {}
    stock_price = {}
    company_news= {}
    for stock in stocks:
        print(stock)
        company_profile[stock] = get_data.get_company_profile(stock)
        basic_financial[stock] = get_data.get_basic_basic_financials(stock)
        stock_price[stock] = get_data.get_stock_price(stock)
        company_news[stock] = get_data.get_company_news(stock, from_date=from_date, to_date=to_date)

    finance_data = [all_us_stocks, company_profile, basic_financial, stock_price, company_news]

    # produce and consume data in kafka
    kafka = KafkaHandler(kafka_server)

    for data in finance_data:
        producer = kafka.produce_message(topic=kafka_topic, message=data)

    print("done producer")

    consumer = kafka.create_consumer(topic=kafka_topic)
    for msg in consumer:
        with open('tests/test.json', 'w') as f:
            json.loads(msg.value, f)

    # file_path = "test.json"
    # s3 = boto3.client('s3')
    # print("hithere")
    # for file in kafka.consume_message(topic=kafka_topic):
    #     print("hi")
    #     print(file)
    #     print(file.value)
    # s3.put_object(Body='tests/test.json', Bucket=bucket_name, Key=file_path)


if __name__ == "__main__":
    main()