import sys
sys.path.append(r'/mnt/e/school24/deproject/financial-market-data-pipeline')  # Adjust the path based on the directory structure

import yaml
import time
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


    get_data = GetDataFromAPI(api_key)

    #get all us stock data
    all_us_stocks_raw = get_data.get_list_symbols('US')

    #convert stock format from a list of dicts to a dict of dicts, in order to send to producer at once
    all_us_stocks = {} 
    for stock in all_us_stocks_raw:
        single_stock = {stock['symbol']: stock}
        all_us_stocks.update(single_stock)
    
    #add category
    all_us_stocks.update({"category": "stock_info"})

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

    finance_data = [company_profile, basic_financial, stock_price, company_news]

    kafka = KafkaHandler(kafka_server)
    
    # send all stock to kafka
    kafka.produce_message(topic=kafka_topic, message=all_us_stocks)

    #send finance data to producer
    for idx, data in enumerate(finance_data):
        print(idx)
        kafka.produce_message(topic=kafka_topic, message=data)
        time.sleep(5)

if __name__ == "__main__":
    main()