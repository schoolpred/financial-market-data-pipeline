import sys
sys.path.append(r'/mnt/e/school24/deproject/financial-market-data-pipeline')  # Adjust the path based on the directory structure
import json
import yaml
import time
from datetime import datetime, timedelta

from etl import GetDataFromAPI, KafkaHandler, split_df_2_smaller_chunk

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

    #get all us stock data and split it into chunks
    all_us_stocks_raw = get_data.get_list_symbols('US')
    us_stock_2_kafka = split_df_2_smaller_chunk(list_dict = all_us_stocks_raw, stock_key = 'symbol', cate_key = 'stock_info')

    # get stock detail
    company_profile = []
    basic_financial = []
    stock_price = []
    company_news= []
    for stock in stocks:
        print(stock)
        single_stock_profile = {'symbol' : stock, 'metrics': get_data.get_company_profile(stock)}
        single_stock_financial = {'symbol' : stock, 'metrics': get_data.get_basic_basic_financials(stock)}
        single_stock_price = {'symbol' : stock, 'metrics': get_data.get_stock_price(stock)}
        single_stock_news = {'symbol' : stock, 'metrics': get_data.get_company_news(stock, from_date=from_date, to_date=to_date)}

        company_profile.append(single_stock_profile)
        basic_financial.append(single_stock_financial)
        stock_price.append(single_stock_price)
        company_news.append(single_stock_news)

    #split to smaller chunk if info size > max kafka message size (1MB)
    company_profile = split_df_2_smaller_chunk(list_dict = company_profile, stock_key= 'symbol', cate_key='company_info')
    basic_financial = split_df_2_smaller_chunk(list_dict = basic_financial, stock_key= 'symbol', cate_key='basic_financial')
    stock_price = split_df_2_smaller_chunk(list_dict = stock_price, stock_key= 'symbol', cate_key='price')
    company_news = split_df_2_smaller_chunk(list_dict = company_news, stock_key= 'symbol', cate_key='news')

    #final all data
    finance_data = [us_stock_2_kafka, company_profile, basic_financial , stock_price, company_news] 

    #initialize kafka
    kafka = KafkaHandler(kafka_server)

    # send finance data to producer
    for idx, data in enumerate(finance_data):
        print(idx)
        for msg in data:
            kafka.produce_message(topic=kafka_topic, message=msg)
            time.sleep(5)

if __name__ == "__main__":
    main()