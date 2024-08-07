import requests
from kafka import KafkaConsumer, KafkaProducer
import json

class GetDataFromAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://finnhub.io/api/v1"

    def _make_request(self, endpoint, params=None):
        """
        Helper method to make API requests.
        """
        params = params or {}
        params['token'] = self.api_key
        response = requests.get(f"{self.base_url}/{endpoint}", params=params)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def get_list_symbols(self, exchange_code):
        """
        Get list stock symbols for a given exchange code.
        """
        endpoint = "/stock/symbol"
        params = {'exchange': exchange_code}
        return self._make_request(endpoint, params)

    def get_company_profile(self, symbol):
        """
        Get general information of company
        """
        endpoint = "/stock/profile2"
        params = {'symbol': symbol}
        return self._make_request(endpoint, params)

    def get_basic_basic_financials(self, symbols, metric='all'):
        """
        Get basic financials given symbol such as margin, P/E ratio, 52-week high/low etc.
        """
        endpoint = "/stock/metric"
        params = {'symbol': symbols, 'metric' : metric}
        return self._make_request(endpoint, params)

    def get_stock_price(self, symbol):
        """
        Get stock price for a given symbol
        """
        endpoint = "/quote"
        params = {'symbol': symbol}
        return self._make_request(endpoint, params)
    
    def get_company_news(self, symbol, from_date, to_date, date_format="%Y-%m-%d"):
        """
        Get company news for a given symbol
        """
        endpoint = "/company-news"
        params = {'symbol': symbol, 'from': from_date.strftime(date_format), 'to': to_date.strftime(date_format)}
        return self._make_request(endpoint, params)
    

class KafkaHandler:
    def __init__(self, bootstrap_server):
        self.bootstrap_server = bootstrap_server

    def create_producer(self, encode=True):
        """
        Create kafka producer instance
        """
        if encode:
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        else:
            value_serializer = lambda v: v
        producer = KafkaProducer(
            bootstrap_servers = self.bootstrap_server,
            value_serializer = value_serializer
            )
        return producer
    
    def produce_message(self, topic, message):
        """
        Produce a message to given kafka topic
        """
        producer = self.create_producer()
        producer.send(topic, value=message)
        producer.flush()
        producer.close()

    def create_consumer(self, topic, consumer_timeout_ms=None):
        """
        Create kafka consumer instance
        """
        consumer_config = {
            'bootstrap_servers': self.bootstrap_server,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
        }

        if consumer_timeout_ms is not None:
            consumer_config['consumer_timeout_ms'] = consumer_timeout_ms

        consumer = KafkaConsumer(
            topic,
            **consumer_config
        )
        return consumer