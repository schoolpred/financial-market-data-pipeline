import requests

class get_data_from_finnhub():
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