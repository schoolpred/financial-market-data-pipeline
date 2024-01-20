import sys
sys.path.append(r'D:\school\depro\financial-market-data-pipeline')  # Adjust the path based on the directory structure

print(sys.path)

import yaml
import etl
import pandas as pd




# with open('../config/config.yaml', 'r') as file:
#     config = yaml.safe_load(file)

# # Accessing the API key
# api_key = config['api']['key']

# res = get_data_from_finnhub(api_key).get_list_symbols("US")

# response_df = pd.DataFrame.from_records(res)

# print(response_df.head())