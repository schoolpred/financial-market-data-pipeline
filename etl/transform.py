import sys
import math
import json
import copy

def dum_js(v):
    """
    convert to json string to uft8 encoded
    """
    return json.dumps(v).encode('utf-8')

def split_df_2_smaller_chunk(list_dict, stock_key, cate_key, max_chunk_size = 1000000):
    """
    To run the code faster, collect all stock information into a huge dictionary
    check the bytes size, then divide by 1,0000,000 to figure out how many chunks should be separated into.

    Get input:
    list_dict(list): a list of dictionary
    cate_key(str): category key to recognize category while producing to kafka
    stock_key(str): a key to indentify a stock
    max_chunk_size(int): default is 1000000 bytes
    

    Output:
    A list of dictionary with max_chunk_size and ready to produce to kafka
    """

    #first check the large dictionary size
    all_us_stocks = {} 
    additional_key = {"category": cate_key}
    for stock in list_dict:
        single_stock = {stock[stock_key]: stock}
        all_us_stocks.update(single_stock)
    all_stock_size = sys.getsizeof(dum_js(all_us_stocks))
    num_chunk_split_into = math.ceil(all_stock_size / (max_chunk_size - sys.getsizeof(dum_js(additional_key))))
    len_each_chunk = math.ceil(len(list_dict) /  num_chunk_split_into)


    #split all stock into chunks
    all_us_stocks_list = []
    stocks_chunk = {}
    start_key = 1
    key_set = set()
    for idx, stock in enumerate(list_dict):
        additional_key = {"category": cate_key + f"_{start_key}"}
        # print(additional_key)
        key_set.add(additional_key["category"])

        if len(stocks_chunk) < len_each_chunk - 1 :
            # print(f"true {len(stocks_chunk)}")
            single_stock = {stock[stock_key]: stock}
            stocks_chunk.update(single_stock)
        else:
            # print(f"false ")
            stocks_chunk.update(additional_key)
            final_chunk = copy.deepcopy(stocks_chunk)
            all_us_stocks_list.append(final_chunk)
            stocks_chunk.clear()
            start_key += 1
            
    #the remain stocks in last chunk
    if stocks_chunk:
        stocks_chunk.update(additional_key)
        all_us_stocks_list.append(stocks_chunk)

    return all_us_stocks_list