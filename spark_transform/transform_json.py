from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.utils
import yaml
import os

def flatten_df(df, symbol, series_to_flatten):
    """
    convert nested json format into flatten dataframe
    """
    list_col = df.select(series_to_flatten + '.*').columns
    #create a dataframe with a new zipped column
    df_w_zipped_column = df.withColumn("new", F.arrays_zip(*[series_to_flatten + "." + colu for colu in list_col]))\
                            .withColumn("new", F.explode("new"))\
                            .selectExpr(symbol + ".symbol","new.*")
    
    #get list of null columns
    null_col = []
    for col in df_w_zipped_column.columns:
        try:
            df_w_zipped_column.select(f'{col}.*').columns
        except pyspark.sql.utils.AnalysisException:
            null_col.append(col)

    #create a query to extract all columns in zipped column with the defined name
    select_str_v = "{} as {}_v"
    select_str_period = "to_date({}, 'yyyy-MM-dd') as {}_period"
    list_columns_selected = []
    for cl in list_col:
        first_sl = select_str_v.format(cl if cl in null_col else cl+'.v', cl) #check if null, not contain v and period column, replace by null value
        second_sl = select_str_period.format(cl if cl in null_col else cl+'.period', cl) #check if null, not contain v and period column, replace by null value
        list_columns_selected.extend([first_sl, second_sl])
    #extract all column using selectExpr
    df_w_extracted_column = df_w_zipped_column.selectExpr("symbol",*list_columns_selected)
    return df_w_extracted_column

def merge_period_col(df, list_period_col, list_value_col, period_format):
    """
    merge all period column into one avoiding self join multiple times, waste of memories
    """
    # Creating an array of structs to explode
    df_with_arrays = df.withColumn("period_values", F.array(
        *list_period_col
    ))
    df_with_arrays = df_with_arrays.withColumn("distinc_array", F.array_compact(F.array_distinct("period_values")))
    # Explode the array into individual rows
    exploded_df = df_with_arrays.withColumn("period", F.explode("distinc_array"))\
        .drop(
            *["period_values", "distinc_array"]
    )
    #select string
    sl_str = "case when trunc({}, 'Year') = trunc(period, 'Year') then {} else 0 end as f_{}"
    final_sl_tr = []
    for p, v in zip(list_period_col, list_value_col):
        final_sl_tr.append(sl_str.format(p, v, v))
    #aggregate func
    exprs = [F.first("f_" + value_col, ignorenulls=True).alias(value_col) for value_col in list_value_col]
    #check each metrics value
    exploded_df= exploded_df.selectExpr(
        "symbol",
        f"date_trunc('{period_format}', period) as period",
        *final_sl_tr
    )\
    .groupBy("symbol", "period")\
    .agg(*exprs)
    
    return exploded_df

def transform_quarterly_yearly_metrics(stocks, df1, df2):
    """
    transform quarterly and yearly metrics in basic_financial file
    """
    #quarterly metrics
    print("quarterly")
    for idx, stock in enumerate(stocks):
        print(stock)
        if stock in df1.columns:
            if idx < 1:
                final = flatten_df(df1, stock, f'{stock}.metrics.series.quarterly')
            else:
                sub_df = flatten_df(df1, stock, f'{stock}.metrics.series.quarterly')
                final = final.union(sub_df)
        elif stock in df2.columns:
            if idx < 1:
                final = flatten_df(df2, stock, f'{stock}.metrics.series.quarterly')
            else:
                sub_df = flatten_df(df2, stock, f'{stock}.metrics.series.quarterly')
                final = final.union(sub_df)
    list_period = [col for col in final.columns if col.endswith("_period")]
    list_v = [col for col in final.columns if col.endswith("_v")]
    qt_final = merge_period_col(df=final, list_period_col=list_period, list_value_col=list_v, period_format='QUARTER')
    print("done write quarterly")

    #yearly metrics
    print("yearly")
    for idx, stock in enumerate(stocks):
        print(stock)
        if stock in df1.columns:
            if idx < 1:
                final = flatten_df(df1, stock, f'{stock}.metrics.series.annual')
            else:
                sub_df = flatten_df(df1, stock, f'{stock}.metrics.series.annual')
                final = final.union(sub_df)
        elif stock in df2.columns:
            if idx < 1:
                final = flatten_df(df2, stock, f'{stock}.metrics.series.annual')
            else:
                sub_df = flatten_df(df2, stock, f'{stock}.metrics.series.annual')
                final = final.union(sub_df)
    list_period = [col for col in final.columns if col.endswith("_period")]
    list_v = [col for col in final.columns if col.endswith("_v")]
    year_final = merge_period_col(df=final, list_period_col=list_period, list_value_col=list_v, period_format='YEAR')
    print("done write yearly")

    return qt_final, year_final

def transform_basic_metrics(stocks, df1, df2):
    """
    transform other basic metrics in basic_financial file
    """
    for idx, stock in enumerate(stocks):
        print(stock)
        if stock in df1.columns:
            if idx < 1:
                basic_final = df1.select(stock + ".symbol",stock + ".metrics.metric.*")
            else:
                basic_sub_df = df1.select(stock + ".symbol",stock + ".metrics.metric.*")
                basic_final = basic_final.unionByName(basic_sub_df, allowMissingColumns=True)
        elif stock in df2.columns:
            if idx < 1:
                basic_final = df2.select(stock + ".symbol",stock + ".metrics.metric.*")
            else:
                basic_sub_df = df2.select(stock + ".symbol",stock + ".metrics.metric.*")
                basic_final = basic_final.unionByName(basic_sub_df, allowMissingColumns=True)
    
    return basic_final

def transform_company_price_info(stocks, df):
    """
    transform company info file
    """
    for idx, stock in enumerate(stocks):
        try:
            if idx < 1:
                price_final = df.select(stock + ".symbol", stock + ".metrics.*")
            else:
                price_sub_df = df.select(stock + ".symbol", stock + ".metrics.*")
                
        except Exception as e:
            print(f"Error at: {stock}, Exception: {e}")
        finally:
            price_final = price_final.unionByName(price_sub_df, allowMissingColumns=True)
        
    return price_final

def transform_news(stocks, df1, df2):
    """
    transform news
    """
    for idx, stock in enumerate(stocks):
        if stock in df1.columns:
            try:
                if idx < 1:
                    news_final = df1.withColumn("metrics", F.explode(f"{stock}.metrics"))\
                                .select(f"{stock}.symbol", "metrics.*")
                else:
                    news_sub_df = df1.withColumn("metrics", F.explode(f"{stock}.metrics"))\
                                .select(f"{stock}.symbol", "metrics.*")
                    
            except Exception as e:
                print(f"Error at: {stock}, Exception: {e}")
            finally:
                news_final = news_final.unionByName(news_sub_df, allowMissingColumns=True)
        elif stock in df2.columns:
            try:
                if idx < 1:
                    news_final = df2.withColumn("metrics", F.explode(f"{stock}.metrics"))\
                                .select(f"{stock}.symbol", "metrics.*")
                else:
                    news_sub_df = df2.withColumn("metrics", F.explode(f"{stock}.metrics"))\
                                .select(f"{stock}.symbol", "metrics.*")
                    
            except Exception as e:
                print(f"Error at: {stock}, Exception: {e}")
            finally:
                news_final = news_final.unionByName(news_sub_df, allowMissingColumns=True)
    return news_final


def transform_stock_info(dir_path):
    """
    transform message stock info files
    """

    stock_info_file = [file_name for file_name in os.listdir(dir_path) if file_name.startswith("message_stock_info")]
    continue_run = 0 #limit get stock info

    for file in stock_info_file:
        while continue_run == 0:
            df = spark.read.format("json").load(f"{dir_path}/{file}")
            df.cache()
            print(file)
            for idx, col in enumerate(df.columns):
                print(col)
                if idx < 1:
                    info_final = df.select(f"{col}.*")
                elif idx <= 50:
                    info_sub_df = df.select(f"{col}.*")
                    info_final = info_final.unionByName(info_sub_df, allowMissingColumns=True)
                else:
                    continue_run = 1 #stop
                    break
            df.unpersist()
    return info_final

def write_csv_to_gcp(df, gcp_path):
    """
    write csv file google cloud
    """
    return df\
            .coalesce(1)\
            .write\
            .mode('overwrite')\
            .option("header", "true")\
            .format("csv")\
            .save(gcp_path)

if __name__ == "__main__":
    #load_config
    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        stocks = config['stocks']
        gch_path = config['gcp']['path']
    with open('../config/aws_key.yaml', 'r') as file:
        config = yaml.safe_load(file)

    #initianilize spark
    spark = SparkSession.builder \
        .appName("TransformJSONToGCP") \
        .getOrCreate()
    
    # spark.sparkContext.setLogLevel("DEBUG")
    financial_file1 = spark.read.format("json").load("../raw_data/message_basic_financial_1.json")
    financial_file2 = spark.read.format("json").load("../raw_data/message_basic_financial_2.json")

    quarterly_financial , yearly_financial = transform_quarterly_yearly_metrics(stocks=stocks, df1=financial_file1, df2=financial_file2)
    basic_financial = transform_basic_metrics(stocks=stocks, df1=financial_file1, df2=financial_file2)
    print("done financial")

    #company info transform
    company_info_file = spark.read.format("json").load("../raw_data/message_company_info_1.json")
    company_info = transform_company_price_info(stocks, company_info_file)
    print("done company_info")

    #news transform
    news1 = spark.read.format("json").load("../raw_data/message_news_1.json")
    news2 = spark.read.format("json").load("../raw_data/message_news_2.json")
    news = transform_news(stocks, news1, news2)
    print("done news")

    #price transform
    price_file = spark.read.format("json").load("../raw_data/message_price_1.json")
    price = transform_company_price_info(price_file, stocks)
    print("done price")

    #stock info
    stock_info = transform_stock_info("../raw_data")
    print("done stock_info")
    
    #write to gcp
    write_csv_to_gcp(quarterly_financial, gch_path + "quarterly_financial" )
    write_csv_to_gcp(yearly_financial, gch_path + "yearly_financial" )
    write_csv_to_gcp(basic_financial, gch_path + "basic_financial" )
    write_csv_to_gcp(company_info, gch_path + "company_info" )
    write_csv_to_gcp(news, gch_path + "news" )
    write_csv_to_gcp(price, gch_path + "price" )
    write_csv_to_gcp(stock_info, gch_path + "stock_info" )

    #stop spark
    spark.stop()