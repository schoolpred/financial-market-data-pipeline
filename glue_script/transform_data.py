import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Extract data from the source JSON files
source_df = spark.read.json("s3://kafka-finance/raw_json/message_stock_info_1.json")


print(source_df.printSchema())

print(source_df.show(5))