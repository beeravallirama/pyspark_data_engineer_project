# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType
#
# # Initialize Spark session
# spark = SparkSession.builder.appName("example").getOrCreate()
#
# # Define the schema
# schema = StructType([StructField("column1", StringType(), True)])
#
# # Correct data input (list of tuples matching the schema)
# data = [("1",)]
#
# # Create DataFrame
# df = spark.createDataFrame(data, schema=schema)
#
# # Show the DataFrame
# df.show()

# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
            StructField("seq", StringType(), True)])

dates = ['1']

df = spark.createDataFrame(list['1'], schema=schema)

df.show()