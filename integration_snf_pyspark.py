# import snowflake.connector

# import os
#
# sfOptions ={}
#
#
# from pyspark.sql import SparkSession
#
# # Initialize Spark session and specify the Snowflake connector JARs
# spark = SparkSession.builder \
#     .appName("sriram") \
#     .config("spark.jars.packages", "net.snowflake:spark-snowflake_3.12:2.10.0-spark_3.2") \
#     .getOrCreate()
#
# # Snowflake connection options
# sfOptions = {
#   "sfURL" : "ewniymx-sx75283.snowflakecomputing.com",
#   "sfUser" : "RAMANJANEYAAWSSNOWFLAKEMOUNIKA",
#   "sfPassword" : "Snowflake@12345",
#   "sfDatabase" : "RAMA_DB",
#   "sfSchema" : "PUBLIC",
#   "sfWarehouse" : "COMPUTE_WH",
#   "sfRole" : "ACCOUNTADMIN"
# }
#
# # Table name you want to read from Snowflake
# table_name = "EMP"
#
# # Read data from Snowflake table into a DataFrame
# df = spark.read \
#     .format("net.snowflake.spark.snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", table_name) \
#     .load()
#
# # Show the data
# df.show()

# import snowflake.connector
# print(snowflake.connector.__version__)

! pip install snowflake-connector-python
