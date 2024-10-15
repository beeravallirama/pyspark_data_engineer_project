from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

from ex_1 import schema

spark = SparkSession.builder.appName("EmployeeData").getOrCreate()
# Define the Schema
schema = StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("emp_name",StringType(),True),
    StructField("dept_id",IntegerType(),True)
])
data = [
    (101, "James Smith", 10),
    (102, "Michael Rose", 20),
    (103, "Robert Williams", 10),
    (104, "Maria Jones", 30),
    (105, "Jen Brown", 20)
]

# create schema
schema = ["emp_id", "emp_name", "dept_id"]

# Step 4: Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Step 5: Group by dept_id and count the number of employees in each department
result_df = df.groupBy("dept_id").agg(count("emp_id").alias("employee_count"))

# Step 5: Show the DataFrame

df.show()

# Optional: Print the schema to confirm
df.printSchema()



from pyspark.sql import SparkSession
from pyspark.sql.functions import count  # Make sure to import the count function

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

# Step 2: Create the data
data = [
    (101, "James Smith", 10),
    (102, "Michael Rose", 20),
    (103, "Robert Williams", 10),
    (104, "Maria Jones", 30),
    (105, "Jen Brown", 20)
]

# Step 3: Define the schema for the DataFrame
schema = ["emp_id", "emp_name", "dept_id"]

# Step 4: Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Step 5: Group by dept_id and count the number of employees in each department
result_df = df.groupBy("dept_id").agg(count("emp_id").alias("employee_count"))

# Step 6: Show the result
result_df.show()
