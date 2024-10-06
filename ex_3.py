#spark session
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import count,sum,avg,min,max
# from pyspark.sql.types import StructType,StructField,IntegerType,StructType
#
#
# spark = SparkSession.builder.appName("emp_1").getOrCreate()

# df.show()
# df.printSchema()

from pyspark.sql import SparkSession
from pyspark.sql.functions import count,sum,min,max,avg
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

# Make sure to import the count function

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("emp_2").getOrCreate()

# Step 2: Create the data
data = [
    (101, "James Smith", 10, 70000, "New York"),
    (102, "Michael Rose", 20, 80000, "Los Angeles"),
    (103, "Robert Williams", 10, 75000, "New York"),
    (104, "Maria Jones", 30, 90000, "Chicago"),
    (105, "Jen Brown", 20, 85000, "Los Angeles")
]

# Step 3: Define the schema for the DataFrame, including the new columns
schema = ["emp_id", "emp_name", "dept_id", "salary", "location"]

# Step 4: Create the DataFrame
df = spark.createDataFrame(data, schema=schema)


# Step 5: Group by dept_id and count the number of employees in each department
result_df = df.groupBy("dept_id").agg(count("emp_id").alias("employee_count"))
result_df = df.groupBy("dept_id").agg(sum("salary").alias("employee_sum_sal"))
result_df = df.groupBy("dept_id").agg(max("salary").alias("employee_max_sal"))
result_df = df.groupBy("dept_id").agg(min("salary").alias("employee_min_sal"))
result_df = df.groupBy("dept_id").agg(avg("salary").alias("employee_avg_sal"))
# Step 6: Show the result
#result_df.show()
# Step 5: Define the window specification
windowSpec = Window.orderBy(col("salary").desc())

# Step 6: Apply the dense_rank function to rank the salaries
df_with_rank = df.withColumn("rank", dense_rank().over(windowSpec))

# Step 7: Filter the DataFrame to get the second-highest salary
second_highest_salary_df = df_with_rank.filter(col("rank") == 2)

# Show the result
second_highest_salary_df.show()
