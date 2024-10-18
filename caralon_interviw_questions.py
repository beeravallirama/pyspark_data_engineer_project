from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("MaxSalary").getOrCreate()

# Sample data
data = [
    (2, 'Peter', 'AU', 60000, 60000),
    (3, 'Sam', 'AU', 60000, 60000),
    (6, 'Elliot', 'AU', 50000, 60000),
    (1, 'John', 'USA', 50000, 70000),
    (4, 'Susan', 'USA', 50000, 70000),
    (5, 'David', 'USA', 70000, 70000)
]

# Define schema for the DataFrame
columns = ['id', 'name', 'location', 'salary', 'max_salary']

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()

# Define a window specification to partition by location
windowSpec = Window.partitionBy('location')

# Add a new column for max salary per location using the window function
df_with_max_salary = df.withColumn('max_salary_per_location', spark_max(col('salary')).over(windowSpec))

# Show the result
df_with_max_salary.show()


# we have list of file path, write a python function to get the file names in a list
#  arranged in order they were created based on the timestamp.

path_list = ["/test/001/a/a/pqr/df/app/c/b/a/work/json_output/abcs/xyx/test_output_0_20230912062652796649.json",
             "/test/001/a/a/pqr/df/app/c/b/a/work/json_output/abcs/xyx/test_output_1_20230912062652796849.json",
             "/test/001/a/a/pqr/df/app/c/b/a/work/json_output/abcs/xyx/test_output_2_20230912062652797649.json",
             "/test/001/a/a/pqr/df/app/c/b/a/work/json_output/abcs/xyx/test_output_5_20230912062651796649.json",
             "/test/001/a/a/pqr/df/app/c/b/a/work/json_output/abcs/xyx/test_output_6_20230912062650796849.json",
             "/test/001/a/a/pqr/df/app/c/b/a/work/json_output/abcs/xyx/test_output_7_20230912062659797649.json"]



file_name_time_dict = {}
for path in path_list:
    filename = path.split('/')[-1]
    file_name_time_dict.put(filename, filename.split('_')[-1]))

    # value bsed sorting
    sorted_filenames = sorted(file_name_time_dict.items(), key=lambda file_name_time_dict)



# Using slicing:
#
# my_list = [1, 2, 3, 4, 5]
# reversed_list = my_list[::-1]
#
# Using the reverse() method
# my_list.reverse()
#
# Using reversed() function
#
# reversed_list = list(reversed(my_list))





