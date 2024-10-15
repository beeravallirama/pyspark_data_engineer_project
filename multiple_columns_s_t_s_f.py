# select single and multiple columns
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession


# Initialize our data
data2 = [("Pulkit", 12, "CS32", 82, "Programming"),
		("Ritika", 20, "CS32", 94, "Writing"),
		("Atirikt", 4, "BB21", 78, None),
		("Reshav", 18, None, 56, None)
		]

# Start spark session
spark = SparkSession.builder.appName("Student_Info").getOrCreate()

# Define schema
schema = StructType([
	StructField("Name", StringType(), True),
	StructField("Roll Number", IntegerType(), True),
	StructField("Class ID", StringType(), True),
	StructField("Marks", IntegerType(), True),
	StructField("Extracurricular", StringType(), True)
])

# read the dataframe
df = spark.createDataFrame(data=data2, schema=schema)

# slelct columns
df.select("Name", "Marks").show()

# stop the session
spark.stop()
