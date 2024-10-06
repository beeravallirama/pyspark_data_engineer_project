from pyspark.sql import SparkSession

from pyspark.sql import functions as F

# # Create a Spark session

spark = SparkSession.builder.appName("sriram").getOrCreate()

data = [("rama", 25, "m"),
        ("koti", 25, "m"),
        ("srinu", 23, "m"),
        ("gopi", 28, "m"),
        ("naveen", 26, "m")]


columns = ["name", "age", "gender"]

df = spark.createDataFrame(data, schema =columns)

df.show()

# df.printSchema()

df.filter(df.age ==  25).show()

df.filter(df.age > 25).show()

df.filter(df.age < 25).show()

df.filter(df.name == "koti").show()

df.filter((df.age > 25) & (df.gender == "m")).show()

df.filter(df.age.between(24, 27)).show()

df.select(F.count("*")).show()

df.select(F.count("*").alias("ttl_count")).show()

df.select(F.sum("age")).show()

df.select(F.sum("age").alias("sum_of_ages")).show()

df.select(F.avg("age")).show()

df.select(F.avg("age").alias("avg_of_age")).show()

df.select(F.min("age")).show()

df.select(F.min("age").alias("min_of_age")).show()

df.select(F.max("age")).show()

df.select(F.max("age").alias("max_of_age")).show()



