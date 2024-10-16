from pyspark.sql import SparkSession

from pyspark.sql.functions import col, row_number, rank, dense_rank

from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sriram").getOrCreate()

data = [(1,), (2,), (3,), (4,), (5,), (5,), (6,)]

columns = ["id"]

df = spark.createDataFrame(data, columns)

window_spec = Window.orderBy(col("id"))

df_with_row_number = df.withColumn("row_number", row_number().over(window_spec))

df_with_rank = df.withColumn("rank",rank().over(window_spec))

df_with_row_number.show()

df_with_rank.show()

df_dense_rank = df.withColumn("dense_rank",dense_rank().over(window_spec))

df_dense_rank.show()

