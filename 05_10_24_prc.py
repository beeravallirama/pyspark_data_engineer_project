from pyspark.sql import SparkSession

from pyspark.sql import functions as F

Spark = SparkSession.builder.appName("sriram").getOrCreate()

data = [(1, "rama", 1000, "hyd", 10),
        (2, "surdhara", 200, "ts", 20),
        (3, "charan", 3000, "kadapa", 30),
        (4, "brhma", 5000, "gnt", 20),
        (5, "srinu", 2000, "andhra", 30),
        (6, "naveen", 5000, "banjara", 30)]

columns = ["id","name","sal","loc","dept_id"]

df = Spark.createDataFrame(data, schema=columns)

df.show()

# agg_df = df.groupBy("dept_id").agg(F.count("id").alias("count")).show()
# agg_df = df.groupBy("dept_id").agg(F.sum("sal").alias("sum_of_sal")).show()
# agg_df = df.groupBy("dept_id").agg(F.avg("sal").alias("avg_of_sal")).show()
# aag_df = df.groupBy("dept_id").agg(F.max("sal").alias("max_of_sal")).show()
# agg_df = df.groupBy("dept_id").agg(F.min("sal").alias("min_of_sal")).show()
#
# agg_df =df.groupBy("dept_id").agg(F.count("id")).show()

# df_filtered = df.filter(df["sal"] >= 3000).show()
#
# df_filtered = df.filter(df["sal"] < 2000).show()
#
# df_filtered = df.filter(df["loc"] != 'hyd').show()

df_filtered = df.filter(df["dept_id"].isin(10)).show()

df_filtered = df.filter(df["name"].like("%ram%")).show()

df_filtered = df.filter(df["name"].like("bra%")).show()

df_filtered = df.filter(df["name"].rlike("^[sn]")).show()

df_filtered = df.filter(df["loc"].isNull()).show()

df_filtered = df.filter(df["loc"].isNotNull()).show()

df_filtered = df.filter(df["sal"].between(2000,40000)).show()

df_filtered = df.filter(F.lower(df["loc"]) == "hyd").show()

df_filtered = df.filter((df["sal"]>2000) & (df["dept_id"] ==30)).show()

df_filtered = df.filter((df["sal"] < 3000) | (df["loc"] == "gnt")).show()

