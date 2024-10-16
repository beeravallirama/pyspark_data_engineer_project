#
# from pyspark.sql import SparkSession
#
# from pyspark.sql.types import StructType,StructField,IntegerType
#
# spark = SparkSession.builder.appName("sriram").getOrCreate()
#
# data1 = [(1,),(2,),(3,)]
# data2 = [(1,),(2,),(2,)]
#
# columns1 = ["id"]
# columns2 = ["id"]
#
# df1 = spark.createDataFrame(data1, columns1)
# df2 = spark.createDataFrame(data2, columns2)
#
# schema = StructType([StructField("id", IntegerType(), True)])
#
#
# result1_inner = df1.join(df2, df1["id"] == df2["id"],"inner")
#
# result2_left = df1.join(df2, df1["id"] == df2["id"],"left")
#
# result3_right = df1.join(df2,df1["id"] == df2["id"],"right")
#
# result4_outer = df1.join(df2,df1["id"] == df2["id"], "outer")
#
# result5_left_semi = df1.join(df2,df1["id"] == df2["id"], "left_semi")
#
# result6_left_anti = df1.join(df2,df1["id"] == df2["id"], "left_anti")
#
# result7_cross_join =df1.crossJoin(df2)
#
#
# result1_inner.show()
# # +---+---+
# # | id| id|
# # +---+---+
# # |  1|  1|
# # |  2|  2|
# # |  2|  2|
# # +---+---+
# result2_left.show()
# # +---+----+
# # | id|  id|
# # +---+----+
# # |  1|   1|
# # |  2|   2|
# # |  2|   2|
# # |  3|NULL|
# # +---+----+
# result3_right.show()
# # +---+---+
# # | id| id|
# # +---+---+
# # |  1|  1|
# # |  2|  2|
# # |  2|  2|
# # +---+---+
# result4_outer.show()
# # +---+----+
# # | id|  id|
# # +---+----+
# # |  1|   1|
# # |  2|   2|
# # |  2|   2|
# # |  3|NULL|
# # +---+----+
# result5_left_semi.show()
# # +---+
# # | id|
# # +---+
# # |  1|
# # |  2|
# # +---+
# result6_left_anti.show()
# # +---+
# # | id|
# # +---+
# # |  3|
# # +---+
#
# result7_cross_join.show()
#
#
#
#
