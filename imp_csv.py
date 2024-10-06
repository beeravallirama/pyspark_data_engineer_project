from pandas import DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('empcsv').getOrCreate()

df=spark.read.csv("C:\\Users\\ANJI\\OneDrive\\Desktop\\all in one\\FF_IICS\\FF_IICS_SRC\\EMP_1.txt", inferSchema=True, header=True )

# df.select("JOB","SAL").show()
# pandas_df=df.select("*").toPandas()
# pandas_df=df.toPandas()
# pandas_df.show()
# pandas_df.select("JOB","SAL")

# pandas_df= df[['JOB', 'SAL']].show()



