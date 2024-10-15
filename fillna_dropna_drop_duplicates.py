from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,FloatType

spark = SparkSession.builder.appName("sriram").getOrCreate()

data = [
    (1,'john',28,50000.0),
    (2,'anna',34,None),
    (3,None,None,45000.0),
    (5,'sara',None,6000.0),
    (6,None,28,5000.0),
    (7,'john',28,50000.0)
]

schema = ['emp_id', 'name', 'age', 'salary']


df =spark.createDataFrame(data,schema=schema)

df.show()

# fillna() - Filling Missing Values:
# In PySpark, you can fill missing values with the fillna() method.
# age_mean = df.select("age").agg({"age": "mean"}).collect()[0][0]
# df_filled_age = df.fillna({'age':age_mean})
#
# df_filled_salary = df_filled_age.fillna({'salary': 0})
#
# df_filled_all = df.fillna('unknown')
#
# df_filled_salary.show()

# dropna() - Dropping Missing Values:
# In PySpark, you can drop rows containing missing values using dropna().

# df_droped_any = df.dropna()
#
# df_droped_all = df.dropna(how='all')
#
# df_droped_spcific = df.dropna(subset=['age'])
#
# df_droped_any.show()


# drop_duplicates() - Removing Duplicates:
# In PySpark, you can remove duplicate rows using dropDuplicates().


# df_no_duplicates = df.dropDuplicates()
#
# df_no_duplicates_specific = df.dropDuplicates(['name', 'age'])
#
# df_no_duplicates_last = df.dropDuplicates(['name', 'age', 'salary'])
#
# df_no_duplicates.show()


