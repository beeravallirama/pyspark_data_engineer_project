from pyspark.sql import SparkSession

from pyspark.sql.functions import substring

spark = SparkSession.builder.appName("sriram").getOrCreate()

path = "C:\\Users\\ANJI\\OneDrive\\Desktop\\pyspark\\fixed_width.txt"

df = spark.read.text(path)

df_fixed_width = df.withColumn("emp_id", substring("value", 1, 5)) \
                   .withColumn("emp_name", substring("value", 6, 10)) \
                   .withColumn("salary", substring("value", 16, 6)) \
                   .withColumn("department", substring("value", 22, 4)) \
                   .withColumn("year_of_joining", substring("value", 26, 4))

# Cast columns to appropriate types
df_fixed_width = df_fixed_width.withColumn("salary", df_fixed_width["salary"].cast("int")) \
                               .withColumn("year_of_joining", df_fixed_width["year_of_joining"].cast("int"))


df_fixed_width.show(truncate=False)



# path = "C:\\\\Users\\\\ANJI\\\\PycharmProjects\\\\pyspark_data_engineer_project\\\\input\\\\emp.json"
# 
# df_json = spark.read.json(path)
# 
# df_json.show()
# 
# df_json.printSchema()
# df_json.show(truncate = False)

# df_json = spark.read.json("C:\\\\Users\\\\ANJI\\\\PycharmProjects\\\\pyspark_data_engineer_project\\\\input\\\\emp.json")

# df_json.prientSchema()
# df_json.show()


# df_csv = spark.read.csv("C:\\\\Users\\\\ANJI\\\\PycharmProjects\\\\pyspark_data_engineer_project\\\\input\\\\sample_emp.csv",header = True, inferSchema = True)
# 
# df_csv.printSchema()
# root
#  |-- id: integer (nullable = true)
#  |-- first_name: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- email: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- ip_address: string (nullable = true)


# df_csv.show()
# id|first_name|         last_name|               email|  gender|     ip_address|
# +---+----------+------------------+--------------------+--------+---------------+
# |  1|    Brodie|             Wemes|bwemes0@infoseek....|    Male|  119.176.31.95|
# |  2|    Colver|           Shotter|cshotter1@wikispa...|    Male| 246.205.44.155|
# |  3|   Quincey|           Sweeney|qsweeney2@tinyurl...|    Male| 230.11.238.114|
# |  4|   Lucille|Schleswig-Holstein|lschleswigholstei...|  Female|   21.24.113.62|
# |  5|     Carey|          Whitaker|cwhitaker4@sakura...|    Male|   30.45.52.250|

# df_csv.show(truncate = False)
# +---+----------+------------------+------------------------------------+--------+---------------+
# |id |first_name|last_name         |email                               |gender  |ip_address     |
# +---+----------+------------------+------------------------------------+--------+---------------+
# |1  |Brodie    |Wemes             |bwemes0@infoseek.co.jp              |Male    |119.176.31.95  |
# |2  |Colver    |Shotter           |cshotter1@wikispaces.com            |Male    |246.205.44.155 |
# |3  |Quincey   |Sweeney           |qsweeney2@tinyurl.com               |Male    |230.11.238.114 |
# |4  |Lucille   |Schleswig-Holstein|lschleswigholstein3@businessweek.com|Female  |21.24.113.62   |
# |5  |Carey     |Whitaker          |cwhitaker4@sakura.ne.jp             |Male    |30.45.52.250   |

# df_csv.show(vertical= True)
# -RECORD 0--------------------------
#  id         | 1
#  first_name | Brodie
#  last_name  | Wemes
#  email      | bwemes0@infoseek....
#  gender     | Male
#  ip_address | 119.176.31.95
# -RECORD 1--------------------------
#  id         | 2
#  first_name | Colver
#  last_name  | Shotter
#  email      | cshotter1@wikispa...
#  gender     | Male
#  ip_address | 246.205.44.155

# df_csv.show(5)
# +---+----------+------------------+--------------------+-----------+---------------+
# | id|first_name|         last_name|               email|     gender|     ip_address|
# +---+----------+------------------+--------------------+-----------+---------------+
# |  1|    Brodie|             Wemes|bwemes0@infoseek....|       Male|  119.176.31.95|
# |  2|    Colver|           Shotter|cshotter1@wikispa...|       Male| 246.205.44.155|
# |  3|   Quincey|           Sweeney|qsweeney2@tinyurl...|       Male| 230.11.238.114|
# |  4|   Lucille|Schleswig-Holstein|lschleswigholstei...|     Female|   21.24.113.62|
# |  5|     Carey|          Whitaker|cwhitaker4@sakura...|       Male|   30.45.52.250|


# df_csv.show(vertical= False)
# +---+----------+------------------+--------------------+--------+---------------+
# | id|first_name|         last_name|               email|  gender|     ip_address|
# +---+----------+------------------+--------------------+--------+---------------+
# |  1|    Brodie|             Wemes|bwemes0@infoseek....|    Male|  119.176.31.95|
# |  2|    Colver|           Shotter|cshotter1@wikispa...|    Male| 246.205.44.155|
# |  3|   Quincey|           Sweeney|qsweeney2@tinyurl...|    Male| 230.11.238.114|
# |  4|   Lucille|Schleswig-Holstein|lschleswigholstei...|  Female|   21.24.113.62|
# |  5|     Carey|          Whitaker|cwhitaker4@sakura...|    Male|   30.45.52.250|




