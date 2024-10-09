from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sriram").getOrCreate()

df_csv = spark.read.csv("C:\\Users\\ANJI\\PycharmProjects\\pyspark_data_engineer_project\\input\\sample_emp.csv",header = True, inferSchema = True)

df_csv.printSchema()
# root
#  |-- id: integer (nullable = true)
#  |-- first_name: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- email: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- ip_address: string (nullable = true)


df_csv.show()
# id|first_name|         last_name|               email|  gender|     ip_address|
# +---+----------+------------------+--------------------+--------+---------------+
# |  1|    Brodie|             Wemes|bwemes0@infoseek....|    Male|  119.176.31.95|
# |  2|    Colver|           Shotter|cshotter1@wikispa...|    Male| 246.205.44.155|
# |  3|   Quincey|           Sweeney|qsweeney2@tinyurl...|    Male| 230.11.238.114|
# |  4|   Lucille|Schleswig-Holstein|lschleswigholstei...|  Female|   21.24.113.62|
# |  5|     Carey|          Whitaker|cwhitaker4@sakura...|    Male|   30.45.52.250|

df_csv.show(truncate = False)
# +---+----------+------------------+------------------------------------+--------+---------------+
# |id |first_name|last_name         |email                               |gender  |ip_address     |
# +---+----------+------------------+------------------------------------+--------+---------------+
# |1  |Brodie    |Wemes             |bwemes0@infoseek.co.jp              |Male    |119.176.31.95  |
# |2  |Colver    |Shotter           |cshotter1@wikispaces.com            |Male    |246.205.44.155 |
# |3  |Quincey   |Sweeney           |qsweeney2@tinyurl.com               |Male    |230.11.238.114 |
# |4  |Lucille   |Schleswig-Holstein|lschleswigholstein3@businessweek.com|Female  |21.24.113.62   |
# |5  |Carey     |Whitaker          |cwhitaker4@sakura.ne.jp             |Male    |30.45.52.250   |

df_csv.show(vertical= True)
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

df_csv.show(5)
# +---+----------+------------------+--------------------+-----------+---------------+
# | id|first_name|         last_name|               email|     gender|     ip_address|
# +---+----------+------------------+--------------------+-----------+---------------+
# |  1|    Brodie|             Wemes|bwemes0@infoseek....|       Male|  119.176.31.95|
# |  2|    Colver|           Shotter|cshotter1@wikispa...|       Male| 246.205.44.155|
# |  3|   Quincey|           Sweeney|qsweeney2@tinyurl...|       Male| 230.11.238.114|
# |  4|   Lucille|Schleswig-Holstein|lschleswigholstei...|     Female|   21.24.113.62|
# |  5|     Carey|          Whitaker|cwhitaker4@sakura...|       Male|   30.45.52.250|


df_csv.show(vertical= False)
# +---+----------+------------------+--------------------+--------+---------------+
# | id|first_name|         last_name|               email|  gender|     ip_address|
# +---+----------+------------------+--------------------+--------+---------------+
# |  1|    Brodie|             Wemes|bwemes0@infoseek....|    Male|  119.176.31.95|
# |  2|    Colver|           Shotter|cshotter1@wikispa...|    Male| 246.205.44.155|
# |  3|   Quincey|           Sweeney|qsweeney2@tinyurl...|    Male| 230.11.238.114|
# |  4|   Lucille|Schleswig-Holstein|lschleswigholstei...|  Female|   21.24.113.62|
# |  5|     Carey|          Whitaker|cwhitaker4@sakura...|    Male|   30.45.52.250|

