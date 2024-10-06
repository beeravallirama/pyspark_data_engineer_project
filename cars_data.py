from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, count, sum, desc

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("CarSalesAnalysis").getOrCreate()

# Sample data for the cars table
cars_data = [
    (1, 'Honda', 'Civic', 'Sedan', 30000),
    (2, 'Toyota', 'Corolla', 'Hatchback', 25000),
    (3, 'Ford', 'Explorer', 'SUV', 40000),
    (4, 'Chevrolet', 'Camaro', 'Coupe', 36000),
    (5, 'BMW', 'X5', 'SUV', 55000),
    (6, 'Audi', 'A4', 'Sedan', 48000),
    (7, 'Mercedes', 'C-Class', 'Coupe', 60000),
    (8, 'Nissan', 'Altima', 'Sedan', 26000)
]

# Sample data for the salespersons table
salespersons_data = [
    (1, 'John Smith', 28, 'New York'),
    (2, 'Emily Wong', 35, 'San Fran'),
    (3, 'Tom Lee', 42, 'Seattle'),
    (4, 'Lucy Chen', 31, 'LA')
]

# Sample data for the sales table
sales_data = [
    (1, 1, 1, '2021-01-01'),
    (2, 3, 3, '2021-02-03'),
    (3, 2, 2, '2021-02-10'),
    (4, 5, 4, '2021-03-01'),
    (5, 8, 1, '2021-04-02'),
    (6, 2, 1, '2021-05-05'),
    (7, 4, 2, '2021-06-07'),
    (8, 5, 3, '2021-07-09'),
    (9, 2, 4, '2022-01-01'),
    (10, 1, 3, '2022-02-03'),
    (11, 8, 2, '2022-02-10'),
    (12, 7, 2, '2022-03-01'),
    (13, 5, 3, '2022-04-02'),
    (14, 3, 1, '2022-05-05'),
    (15, 5, 4, '2022-06-07'),
    (16, 1, 2, '2022-07-09'),
    (17, 2, 3, '2023-01-01'),
    (18, 6, 3, '2023-02-03'),
    (19, 7, 1, '2023-02-10'),
    (20, 4, 4, '2023-03-01')
]

# Creating DataFrames
cars_df = spark.createDataFrame(cars_data, ['car_id', 'make', 'type', 'style', 'cost_$'])
salespersons_df = spark.createDataFrame(salespersons_data, ['salesman_id', 'name', 'age', 'city'])
sales_df = spark.createDataFrame(sales_data, ['sale_id', 'car_id', 'salesman_id', 'purchase_date'])

# Convert purchase_date to DateType
# from pyspark.sql.functions import to_date
# sales_df = sales_df.withColumn("purchase_date", to_date(col("purchase_date")))


# cars_2022 = sales_df.join(cars_df, "car_id").filter(year("purchase_date") == 2022)
# cars_2022.show()

# cars_sold_per_salesperson = sales_df.groupBy("salesman_id").count().alias("total_cars_sold")
# cars_sold_per_salesperson.show()

# sales_with_costs = sales_df.join(cars_df, "car_id")
# revenue_per_salesperson = sales_with_costs.groupBy("salesman_id").agg(sum("cost_$").alias("total_revenue"))
# revenue_per_salesperson.show()

# car_details_salesperson = sales_with_costs.join(salespersons_df, "salesman_id")
# car_details_salesperson.show()

# revenue_per_type = sales_with_costs.groupBy("type").agg(sum("cost_$").alias("total_revenue"))
# revenue_per_type.show()

# cars_2021_emily = car_details_salesperson.filter((year("purchase_date") == 2021) & (col("name") == "Emily Wong"))
# cars_2021_emily.show()

# hatchback_revenue = sales_with_costs.filter(col("type") == "Hatchback").agg(sum("cost_$").alias("total_revenue"))
# hatchback_revenue.show()

# suv_2022_revenue = sales_with_costs.filter((col("type") == "SUV") & (year("purchase_date") == 2022)).agg(sum("cost_$").alias("total_revenue"))
# suv_2022_revenue.show()


# cars_sold_2023 = sales_with_costs.filter(year("purchase_date") == 2023)
# top_salesperson_2023 = cars_sold_2023.groupBy("salesman_id").count().alias("total_cars_sold").orderBy(desc("count")).limit(1)
# top_salesperson_details = top_salesperson_2023.join(salespersons_df, "salesman_id").select("name", "city")
# top_salesperson_details.show()











