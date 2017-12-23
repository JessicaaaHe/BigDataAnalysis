from __future__ import print_function
import sys
import datetime
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

# pyspark --packages com.databricks:spark-csv_2.10:1.2.0

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

count_by_month_level_data = sc.textFile('count_by_month_level_result.txt')
count_by_month_data = sc.textFile('count_by_month_result.txt')
count_by_year_data = sc.textFile('count_by_year_result.txt')

count_by_month_level_line = count_by_month_level_data.map(lambda line: line.split(','))
count_by_month_line = count_by_month_data.map(lambda line: line.split(','))
count_by_year_line = count_by_year_data.map(lambda line: line.split(','))

schema_ml = sqlContext.createDataFrame(count_by_month_level_line,('report_year', 'report_month', 'level', 'sum_of_month_level'))
schema_ml.registerTempTable("month_level_count")

schema_m = sqlContext.createDataFrame(count_by_month_line,('report_year', 'report_month', 'sum_of_month'))
schema_m.registerTempTable("month_count")

schema_y = sqlContext.createDataFrame(count_by_year_line,('report_year', 'sum_of_year'))
schema_y.registerTempTable("year_count")

# Data for Crime Trend Chart
# Trend data for felony counted by month
felony_result = sqlContext.sql("SELECT CONCAT(month_level_count.report_month, '/00/', month_level_count.report_year) as report_date, sum_of_month_level FROM month_level_count WHERE level = 'FELONY'")
# Trend data for misdemeanor counted by month
misdemeanor_result = sqlContext.sql("SELECT CONCAT(month_level_count.report_month, '/00/', month_level_count.report_year) as report_date, sum_of_month_level FROM month_level_count WHERE level = 'MISDEMEANOR'")
# Trend data for violation counted by month
violation_result = sqlContext.sql("SELECT CONCAT(month_level_count.report_month, '/00/', month_level_count.report_year) as report_date, sum_of_month_level FROM month_level_count WHERE level = 'VIOLATION'")

# Get the data
felony_result.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("felony_result")
misdemeanor_result.coalesce(1).write.option("header","true").format("com.databricks.spark.csv").save("misdemeanor_result")
violation_result.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("violation_result")

#Data for level distribution chart
# Find the sum amount of crimes classified by level and year
level_year_result = sqlContext.sql("SELECT report_year, level, sum(sum_of_month_level) as sum_of_year FROM month_level_count GROUP BY report_year, level")
level_year_result.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("level_year_result")

# Find the average amount of crimes classified by level in each year
level_year_result_data = sc.textFile('level_year_result/part-00000')
level_year_result_line = level_year_result_data.map(lambda line: line.split(','))
schema_ly = sqlContext.createDataFrame(level_year_result_line,('year','level','sum'))
schema_ly.registerTempTable("level_year")
avg_amount_result = sqlContext.sql("SELECT level, avg(sum) as avg FROM level_year GROUP BY level")
avg_amount_result.show()


