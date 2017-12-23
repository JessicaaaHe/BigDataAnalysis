from __future__ import print_function
import sys
import datetime
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# read the detail_date_level_count_result.txt to get detail report date and level data
detail_date_level_data = sc.textFile('detail_date_level_count_result.txt')
detail_date_level_line = detail_date_level_data.map(lambda line: line.split(','))

schema_ddl = sqlContext.createDataFrame(detail_date_level_line,('report_date', 'report_year', 'report_month', 'level', 'count'))
schema_ddl.registerTempTable("detailed_report_date_level_count")

count_by_month_level = sqlContext.sql("SELECT report_year, report_month, level, sum(count) as sum_of_month_level FROM detailed_report_date_level_count  GROUP BY report_year, report_month, level")
count_by_month = sqlContext.sql("SELECT report_year, report_month, sum(count) as sum_of_month FROM detailed_report_date_level_count  GROUP BY report_year, report_month")
count_by_year = sqlContext.sql("SELECT report_year, sum(count) as sum_of_year FROM detailed_report_date_level_count  GROUP BY report_year")

count_by_month.coalesce(1).write.format("com.databricks.spark.csv").save("count_by_month")
count_by_year.coalesce(1).write.format("com.databricks.spark.csv").save("count_by_year")
count_by_month_level.coalesce(1).write.format("com.databricks.spark.csv").save("count_by_month_level")






