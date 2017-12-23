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

# read the generated report_date_level.txt to get Crimes trend data
report_date_level_data = sc.textFile('report_date_level.txt')
report_date_level_line = report_date_level_data.map(lambda line: line.split(','))

schema_rd = sqlContext.createDataFrame(report_date_level_line,('report_date', 'level', 'count'))

from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType

# This function converts the string cell into a date:
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
df = schema_rd.withColumn('report_date', func(col('report_date')))

schema_rd.registerTempTable("report_date_level_count")


# Detail data for report_date, level and count
date_level_count = sqlContext.sql("SELECT substring(report_date,7,10) as year, substring(report_date,1,5) as month, level, count FROM report_date_level_count")
date_level_count.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("date_level_count_result")
detail_date_level_count = sqlContext.sql("SELECT report_date, substring(report_date,7,10) as year, substring(report_date,1,2) as month, level, count FROM report_date_level_count")
detail_date_level_count.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("detail_date_level_count_result")



