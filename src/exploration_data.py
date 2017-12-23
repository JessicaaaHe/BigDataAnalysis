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

date_level_data = sc.textFile('date_level_count_result.txt')
date_level_line = date_level_data.map(lambda line: line.split(','))

schema_dl = sqlContext.createDataFrame(date_level_line,('year', 'day', 'level','count'))
schema_dl.registerTempTable("date_level_count")


data_2006 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2006' GROUP BY day")
data_2006.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2006")

data_2007 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2007' GROUP BY day")
data_2007.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2007")

data_2008 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2008' GROUP BY day")
data_2008.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2008")

data_2009 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2009' GROUP BY day")
data_2009.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2009")

data_2010 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2010' GROUP BY day")
data_2010.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2010")

data_2011 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2011' GROUP BY day")
data_2011.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2011")

data_2012 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2012' GROUP BY day")
data_2012.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2012")

data_2013 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2013' GROUP BY day")
data_2013.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2013")

data_2014 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2014' GROUP BY day")
data_2014.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2014")

data_2015 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2015' GROUP BY day")
data_2015.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2015")

data_2016 = sqlContext.sql("SELECT day, sum(count) FROM date_level_count WHERE year = '2016' GROUP BY day")
data_2016.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("data_result_2016")


data_2006_result_data = sc.textFile('data_result_2006/part-00000')
data_2006_result_line = data_2006_result_data.map(lambda line: line.split(','))
schema_2006 = sqlContext.createDataFrame(data_2006_result_line,('day','count'))
schema_2006.registerTempTable("table_2006")

avg_result_2006 = sqlContext.sql("SELECT avg(count) FROM table_2006")
chrismas_result_2006 = sqlContext.sql("SELECT count FROM table_2006 WHERE day = '09/11'")
avg_result_2006.show()
chrismas_result_2006.show()

data_2007_result_data = sc.textFile('data_result_2007/part-00000')
data_2007_result_line = data_2007_result_data.map(lambda line: line.split(','))
schema_2007 = sqlContext.createDataFrame(data_2007_result_line,('day','count'))
schema_2007.registerTempTable("table_2007")

avg_result_2007 = sqlContext.sql("SELECT avg(count) FROM table_2007")
chrismas_result_2007 = sqlContext.sql("SELECT count FROM table_2007 WHERE day = '09/11'")
avg_result_2007.show()
chrismas_result_2007.show()

data_2008_result_data = sc.textFile('data_result_2008/part-00000')
data_2008_result_line = data_2008_result_data.map(lambda line: line.split(','))
schema_2008 = sqlContext.createDataFrame(data_2008_result_line,('day','count'))
schema_2008.registerTempTable("table_2008")

avg_result_2008 = sqlContext.sql("SELECT avg(count) FROM table_2008")
chrismas_result_2008 = sqlContext.sql("SELECT count FROM table_2008 WHERE day = '09/11'")
avg_result_2008.show()
chrismas_result_2008.show()

data_2009_result_data = sc.textFile('data_result_2009/part-00000')
data_2009_result_line = data_2009_result_data.map(lambda line: line.split(','))
schema_2009 = sqlContext.createDataFrame(data_2009_result_line,('day','count'))
schema_2009.registerTempTable("table_2009")

avg_result_2009 = sqlContext.sql("SELECT avg(count) FROM table_2009")
chrismas_result_2009 = sqlContext.sql("SELECT count FROM table_2009 WHERE day = '09/11'")
avg_result_2009.show()
chrismas_result_2009.show()

data_2010_result_data = sc.textFile('data_result_2010/part-00000')
data_2010_result_line = data_2010_result_data.map(lambda line: line.split(','))
schema_2010 = sqlContext.createDataFrame(data_2010_result_line,('day','count'))
schema_2010.registerTempTable("table_2010")

avg_result_2010 = sqlContext.sql("SELECT avg(count) FROM table_2010")
chrismas_result_2010 = sqlContext.sql("SELECT count FROM table_2010 WHERE day = '09/11'")
avg_result_2010.show()
chrismas_result_2010.show()

data_2011_result_data = sc.textFile('data_result_2011/part-00000')
data_2011_result_line = data_2011_result_data.map(lambda line: line.split(','))
schema_2011 = sqlContext.createDataFrame(data_2011_result_line,('day','count'))
schema_2011.registerTempTable("table_2011")

avg_result_2011 = sqlContext.sql("SELECT avg(count) FROM table_2011")
chrismas_result_2011 = sqlContext.sql("SELECT count FROM table_2011 WHERE day = '09/11'")
avg_result_2011.show()
chrismas_result_2011.show()

data_2012_result_data = sc.textFile('data_result_2012/part-00000')
data_2012_result_line = data_2012_result_data.map(lambda line: line.split(','))
schema_2012 = sqlContext.createDataFrame(data_2012_result_line,('day','count'))
schema_2012.registerTempTable("table_2012")

avg_result_2012 = sqlContext.sql("SELECT avg(count) FROM table_2012")
chrismas_result_2012 = sqlContext.sql("SELECT count FROM table_2012 WHERE day = '09/11'")
avg_result_2012.show()
chrismas_result_2012.show()

data_2013_result_data = sc.textFile('data_result_2013/part-00000')
data_2013_result_line = data_2013_result_data.map(lambda line: line.split(','))
schema_2013 = sqlContext.createDataFrame(data_2013_result_line,('day','count'))
schema_2013.registerTempTable("table_2013")

avg_result_2013 = sqlContext.sql("SELECT avg(count) FROM table_2013")
chrismas_result_2013 = sqlContext.sql("SELECT count FROM table_2013 WHERE day = '09/11'")
avg_result_2013.show()
chrismas_result_2013.show()

data_2014_result_data = sc.textFile('data_result_2014/part-00000')
data_2014_result_line = data_2014_result_data.map(lambda line: line.split(','))
schema_2014 = sqlContext.createDataFrame(data_2014_result_line,('day','count'))
schema_2014.registerTempTable("table_2014")

avg_result_2014 = sqlContext.sql("SELECT avg(count) FROM table_2014")
chrismas_result_2014 = sqlContext.sql("SELECT count FROM table_2014 WHERE day = '09/11'")
avg_result_2014.show()
chrismas_result_2014.show()

data_2015_result_data = sc.textFile('data_result_2015/part-00000')
data_2015_result_line = data_2015_result_data.map(lambda line: line.split(','))
schema_2015 = sqlContext.createDataFrame(data_2015_result_line,('day','count'))
schema_2015.registerTempTable("table_2015")

avg_result_2015 = sqlContext.sql("SELECT avg(count) FROM table_2015")
chrismas_result_2015 = sqlContext.sql("SELECT count FROM table_2015 WHERE day = '09/11'")
avg_result_2015.show()
chrismas_result_2015.show()

data_2016_result_data = sc.textFile('data_result_2016/part-00000')
data_2016_result_line = data_2016_result_data.map(lambda line: line.split(','))
schema_2016 = sqlContext.createDataFrame(data_2016_result_line,('day','count'))
schema_2016.registerTempTable("table_2016")

avg_result_2016 = sqlContext.sql("SELECT avg(count) FROM table_2016")
chrismas_result_2016 = sqlContext.sql("SELECT count FROM table_2016 WHERE day = '09/11'")
avg_result_2016.show()
chrismas_result_2016.show()



