
from __future__ import print_function

import sys
import datetime
from operator import add
from pyspark import SparkContext
from csv import reader


sc = SparkContext.getOrCreate()
data = sc.textFile('NYPD_Complaint_Data_Historic.csv')
head = data.first() 
lines = data.filter(lambda row: row != head) 
line = lines.map(lambda x:(x.encode('ascii','ignore')))\
        .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))


def normal_validate(content):
    try:
        content = content.lower()
    except AttributeError:
        content = content
    if content == '':
        return "Missing"
    elif content == 'n/a' or content == 'na':
        return "N/A"
    elif content == 'unspecified':
        return "Unspecified"
    elif content == 'null':
        return "Null"
    return "Valid"

def get_date_level(date, level):
    nv_date = normal_validate(date) 
    nv_level = normal_validate(level)
    if nv_date != "Valid" or nv_level != "Valid":
        return "Invalid"
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016:
                return "Invalid"
            elif level != "FELONY" and level != "MISDEMEANOR" and level != "VIOLATION":
                return "Invalid"
            else:
                return date +","+ level
        except ValueError:
            return "Invalid"

def get_date_level_location(date, level, latitude, longitude):
    nv_date = normal_validate(date)
    nv_level = normal_validate(level)
    nv_latitude = normal_validate(latitude)
    nv_longitude = normal_validate(longitude)
    if nv_date != "Valid" or nv_level != "Valid" or nv_latitude != "Valid" or nv_longitude != "Valid":
        return "Invalid"
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016:
                return "Invalid"
            elif level != "FELONY" and level != "MISDEMEANOR" and level != "VIOLATION":
                return "Invalid"
            else:
                return date +","+ level +"," +latitude+ ","+longitude
        except ValueError:
            return "Invalid"


date = "10/12/2015"
level = "FELONY"
test = get_date_level(date, level)
print(test)

# strip the report date and level
report_date_level = line.map(lambda x: [x[5].encode('utf-8'),x[11].encode('utf-8')])\
            .map(lambda x:(get_date_level(x[0],x[1]), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+","+str(x[1]))\
            .saveAsTextFile("report_date_level.out")

# strip report date, Latitude, Logitude, level
location_date_level = line.map(lambda x: [x[5].encode('utf-8'),x[11].encode('utf-8'),x[21].encode('utf-8'),x[22].encode('utf-8')])\
    .map(lambda x:(get_date_level_location(x[0],x[1],x[2],x[3]), 1))\
        .reduceByKey(add)\
            .map(lambda x: x[0]+","+str(x[1]))\
            .saveAsTextFile("location_date_level.out")



