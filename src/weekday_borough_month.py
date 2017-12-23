
# coding: utf-8

# In[ ]:


import sys
import re
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


# In[ ]:


filepath='D:/tandon/big data analytics/project/NYPD_Complaint_Data_Historic.csv'
sc = SparkContext.getOrCreate()
data = sc.textFile(filepath, 1)

head = data.first() 
lines = data.filter(lambda row: row != head) 
line = lines.map(lambda x:(x.encode('ascii','ignore')))    .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))


# In[ ]:


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


# In[ ]:


def weekday(date, cat):
    nv = normal_validate(date) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016 or int(d.year) < 2006:
                date = "Invalid"
            else:
                date =  d.strftime("%A") + " " + cat
        except ValueError:
            date = "Invalid"
    return date


# In[ ]:


weekday_sum = line.map(lambda x: [x[5].encode('utf-8'),x[11].encode('utf-8')])            .map(lambda x:(weekday(x[0],x[1]), 1))            .reduceByKey(add)            .map(lambda x: x[0]+'\t'+str(x[1]))            .saveAsTextFile("weekday.out")


# In[ ]:


def borough(bor, cat):
    nv = normal_validate(bor) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            date =  bor + " " + cat
        except ValueError:
            date = "Invalid"
    return date


# In[ ]:


borough_sum = line.map(lambda x: [x[13].encode('utf-8'),x[11].encode('utf-8')])            .map(lambda x:(borough(x[0],x[1]), 1))            .reduceByKey(add)            .map(lambda x: x[0]+'\t'+str(x[1]))            .saveAsTextFile("borough.out")


# In[ ]:


def month(date, cat):
    nv = normal_validate(date) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016 or int(d.year) < 2006:
                date = "Invalid"
            else:
                date =  str(d.month) + " " + cat
        except ValueError:
            date = "Invalid"
    return date


# In[ ]:


month_sum = line.map(lambda x: [x[5].encode('utf-8'),x[11].encode('utf-8')])            .map(lambda x:(month(x[0],x[1]), 1))            .reduceByKey(add)            .sortByKey()            .map(lambda x: x[0]+'\t'+str(x[1]))            .saveAsTextFile("month.out")

